"""按信号池自动市价开仓并挂 reduceOnly 止损,维护开仓台账与执行状态。

执行规则(与用户确认的决策一致):
- 三种信号全部市价单开仓;仓位按风险制计算:单笔最大亏损 = 可用余额的 risk_per_trade_pct;
- 止损 = 参考价 + buffer(1σ×收盘价):低吸多→箱体窗口内最低价(含影线 extremeLow)外侧,高抛空→箱体窗口内最高价(含影线 extremeHigh)外侧,上破多→箱体中轴下方、下破空→箱体中轴上方(信号 box 无 mid,按 (upper+lower)/2 计算);
- 止损单为 STOP_MARKET + reduceOnly,按标记价格触发;
- 同一信号(交易对 + 信号 K 线起点)仅执行一次,台账去重;已有持仓的交易对不再开仓;
- 模拟验证统一走 Binance 测试网(环境切换),本模块不提供本地模拟。

官方文档依据(AGENTS.md 要求登记):
- filters 语义与 MIN_NOTIONAL 评估: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition
- 市价单参数校验按 MARK_PRICE 与 MARKET_LOT_SIZE(同上)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any

from binance_futures_client import BinanceFuturesClient
from logging_utils import configure_logging, get_logger
from secret_utils import get_secret
from scheduler import (
    CONFIG_PATH,
    SchedulerConfig,
    bucket_start,
    load_config,
    previous_bucket_start,
    utc_milliseconds,
    write_json_atomically,
)


# 生产与测试网密钥分开存放,避免误用生产密钥在测试网下单
API_KEY_ENVIRONMENT_VARIABLE = "BINANCE_API_KEY"
API_SECRET_ENVIRONMENT_VARIABLE = "BINANCE_API_SECRET"
TESTNET_API_KEY_ENVIRONMENT_VARIABLE = "BINANCE_TESTNET_API_KEY"
TESTNET_API_SECRET_ENVIRONMENT_VARIABLE = "BINANCE_TESTNET_API_SECRET"
# 只追加实际成交后的开仓初始事实；跳过/失败明细仅写 trading_status.json。
LEDGER_FILENAME = "order_ledger.json"
STATUS_FILENAME = "trading_status.json"

BREAKOUT_SIGNAL = "上破箱体上沿"
BREAKDOWN_SIGNAL = "下破箱体下沿"
SELL_IN_BOX_SIGNAL = "箱体内高抛"
BUY_IN_BOX_SIGNAL = "箱体内低吸"
# 交易成本率:开平两次手续费(万分之五 × 2 = 千分之一)+ 成交滑点 0.05% = 0.15%
TRADE_FEE_RATE = Decimal("0.001")
TRADE_SLIPPAGE_RATE = Decimal("0.0005")
TRADE_COST_RATE = TRADE_FEE_RATE + TRADE_SLIPPAGE_RATE
# 交易限制类跳过原因:命中即「放弃开仓」并消费信号
# (低于最小数量/最小名义等规则限制;数量超过最大下单量已改为截断到上限下单,不再跳过)
TRADE_LIMIT_REASONS = {
    "交易规则缺少步长",
    "数量低于最小下单量",
    "名义金额低于最小下单限额",
    "杠杆截断后数量低于最小下单量",
    "杠杆截断后名义金额低于最小下单限额",
}

LOGGER = get_logger("trading")


def _decimal(value: Any) -> Decimal | None:
    """将任意值转换为有限 Decimal,失败或非法时返回 None。"""
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return number if number.is_finite() else None


def _now_iso() -> str:
    """当前 UTC 时间的 ISO 字符串。"""
    return datetime.now(timezone.utc).isoformat()


class TradingExecutor:
    """单轮信号池执行器:风险制仓位计算、市价开仓、止损挂单、台账与状态维护。"""

    def __init__(self, config: SchedulerConfig, signal_pool_path: Path) -> None:
        self.config = config
        self.signal_pool_path = signal_pool_path
        self.ledger_path = config.runtime_directory / LEDGER_FILENAME
        self.status_path = config.runtime_directory / STATUS_FILENAME
        self.contract_pool_path = config.runtime_directory / "contract_pool.json"
        # 按运行环境选择对应的密钥环境变量,生产与测试网互不串用
        if config.use_testnet:
            self.api_key_env, self.api_secret_env = (
                TESTNET_API_KEY_ENVIRONMENT_VARIABLE, TESTNET_API_SECRET_ENVIRONMENT_VARIABLE,
            )
        else:
            self.api_key_env, self.api_secret_env = (
                API_KEY_ENVIRONMENT_VARIABLE, API_SECRET_ENVIRONMENT_VARIABLE,
            )
        # 优先环境变量,缺失时回退旧程序配置文件(兼容旧程序迁移)
        self.api_key = get_secret(self.api_key_env)
        self.api_secret = get_secret(self.api_secret_env)
        self.client: BinanceFuturesClient | None = None
        self.balance: Decimal = Decimal("0")
        self.positions: dict[str, float] = {}
        self.logger = LOGGER

    def execute(self) -> dict[str, int]:
        """执行一轮信号交易,返回 {executed, skipped, failed} 摘要。

        任何异常均不会向上抛出,只写入状态文件,保证调度主循环不中断。
        """
        summary = {"executed": 0, "skipped": 0, "failed": 0}
        if not self.config.trading_enabled:
            return summary

        try:
            signals, run_id, signal_payload = self._load_signals()
        except Exception as exc:
            self.logger.warning("读取信号池失败,本轮不执行交易: %s", exc)
            self._write_status({"error": f"读取信号池失败:{exc}"}, summary, run_id=None)
            return summary

        status: dict[str, Any] = {
            "tradingEnabled": self.config.trading_enabled,
            "apiKeysPresent": False,
            "lastExecutionAt": _now_iso(),
            "lastRunId": run_id,
            "balanceUsdt": None,
            "openPositions": 0,
            "error": None,
        }

        # 信号新鲜度检查:信号必须基于最近一根已收盘的执行周期 K 线。
        # 程序终止重启后信号池文件可能残留旧信号,过期信号一律不执行,
        # 避免用陈旧价格与箱体状态开仓。
        expected_open_time = utc_milliseconds(
            previous_bucket_start(
                self.config.execution_interval,
                bucket_start(self.config.execution_interval, datetime.now(timezone.utc)),
            )
        )
        stale_signals = [
            signal
            for signal in signals
            if not (
                isinstance(signal.get("signalKline"), dict)
                and signal["signalKline"].get("openTime") == expected_open_time
            )
        ]
        if stale_signals:
            self.logger.warning(
                "信号池已过期,本轮不执行交易 expected_open_time=%s stale=%s",
                expected_open_time, len(stale_signals),
            )
            status["error"] = f"信号池已过期(期望最新 K 线起点 {expected_open_time})"
            self._write_status(status, summary, run_id)
            return summary

        if self.api_key and self.api_secret:
            try:
                self.client = BinanceFuturesClient(
                    self.api_key, self.api_secret, self.config.use_testnet, self.config.http_timeout_seconds
                )
                status["apiKeysPresent"] = True
            except Exception as exc:
                self.logger.warning("交易客户端初始化失败,本轮不执行交易: %s", exc)
                status["error"] = f"交易客户端初始化失败:{exc}"
                self._write_status(status, summary, run_id)
                return summary
        else:
            self.logger.warning("缺少 %s/%s 环境变量,本轮不执行交易", self.api_key_env, self.api_secret_env)
            status["error"] = f"缺少 {self.api_key_env}/{self.api_secret_env} 环境变量"
            self._write_status(status, summary, run_id)
            return summary

        try:
            self.balance = Decimal(str(self.client.get_balance_usdt()))
            self.positions = self.client.get_position_risk()
        except Exception as exc:
            self.logger.warning("查询余额与持仓失败,本轮不执行交易: %s", exc)
            status["error"] = f"查询余额与持仓失败:{exc}"
            self._write_status(status, summary, run_id)
            return summary

        status["balanceUsdt"] = float(self.balance)
        status["openPositions"] = sum(1 for amount in self.positions.values() if abs(amount) > 0)

        ledger = self._load_ledger()
        records: list[dict[str, Any]] = []
        # 账户仓位上限:初始持仓数 + 本轮已成交数,随处理推进实时累计
        opened_in_round = sum(1 for amount in self.positions.values() if abs(amount) > 0)
        for signal in signals:
            try:
                record = self._process_signal(signal, ledger, run_id, opened_in_round)
            except Exception as exc:
                record = self._failed_record(signal, run_id, f"未预期异常:{exc}")
            if record.get("status") == "filled" or record.get("consumeSignal"):
                # 成交或「消费但不下单」(预估盈利不足/交易限制)均标记消费,防止信号重放;
                # consumedStatus 区分:filled=已按信号开仓,abandoned=放弃开仓(原因见 consumedReason)
                signal["consumed"] = True
                signal["consumedAt"] = _now_iso()
                signal["consumedStatus"] = "filled" if record.get("status") == "filled" else "abandoned"
                if record.get("consumedReason"):
                    signal["consumedReason"] = record["consumedReason"]
            if record.get("status") == "filled":
                opened_in_round += 1
                self._register_position(record)
                self._notify_open(record)
            records.append(record)
            if record.get("status") == "filled":
                ledger.append(self._entry_ledger_record(record))
            summary[record.get("summaryKey", "failed")] = summary.get(record.get("summaryKey", "failed"), 0) + 1

        if any(signal.get("consumed") for signal in signals):
            self._write_back_signal_pool(signal_payload, signals)
        self._write_ledger(ledger)
        self._write_status(status, summary, run_id, records)
        self.logger.info(
            "信号交易执行完成 executed=%s skipped=%s failed=%s",
            summary["executed"], summary["skipped"], summary["failed"],
        )
        return summary

    # ---- 数据加载 ----

    def _load_signals(self) -> tuple[list[dict[str, Any]], str | None, dict[str, Any]]:
        """读取信号池并返回 (信号列表, runId, 原始载荷)。

        原始载荷用于将消费状态写回信号池文件;信号元素为原字典引用,
        在元素上追加消费字段即可同步反映到载荷中。
        """
        if not self.signal_pool_path.is_file():
            raise FileNotFoundError(f"找不到信号池文件:{self.signal_pool_path}")
        payload = json.loads(self.signal_pool_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError("信号池文件格式无效。")
        signals = payload.get("signals")
        if not isinstance(signals, list):
            raise RuntimeError("信号池缺少 signals 列表。")
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        run_id = source.get("runId") if isinstance(source.get("runId"), str) else None
        return [signal for signal in signals if isinstance(signal, dict)], run_id, payload

    def _load_ledger(self) -> list[dict[str, Any]]:
        """读取开仓台账；兼容旧版本后仅保留已成交的开仓记录。"""
        if not self.ledger_path.is_file():
            return []
        try:
            payload = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self.logger.warning("订单台账损坏,按空台账处理: %s", self.ledger_path)
            return []
        ledger = payload.get("ledger") if isinstance(payload, dict) else None
        return [
            item for item in ledger
            if isinstance(item, dict) and item.get("status") == "filled"
        ] if isinstance(ledger, list) else []

    def _load_contract_filters(self, symbol: str) -> dict[str, dict[str, Any]] | None:
        """从合约池读取交易对 filters,按 filterType 建立索引。"""
        if not self.contract_pool_path.is_file():
            return None
        try:
            payload = json.loads(self.contract_pool_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        for item in payload.get("symbols", []):
            if not isinstance(item, dict) or item.get("symbol") != symbol:
                continue
            filters = {
                rule.get("filterType"): rule
                for rule in item.get("filters", [])
                if isinstance(rule, dict) and isinstance(rule.get("filterType"), str)
            }
            return filters or None
        return None

    # ---- 单信号处理 ----

    def _process_signal(
        self,
        signal: dict[str, Any],
        ledger: list[dict[str, Any]],
        run_id: str | None,
        open_position_count: int,
    ) -> dict[str, Any]:
        symbol = signal.get("symbol")
        signal_kline = signal.get("signalKline") if isinstance(signal.get("signalKline"), dict) else {}
        open_time = signal_kline.get("openTime")
        base: dict[str, Any] = {
            "fingerprint": f"{symbol}|{open_time}",
            "runId": run_id,
            "symbol": symbol,
            "signalType": signal.get("signalType"),
            "direction": signal.get("direction"),
            "signalKlineOpenTime": open_time,
        }
        if not isinstance(symbol, str) or not symbol:
            return self._skipped_record(base, "信号缺少交易对")
        if signal.get("consumed") is True:
            return self._skipped_record(base, "信号已消费")
        # 方向模式过滤:只做多时跳过空头信号,只做空时跳过多头信号(不消费信号,便于切换后重新处理)
        direction = base.get("direction")
        if self.config.direction_mode == "long" and direction != "多":
            return self._skipped_record(base, "仅做多模式,跳过空头信号")
        if self.config.direction_mode == "short" and direction != "空":
            return self._skipped_record(base, "仅做空模式,跳过多头信号")

        box = signal.get("box") if isinstance(signal.get("box"), dict) else {}
        # 高抛低吸的止损缓冲:放在突破信号确认突破的位置之外,防止插针扫损
        stop_buffer = self._stop_buffer_for_signal(signal)
        stop_price, side, stop_side, reason = self._stop_for_signal(base, box, stop_buffer)
        if stop_price is None:
            return self._skipped_record(base, reason)
        if self._is_executed(ledger, symbol, open_time):
            return self._skipped_record(base, "台账已执行")
        if abs(self.positions.get(symbol, 0.0)) > 0:
            return self._skipped_record(base, "已有持仓")
        # 账户仓位上限:持仓数(含本轮已成交)不低于上限时不再开新仓
        if open_position_count >= self.config.max_positions:
            return self._skipped_record(base, f"账户仓位已达上限 {self.config.max_positions}")

        filters = self._load_contract_filters(symbol)
        if filters is None:
            return self._skipped_record(base, "合约池缺少交易对规则")
        stop_price = self._round_stop_price(stop_price, filters)
        if stop_price is None:
            return self._skipped_record(base, "止损价超出价格区间")

        est_entry = self._estimate_entry(signal, symbol)
        if est_entry is None:
            return self._failed_record(base, "获取预估入场价失败", run_id)
        if (side == "BUY" and est_entry <= stop_price) or (side == "SELL" and est_entry >= stop_price):
            return self._skipped_record(base, "预估入场价已越过止损价")

        # 风险制确定名义价值(与杠杆无关),按名义匹配杠杆档位取可用最大杠杆
        quantity, notional, max_loss, effective_leverage, reason = self._compute_quantity(
            symbol, stop_price, est_entry, filters
        )
        if quantity is None:
            record = self._skipped_record(base, reason)
            if reason in TRADE_LIMIT_REASONS:
                # 交易对规则限制(数量/名义超出限制):消费信号但放弃开仓,记录具体原因
                record["consumeSignal"] = True
                record["consumedReason"] = reason
            return record

        # 箱体内高抛低吸仓位:止盈价 = 箱体中轨(限价单);向上突破仓位不挂止盈
        take_profit_price: Decimal | None = None
        if base.get("signalType") in (BUY_IN_BOX_SIGNAL, SELL_IN_BOX_SIGNAL):
            upper = _decimal(box.get("upper"))
            lower = _decimal(box.get("lower"))
            mid = (upper + lower) / 2 if upper is not None and lower is not None else None
            if mid is not None:
                # 复用 tickSize 对齐逻辑,越界时返回 None 表示不挂止盈
                take_profit_price = self._round_stop_price(mid, filters)
            # 预估盈利检查:毛利润(止盈距离)须覆盖交易成本(开平手续费 + 成交滑点),
            # 否则消费信号但不下单(箱体过窄时止盈到中轨无法覆盖 0.15% 成本)
            profit_distance = abs(take_profit_price - est_entry) if take_profit_price is not None else None
            if profit_distance is None or profit_distance <= est_entry * TRADE_COST_RATE:
                profit_pct = (profit_distance / est_entry * 100) if profit_distance is not None else None
                detail = f"止盈空间 {profit_pct:.3f}%" if profit_pct is not None else "止盈价不可用"
                record = self._skipped_record(base, f"预估盈利不覆盖成本({detail} vs 成本 0.15%),消费信号但不下单")
                record["consumeSignal"] = True
                return record

        computed = self._computed_fields(
            base, box, stop_price, est_entry, quantity, notional, max_loss, effective_leverage, stop_buffer
        )
        return self._place_orders(
            computed, side, stop_side, quantity, stop_price, effective_leverage, take_profit_price
        )

    def _notify_open(self, record: dict[str, Any]) -> None:
        """开仓成功后推送 PushDeer 通知(异步,失败不影响交易)。"""
        try:
            from pushdeer_notifier import push_message_async

            symbol = record.get("symbol", "?")
            direction = record.get("direction", "")
            quantity = record.get("quantity")
            entry = record.get("entryPrice")
            stop = record.get("stopPrice")
            leverage = record.get("leverage")
            take_profit = record.get("takeProfitPrice")
            lines = [
                f"**{symbol} 开仓成功** 方向:{direction}",
                f"- 数量: {quantity:g}" if quantity is not None else "- 数量: ?",
                f"- 开仓价: {entry:.8g}" if entry is not None else "",
                f"- 止损: {stop:.8g}" if stop is not None else "",
                f"- 止盈: {take_profit:.8g}" if take_profit is not None else "- 止盈: 无(移动止损)",
                f"- 杠杆: {leverage}x" if leverage is not None else "",
            ]
            push_message_async("Binance Futures Bot 开仓", "\n".join(line for line in lines if line))
        except Exception:
            self.logger.exception("开仓通知推送失败")

    def _register_position(self, record: dict[str, Any]) -> None:
        """成功开仓后登记到本地持仓列表,供持仓监控器对齐与移动止损使用。"""
        entry = record.get("entryPrice")
        stop = record.get("stopPrice")
        if entry is None or stop is None:
            self.logger.warning("开仓登记缺失价格信息 symbol=%s,未写入本地持仓", record.get("symbol"))
            return
        from position_lifecycle import create_lifecycle
        from position_monitor import add_position, position_key

        order = record.get("order") if isinstance(record.get("order"), dict) else {}
        position_side = str(order.get("positionSide") or "BOTH")
        open_time = order.get("updateTime") or order.get("transactTime") or int(time.time() * 1000)
        position = {
            "symbol": record.get("symbol"),
            # 单向模式明确保存 BOTH；对冲模式保存交易所回执中的 LONG/SHORT。
            "positionSide": position_side,
            "status": "active",
            "signalType": record.get("signalType"),
            "direction": record.get("direction"),
            "entryPrice": entry,
            "initialStop": stop,
            "currentStop": stop,
            "stopAlgoId": order.get("stopOrderId"),
            "takeProfitOrderId": order.get("takeProfitOrderId"),
            # 止盈价(箱体内高抛低吸挂中轨限价止盈;突破仓位为 None),供看盘页标注
            "takeProfitPrice": record.get("takeProfitPrice"),
            "entryOrderId": order.get("orderId"),
            "entryClientOrderId": order.get("clientOrderId"),
            # 实际成交数量,供平仓盈亏统计展示
            "quantity": record.get("quantity"),
            "r": entry - stop,
            "leverage": record.get("leverage"),
            "trailLevel": 0,
            "openTime": open_time,
            "createdAt": _now_iso(),
        }
        add_position(self.config.environment, position)
        create_lifecycle(self.config.environment, {
            **position,
            "positionKey": position_key(position.get("symbol"), position_side),
        })

    def _leverage_for_notional(self, symbol: str, notional: Decimal) -> int:
        """按风险制名义价值匹配杠杆档位,返回该档位可用杠杆(查询失败回退配置值)。

        名义价值由风险制独立确定(与杠杆无关),杠杆仅影响保证金占用;
        notionalCap 仅用于客户端档位区间判定,不参与此处计算。
        """
        if self.client is not None:
            try:
                info = self.client.get_leverage_for_notional(symbol, notional)
            except Exception as exc:
                info = None
                self.logger.warning(
                    "查询杠杆档位失败 symbol=%s 回退配置杠杆 %s: %s", symbol, self.config.leverage, exc
                )
            if info is not None:
                return info[0]
        return self.config.leverage

    def _stop_for_signal(
        self, base: dict[str, Any], box: dict[str, Any], stop_buffer: Decimal
    ) -> tuple[Decimal | None, str, str, str]:
        """按信号类型与方向返回 (止损价, 开仓方向, 止损单方向, 跳过原因)。

        高抛低吸的止损参考价 = 箱体窗口内含影线的最高/最低价(extremeHigh/extremeLow,
        而非按开收盘计算的 upper/lower),放在其外侧 stop_buffer 处,插针到边界附近
        不会扫损;突破多单止损在箱体中轴下方 stop_buffer 处,同样保留 1σ×收盘价
        的缓冲,避免价格回踩中轴附近即被扫损。
        """
        upper = _decimal(box.get("upper"))
        lower = _decimal(box.get("lower"))
        mid = (upper + lower) / 2 if upper is not None and lower is not None else None
        signal_type = str(base.get("signalType", ""))
        direction = str(base.get("direction", ""))
        if signal_type == BUY_IN_BOX_SIGNAL and direction == "多":
            # 旧信号缺 extremeLow 时回退箱体下沿
            reference = _decimal(box.get("extremeLow")) or lower
            return (reference - stop_buffer, "BUY", "SELL", "") if reference is not None else (None, "", "", "箱体参数无效")
        if signal_type == SELL_IN_BOX_SIGNAL and direction == "空":
            # 旧信号缺 extremeHigh 时回退箱体上沿
            reference = _decimal(box.get("extremeHigh")) or upper
            return (reference + stop_buffer, "SELL", "BUY", "") if reference is not None else (None, "", "", "箱体参数无效")
        if signal_type == BREAKOUT_SIGNAL and direction == "多":
            return (mid - stop_buffer, "BUY", "SELL", "") if mid is not None else (None, "", "", "箱体参数无效")
        if signal_type == BREAKDOWN_SIGNAL and direction == "空":
            # 下破空单止损在箱体中轴上方 stop_buffer 处(与上破多单镜像)
            return (mid + stop_buffer, "SELL", "BUY", "") if mid is not None else (None, "", "", "箱体参数无效")
        return (None, "", "", "信号类型与方向不匹配")

    def _stop_buffer_for_signal(self, signal: dict[str, Any]) -> Decimal:
        """计算止损的缓冲距离(高抛低吸与突破统一使用,用户确认:1×ATR 缓冲)。

        基准缓冲优先复用信号生成时下发的 executionMetrics.breakoutBuffer
        (与突破信号确认逻辑一致:收盘越过 上沿+缓冲 才确认真突破);
        缺失时按相同公式用 sigma15m 回退计算;均缺失则不设缓冲。
        基准缓冲取 1σ×收盘价,较 0.5σ 加大,降低插针扫损概率;
        再与 1×ATR(14) 取较大者:ATR 由高/低价直接度量含影线的真实波幅,
        对插针深度更敏感(收盘价 σ 只统计收盘变动),保证缓冲至少容纳
        一次箱体内典型插针,避免插针略微超过历史极值即扫损。
        """
        metrics = signal.get("executionMetrics") if isinstance(signal.get("executionMetrics"), dict) else {}
        buffer = _decimal(metrics.get("breakoutBuffer"))
        if buffer is None or buffer <= 0:
            sigma = _decimal(metrics.get("sigma15m"))
            if sigma is None or sigma <= 0:
                return Decimal("0")
            signal_kline = signal.get("signalKline") if isinstance(signal.get("signalKline"), dict) else {}
            close = _decimal(signal_kline.get("close"))
            if close is None or close <= 0:
                return Decimal("0")
            buffer = Decimal("1.0") * sigma * close
        atr14 = _decimal(metrics.get("atr14"))
        if atr14 is not None and atr14 > 0:
            buffer = max(buffer, atr14)
        return buffer

    def _round_stop_price(self, stop: Decimal, filters: dict[str, dict[str, Any]]) -> Decimal | None:
        """将止损价对齐到 PRICE_FILTER tickSize 并校验价格区间。

        官方文档:stopPrice 必须是 tickSize 的整数倍且落在 minPrice~maxPrice 之间;
        向下取整保证止损价永远合法,不改变信号本身的风险边界方向。
        """
        rule = filters.get("PRICE_FILTER")
        if not isinstance(rule, dict):
            return stop
        tick = _decimal(rule.get("tickSize")) or Decimal("0")
        if tick <= 0:
            return stop
        min_price = _decimal(rule.get("minPrice")) or Decimal("0")
        max_price = _decimal(rule.get("maxPrice"))
        rounded = (stop / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        if rounded < min_price or (max_price is not None and rounded > max_price):
            return None
        return rounded

    def _estimate_entry(self, signal: dict[str, Any], symbol: str) -> Decimal | None:
        """预估入场价:优先最新价,失败回退信号 K 线收盘价。"""
        if self.client is not None:
            try:
                return Decimal(str(self.client.get_last_price(symbol)))
            except Exception as exc:
                self.logger.warning("获取最新价失败 symbol=%s 回退信号收盘价: %s", symbol, exc)
        signal_kline = signal.get("signalKline") if isinstance(signal.get("signalKline"), dict) else {}
        close = _decimal(signal_kline.get("close"))
        return close if close is not None and close > 0 else None

    def _compute_quantity(
        self,
        symbol: str,
        stop: Decimal,
        entry: Decimal,
        filters: dict[str, dict[str, Any]],
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None, int, str | None]:
        """按风险制确定名义价值并匹配杠杆档位计算开仓数量。

        返回 (数量, 名义金额, 最大亏损额, 有效杠杆, 跳过原因)。

        流程(与用户确认的逻辑):
        1. 名义价值 = 单笔最大亏损 / 止损距离比例 = (余额×1%) / |开仓价−止损价| × 开仓价
           ——纯风险制,与杠杆无关;
        2. 按名义价值匹配 leverageBracket 档位(notionalFloor ≤ 名义 < notionalCap),
           使用该档位的可用最大杠杆(杠杆不扩大名义,仅影响保证金占用);
        3. 按 MARKET_LOT_SIZE stepSize 向下取整(官方:市价单按 MARKET_LOT_SIZE 校验),
           数量超过 maxQty 时截断到该上限(实际仓位比风险预算更小,方向安全),
           仅当低于 minQty 或 MIN_NOTIONAL 时才放弃开仓;
        4. 保证金约束:名义 ≤ 杠杆 × 可用余额,超出则截断
           (截断只会减小数量,天然不超 maxQty;名义 < 档位 cap,不会触发 -2027)。
        """
        if self.balance <= 0:
            return None, None, None, self.config.leverage, "余额为零"
        step = self._filter_number(filters, "MARKET_LOT_SIZE", "stepSize") or self._filter_number(filters, "LOT_SIZE", "stepSize")
        if step is None or step <= 0:
            return None, None, None, self.config.leverage, "交易规则缺少步长"
        min_qty = (
            self._filter_number(filters, "MARKET_LOT_SIZE", "minQty")
            or self._filter_number(filters, "LOT_SIZE", "minQty")
            or Decimal("0")
        )
        max_qty = (
            self._filter_number(filters, "MARKET_LOT_SIZE", "maxQty")
            or self._filter_number(filters, "LOT_SIZE", "maxQty")
        )
        min_notional = self._filter_number(filters, "MIN_NOTIONAL", "notional") or Decimal("0")
        max_loss = self.balance * Decimal(str(self.config.risk_per_trade_pct))
        distance = abs(entry - stop)
        if distance <= 0:
            return None, None, None, self.config.leverage, "开仓价与止损价重合"
        notional_raw = max_loss / distance * entry
        leverage = self._leverage_for_notional(symbol, notional_raw)
        quantity = (max_loss / distance / step).to_integral_value(rounding=ROUND_FLOOR) * step
        notional = quantity * entry
        # 最大下单量约束:风险制数量超 maxQty 时截断到上限(向下取整到 step),而非放弃开仓
        if max_qty is not None and quantity > max_qty:
            quantity = (max_qty / step).to_integral_value(rounding=ROUND_FLOOR) * step
            notional = quantity * entry
            self.logger.warning("数量超过最大下单量,已截断到上限 symbol=%s qty=%s maxQty=%s", symbol, quantity, max_qty)
        # 最低数量约束:截断后仍低于最低限制才不开仓
        if quantity <= 0 or quantity < min_qty:
            return None, None, None, leverage, "数量低于最小下单量"
        if min_notional > 0 and notional < min_notional:
            return None, None, None, leverage, "名义金额低于最小下单限额"
        # 保证金约束:名义 ≤ 杠杆 × 可用余额。
        # notionalCap 仅用于档位区间匹配(名义在档内恒 < cap),此处无需再校验;
        # 截断后名义 = 杠杆×余额 < 原名义 < 档位 cap,不会触发交易所 -2027;
        # 截断只会减小数量,天然不超 maxQty。
        max_notional = self.balance * Decimal(str(leverage))
        if notional > max_notional:
            quantity = (max_notional / entry / step).to_integral_value(rounding=ROUND_FLOOR) * step
            notional = quantity * entry
            if quantity < min_qty or quantity <= 0:
                return None, None, None, leverage, "杠杆截断后数量低于最小下单量"
            if min_notional > 0 and notional < min_notional:
                return None, None, None, leverage, "杠杆截断后名义金额低于最小下单限额"
            self.logger.warning("名义金额超保证金上限,数量已截断 symbol=%s qty=%s notional=%s", symbol, quantity, notional)
        return quantity, notional, max_loss, leverage, None

    def _place_orders(
        self,
        computed: dict[str, Any],
        side: str,
        stop_side: str,
        quantity: Decimal,
        stop_price: Decimal,
        leverage: int,
        take_profit_price: Decimal | None,
    ) -> dict[str, Any]:
        """真实执行:设置杠杆 → 市价开仓 → 查询真实持仓 → 成交守卫 → 挂全平止损单 → 挂中轨限价止盈。"""
        symbol = computed["symbol"]
        client = self.client
        if client is None:
            return self._failed_record(computed, "缺少交易客户端", computed.get("runId"))
        # 持仓模式适配(官方文档):
        # 对冲模式:开仓/平仓均带与持仓方向一致的 positionSide,反向单天然减仓,拒收 reduceOnly(-1106);
        # 单向模式:不带 positionSide,平仓用 reduceOnly 防止反向开仓。
        if client.is_dual_side_position():
            position_side: str | None = "LONG" if side == "BUY" else "SHORT"
            reduce_only = False
        else:
            position_side = None
            reduce_only = True
        quantity_text = format(quantity, "f")

        try:
            client.set_leverage(symbol, leverage)
        except Exception as exc:
            return self._failed_record(computed, f"设置杠杆失败:{exc}", computed.get("runId"))
        try:
            order = client.place_market_order(symbol, side, quantity_text, position_side)
        except Exception as exc:
            return self._failed_record(computed, f"市价单下单失败:{exc}", computed.get("runId"))
        if order.get("status") != "FILLED":
            return self._failed_record(computed, f"市价单未成交,状态 {order.get('status')}", computed.get("runId"))

        # 开仓回报可能缺少成交价与数量(测试网实测),直接重新查询账户真实持仓,
        # 以持仓记录的开仓均价与数量为准
        position = self._query_position_after_order(client, symbol, position_side)
        if position is None:
            note = self._close_position(
                client, symbol, side, _decimal(order.get("executedQty")), position_side, reduce_only
            )
            return self._failed_record(computed, f"订单已成交但查询不到持仓,{note}", computed.get("runId"))
        entry_price = _decimal(position.get("entryPrice"))
        position_amt = _decimal(position.get("positionAmt"))
        if entry_price is None or entry_price <= 0:
            return self._failed_record(computed, "持仓回报缺少有效开仓价", computed.get("runId"))
        if position_amt is None or position_amt == 0:
            return self._failed_record(computed, "持仓回报数量无效", computed.get("runId"))
        # 方向校验:多仓持仓量为正、空仓为负(双向/对冲模式均适用)
        if (side == "BUY" and position_amt <= 0) or (side == "SELL" and position_amt >= 0):
            return self._failed_record(computed, "持仓方向与开仓不符", computed.get("runId"))
        executed_qty = abs(position_amt)
        computed["orderId"] = order.get("orderId")
        computed["entryPrice"] = float(entry_price)
        computed["quantity"] = float(executed_qty)  # 以真实持仓量覆盖预估数量

        if (side == "BUY" and entry_price <= stop_price) or (side == "SELL" and entry_price >= stop_price):
            # 成交价越过止损价:立即反向平仓,保护资金
            note = self._close_position(client, symbol, side, executed_qty, position_side, reduce_only)
            return self._failed_record(computed, f"成交价越过止损价,{note}", computed.get("runId"))

        try:
            # 全平止损单:closePosition=true,不传 quantity/reduceOnly,双向/对冲统一适用
            stop_order = client.place_stop_market_close_position(
                symbol, stop_side, format(stop_price, "f"), position_side
            )
        except Exception as exc:
            # 止损挂单失败:仓位已开且无保护,立即反向平仓
            self.logger.error("止损单挂单失败 symbol=%s: %s", symbol, exc)
            note = self._close_position(client, symbol, side, executed_qty, position_side, reduce_only)
            return self._failed_record(computed, f"止损单挂单失败,{note}", computed.get("runId"))

        computed["order"] = {
            "orderId": order.get("orderId"),
            "clientOrderId": order.get("clientOrderId"),
            "side": side,
            "positionSide": position_side,
            "type": "MARKET",
            "status": order.get("status"),
            "executedQty": order.get("executedQty"),
            "entryPriceFromPosition": position.get("entryPrice"),
            "positionAmt": position.get("positionAmt"),
            "stopOrderId": stop_order.get("algoId"),
            "stopOrderStatus": stop_order.get("algoStatus"),
            "stopClosePosition": True,
        }

        # 箱体内高抛低吸仓位挂箱体中轨限价止盈;止盈挂单失败不影响整体(止损单已提供保护)
        take_profit_note: str | None = None
        if take_profit_price is not None:
            take_profit_side = "SELL" if side == "BUY" else "BUY"
            # 方向守卫:多单止盈须高于入场价、空单止盈须低于入场价,否则止盈已无意义
            if (side == "BUY" and entry_price < take_profit_price) or (
                side == "SELL" and entry_price > take_profit_price
            ):
                try:
                    take_profit_order = client.place_limit_order(
                        symbol,
                        take_profit_side,
                        format(take_profit_price, "f"),
                        format(executed_qty, "f"),
                        position_side,
                        reduce_only,
                    )
                    computed["takeProfitPrice"] = float(take_profit_price)
                    computed["order"]["takeProfitOrderId"] = take_profit_order.get("orderId")
                    computed["order"]["takeProfitOrderStatus"] = take_profit_order.get("status")
                except Exception as exc:
                    take_profit_note = f"止盈单挂单失败:{exc}"
                    self.logger.error("止盈单挂单失败 symbol=%s: %s", symbol, exc)
            else:
                take_profit_note = "入场价已越过中轨,止盈价无意义,未挂止盈单"
        computed["order"]["takeProfitNote"] = take_profit_note
        return {**computed, "status": "filled", "summaryKey": "executed"}

    def _query_position_after_order(
        self, client: BinanceFuturesClient, symbol: str, position_side: str | None
    ) -> dict[str, Any] | None:
        """开仓后查询账户真实持仓;撮合结果可能稍后可见,重试一次。"""
        for attempt in range(2):
            try:
                position = client.get_position(symbol, position_side)
            except Exception as exc:
                self.logger.warning("查询持仓失败 symbol=%s: %s", symbol, exc)
                return None
            if position is not None:
                return position
            if attempt == 0:
                time.sleep(1)
        return None

    def _close_position(
        self,
        client: BinanceFuturesClient,
        symbol: str,
        side: str,
        quantity: Decimal,
        position_side: str | None,
        reduce_only: bool,
    ) -> str:
        """市价反向平仓已开仓位,返回处理说明。"""
        if quantity is None or quantity <= 0:
            self.logger.error("无法反向平仓 symbol=%s: 缺少有效数量", symbol)
            return "无法平仓(缺少有效数量),请手动处理"
        close_side = "SELL" if side == "BUY" else "BUY"
        try:
            client.place_market_order(symbol, close_side, format(quantity, "f"), position_side, reduce_only)
            return "已反向平仓"
        except Exception as exc:
            self.logger.error("反向平仓失败 symbol=%s: %s", symbol, exc)
            return "反向平仓失败,请手动处理"

    # ---- 台账与状态 ----

    def _computed_fields(
        self,
        base: dict[str, Any],
        box: dict[str, Any],
        stop_price: Decimal,
        est_entry: Decimal,
        quantity: Decimal,
        notional: Decimal,
        max_loss: Decimal,
        leverage: int,
        stop_buffer: Decimal,
    ) -> dict[str, Any]:
        """汇总本轮计算参数与箱体信息。"""
        return {
            **base,
            "stopPrice": float(stop_price),
            "stopBuffer": float(stop_buffer),
            "estimatedEntry": float(est_entry),
            "quantity": float(quantity),
            "notionalUsdt": float(notional),
            "maxLossUsdt": float(max_loss),
            "balanceUsdt": float(self.balance),
            "leverage": leverage,
            "riskPercent": self.config.risk_per_trade_pct,
            "box": {
                "window": box.get("window"),
                "upper": box.get("upper"),
                "lower": box.get("lower"),
                "extremeHigh": box.get("extremeHigh"),
                "extremeLow": box.get("extremeLow"),
                "boxScore": box.get("boxScore"),
            },
            "createdAt": _now_iso(),
        }

    @staticmethod
    def _is_executed(ledger: list[dict[str, Any]], symbol: str, open_time: Any) -> bool:
        """台账去重:同交易对 + 信号 K 线起点已成功执行过。"""
        fingerprint = f"{symbol}|{open_time}"
        return any(
            item.get("fingerprint") == fingerprint and item.get("status") == "filled"
            for item in ledger
        )

    @staticmethod
    def _filter_number(filters: dict[str, dict[str, Any]], filter_type: str, field: str) -> Decimal | None:
        """读取过滤器数值字段,缺失或非法时返回 None。"""
        rule = filters.get(filter_type)
        if not isinstance(rule, dict):
            return None
        return _decimal(rule.get(field))

    @staticmethod
    def _skipped_record(base: dict[str, Any], reason: str) -> dict[str, Any]:
        """构建跳过记录。"""
        return {**base, "status": "skipped", "summaryKey": "skipped", "reason": reason}

    @staticmethod
    def _failed_record(base: dict[str, Any], reason: str, run_id: str | None) -> dict[str, Any]:
        """构建失败记录。"""
        return {**base, "runId": run_id, "status": "failed", "summaryKey": "failed", "reason": reason}

    def _write_back_signal_pool(self, payload: dict[str, Any], signals: list[dict[str, Any]]) -> None:
        """将消费状态原子写回信号池文件,防止信号重放。

        调度器下一轮会整体覆盖信号池(新信号),旧消费标记随之自然失效,
        不影响下一轮信号;消费标记只用于同一轮信号的生命周期内防重放。
        """
        payload["signals"] = signals
        write_json_atomically(payload, self.signal_pool_path)

    def _write_ledger(self, ledger: list[dict[str, Any]]) -> None:
        """原子写入只含开仓初始真实信息的台账。"""
        payload = {
            "source": {"environment": self.config.environment, "updatedAt": _now_iso()},
            "ledger": ledger,
        }
        write_json_atomically(payload, self.ledger_path)

    @staticmethod
    def _entry_ledger_record(record: dict[str, Any]) -> dict[str, Any]:
        """提取成交后可审计的开仓初始事实，避免台账混入预估与运行过程状态。"""
        order = record.get("order") if isinstance(record.get("order"), dict) else {}
        return {
            key: record.get(key)
            for key in (
                "fingerprint", "runId", "symbol", "signalType", "direction",
                "signalKlineOpenTime", "entryPrice", "quantity", "stopPrice",
                "stopBuffer", "leverage", "riskPercent", "createdAt",
            )
        } | {
            "status": "filled",
            "entryOrder": {
                key: order.get(key)
                for key in (
                    "orderId", "clientOrderId", "side", "positionSide", "type",
                    "status", "executedQty", "entryPriceFromPosition",
                )
            },
            "initialProtection": {
                key: order.get(key)
                for key in (
                    "stopOrderId", "stopOrderStatus", "stopClosePosition",
                    "takeProfitOrderId", "takeProfitOrderStatus", "takeProfitNote",
                )
            },
            "takeProfitPrice": record.get("takeProfitPrice"),
        }

    def _write_status(
        self,
        status: dict[str, Any],
        summary: dict[str, int],
        run_id: str | None,
        records: list[dict[str, Any]] | None = None,
    ) -> None:
        """原子写入执行状态文件(含摘要与本轮明细)。"""
        status = dict(status)
        status["summary"] = summary
        if records is not None:
            for group, statuses in (
                ("executed", {"filled"}),
                ("skipped", {"skipped"}),
                ("failed", {"failed"}),
            ):
                status[group] = [
                    {key: record[key] for key in ("symbol", "signalType", "direction", "quantity", "entryPrice", "stopPrice", "orderId", "status", "reason") if key in record}
                    for record in records
                    if record.get("runId") == run_id and record.get("status") in statuses
                ]
        payload = {
            "source": {
                "environment": self.config.environment,
                "updatedAt": _now_iso(),
                "officialDocumentation": "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info",
            },
            **status,
        }
        write_json_atomically(payload, self.status_path)


def execute_signal_pool(signal_pool_path: Path, config: SchedulerConfig) -> dict[str, int]:
    """调度器便捷入口:按当前配置执行一轮信号交易。"""
    return TradingExecutor(config, signal_pool_path).execute()


def main() -> None:
    """独立执行入口:按配置开关读取信号池执行一轮交易。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="调度配置 JSON 路径。")
    parser.add_argument("--signal-pool", type=Path, help="信号池 JSON 路径(默认按配置推导)。")
    arguments = parser.parse_args()
    config = load_config(arguments.config)
    configure_logging(config.environment)
    signal_pool_path = arguments.signal_pool or config.runtime_directory / f"signal_pool_{config.execution_interval}.json"
    summary = TradingExecutor(config, signal_pool_path).execute()
    print(
        "执行完成: 成交 {executed} 跳过 {skipped} 失败 {failed}".format(
            executed=summary.get("executed", 0),
            skipped=summary.get("skipped", 0),
            failed=summary.get("failed", 0),
        )
    )


if __name__ == "__main__":
    main()
