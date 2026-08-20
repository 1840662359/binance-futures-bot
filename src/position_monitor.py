"""持仓监控模块:本地持仓与交易所实际持仓高频对齐,幽灵清理,突破仓位 R 阶移动止损。

监控范围(与用户确认的规则):
- 仅监控「本地有 ∧ 交易所也有」的仓位;
- 本地有、交易所无 → 幽灵持仓:清理本地记录,并同步取消该 symbol 残留的止盈/止损挂单;
- 其它情况一律忽略、不监控;
- 高抛低吸仓位:仅存在性监控,不动止盈止损挂单;
- 上破/下破仓位(多空镜像):按 R 阶移动止损——R = 开仓价 − 初始止损(箱体中轨±缓冲),
  多仓标记价格累计上移 1R 止损上移至「mark − R − 1h ATR(14)」,
  空仓标记价格累计下移 1R 止损下移至「mark + |R| + 1h ATR(14)」,
  经 /fapi/v1/algoOrder 以 cancel+place 方式更新(官方明确未触发条件单不支持修改)。

对齐数据源:Private 用户数据流(ACCOUNT_UPDATE)+ 每 5 分钟 positionRisk 全量快照兜底。
标记价格流为 Market 全市场流 !markPrice@arr(一次订阅全部交易对,全量+增量交替推送),
不再按交易对逐个订阅、不再因持仓变化重建连接。
WebSocket Base URL 按官方 2026-04-23 迁移公告使用 {public|market|private} 新三入口。
"""

from __future__ import annotations

import json
import math
import queue
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable

from app_paths import runtime_directory
from binance_futures_client import BinanceFuturesClient, BinanceFuturesError
from detect_box_consolidation import parse_candles
from fetch_klines_for_symbols import fetch_klines_with_failures
from logging_utils import get_logger
from pnl_tracker import append_pnl_record
from scheduler import SchedulerConfig
from secret_utils import get_secret
from websocket_streams import MarkPriceStream, UserDataStream
from ws_api_client import WsApiClient

POSITION_STATE_FILENAME = "position_state.json"
BREAKOUT_SIGNAL = "上破箱体上沿"
BREAKDOWN_SIGNAL = "下破箱体下沿"
# 受移动止损监管的信号(上破多/下破空,多空镜像)
TRAILED_SIGNALS = frozenset({BREAKOUT_SIGNAL, BREAKDOWN_SIGNAL})
FULL_SNAPSHOT_SECONDS = 300
LISTEN_KEY_RENEW_SECONDS = 1800
ATR_REFRESH_SECONDS = 1800
# 官方盈亏校准间隔:展示层由 markPrice@1s 逐秒计算(插值),
# 每 30 秒用 WS API v2/account.status 官方值校准(权重 10,消耗可接受)
PNL_REFRESH_SECONDS = 30
ATR_PERIOD = 14
# 移动止损缓冲:1.5×ATR(1h),在容纳小时级正常波动与锁利回吐之间折中
TRAIL_STOP_ATR_MULTIPLIER = Decimal("1.5")
# 移动止损请求失败重试:次数与间隔(秒);-2011 视为取消成功
MOVE_STOP_MAX_ATTEMPTS = 3
MOVE_STOP_RETRY_DELAY = 1.0
SNAPSHOT_MAX_ATTEMPTS = 3
SNAPSHOT_RETRY_DELAY = 1.0

LOGGER = get_logger("position_monitor")

# 本地持仓账本的模块级线程锁(交易执行器与监控器跨线程共享)
_LEDGER_LOCK = threading.Lock()

# 活跃监控器注册(供 GUI 线程只读账户快照)
_ACTIVE_MONITOR: "PositionMonitor | None" = None
_MONITOR_LOCK = threading.Lock()


def get_active_monitor() -> "PositionMonitor | None":
    """返回当前运行的持仓监控器实例(GUI 线程只读账户快照用)。"""
    with _MONITOR_LOCK:
        return _ACTIVE_MONITOR


def _ledger_path(environment: str) -> Path:
    return runtime_directory(environment) / POSITION_STATE_FILENAME


def position_key(symbol: Any, position_side: Any = "BOTH") -> str:
    """返回程序仓位的稳定键。

    Binance 在对冲模式下会为同一 symbol 返回 LONG/SHORT 两条独立记录，
    因此禁止再用 symbol 作为本地台账与对账键。
    """
    return f"{str(symbol or '')}|{str(position_side or 'BOTH')}"


def _read_positions_unlocked(path: Path) -> list[dict[str, Any]]:
    """无锁读取本地持仓列表;文件不存在或损坏时返回空列表。"""
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        LOGGER.warning("本地持仓文件损坏,按空列表处理: %s", path)
        return []
    positions = payload.get("positions") if isinstance(payload, dict) else None
    return [item for item in positions if isinstance(item, dict)] if isinstance(positions, list) else []


def _write_positions_unlocked(environment: str, positions: list[dict[str, Any]]) -> None:
    """无锁原子写本地持仓列表。"""
    path = _ledger_path(environment)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": {"environment": environment, "updatedAt": datetime.now(timezone.utc).isoformat()},
        "positions": positions,
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_positions(environment: str) -> list[dict[str, Any]]:
    """读取本地持仓列表(线程安全)。"""
    with _LEDGER_LOCK:
        return _read_positions_unlocked(_ledger_path(environment))


def add_position(environment: str, record: dict[str, Any]) -> None:
    """登记本地持仓(同 symbol+positionSide 覆盖,线程安全)。"""
    with _LEDGER_LOCK:
        path = _ledger_path(environment)
        positions = _read_positions_unlocked(path)
        key = position_key(record.get("symbol"), record.get("positionSide"))
        record = {**record, "positionSide": str(record.get("positionSide") or "BOTH"), "positionKey": key}
        positions = [
            item for item in positions
            if position_key(item.get("symbol"), item.get("positionSide")) != key
        ]
        positions.append(record)
        _write_positions_unlocked(environment, positions)


def update_position(environment: str, symbol: str, position_side: str = "BOTH", **fields: Any) -> None:
    """更新本地持仓字段(线程安全)。"""
    with _LEDGER_LOCK:
        path = _ledger_path(environment)
        positions = _read_positions_unlocked(path)
        key = position_key(symbol, position_side)
        for item in positions:
            if position_key(item.get("symbol"), item.get("positionSide")) == key:
                item.update(fields)
                break
        _write_positions_unlocked(environment, positions)


def remove_position(environment: str, symbol: str, position_side: str = "BOTH") -> None:
    """删除本地持仓记录(线程安全)。"""
    with _LEDGER_LOCK:
        path = _ledger_path(environment)
        positions = _read_positions_unlocked(path)
        key = position_key(symbol, position_side)
        remaining = [
            item for item in positions
            if position_key(item.get("symbol"), item.get("positionSide")) != key
        ]
        if len(remaining) != len(positions):
            _write_positions_unlocked(environment, remaining)


class PositionMonitor:
    """持仓监控器:WebSocket 对齐 + 幽灵清理 + 突破仓位移动止损。

    生命周期:start() 启动全部线程,stop() 优雅关闭(关流、续期收尾、关闭 listenKey)。
    """

    def __init__(self, config: SchedulerConfig) -> None:
        self.config = config
        self.logger = LOGGER
        self._stop = threading.Event()
        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._api_key = ""
        self._api_secret = ""
        self._client = self._build_client()
        # WebSocket API 客户端:周期查询官方未实现盈亏(价格变动不触发 ACCOUNT_UPDATE);
        # 传入已校准的时钟偏移,签名请求遇 -1021 时内部自动重新校准
        self._ws_api = WsApiClient(
            self._api_key,
            self._api_secret,
            self.config.use_testnet,
            self.config.proxy_url if self.config.proxy_enabled else None,
            time_offset_ms=self._client.time_offset_ms,
        )
        self._listen_key = ""
        self._user_stream: UserDataStream | None = None
        self._mark_stream: MarkPriceStream | None = None
        self._thread: threading.Thread | None = None
        # 交易所持仓状态:{symbol: positionAmt}(启动快照 + ACCOUNT_UPDATE 增量 + 定时快照兜底)
        self._exchange_positions: dict[str, float] = {}
        # 交易所持仓数量的 Decimal 加总:{symbol: Decimal}(原始字符串保真,供 GUI 平仓下单使用)
        self._exchange_position_strs: dict[str, Decimal] = {}
        # 最新标记价格:{symbol: float}
        self._marks: dict[str, float] = {}
        # 全市场标记价格流中需要入队处理的交易对集合(移动止损的突破仓位,内存过滤用)
        self._mark_symbols: set[str] = set()
        # 1h ATR(14):{symbol: Decimal}
        self._atr: dict[str, Decimal] = {}
        # 账户快照(GUI 账户持仓页只读):余额与持仓明细,由 ACCOUNT_UPDATE 增量 + positionRisk 全量维护
        self._account_balances: dict[str, dict[str, float]] = {}
        self._account_positions: dict[str, dict[str, float]] = {}
        self._snapshot_updated_at = 0.0
        # 盈亏自动全平:账户未实现盈亏(以钱包余额为基准)达到阈值时全平全部仓位,
        # 全平后继续监控,再次达标可再触发(用户确认的规则)
        self._auto_close_enabled = bool(config.auto_close_enabled)
        self._auto_close_profit_pct = float(config.auto_close_profit_pct)
        self._auto_close_loss_pct = float(config.auto_close_loss_pct)
        self._auto_closing = False
        # 定时器基准:以启动时刻为起点,避免启动后立即触发首轮快照/续期
        started_at = time.monotonic()
        self._last_snapshot = started_at
        self._last_renew = started_at
        self._last_atr = started_at
        self._last_pnl_refresh = started_at
        # 只有该时点后的完整 positionRisk 快照才可参与风险动作；网络失败绝不清空旧状态。
        self._last_successful_snapshot = 0.0

    def _build_client(self) -> BinanceFuturesClient:
        """按环境读取密钥并构建签名客户端。"""
        if self.config.use_testnet:
            self._api_key = _env("BINANCE_TESTNET_API_KEY")
            self._api_secret = _env("BINANCE_TESTNET_API_SECRET")
        else:
            self._api_key = _env("BINANCE_API_KEY")
            self._api_secret = _env("BINANCE_API_SECRET")
        if not self._api_key or not self._api_secret:
            raise RuntimeError("缺少 Binance API 密钥,持仓监控器无法启动。")
        return BinanceFuturesClient(self._api_key, self._api_secret, self.config.use_testnet, self.config.http_timeout_seconds)

    # ---- 生命周期 ----

    def start(self) -> None:
        """启动 listenKey、双 WebSocket 流与监控线程。"""
        self._listen_key = self._client.create_listen_key()
        self.logger.info("listenKey 已创建 key=%s", self._listen_key[:8])
        proxy_url = self.config.proxy_url if self.config.proxy_enabled else None
        self._user_stream = UserDataStream(
            self.config.use_testnet, self._listen_key, proxy_url, self._on_account_event
        )
        # 全市场标记价格流:一次订阅全部交易对,无需按持仓维护订阅列表
        self._mark_stream = MarkPriceStream(
            self.config.use_testnet, proxy_url, self._on_mark_event
        )
        self._user_stream.start()
        self._mark_stream.start()
        # 初始全量对齐(重连兜底与增量事件之前先建立基线)
        self._full_snapshot()
        # 启动即刷新 ATR,保证移动止损首档触发时有数据可用
        self._refresh_atr()
        self._thread = threading.Thread(target=self._run, name="position-monitor", daemon=True)
        self._thread.start()
        with _MONITOR_LOCK:
            global _ACTIVE_MONITOR
            _ACTIVE_MONITOR = self
        self.logger.info("持仓监控器已启动 positions=%s", len(load_positions(self.config.environment)))

    def stop(self) -> None:
        """停止监控线程与 WebSocket 流,并关闭 listenKey。"""
        self._stop.set()
        if self._user_stream is not None:
            self._user_stream.stop()
        if self._mark_stream is not None:
            self._mark_stream.stop()
        if self._thread is not None:
            self._thread.join(timeout=10)
        if self._listen_key:
            try:
                self._client.close_listen_key(self._listen_key)
            except Exception as exc:
                self.logger.warning("关闭 listenKey 失败: %s", exc)
        with _MONITOR_LOCK:
            global _ACTIVE_MONITOR
            if _ACTIVE_MONITOR is self:
                _ACTIVE_MONITOR = None
        self.logger.info("持仓监控器已停止")

    # ---- 主循环 ----

    def _run(self) -> None:
        """事件驱动的监控主循环,附带定时快照/续期/ATR 刷新。"""
        while not self._stop.is_set():
            try:
                event = self._queue.get(timeout=1)
                self._handle_event(event)
            except queue.Empty:
                pass
            except Exception:
                self.logger.exception("监控事件处理异常")
            try:
                now = time.monotonic()
                if now - self._last_snapshot >= FULL_SNAPSHOT_SECONDS:
                    self._full_snapshot()
                if now - self._last_renew >= LISTEN_KEY_RENEW_SECONDS:
                    self._renew_listen_key()
                if now - self._last_atr >= ATR_REFRESH_SECONDS:
                    self._refresh_atr()
                if now - self._last_pnl_refresh >= PNL_REFRESH_SECONDS:
                    self._refresh_pnl_from_ws_api()
                if self._auto_close_enabled:
                    self._check_auto_close()
            except BaseException:
                # 定时任务任何未预期异常不允许监控线程静默退出,记录后继续
                self.logger.exception("监控定时任务异常")

    def _handle_event(self, event: tuple[str, Any]) -> None:
        kind, payload = event
        if kind == "account":
            self._apply_account_update(payload)
            self._reconcile()
        elif kind == "mark":
            symbol, mark = payload
            self._marks[symbol] = mark
            self._check_trailing(symbol, mark)
        elif kind == "snapshot":
            # 外部(如 GUI 平仓完成)请求立即全量刷新快照,在监控线程内执行避免竞态
            self._full_snapshot()

    def request_full_snapshot(self) -> None:
        """请求监控线程立即执行一次全量快照(positionRisk),供平仓等操作后快速同步。"""
        self._queue.put(("snapshot", None))

    # ---- WebSocket 回调(接收线程,仅入队) ----

    def _on_account_event(self, payload: dict[str, Any]) -> None:
        event_type = payload.get("e")
        if event_type == "ACCOUNT_UPDATE":
            self._queue.put(("account", payload))
        elif event_type in {"ORDER_TRADE_UPDATE", "ALGO_UPDATE", "listenKeyExpired"}:
            # 订单/条件单状态变更及 listenKey 失效均要求用 REST 完整对账；
            # 不依据网络事件缺失直接清理程序仓位。
            self._queue.put(("snapshot", None))

    def _on_mark_event(self, payload: dict[str, Any]) -> None:
        """全市场标记价格流回调(接收线程,仅入队)。

        组合流响应为 {"stream": "!markPrice@arr", "data": [markPriceUpdate, ...]}:
        data 是数组,每轮含全量推送(全部交易对)与增量推送(仅价格变更交易对),
        对每个元素按交易对过滤后处理;未出现在本轮推送中的交易对保持原值。
        """
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        items = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
        for item in items:
            if not isinstance(item, dict) or item.get("e") != "markPriceUpdate":
                continue
            symbol = item.get("s")
            try:
                mark = float(item.get("p"))
            except (TypeError, ValueError):
                continue
            if not isinstance(symbol, str) or not math.isfinite(mark):
                continue
            # 实时更新快照 markPrice 与更新时间(GUI 依据 updatedAt 逐秒重建盈亏展示)
            for position in self._account_positions.values():
                if position.get("symbol") == symbol:
                    position["markPrice"] = mark
                    self._snapshot_updated_at = time.monotonic()
            # 仅移动止损的突破仓位入队(其余交易对只更新快照,不触发本地持仓文件读取)
            if symbol not in self._mark_symbols:
                continue
            self._marks[symbol] = mark
            self._queue.put(("mark", (symbol, mark)))

    # ---- 对齐 ----

    def _apply_account_update(self, payload: dict[str, Any]) -> None:
        """用 ACCOUNT_UPDATE 事件更新账户快照(余额 B 数组 + 持仓明细 P 数组)。"""
        account = payload.get("a")
        if not isinstance(account, dict):
            return
        balances = account.get("B")
        if isinstance(balances, list):
            for item in balances:
                if not isinstance(item, dict):
                    continue
                asset = item.get("a")
                try:
                    wallet = float(item.get("wb"))
                    available = float(item.get("cw"))
                except (TypeError, ValueError):
                    continue
                if isinstance(asset, str) and math.isfinite(wallet) and math.isfinite(available):
                    previous = self._account_balances.get(asset, {})
                    self._account_balances[asset] = {
                        "balance": wallet,
                        "availableBalance": available,
                        "unrealizedProfit": previous.get("unrealizedProfit", 0.0),
                    }
        positions = account.get("P")
        if isinstance(positions, list):
            for item in positions:
                if not isinstance(item, dict):
                    continue
                symbol = item.get("s")
                try:
                    amount = float(item.get("pa"))
                    entry = float(item.get("ep"))
                    unrealized = float(item.get("up"))
                except (TypeError, ValueError):
                    continue
                side = str(item.get("ps") or "BOTH")
                if not isinstance(symbol, str) or not math.isfinite(amount):
                    continue
                key = position_key(symbol, side)
                quantity = _decimal_of(item.get("pa")) or Decimal("0")
                # ACCOUNT_UPDATE 只推送发生变化的方向，不能把未出现的另一方向归零。
                self._exchange_positions[key] = amount
                self._exchange_position_strs[key] = quantity
                previous = self._account_positions.get(key, {})
                self._account_positions[key] = {
                    "symbol": symbol,
                    "positionSide": side,
                    "positionAmt": amount,
                    "positionAmtStr": format(quantity, "f"),
                    "entryPrice": entry if math.isfinite(entry) else previous.get("entryPrice", 0.0),
                    "markPrice": previous.get("markPrice", 0.0),
                    "unrealizedProfit": unrealized if math.isfinite(unrealized) else previous.get("unrealizedProfit", 0.0),
                    "leverage": previous.get("leverage", 1),
                }
        self._snapshot_updated_at = time.monotonic()

    def _full_snapshot(self) -> None:
        """positionRisk 全量快照覆盖持仓状态,并触发对齐(断线/丢事件兜底)。"""
        self._last_snapshot = time.monotonic()
        last_error: Exception | None = None
        for attempt in range(1, SNAPSHOT_MAX_ATTEMPTS + 1):
            try:
                # 先完整取得数据到候选快照，任一请求失败均不污染正在使用的状态。
                leverage_map = self._client.get_leverage_map()
                raw_positions = self._client.get_positions_detail()
                balance = self._client.get_balance_usdt()
                wallet = self._client.get_wallet_balance_usdt()
            except Exception as exc:
                last_error = exc
                if attempt < SNAPSHOT_MAX_ATTEMPTS:
                    self.logger.warning("持仓快照查询失败(第 %s/%s 次),稍后重试: %s", attempt, SNAPSHOT_MAX_ATTEMPTS, exc)
                    time.sleep(SNAPSHOT_RETRY_DELAY * attempt)
                continue
            candidate_positions: dict[str, float] = {}
            candidate_position_strs: dict[str, Decimal] = {}
            candidate_account_positions: dict[str, dict[str, Any]] = {}
            local_leverage = {
                position_key(position.get("symbol"), position.get("positionSide")): position.get("leverage")
                for position in load_positions(self.config.environment)
            }
            for item in raw_positions:
                symbol = item.get("symbol")
                try:
                    amount = float(item.get("positionAmt"))
                except (TypeError, ValueError):
                    continue
                side = str(item.get("positionSide") or "BOTH")
                if not isinstance(symbol, str) or not math.isfinite(amount):
                    continue
                key = position_key(symbol, side)
                quantity = _decimal_of(item.get("positionAmt")) or Decimal("0")
                candidate_positions[key] = amount
                candidate_position_strs[key] = quantity
                candidate_account_positions[key] = {
                    "symbol": symbol, "positionSide": side, "positionAmt": amount,
                    "positionAmtStr": format(quantity, "f"),
                    "entryPrice": _safe_float(item.get("entryPrice")),
                    "markPrice": _safe_float(item.get("markPrice")),
                    "unrealizedProfit": _safe_float(item.get("unrealizedProfit")),
                    "leverage": leverage_map.get(symbol) or local_leverage.get(key) or 0,
                }
            self._exchange_positions = candidate_positions
            self._exchange_position_strs = candidate_position_strs
            self._account_positions = candidate_account_positions
            usdt = self._account_balances.setdefault("USDT", {})
            usdt["availableBalance"] = balance
            usdt["balance"] = wallet
            self._snapshot_updated_at = time.monotonic()
            self._last_successful_snapshot = self._snapshot_updated_at
            self._reconcile()
            return
        self.logger.warning("持仓快照连续失败,沿用上次成功状态 attempts=%s: %s", SNAPSHOT_MAX_ATTEMPTS, last_error)

    def get_account_snapshot(self) -> dict[str, Any]:
        """返回仅包含程序登记仓位的账户快照(GUI 线程只读)。"""
        managed = {
            position_key(item.get("symbol"), item.get("positionSide")): item
            for item in load_positions(self.config.environment)
        }
        return {
            "balances": {asset: dict(info) for asset, info in self._account_balances.items()},
            "positions": {
                key: {**dict(info), "managed": True, "local": dict(managed[key])}
                for key, info in self._account_positions.items() if key in managed
            },
            "updatedAt": self._snapshot_updated_at,
        }

    def _reconcile(self) -> None:
        """对齐:本地有而交易所无的仓位视为幽灵,清理本地并取消残留挂单。"""
        for position in load_positions(self.config.environment):
            key = position_key(position.get("symbol"), position.get("positionSide"))
            amount = self._exchange_positions.get(key)
            expected = _decimal_of(position.get("quantity"))
            # 本轮完整 V3 快照成功后，未返回该键等同该方向无持仓；查询失败时本函数不会执行。
            if amount is None:
                self._cleanup_phantom(position)
                continue
            if abs(amount) == 0:
                self._cleanup_phantom(position)
                continue
            if expected is not None and abs(abs(Decimal(str(amount))) - abs(expected)) > Decimal("0.00000001"):
                self._mark_conflict(position, f"交易所数量 {amount} 与程序登记数量 {expected} 不一致")
                continue
            if position.get("status") == "conflict":
                update_position(self.config.environment, str(position.get("symbol")), str(position.get("positionSide") or "BOTH"), status="active", conflictReason=None)
        # 持仓状态变化后刷新移动止损跟踪集合(全市场流只做内存过滤,无需重建连接)
        self._refresh_mark_symbols()

    def _mark_conflict(self, position: dict[str, Any], reason: str) -> None:
        """标记外部干预冲突；冲突仓位不再被自动撤单、移动止损或平仓。"""
        if position.get("status") != "conflict" or position.get("conflictReason") != reason:
            self.logger.error("程序仓位进入冲突保护 symbol=%s side=%s: %s", position.get("symbol"), position.get("positionSide"), reason)
            update_position(self.config.environment, str(position.get("symbol")), str(position.get("positionSide") or "BOTH"), status="conflict", conflictReason=reason, conflictAt=datetime.now(timezone.utc).isoformat())

    def _log_cancel_outcome(self, label: str, symbol: str, order_id: Any, exc: Exception) -> None:
        """记录撤单结果:订单已不存在(-2011,可能已被成交/取消)降为 INFO,其余保持 WARNING。"""
        if isinstance(exc, BinanceFuturesError) and exc.code == -2011:
            self.logger.info(
                "幽灵持仓清理:%s单已不存在(可能已成交或被取消),跳过 symbol=%s id=%s",
                label, symbol, order_id,
            )
        else:
            self.logger.warning("幽灵持仓清理:%s单取消失败 symbol=%s: %s", label, symbol, exc)

    def _cleanup_phantom(self, position: dict[str, Any]) -> None:
        """清理幽灵持仓:记录平仓盈亏 → 取消残留挂单 → 删除本地记录。

        止损触发/仓位关闭后挂单已被交易所消费,撤单返回 -2011 属正常,
        此时按「单已不存在」处理(INFO),不再告警;
        盈亏记录失败时保留本地记录,待下次对齐重试,避免平仓数据丢失。
        """
        symbol = str(position.get("symbol", ""))
        if not symbol:
            return
        self.logger.warning("发现幽灵持仓 symbol=%s 交易所无此仓位,开始清理", symbol)
        take_profit_id = position.get("takeProfitOrderId")
        stop_algo_id = position.get("stopAlgoId")
        # 撤单结果同时用于推断平仓原因:订单已被消费(-2011)说明曾被触发/自动取消
        tp_missing = False
        stop_missing = False
        if take_profit_id:
            try:
                self._client.cancel_order(symbol, int(take_profit_id))
                self.logger.info("幽灵持仓清理:已取消止盈挂单 symbol=%s orderId=%s", symbol, take_profit_id)
            except Exception as exc:
                tp_missing = isinstance(exc, BinanceFuturesError) and exc.code == -2011
                self._log_cancel_outcome("止盈", symbol, take_profit_id, exc)
        if stop_algo_id:
            try:
                self._client.cancel_algo_order(symbol, int(stop_algo_id))
                self.logger.info("幽灵持仓清理:已取消止损单 symbol=%s algoId=%s", symbol, stop_algo_id)
            except Exception as exc:
                stop_missing = isinstance(exc, BinanceFuturesError) and exc.code == -2011
                self._log_cancel_outcome("止损", symbol, stop_algo_id, exc)
        try:
            self._record_position_pnl(position, tp_missing, stop_missing)
        except Exception as exc:
            self.logger.warning("平仓盈亏记录失败,保留本地记录待下轮重试 symbol=%s: %s", symbol, exc)
            return
        remove_position(self.config.environment, symbol, str(position.get("positionSide") or "BOTH"))
        self.logger.info("幽灵持仓已清理 symbol=%s", symbol)

    def _record_position_pnl(self, position: dict[str, Any], tp_missing: bool, stop_missing: bool) -> None:
        """平仓盈亏统计:按开仓信息查询交易所已实现盈亏并记录到本地盈亏目录。

        任何形式的平仓(止损/止盈/手动)都会让仓位从交易所消失,经对齐发现后
        在此记录;仅统计程序开出的单(本地台账记录的开仓信息)。
        """
        symbol = str(position.get("symbol") or "")
        open_time = position.get("openTime")
        if not symbol:
            return
        realized, commission = self._query_realized_pnl(symbol, open_time)
        quantity = position.get("quantity")
        if quantity is None:
            quantity = self._ledger_quantity(symbol, open_time)
        record = {
            "symbol": symbol,
            "signalType": position.get("signalType"),
            "direction": position.get("direction"),
            "entryPrice": position.get("entryPrice"),
            "quantity": quantity,
            "leverage": position.get("leverage"),
            "openTime": open_time,
            "closeTime": int(time.time() * 1000),
            "realizedPnlUsdt": realized,
            "commissionUsdt": commission,
            "netPnlUsdt": realized + commission,
            "closeReason": self._detect_close_reason(position, tp_missing, stop_missing),
            "recordedAt": datetime.now(timezone.utc).isoformat(),
        }
        append_pnl_record(self.config.environment, record)
        self._notify_close(record)
        self.logger.info(
            "平仓盈亏已记录 symbol=%s net=%s 原因=%s",
            symbol, record["netPnlUsdt"], record["closeReason"],
        )

    def _notify_close(self, record: dict[str, Any]) -> None:
        """平仓盈亏记录后推送 PushDeer 通知(异步,失败不影响监控)。"""
        try:
            from pushdeer_notifier import push_message_async

            symbol = str(record.get("symbol") or "?")
            direction = record.get("direction") or ""
            net = float(record.get("netPnlUsdt") or 0.0)
            commission = float(record.get("commissionUsdt") or 0.0)
            reason = record.get("closeReason") or ""
            lines = [
                f"**{symbol} 已平仓** 方向:{direction}",
                f"- 净盈亏: {net:+.2f} USDT",
                f"- 手续费: {commission:+.2f} USDT",
                f"- 平仓原因: {reason}",
            ]
            push_message_async("Binance Futures Bot 平仓", "\n".join(lines))
        except Exception:
            self.logger.exception("平仓通知推送失败")

    def _query_realized_pnl(self, symbol: str, open_time: Any) -> tuple[float, float]:
        """查询交易对自开仓以来的已实现盈亏与手续费(/fapi/v1/income)。

        返回 (realized, commission);income 记录按实际成交结算,为权威盈亏来源。
        """
        try:
            start = int(open_time)
        except (TypeError, ValueError):
            start = int(time.time() * 1000) - 7 * 86400_000
        end = int(time.time() * 1000)
        realized = 0.0
        commission = 0.0
        for income_type in ("REALIZED_PNL", "COMMISSION"):
            for item in self._client.get_income(symbol, start, end, income_type):
                try:
                    value = float(item.get("income", 0.0))
                except (TypeError, ValueError):
                    continue
                if income_type == "REALIZED_PNL":
                    realized += value
                else:
                    commission += value
        return realized, commission

    def _detect_close_reason(self, position: dict[str, Any], tp_missing: bool, stop_missing: bool) -> str:
        """推断平仓原因:以挂单状态为准,避免手动平仓被误判为止损/止盈。

        判定顺序:
        1. 止盈限价单被消费且状态 FILLED → 止盈;
        2. 止损条件单被消费且状态 TRIGGERED(已触发)→ 止损;
        3. 两者均未触发(含手动平仓联动取消挂单)→ 手动平仓。
        止损触发时交易所会联动取消止盈挂单,仅凭「单已不存在」无法区分
        止损与手动平仓,故止损单状态查询为权威依据。
        """
        symbol = str(position.get("symbol") or "")
        tp_order_id = position.get("takeProfitOrderId")
        if tp_order_id is not None and tp_missing:
            try:
                status = self._client.get_order_status(symbol, int(tp_order_id)).get("status")
                if status == "FILLED":
                    return "止盈"
            except Exception as exc:
                self.logger.warning("止盈单状态查询失败 symbol=%s: %s", symbol, exc)
        stop_algo_id = position.get("stopAlgoId")
        if stop_algo_id is not None and stop_missing:
            try:
                status = self._client.get_algo_order_status(symbol, int(stop_algo_id)).get("algoStatus")
                if status == "TRIGGERED":
                    return "止损"
            except BinanceFuturesError as exc:
                # -2011 = 条件单已不存在(未触发被取消/手动平仓联动取消)→ 非止损
                if exc.code != -2011:
                    self.logger.warning("止损单状态查询失败 symbol=%s: %s", symbol, exc)
            except Exception as exc:
                self.logger.warning("止损单状态查询失败 symbol=%s: %s", symbol, exc)
        return "手动平仓"

    def _ledger_quantity(self, symbol: str, open_time: Any) -> float | None:
        """从订单台账回查开仓数量(旧持仓记录未落库 quantity 时的兜底)。"""
        path = self.config.runtime_directory / "order_ledger.json"
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        fingerprint = f"{symbol}|{open_time}"
        for item in payload.get("ledger", []) if isinstance(payload, dict) else []:
            if not isinstance(item, dict) or item.get("fingerprint") != fingerprint:
                continue
            if item.get("status") != "filled":
                continue
            try:
                return float(item.get("quantity"))
            except (TypeError, ValueError):
                return None
        return None

    def _refresh_mark_symbols(self) -> None:
        """刷新全市场流中需要入队处理的交易对集合:本地台账中的受监管仓位(移动止损驱动)。

        全市场标记价格流无需按交易对订阅重建连接,此处仅做内存过滤,
        避免为无关交易对重复触发本地持仓文件的读取。
        """
        self._mark_symbols = {
            item["symbol"]
            for item in load_positions(self.config.environment)
            if item.get("signalType") in TRAILED_SIGNALS and item.get("status", "active") == "active"
        }
        if self._mark_stream is not None:
            self._mark_stream.set_symbols(self._mark_symbols)

    # ---- 移动止损 ----

    def _check_trailing(self, symbol: str, mark: float) -> None:
        """受监管仓位 R 阶移动止损(多空镜像)。

        多仓(上破):mark 累计上移 1R 抬升一档,止损 = mark − R − ATR;
        空仓(下破):mark 累计下移 1R 抬升一档,止损 = mark + |R| + ATR。
        """
        positions = [
            item for item in load_positions(self.config.environment)
            if item.get("symbol") == symbol
            and item.get("signalType") in TRAILED_SIGNALS
            and item.get("status", "active") == "active"
        ]
        for position in positions:
            self._check_trailing_position(position, symbol, mark)

    def _check_trailing_position(self, position: dict[str, Any], symbol: str, mark: float) -> None:
        """对单条已确认归属的突破仓位执行 R 阶止损检查。"""
        actual_amount = self._exchange_positions.get(
            position_key(symbol, position.get("positionSide"))
        )
        if actual_amount is None or abs(actual_amount) == 0:
            # 账户事件已表明仓位不存在/状态未知时，不得再撤换保护单；等待完整快照对账。
            return
        is_long = str(position.get("direction", "")) == "多"
        mark_decimal = Decimal(str(mark))
        entry = _decimal_of(position.get("entryPrice"))
        r = _decimal_of(position.get("r"))
        current_stop = _decimal_of(position.get("currentStop"))
        # 多仓 R>0、空仓 R<0(entry−stop);符号与方向不符视为数据异常
        if entry is None or r is None or r == 0 or current_stop is None:
            return
        if (is_long and r <= 0) or (not is_long and r >= 0):
            self.logger.warning("移动止损:R 与方向不符 symbol=%s direction=%s r=%s", symbol, position.get("direction"), r)
            return
        level = int(position.get("trailLevel", 0))
        # 跨档循环推进(一次跳涨/跳跌可能越过多个档位)
        new_level = level
        while (mark_decimal >= entry + (new_level + 1) * r) if is_long else (
            mark_decimal <= entry + (new_level + 1) * r
        ):
            new_level += 1
        if new_level == level:
            return
        atr = self._atr.get(symbol)
        if atr is None:
            self.logger.warning("移动止损:ATR 缺失 symbol=%s,暂不移动", symbol)
            return
        # 止损缓冲 1.5×ATR(1h):多仓 mark−R−1.5ATR、空仓 mark−R+1.5ATR,
        # 容纳小时级正常波动与锁利回吐之间折中
        new_stop = self._round_to_tick(
            mark_decimal - r - TRAIL_STOP_ATR_MULTIPLIER * atr
            if is_long
            else mark_decimal - r + TRAIL_STOP_ATR_MULTIPLIER * atr,
            symbol,
        )
        if new_stop is None:
            self.logger.warning("移动止损:止损价超出价格区间 symbol=%s,暂不移动", symbol)
            return
        if (is_long and new_stop <= current_stop) or (not is_long and new_stop >= current_stop):
            self.logger.info("移动止损:新止损未朝有利方向移动,跳过 symbol=%s new=%s cur=%s", symbol, new_stop, current_stop)
            return
        self._move_stop(position, symbol, new_stop, new_level, mark_decimal)

    def _cancel_stop_with_retry(self, symbol: str, algo_id: int) -> Exception | None:
        """取消旧止损单,有限次重试;-2011(单已不存在,已被触发或取消)视为成功。

        返回最后一次异常(成功返回 None)。
        """
        last_error: Exception | None = None
        for attempt in range(1, MOVE_STOP_MAX_ATTEMPTS + 1):
            try:
                self._client.cancel_algo_order(symbol, algo_id)
                return None
            except BinanceFuturesError as exc:
                if exc.code == -2011:
                    return None  # 旧单已不存在(已触发/已取消),无占位,视为成功
                last_error = exc
            except Exception as exc:
                last_error = exc
            self.logger.warning(
                "移动止损:旧止损单取消失败(第 %s/%s 次),重试 symbol=%s: %s",
                attempt, MOVE_STOP_MAX_ATTEMPTS, symbol, last_error,
            )
            if attempt < MOVE_STOP_MAX_ATTEMPTS:
                time.sleep(MOVE_STOP_RETRY_DELAY)
        return last_error

    def _place_stop_with_retry(
        self, symbol: str, stop_side: str, stop_price: Decimal, position_side: str | None
    ) -> dict[str, Any] | None:
        """挂全平止损单,有限次重试;全部失败返回 None。

        止损价统一转为 Decimal 文本(兼容台账中 float/字符串形态),
        避免 format 类型错误被误判为请求失败。
        """
        price_decimal = stop_price if isinstance(stop_price, Decimal) else _decimal_of(stop_price)
        if price_decimal is None:
            self.logger.error("移动止损:止损价无效 symbol=%s stop=%s", symbol, stop_price)
            return None
        price_text = format(price_decimal, "f")
        for attempt in range(1, MOVE_STOP_MAX_ATTEMPTS + 1):
            try:
                return self._client.place_stop_market_close_position(
                    symbol, stop_side, price_text, position_side
                )
            except Exception as exc:
                self.logger.warning(
                    "移动止损:止损单挂单失败(第 %s/%s 次) symbol=%s: %s",
                    attempt, MOVE_STOP_MAX_ATTEMPTS, symbol, exc,
                )
                if attempt < MOVE_STOP_MAX_ATTEMPTS:
                    time.sleep(MOVE_STOP_RETRY_DELAY)
        return None

    def _move_stop(
        self, position: dict[str, Any], symbol: str, new_stop: Decimal, new_level: int, mark: Decimal
    ) -> None:
        """cancel+place 替换全平止损单(官方明确未触发条件单不支持修改)。

        注意 -4130:同一方向仅允许存在一个 closePosition 条件单,
        因此必须先取消旧单再挂新单;各步骤先有限次重试(-2011 视为取消成功),
        完全失败才走对应处理:取消失败保留原止损、挂新单失败用原止损价恢复保护。
        """
        stored_side = str(position.get("positionSide") or "BOTH")
        position_side = stored_side if stored_side in {"LONG", "SHORT"} else None
        stop_side = "SELL" if position.get("direction") == "多" else "BUY"
        old_algo_id = position.get("stopAlgoId")
        if old_algo_id:
            cancel_error = self._cancel_stop_with_retry(symbol, int(old_algo_id))
            if cancel_error is not None:
                self.logger.warning(
                    "移动止损:旧止损单取消重试均失败,保留原止损 symbol=%s algoId=%s: %s",
                    symbol, old_algo_id, cancel_error,
                )
                return
        stop_order = self._place_stop_with_retry(symbol, stop_side, new_stop, position_side)
        if stop_order is None:
            # 旧单已撤、新单重试均失败:用原止损价恢复保护,并回写新单 ID 保持本地记录一致
            restore_order = self._place_stop_with_retry(
                symbol, stop_side, position["currentStop"], position_side
            )
            if restore_order is None:
                self.logger.error("移动止损:恢复原止损失败,仓位无保护! symbol=%s", symbol)
                return
            update_position(self.config.environment, symbol, str(position.get("positionSide") or "BOTH"), stopAlgoId=restore_order.get("algoId"))
            self.logger.warning("移动止损:已用原止损价恢复保护 symbol=%s stop=%s", symbol, position["currentStop"])
            return
        update_position(
            self.config.environment,
            symbol,
            str(position.get("positionSide") or "BOTH"),
            currentStop=float(new_stop),
            stopAlgoId=stop_order.get("algoId"),
            trailLevel=new_level,
            lastTrailAt=datetime.now(timezone.utc).isoformat(),
        )
        self.logger.info(
            "移动止损已更新 symbol=%s 档位=%s 止损=%s mark=%s 新algoId=%s",
            symbol, new_level, new_stop, mark, stop_order.get("algoId"),
        )

    def _find_position(self, symbol: str, position_side: str = "BOTH") -> dict[str, Any] | None:
        for item in load_positions(self.config.environment):
            if position_key(item.get("symbol"), item.get("positionSide")) == position_key(symbol, position_side):
                return item
        return None

    def _round_to_tick(self, value: Decimal, symbol: str) -> Decimal | None:
        """止损价向下取整对齐 PRICE_FILTER tickSize 并校验价格区间。"""
        tick = self._load_tick_size(symbol)
        if tick is None or tick <= 0:
            return value
        return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick

    def _load_tick_size(self, symbol: str) -> Decimal | None:
        """从合约池读取交易对 PRICE_FILTER tickSize。"""
        path = self.config.runtime_directory / "contract_pool.json"
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        for item in payload.get("symbols", []):
            if not isinstance(item, dict) or item.get("symbol") != symbol:
                continue
            for rule in item.get("filters", []):
                if isinstance(rule, dict) and rule.get("filterType") == "PRICE_FILTER":
                    return _decimal_of(rule.get("tickSize"))
        return None

    # ---- 盈亏自动全平 ----

    def _current_pnl_pct(self) -> float | None:
        """账户未实现盈亏占钱包余额的百分比(与 GUI 展示同一口径)。

        仅统计程序开出的仓位(本地台账记录的交易对):手动开的大额仓位
        不计入触发判断。盈亏直接取交易所官方 unrealizedProfit(positionRisk
        快照每 5 分钟 + WS API 每 30 秒校准 + ACCOUNT_UPDATE 事件,均为官方
        权威值),不手工用 (标记价−开仓价)×数量 计算——避免对冲模式多空
        记录覆盖开仓价、快照数据异常导致浮盈虚高误触发。
        """
        # 超过一个全量快照周期仍未成功刷新时，禁止使用旧数据触发任何全平动作。
        if time.monotonic() - self._last_successful_snapshot > FULL_SNAPSHOT_SECONDS:
            self.logger.warning("账户快照已过期,跳过自动全平判断")
            return None
        usdt = self._account_balances.get("USDT", {})
        wallet = float(usdt.get("balance", 0.0))
        if wallet <= 0:
            return None
        ledger_keys = {
            position_key(position.get("symbol"), position.get("positionSide"))
            for position in load_positions(self.config.environment)
            if position.get("status", "active") == "active"
        }
        unrealized = 0.0
        for key, info in self._account_positions.items():
            if key not in ledger_keys:
                continue  # 非程序仓(手动开仓)不计入触发判断
            unrealized += float(info.get("unrealizedProfit", 0.0))
        return unrealized / wallet * 100

    def _check_auto_close(self) -> None:
        """账户盈亏达到阈值时自动全平全部仓位(每秒检查,无冷却)。

        全平后未实现盈亏归零自然回到阈值内;后续新仓再次达标可再触发。
        """
        if self._auto_closing:
            return
        pct = self._current_pnl_pct()
        if pct is None:
            return
        if pct >= self._auto_close_profit_pct or pct <= -self._auto_close_loss_pct:
            # 触发明细:记录各程序仓的官方未实现盈亏,便于核对触发是否合理
            ledger_keys = {
                position_key(position.get("symbol"), position.get("positionSide"))
                for position in load_positions(self.config.environment)
                if position.get("status", "active") == "active"
            }
            detail = ", ".join(
                f"{info.get('symbol')}/{info.get('positionSide')}:amt={info.get('positionAmt')} upnl={info.get('unrealizedProfit')}"
                for key, info in sorted(self._account_positions.items())
                if key in ledger_keys
            )
            self.logger.warning(
                "盈亏自动全平触发 pnl=%+.2f%% 盈利阈值=%s%% 亏损阈值=%s%% 明细: %s",
                pct, self._auto_close_profit_pct, self._auto_close_loss_pct, detail,
            )
            self._auto_close_all(pct)

    def _auto_close_all(self, pct: float) -> None:
        """市价全平**程序开出的仓位**(本地台账记录的交易对),手动仓不动。

        逐仓反向平仓,数量用交易所原始字符串保精度;平仓后仓位从交易所消失,
        本地记录经对齐(幽灵清理)自动记录盈亏并清理;完成后立即全量快照加速对齐。
        触发判断与执行范围一致:仅程序仓参与自动全平。
        """
        self._auto_closing = True
        try:
            dual = self._client.is_dual_side_position()
            managed = {
                position_key(position.get("symbol"), position.get("positionSide")): position
                for position in load_positions(self.config.environment)
                if position.get("status", "active") == "active"
            }
            closed = 0
            for item in self._client.get_positions_detail():
                symbol = item.get("symbol")
                side_key = position_key(symbol, item.get("positionSide"))
                amount_decimal = _decimal_of(item.get("positionAmt"))
                if (
                    not isinstance(symbol, str)
                    or side_key not in managed
                    or amount_decimal is None
                    or amount_decimal == 0
                ):
                    continue  # 非程序仓(手动开仓)不参与自动全平
                side = "SELL" if amount_decimal > 0 else "BUY"
                quantity = format(abs(amount_decimal), "f")
                # 对冲模式带与持仓方向一致的 positionSide;单向模式带 reduceOnly 防反向开仓
                if dual:
                    self._client.place_market_order(symbol, side, quantity, str(item.get("positionSide") or "BOTH"))
                else:
                    self._client.place_market_order(symbol, side, quantity, None, reduce_only=True)
                self.logger.info("盈亏自动全平:已平 symbol=%s %s %s", symbol, side, quantity)
                closed += 1
            self.logger.warning("盈亏自动全平完成 pnl=%+.2f%% 平仓=%s 个", pct, closed)
        except Exception as exc:
            self.logger.error("盈亏自动全平执行失败,剩余仓位继续监控: %s", exc)
        finally:
            self._auto_closing = False
        # 平仓后立即全量快照对齐:本地记录转幽灵清理,平仓盈亏自动落库
        self._full_snapshot()

    # ---- listenKey 续期与 ATR ----

    def _refresh_pnl_from_ws_api(self) -> None:
        """周期通过 WebSocket API v2/account.status 刷新官方未实现盈亏。

        价格变动不触发 ACCOUNT_UPDATE 事件,官方权威盈亏需主动查询;
        查询结果更新账户快照(GUI 依据 updatedAt 自动刷新)。
        """
        self._last_pnl_refresh = time.monotonic()
        if self._ws_api is None:
            return
        try:
            result = self._ws_api.account_status()
        except Exception as exc:
            self.logger.warning("WS API 账户状态查询失败: %s", exc)
            return
        total = result.get("totalUnrealizedProfit")
        if total is not None:
            usdt = self._account_balances.setdefault("USDT", {})
            usdt["unrealizedProfit"] = _safe_float(total)
            wallet = _safe_float(result.get("totalWalletBalance"))
            if wallet > 0:
                # 修复钱包余额:ACCOUNT_UPDATE 的 B 数组 wb 在测试网实测为 0,以官方 totalWalletBalance 为准
                usdt["balance"] = wallet
            available = _safe_float(result.get("availableBalance"))
            if available > 0:
                usdt["availableBalance"] = available
        positions = result.get("positions")
        if isinstance(positions, list):
            for item in positions:
                if not isinstance(item, dict):
                    continue
                symbol = item.get("symbol")
                if isinstance(symbol, str) and symbol in self._account_positions:
                    self._account_positions[symbol]["unrealizedProfit"] = _safe_float(
                        item.get("unrealizedProfit")
                    )
        self._snapshot_updated_at = time.monotonic()

    def _renew_listen_key(self) -> None:
        """每 30 分钟续期 listenKey(官方 60 分钟 TTL)。"""
        self._last_renew = time.monotonic()
        if not self._listen_key:
            return
        try:
            self._client.renew_listen_key(self._listen_key)
            self.logger.info("listenKey 已续期")
        except Exception as exc:
            self.logger.warning("listenKey 续期失败: %s", exc)

    def _refresh_atr(self) -> None:
        """刷新受移动止损监管仓位的 1h ATR(14),基于最新已收盘 1h K 线。"""
        self._last_atr = time.monotonic()
        symbols = [
            item["symbol"]
            for item in load_positions(self.config.environment)
            if item.get("signalType") in TRAILED_SIGNALS
        ]
        if not symbols:
            return
        klines, failures = fetch_klines_with_failures(
            symbols, "1h", ATR_PERIOD + 1, self.config.use_testnet, self.config.http_timeout_seconds
        )
        for symbol, raw_klines in klines.items():
            candles = parse_candles(raw_klines)
            if len(candles) < ATR_PERIOD + 1:
                continue
            true_ranges = [
                max(candle[2] - candle[3], abs(candle[2] - previous[4]), abs(candle[3] - previous[4]))
                for candle, previous in zip(candles[1:], candles[:-1])
            ]
            atr = sum(true_ranges[-ATR_PERIOD:]) / ATR_PERIOD
            self._atr[symbol] = Decimal(str(atr))
            self.logger.info("ATR 已刷新 symbol=%s atr14_1h=%s", symbol, atr)
        if failures:
            self.logger.warning("ATR 刷新部分失败: %s", failures)


def _env(name: str) -> str:
    """读取密钥:优先环境变量,缺失时回退旧程序配置文件。"""
    return get_secret(name)


def _decimal_of(value: Any) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except Exception:
        return None
    return number if number.is_finite() else None


def _safe_float(value: Any) -> float:
    """将接口数值安全转换为 float,失败返回 0.0。"""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0
