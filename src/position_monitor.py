"""持仓监控模块:本地持仓与交易所实际持仓高频对齐,幽灵清理,突破仓位 R 阶移动止损。

监控范围(与用户确认的规则):
- 仅监控「本地有 ∧ 交易所也有」的仓位;
- 本地有、交易所无 → 幽灵持仓:清理本地记录,并同步取消该 symbol 残留的止盈/止损挂单;
- 其它情况一律忽略、不监控;
- 高抛低吸仓位:仅存在性监控,不动止盈止损挂单;
- 上破/下破仓位(多空镜像):按 R 阶移动止损——R = 开仓价 − 初始止损(箱体中轨±缓冲),
  多仓标记价格累计上移 1R 止损上移至「mark − R − 1.5×箱体周期 ATR(14)」,
  空仓标记价格累计下移 1R 止损下移至「mark + |R| + 1.5×箱体周期 ATR(14)」,
  经 /fapi/v1/algoOrder 以 cancel+place 方式更新(官方明确未触发条件单不支持修改)。

对齐数据源:Private 用户数据流(ACCOUNT_UPDATE 仅触发)+ REST 权威快照确认；
账户流断开时每 10 秒 REST 兜底，正常时保留每 5 分钟完整校验。
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
import uuid
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
from position_lifecycle import add_event, create_lifecycle, read_lifecycle, remove_lifecycle
from scheduler import SchedulerConfig
from secret_utils import get_secret
from websocket_streams import MarkPriceStream, UserDataStream

# 程序当前持仓的可变列表；与只追加的开仓台账分离。
PROGRAM_POSITIONS_FILENAME = "program_positions.json"
LEGACY_POSITION_STATE_FILENAME = "position_state.json"
BREAKOUT_SIGNAL = "上破箱体上沿"
BREAKDOWN_SIGNAL = "下破箱体下沿"
# 受移动止损监管的信号(上破多/下破空,多空镜像)
TRAILED_SIGNALS = frozenset({BREAKOUT_SIGNAL, BREAKDOWN_SIGNAL})
FULL_SNAPSHOT_SECONDS = 300
LISTEN_KEY_RENEW_SECONDS = 1800
STREAM_FALLBACK_SECONDS = 10
MARK_STALE_SECONDS = 5
ACCOUNT_EVENT_DEBOUNCE_SECONDS = 0.25
ATR_PERIOD = 14
# 移动止损缓冲:1.5×箱体周期 ATR(14)，在容纳结构周期波动与锁利回吐之间折中
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
    return runtime_directory(environment) / PROGRAM_POSITIONS_FILENAME


def position_key(symbol: Any, position_side: Any = "BOTH") -> str:
    """返回程序仓位的稳定键。

    Binance 在对冲模式下会为同一 symbol 返回 LONG/SHORT 两条独立记录，
    因此禁止再用 symbol 作为本地台账与对账键。
    """
    return f"{str(symbol or '')}|{str(position_side or 'BOTH')}"


def _read_positions_unlocked(path: Path) -> list[dict[str, Any]]:
    """无锁读取程序持仓列表；首次升级时兼容旧 position_state.json。"""
    if not path.is_file():
        legacy_path = path.with_name(LEGACY_POSITION_STATE_FILENAME)
        if legacy_path.is_file():
            path = legacy_path
        else:
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


class _TrailingPositionTask(threading.Thread):
    """单个突破程序仓位的本地移动止损子任务。

    子任务绝不自行建立行情 WebSocket；它只消费 PositionMonitor 共享的
    全市场标记价格缓存。ATR 在该仓位所属箱体周期每次收盘后独立刷新。
    """

    def __init__(self, monitor: "PositionMonitor", key: str) -> None:
        super().__init__(name=f"trail-{key}", daemon=True)
        self._monitor = monitor
        self._key = key
        self._stop_task = threading.Event()
        self._wake = threading.Event()
        self._next_atr_refresh = 0.0

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop_task.set()
        self._wake.set()
        self.join(timeout=5)

    def run(self) -> None:
        self._refresh_atr()
        while not self._stop_task.is_set() and not self._monitor._stop.is_set():
            timeout = max(0.5, self._next_atr_refresh - time.monotonic())
            self._wake.wait(timeout)
            self._wake.clear()
            if self._stop_task.is_set() or self._monitor._stop.is_set():
                return
            if time.monotonic() >= self._next_atr_refresh:
                self._refresh_atr()
            self._monitor._check_trailing_key(self._key)

    def _refresh_atr(self) -> None:
        """在最新已收盘箱体 K 线基础上刷新 ATR，并安排下次收盘后执行。"""
        position = self._monitor._find_position_by_key(self._key)
        if position is None:
            return
        interval = self._monitor.config.structure_interval
        symbol = str(position.get("symbol") or "")
        if symbol:
            try:
                klines, failures = fetch_klines_with_failures(
                    [symbol], interval, ATR_PERIOD + 1,
                    self._monitor.config.use_testnet,
                    self._monitor.config.http_timeout_seconds,
                )
                raw_klines = klines.get(symbol, [])
                candles = parse_candles(raw_klines)
                if len(candles) >= ATR_PERIOD + 1:
                    true_ranges = [
                        max(candle[2] - candle[3], abs(candle[2] - previous[4]), abs(candle[3] - previous[4]))
                        for candle, previous in zip(candles[1:], candles[:-1])
                    ]
                    self._monitor._atr[self._key] = Decimal(str(sum(true_ranges[-ATR_PERIOD:]) / ATR_PERIOD))
                    self._monitor.logger.info("ATR 已刷新 key=%s interval=%s", self._key, interval)
                elif failures:
                    self._monitor.logger.warning("ATR 刷新失败 key=%s: %s", self._key, failures)
            except Exception as exc:
                self._monitor.logger.warning("ATR 刷新异常 key=%s: %s", self._key, exc)
        self._next_atr_refresh = _next_interval_close_monotonic(interval)


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
        self._state_lock = threading.RLock()
        self._listen_key = ""
        self._user_stream: UserDataStream | None = None
        self._mark_stream: MarkPriceStream | None = None
        self._thread: threading.Thread | None = None
        # 交易所持仓状态:{symbol: positionAmt}(启动快照 + ACCOUNT_UPDATE 增量 + 定时快照兜底)
        self._exchange_positions: dict[str, float] = {}
        # 交易所持仓数量的 Decimal 加总:{symbol: Decimal}(原始字符串保真,供 GUI 平仓下单使用)
        self._exchange_position_strs: dict[str, Decimal] = {}
        # 最新标记价格及其接收时刻；所有突破仓位共享该缓存。
        self._marks: dict[str, float] = {}
        self._mark_received_at: dict[str, float] = {}
        # 每个突破程序仓位独立维护箱体周期 ATR(14)。
        self._atr: dict[str, Decimal] = {}
        self._trail_tasks: dict[str, "_TrailingPositionTask"] = {}
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
        self._last_account_fallback = 0.0
        self._last_mark_fallback = 0.0
        self._account_reconcile_due = 0.0
        self._user_stream_connected = False
        self._mark_stream_connected = False
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
            self.config.use_testnet, self._listen_key, proxy_url,
            self._on_account_event, self._on_user_stream_connection,
        )
        # 全市场标记价格流:一次订阅全部交易对,无需按持仓维护订阅列表
        self._mark_stream = MarkPriceStream(
            self.config.use_testnet, proxy_url, self._on_mark_event, self._on_mark_stream_connection
        )
        self._user_stream.start()
        self._mark_stream.start()
        # 初始全量对齐(重连兜底与增量事件之前先建立基线)
        self._full_snapshot()
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
        self._stop_trailing_tasks()
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
                if self._account_reconcile_due and now >= self._account_reconcile_due:
                    self._account_reconcile_due = 0.0
                    self._full_snapshot()
                if not self._user_stream_connected and now - self._last_account_fallback >= STREAM_FALLBACK_SECONDS:
                    self._last_account_fallback = now
                    self.logger.warning("账户信息流不可用，执行 10 秒 REST 仓位兜底")
                    self._full_snapshot()
                    if self._user_stream is None:
                        self._recreate_user_stream()
                if self._mark_fallback_needed(now) and now - self._last_mark_fallback >= STREAM_FALLBACK_SECONDS:
                    self._last_mark_fallback = now
                    self._refresh_marks_from_rest()
                if self._auto_close_enabled:
                    self._check_auto_close()
            except BaseException:
                # 定时任务任何未预期异常不允许监控线程静默退出,记录后继续
                self.logger.exception("监控定时任务异常")

    def _handle_event(self, event: tuple[str, Any]) -> None:
        kind, payload = event
        if kind == "account":
            # 私有流只负责发现变动；状态写入必须等待 REST 权威快照确认。
            self._account_reconcile_due = time.monotonic() + ACCOUNT_EVENT_DEBOUNCE_SECONDS
        elif kind == "order_event":
            self._capture_lifecycle_event(payload)
            self._queue.put(("snapshot", None))
        elif kind == "mark":
            key = str(payload)
            task = self._trail_tasks.get(key)
            if task is not None:
                task.wake()
        elif kind == "snapshot":
            # 外部(如 GUI 平仓完成)请求立即全量刷新快照,在监控线程内执行避免竞态
            self._full_snapshot()
        elif kind == "listen_key_expired":
            self._recreate_user_stream()

    def request_full_snapshot(self) -> None:
        """请求监控线程立即执行一次全量快照(positionRisk),供平仓等操作后快速同步。"""
        self._queue.put(("snapshot", None))

    # ---- WebSocket 回调(接收线程,仅入队) ----

    def _on_account_event(self, payload: dict[str, Any]) -> None:
        event_type = payload.get("e")
        if event_type == "ACCOUNT_UPDATE":
            self._queue.put(("account", payload))
        elif event_type in {"ORDER_TRADE_UPDATE", "ALGO_UPDATE"}:
            # 先持久化程序订单/条件单事实，再以 REST 确认仓位真实变动。
            self._queue.put(("order_event", payload))
        elif event_type == "listenKeyExpired":
            self._queue.put(("listen_key_expired", None))

    def _on_user_stream_connection(self, connected: bool) -> None:
        self._user_stream_connected = connected
        if connected:
            # 重连成功后先全量 REST 补齐断线窗口，再停止兜底轮询。
            self._queue.put(("snapshot", None))

    def _on_mark_stream_connection(self, connected: bool) -> None:
        self._mark_stream_connected = connected

    def _capture_lifecycle_event(self, payload: dict[str, Any]) -> None:
        """将私有流中的程序平仓事实写入生命周期账本。

        只有仍在 program_positions.json 中的 key 才会被记录，因此人工仓、
        外部仓位以及未登记仓位不会进入盈亏统计。
        """
        event_type = str(payload.get("e") or "")
        order = payload.get("o") or payload.get("ao")
        if not isinstance(order, dict):
            return
        symbol = order.get("s") or order.get("symbol")
        position_side = str(order.get("ps") or order.get("positionSide") or "BOTH")
        if not isinstance(symbol, str):
            return
        key = position_key(symbol, position_side)
        position = self._find_position_by_key(key)
        if position is None:
            return
        if event_type == "ALGO_UPDATE":
            algo_id = order.get("i") or order.get("algoId")
            status = str(order.get("X") or order.get("algoStatus") or "")
            if str(algo_id) != str(position.get("stopAlgoId")) or status != "TRIGGERED":
                return
            add_event(self.config.environment, key, {
                "eventId": f"algo:{algo_id}:{status}:{payload.get('E')}",
                "eventType": event_type, "eventTime": payload.get("E"),
                "algoId": algo_id, "closeReason": "程序止损",
                "evidence": "程序登记的止损 Algo Order 已触发",
            })
            return
        if event_type != "ORDER_TRADE_UPDATE":
            return
        execution_type = str(order.get("x") or "")
        status = str(order.get("X") or "")
        if execution_type != "TRADE" and status != "FILLED":
            return
        order_side = str(order.get("S") or order.get("side") or "")
        is_long = str(position.get("direction") or "") == "多"
        closing_side = "SELL" if is_long else "BUY"
        if order_side != closing_side:
            return
        order_id = order.get("i") or order.get("orderId")
        client_order_id = order.get("c") or order.get("clientOrderId")
        reason = "外部平仓"
        evidence = "成交订单不属于已登记的程序止盈、止损或自动全平订单"
        if str(order_id) == str(position.get("takeProfitOrderId")):
            reason, evidence = "程序止盈", "成交订单与程序登记的止盈订单 ID 一致"
        elif str(order_id) == str(position.get("autoCloseOrderId")) or (
            client_order_id and client_order_id == position.get("autoCloseClientOrderId")
        ):
            reason, evidence = "程序盈亏全平", "成交订单与程序生成的自动全平订单一致"
        add_event(self.config.environment, key, {
            "eventId": f"order:{order_id}:{order.get('t')}:{payload.get('E')}",
            "eventType": event_type, "eventTime": payload.get("E"),
            "orderId": order_id, "clientOrderId": client_order_id,
            "tradeId": order.get("t"), "realizedPnl": order.get("rp"),
            "commission": order.get("n"), "commissionAsset": order.get("N"),
            "closeReason": reason, "evidence": evidence,
        })

    def _recreate_user_stream(self) -> None:
        """listenKey 失效后重建私有流；旧 key 不可无限重连使用。"""
        self.logger.warning("listenKey 已失效，重建账户信息流")
        old_stream = self._user_stream
        if old_stream is not None:
            old_stream.stop()
        self._user_stream = None
        try:
            self._listen_key = self._client.create_listen_key()
            proxy_url = self.config.proxy_url if self.config.proxy_enabled else None
            self._user_stream = UserDataStream(
                self.config.use_testnet, self._listen_key, proxy_url,
                self._on_account_event, self._on_user_stream_connection,
            )
            self._user_stream.start()
            self._user_stream_connected = False
            self._full_snapshot()
        except Exception as exc:
            # 连接线程之外的创建失败由 10 秒 REST 兜底覆盖；续期循环会继续尝试。
            self.logger.warning("重建账户信息流失败，继续 REST 兜底: %s", exc)

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
            received_at = time.monotonic()
            with self._state_lock:
                self._marks[symbol] = mark
                self._mark_received_at[symbol] = received_at
                for position in self._account_positions.values():
                    if position.get("symbol") == symbol:
                        position["markPrice"] = mark
                self._snapshot_updated_at = received_at
            # 一个 symbol 可有对冲双向仓位；只唤醒对应的本地子任务。
            for key, task in tuple(self._trail_tasks.items()):
                if key.split("|", 1)[0] == symbol:
                    self._queue.put(("mark", key))

    # ---- 对齐 ----

    def _full_snapshot(self) -> None:
        """positionRisk 全量快照覆盖持仓状态,并触发对齐(断线/丢事件兜底)。"""
        self._last_snapshot = time.monotonic()
        last_error: Exception | None = None
        for attempt in range(1, SNAPSHOT_MAX_ATTEMPTS + 1):
            try:
                # 先完整取得数据到候选快照，任一请求失败均不污染正在使用的状态。
                raw_positions = self._client.get_positions_detail()
                overview = self._client.get_account_overview()
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
                    "leverage": local_leverage.get(key) or 0,
                }
            wallet = _safe_float(overview.get("totalWalletBalance"))
            available = _safe_float(overview.get("availableBalance"))
            if wallet <= 0:
                # 单资产账户异常响应时保持旧快照，不用保证金余额替代钱包余额。
                self.logger.warning("账户钱包余额 totalWalletBalance 无效，沿用上次快照")
                return
            with self._state_lock:
                self._exchange_positions = candidate_positions
                self._exchange_position_strs = candidate_position_strs
                self._account_positions = candidate_account_positions
                usdt = self._account_balances.setdefault("USDT", {})
                usdt["availableBalance"] = available
                usdt["balance"] = wallet
                usdt["totalUnrealizedProfit"] = _safe_float(overview.get("totalUnrealizedProfit"))
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
            # 兼容已存在的旧程序持仓：首次看到时补建生命周期账本。
            create_lifecycle(self.config.environment, {**position, "positionKey": key})
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
                # 交易所为权威来源。程序仓位被部分平仓后仍需继续监管，
                # 仅更新可变持仓列表，绝不修改开仓初始台账。
                update_position(
                    self.config.environment, str(position.get("symbol")),
                    str(position.get("positionSide") or "BOTH"),
                    quantity=float(abs(Decimal(str(amount)))),
                    reconciledAt=datetime.now(timezone.utc).isoformat(),
                    reconciliationNote="数量按交易所权威快照更新",
                )
            if position.get("status") == "conflict":
                update_position(self.config.environment, str(position.get("symbol")), str(position.get("positionSide") or "BOTH"), status="active", conflictReason=None)
        # 仅为本地有且交易所也有的突破仓位维持独立子任务。
        self._sync_trailing_tasks()

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
        remove_lifecycle(self.config.environment, position_key(symbol, position.get("positionSide")))
        self.logger.info("幽灵持仓已清理 symbol=%s", symbol)

    def _record_position_pnl(self, position: dict[str, Any], tp_missing: bool, stop_missing: bool) -> None:
        """按程序仓位生命周期汇总最终平仓盈亏与原因。"""
        symbol = str(position.get("symbol") or "")
        open_time = position.get("openTime")
        if not symbol:
            return
        key = position_key(symbol, position.get("positionSide"))
        lifecycle = read_lifecycle(self.config.environment, key) or {}
        close_time = int(time.time() * 1000)
        realized, commission, funding, commission_by_asset, exit_order_ids = self._query_lifecycle_pnl(
            symbol, open_time, close_time, str(position.get("direction") or ""),
            str(position.get("positionSide") or "BOTH"),
        )
        quantity = position.get("quantity")
        if quantity is None:
            quantity = self._ledger_quantity(symbol, open_time)
        record = {
            "positionKey": key,
            "symbol": symbol,
            "signalType": position.get("signalType"),
            "direction": position.get("direction"),
            "entryPrice": position.get("entryPrice"),
            "quantity": quantity,
            "leverage": position.get("leverage"),
            "openTime": open_time,
            "closeTime": close_time,
            "realizedPnlUsdt": realized,
            "commissionUsdt": commission,
            "fundingFeeUsdt": funding,
            "commissionByAsset": commission_by_asset,
            "netPnlUsdt": realized + commission + funding,
            "closeReason": self._resolve_close_reason(position, lifecycle, tp_missing, stop_missing),
            "closeReasonEvidence": lifecycle.get("closeReasonEvidence"),
            "entryOrderId": position.get("entryOrderId") or lifecycle.get("entryOrderId"),
            "exitOrderIds": exit_order_ids,
            "lifecycleEvents": lifecycle.get("events", []),
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

    def _query_lifecycle_pnl(
        self, symbol: str, open_time: Any, close_time: int, direction: str, position_side: str
    ) -> tuple[float, float, float, dict[str, float], list[int]]:
        """只汇总本程序持仓从实际开仓到最终平仓期间的成交与资金费。"""
        try:
            start = int(open_time)
        except (TypeError, ValueError):
            start = int(time.time() * 1000) - 7 * 86400_000
        realized = 0.0
        commission = 0.0
        commission_by_asset: dict[str, float] = {}
        exit_order_ids: list[int] = []
        trades = self._client.get_user_trades(symbol, start, close_time)
        for trade in trades:
            trade_side = str(trade.get("positionSide") or "BOTH")
            if position_side != "BOTH" and trade_side != position_side:
                continue
            try:
                realized += float(trade.get("realizedPnl", 0.0))
            except (TypeError, ValueError):
                pass
            asset = str(trade.get("commissionAsset") or "USDT")
            try:
                fee = float(trade.get("commission", 0.0))
            except (TypeError, ValueError):
                fee = 0.0
            commission_by_asset[asset] = commission_by_asset.get(asset, 0.0) + fee
            if asset == "USDT":
                commission += fee
            closing_side = "SELL" if direction == "多" else "BUY"
            if str(trade.get("side") or "") == closing_side:
                try:
                    order_id = int(trade.get("orderId"))
                except (TypeError, ValueError):
                    continue
                if order_id not in exit_order_ids:
                    exit_order_ids.append(order_id)
        # 用户成交明细暂不可用时，回退收入历史；仍仅由已登记程序仓位触发。
        if not trades:
            for item in self._client.get_income(symbol, start, close_time, "REALIZED_PNL"):
                realized += _safe_float(item.get("income"))
            for item in self._client.get_income(symbol, start, close_time, "COMMISSION"):
                fee = _safe_float(item.get("income"))
                commission += fee
                commission_by_asset["USDT"] = commission_by_asset.get("USDT", 0.0) + fee
        # funding fee 不会出现在 userTrades，单独以收入历史补入。
        funding = sum(
            _safe_float(item.get("income"))
            for item in self._client.get_income(symbol, start, close_time, "FUNDING_FEE")
        )
        return realized, commission, funding, commission_by_asset, exit_order_ids

    def _resolve_close_reason(
        self, position: dict[str, Any], lifecycle: dict[str, Any], tp_missing: bool, stop_missing: bool
    ) -> str:
        """按已持久化的程序订单事实优先确定原因，REST 状态只作断流恢复。"""
        reason = lifecycle.get("closeReason")
        if reason in {"程序止盈", "程序止损", "程序盈亏全平", "外部平仓"}:
            return str(reason)
        symbol = str(position.get("symbol") or "")
        if position.get("autoCloseOrderId") or position.get("autoCloseClientOrderId"):
            return "程序盈亏全平"
        tp_order_id = position.get("takeProfitOrderId")
        if tp_order_id is not None and tp_missing:
            try:
                status = self._client.get_order_status(symbol, int(tp_order_id)).get("status")
                if status == "FILLED":
                    return "程序止盈"
            except Exception as exc:
                self.logger.warning("止盈单状态查询失败 symbol=%s: %s", symbol, exc)
        stop_algo_id = position.get("stopAlgoId")
        if stop_algo_id is not None and stop_missing:
            try:
                status = self._client.get_algo_order_status(symbol, int(stop_algo_id)).get("algoStatus")
                if status == "TRIGGERED":
                    return "程序止损"
            except BinanceFuturesError as exc:
                # -2011 = 条件单已不存在(未触发被取消/手动平仓联动取消)→ 非止损
                if exc.code != -2011:
                    self.logger.warning("止损单状态查询失败 symbol=%s: %s", symbol, exc)
            except Exception as exc:
                self.logger.warning("止损单状态查询失败 symbol=%s: %s", symbol, exc)
        return "外部平仓"

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

    def _sync_trailing_tasks(self) -> None:
        """按本地列表与交易所权威快照创建/销毁突破仓位子任务。"""
        eligible = {
            position_key(item.get("symbol"), item.get("positionSide"))
            for item in load_positions(self.config.environment)
            if item.get("signalType") in TRAILED_SIGNALS
            and item.get("status", "active") == "active"
            and abs(self._exchange_positions.get(position_key(item.get("symbol"), item.get("positionSide")), 0.0)) > 0
        }
        for key in set(self._trail_tasks) - eligible:
            self._trail_tasks.pop(key).stop()
            self._atr.pop(key, None)
        for key in eligible - set(self._trail_tasks):
            task = _TrailingPositionTask(self, key)
            self._trail_tasks[key] = task
            task.start()

    def _stop_trailing_tasks(self) -> None:
        for task in tuple(self._trail_tasks.values()):
            task.stop()
        self._trail_tasks.clear()

    # ---- 移动止损 ----

    def _find_position_by_key(self, key: str) -> dict[str, Any] | None:
        for item in load_positions(self.config.environment):
            if position_key(item.get("symbol"), item.get("positionSide")) == key:
                return item
        return None

    def _check_trailing_key(self, key: str) -> None:
        """由单仓位子任务消费共享行情缓存，并判断该仓位的 R 阶止损。"""
        position = self._find_position_by_key(key)
        if (
            position is None
            or position.get("signalType") not in TRAILED_SIGNALS
            or position.get("status", "active") != "active"
        ):
            return
        symbol = str(position.get("symbol") or "")
        with self._state_lock:
            mark = self._marks.get(symbol)
        if mark is None:
            return
        self._check_trailing_position(position, key, symbol, mark)

    def _check_trailing_position(self, position: dict[str, Any], key: str, symbol: str, mark: float) -> None:
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
        atr = self._atr.get(key)
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

    def _program_position_keys(self) -> set[str]:
        return {
            position_key(position.get("symbol"), position.get("positionSide"))
            for position in load_positions(self.config.environment)
            if position.get("status", "active") == "active"
        }

    def _current_pnl_pct(self) -> float | None:
        """返回权威程序组合未实现盈亏 / 当前钱包余额百分比。"""
        if time.monotonic() - self._last_successful_snapshot > FULL_SNAPSHOT_SECONDS:
            return None
        with self._state_lock:
            wallet = float(self._account_balances.get("USDT", {}).get("balance", 0.0))
            positions = {key: dict(value) for key, value in self._account_positions.items()}
        if wallet <= 0:
            return None
        keys = self._program_position_keys()
        unrealized = sum(float(info.get("unrealizedProfit", 0.0)) for key, info in positions.items() if key in keys)
        return unrealized / wallet * 100

    def _estimated_pnl_pct(self) -> float | None:
        """使用共享标记价格作每秒级预警；越线后必须再由 REST 权威确认。"""
        with self._state_lock:
            wallet = float(self._account_balances.get("USDT", {}).get("balance", 0.0))
            positions = {key: dict(value) for key, value in self._account_positions.items()}
            marks = dict(self._marks)
        if wallet <= 0:
            return None
        total = Decimal("0")
        for key in self._program_position_keys():
            position = positions.get(key)
            if position is None:
                continue
            mark = _decimal_of(marks.get(str(position.get("symbol") or "")))
            amount = _decimal_of(position.get("positionAmt"))
            entry = _decimal_of(position.get("entryPrice"))
            if mark is None or amount is None or entry is None:
                continue
            total += amount * (mark - entry)
        return float(total / Decimal(str(wallet)) * Decimal("100"))

    def _threshold_reached(self, pct: float | None) -> bool:
        return pct is not None and (pct >= self._auto_close_profit_pct or pct <= -self._auto_close_loss_pct)

    def _check_auto_close(self) -> None:
        """估算越线后用 REST 权威确认，再仅全平程序仓位。"""
        if self._auto_closing:
            return
        if not self._threshold_reached(self._estimated_pnl_pct()):
            return
        # 标记价格仅作为预警；触发操作前强制刷新同轮权威仓位与钱包余额。
        self._full_snapshot()
        pct = self._current_pnl_pct()
        if not self._threshold_reached(pct):
            return
        self.logger.warning(
            "程序组合盈亏全平触发 pnl=%+.2f%% 盈利阈值=%s%% 亏损阈值=%s%%",
            pct, self._auto_close_profit_pct, self._auto_close_loss_pct,
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
                client_order_id = f"bot-autoclose-{uuid.uuid4().hex[:20]}"
                update_position(
                    self.config.environment, symbol, str(item.get("positionSide") or "BOTH"),
                    autoCloseClientOrderId=client_order_id,
                    autoCloseRequestedAt=datetime.now(timezone.utc).isoformat(),
                )
                # 对冲模式带与持仓方向一致的 positionSide;单向模式带 reduceOnly 防反向开仓
                try:
                    if dual:
                        result = self._client.place_market_order(
                            symbol, side, quantity, str(item.get("positionSide") or "BOTH"),
                            client_order_id=client_order_id,
                        )
                    else:
                        result = self._client.place_market_order(
                            symbol, side, quantity, None, reduce_only=True, client_order_id=client_order_id,
                        )
                except Exception:
                    # 下单请求明确失败时撤销本地“自动全平已提交”标记，避免误归因。
                    update_position(
                        self.config.environment, symbol, str(item.get("positionSide") or "BOTH"),
                        autoCloseClientOrderId=None, autoCloseRequestedAt=None,
                    )
                    raise
                update_position(
                    self.config.environment, symbol, str(item.get("positionSide") or "BOTH"),
                    autoCloseOrderId=result.get("orderId"),
                )
                add_event(self.config.environment, side_key, {
                    "eventId": f"auto-close-request:{result.get('orderId') or client_order_id}",
                    "eventType": "PROGRAM_AUTO_CLOSE", "eventTime": int(time.time() * 1000),
                    "orderId": result.get("orderId"), "clientOrderId": client_order_id,
                    "closeReason": "程序盈亏全平", "evidence": "程序组合盈亏阈值触发的 reduceOnly 市价平仓",
                })
                self.logger.info("盈亏自动全平:已平 symbol=%s %s %s", symbol, side, quantity)
                closed += 1
            self.logger.warning("盈亏自动全平完成 pnl=%+.2f%% 平仓=%s 个", pct, closed)
        except Exception as exc:
            self.logger.error("盈亏自动全平执行失败,剩余仓位继续监控: %s", exc)
        finally:
            self._auto_closing = False
        # 平仓后立即全量快照对齐:本地记录转幽灵清理,平仓盈亏自动落库
        self._full_snapshot()

    # ---- 流失效 REST 兜底与 listenKey 续期 ----

    def _mark_fallback_needed(self, now: float) -> bool:
        if not self._trail_tasks or not self._mark_stream_connected:
            return bool(self._trail_tasks)
        return any(now - self._mark_received_at.get(key.split("|", 1)[0], 0.0) > MARK_STALE_SECONDS for key in self._trail_tasks)

    def _refresh_marks_from_rest(self) -> None:
        """行情流中断/陈旧时以 10 秒 REST 批量标记价格兜底。"""
        try:
            marks = self._client.get_mark_prices()
        except Exception as exc:
            self.logger.warning("标记价格 REST 兜底失败: %s", exc)
            return
        now = time.monotonic()
        with self._state_lock:
            for symbol, mark in marks.items():
                self._marks[symbol] = mark
                self._mark_received_at[symbol] = now
        for task in tuple(self._trail_tasks.values()):
            task.wake()

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


def _next_interval_close_monotonic(interval: str) -> float:
    """返回下一根箱体周期 K 线收盘后的单调时钟时刻（额外等待 2 秒结算）。"""
    seconds_by_interval = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
        "8h": 28800, "12h": 43200, "1d": 86400,
    }
    seconds = seconds_by_interval.get(interval, 3600)
    now_wall = time.time()
    next_close = (math.floor(now_wall / seconds) + 1) * seconds + 2
    return time.monotonic() + max(1.0, next_close - now_wall)
