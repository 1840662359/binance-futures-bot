"""Binance USDⓈ-M WebSocket 流封装:Private 用户数据流 + Market 全市场标记价格流。

官方公告(2026-04-23 生效,旧 URL 已永久下线):WebSocket Base URL 按数据类别分流:
- Public(高频盘口): wss://fstream.binance.com/public
- Market(常规行情,含 markPrice): wss://fstream.binance.com/market
- Private(用户数据): wss://fstream.binance.com/private
公告: https://developers.binance.com/zh-CN/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice
测试网(文档 llms-full.txt 记录): wss://demo-fstream.binance.com/{public|market|private}
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable
from urllib.parse import urlparse

from websocket import WebSocket, WebSocketTimeoutException, create_connection

from logging_utils import get_logger


LIVE_WS_BASE_URL = "wss://fstream.binance.com"
TESTNET_WS_BASE_URL = "wss://demo-fstream.binance.com"
RECONNECT_BACKOFF_SECONDS = 5
MAX_RECONNECT_BACKOFF_SECONDS = 60

LOGGER = get_logger("websocket")


def get_websocket_base_url(use_testnet: bool) -> str:
    """返回实盘或测试网的 WebSocket Base URL。"""
    return TESTNET_WS_BASE_URL if use_testnet else LIVE_WS_BASE_URL


def parse_proxy(proxy_url: str | None) -> tuple[str | None, int | None]:
    """从代理 URL 解析出 websocket-client 需要的 (host, port);未配置时返回 (None, None)。"""
    if not proxy_url:
        return None, None
    parsed = urlparse(proxy_url)
    if not parsed.hostname:
        return None, None
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return parsed.hostname, port


class _ReconnectingStream(threading.Thread):
    """带指数退避自动重连的 WebSocket 流基类。

    注:websocket-client 1.9 起 WebSocketApp 不再支持代理参数,故使用底层
    create_connection(支持 http_proxy_host/port)自管理连接与 ping 保活。
    """

    def __init__(self, name: str, proxy_url: str | None, on_message: Callable[[dict[str, Any]], None]) -> None:
        super().__init__(name=name, daemon=True)
        self._proxy_url = proxy_url
        self._on_message_callback = on_message
        self._stop = threading.Event()
        # 订阅变化唤醒事件:无订阅时线程安静等待,新持仓出现立即唤醒连接(不空转轮询)
        self._wake = threading.Event()
        self._ws: WebSocket | None = None
        self._connected = threading.Event()
        self._logger = LOGGER

    def _build_url(self) -> str:
        """子类实现:返回要连接的 WebSocket URL。"""
        raise NotImplementedError

    def _symbols_snapshot(self) -> list[str]:
        """子类实现:返回当前需要订阅的交易对(为空时不连接)。"""
        raise NotImplementedError

    def run(self) -> None:
        backoff = RECONNECT_BACKOFF_SECONDS
        while not self._stop.is_set():
            try:
                symbols = self._symbols_snapshot()
                if not symbols:
                    # 无订阅:不连接、不轮询,安静等待订阅变化唤醒或停止请求
                    self._connected.clear()
                    self._wake.wait(RECONNECT_BACKOFF_SECONDS)
                    self._wake.clear()
                    if self._stop.is_set():
                        return
                    continue
                url = self._build_url()
                proxy_host, proxy_port = parse_proxy(self._proxy_url)
                try:
                    self._logger.info("WebSocket 连接中 %s", url)
                    ws = create_connection(
                        url,
                        timeout=20,
                        http_proxy_host=proxy_host,
                        http_proxy_port=proxy_port,
                        proxy_type="http",
                    )
                except Exception as exc:
                    self._logger.warning("WebSocket 连接失败 %s: %s", url, exc)
                    self._connected.clear()
                    if self._wait_or_wake(backoff):
                        return
                    backoff = min(backoff * 2, MAX_RECONNECT_BACKOFF_SECONDS)
                    continue
                self._connected.set()
                self._logger.info("WebSocket 已连接 %s", self.name)
                # 连接成功后重置退避,避免多次断线后重连延迟累积到上限
                backoff = RECONNECT_BACKOFF_SECONDS
                self._ws = ws
                try:
                    while not self._stop.is_set():
                        try:
                            raw_message = ws.recv()
                        except WebSocketTimeoutException:
                            # recv 超时即发送 ping 帧保活
                            try:
                                ws.ping()
                            except Exception:
                                break
                            continue
                        if raw_message is None:
                            break  # 连接已关闭
                        self._on_message(raw_message)
                except Exception as exc:
                    self._logger.warning("WebSocket 接收异常 %s: %s", self.name, exc)
                finally:
                    try:
                        ws.close()
                    except Exception:
                        pass
                    self._ws = None
                    self._connected.clear()
                    self._logger.warning("WebSocket 已断开 %s", self.name)
                if self._wait_or_wake(backoff):
                    return
                backoff = min(backoff * 2, MAX_RECONNECT_BACKOFF_SECONDS)
            except BaseException as exc:
                # 任何未预期异常都不允许重连线程静默退出,记录后继续重连
                self._logger.exception("WebSocket 重连循环异常 %s: %r", self.name, exc)
                if self._wait_or_wake(RECONNECT_BACKOFF_SECONDS):
                    return

    def _on_message(self, raw_message: str) -> None:
        if not raw_message or not raw_message.strip():
            # 空消息(连接关闭帧等)不是业务数据,直接忽略,避免无意义告警
            return
        try:
            payload = json.loads(raw_message)
        except (TypeError, ValueError):
            self._logger.warning("WebSocket 消息不是有效 JSON: %.200s", raw_message)
            return
        if isinstance(payload, dict):
            try:
                self._on_message_callback(payload)
            except Exception:
                # 回调异常不中断接收线程
                self._logger.exception("WebSocket 消息处理异常 %s", self.name)

    def request_rebuild(self) -> None:
        """订阅列表变化后调用:唤醒等待线程,并关闭当前连接由重连循环按新订阅重建。"""
        self._wake.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass

    def _wait_or_wake(self, timeout: float) -> bool:
        """等待超时或订阅唤醒;返回 True 表示收到停止请求。"""
        woke = self._wake.wait(timeout)
        if self._stop.is_set():
            return True
        if woke:
            self._wake.clear()
        return False

    def stop(self) -> None:
        """请求停止并等待线程退出。

        同时设置唤醒事件:线程若正阻塞在退避等待(_wake.wait)中,
        不唤醒需等满整个退避期(最长 60s)才能发现停止,导致 GUI 卡顿。
        """
        self._stop.set()
        self._wake.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        self.join(timeout=10)


class UserDataStream(_ReconnectingStream):
    """Private 用户数据流:接收账户、订单及条件单状态事件。"""

    def __init__(self, use_testnet: bool, listen_key: str, proxy_url: str | None,
                 on_message: Callable[[dict[str, Any]], None]) -> None:
        super().__init__(f"user-data-{listen_key[:8]}", proxy_url, on_message)
        self._use_testnet = use_testnet
        self._listen_key = listen_key

    def _build_url(self) -> str:
        # 官方 User Data Stream 示例: /private/ws/<listenKey>，该连接推送全部私有事件。
        return f"{get_websocket_base_url(self._use_testnet)}/private/ws/{self._listen_key}"

    def _symbols_snapshot(self) -> list[str]:
        return [self._listen_key]


class MarkPriceStream(_ReconnectingStream):
    """Market 标记价格流:仅订阅执行 R 阶移动止损的程序仓位。

    官方文档(AGENTS.md 登记):
    - All Market Mark Price Stream: !markPrice@arr(约 3 秒全量推送一轮,期间
      夹杂仅变更交易对的增量推送;实测 !markPrice@arr@1s 同样可用,推送语义
      一致——全量与增量交替,全量轮覆盖全部交易对);
    - 流名称大小写敏感(实测小写 !markprice@arr 连接成功但不推送数据);
    - 组合流 URL 示例: wss://fstream.binance.com/market/stream?streams=!markPrice@arr,
      单连接最多 1024 条流;
    - 文档: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/websocket-market-streams
    """

    def __init__(self, use_testnet: bool, proxy_url: str | None,
                 on_message: Callable[[dict[str, Any]], None]) -> None:
        super().__init__("mark-price", proxy_url, on_message)
        self._use_testnet = use_testnet
        self._subscription_lock = threading.Lock()
        self._symbols: set[str] = set()

    def set_symbols(self, symbols: set[str]) -> None:
        """更新程序突破仓位订阅；无仓位时不建立市场连接。"""
        normalized = {symbol.lower() for symbol in symbols if symbol}
        with self._subscription_lock:
            changed = normalized != self._symbols
            self._symbols = normalized
        if changed:
            self.request_rebuild()

    def _symbols_snapshot(self) -> list[str]:
        with self._subscription_lock:
            return [f"{symbol}@markPrice@1s" for symbol in sorted(self._symbols)]

    def _build_url(self) -> str:
        streams = "/".join(self._symbols_snapshot())
        return (
            f"{get_websocket_base_url(self._use_testnet)}/market/stream"
            f"?streams={streams}"
        )


class KlineStream(_ReconnectingStream):
    """Market 单交易对 K 线流:实时看盘页按当前交易对与周期订阅。

    官方文档(AGENTS.md 登记):
    - K 线流: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/websocket-market-streams
    - 组合流 URL: wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m
    - 负载: {"stream":"btcusdt@kline_1m","data":{"e":"kline","s":"BTCUSDT","k":{...}}},
      k 字段含 t(起点毫秒)/T(终点毫秒)/o/h/l/c(开高低收)/x(是否已收盘);
    - 流名称对 symbol 大小写敏感(与标记价格流一致,必须使用小写)。
    订阅变化(切换交易对或周期)时 request_rebuild 重建连接;未设置订阅时静默等待。
    """

    def __init__(self, use_testnet: bool, proxy_url: str | None,
                 on_message: Callable[[dict[str, Any]], None]) -> None:
        super().__init__("kline", proxy_url, on_message)
        self._use_testnet = use_testnet
        self._subscription_lock = threading.Lock()
        self._stream = ""  # 形如 "btcusdt@kline_1m";空 = 未订阅,不连接

    def set_subscription(self, symbol: str, interval: str) -> None:
        """更新订阅的交易对与 K 线周期;发生变化时重建连接。"""
        stream = f"{symbol.lower()}@kline_{interval}"
        with self._subscription_lock:
            changed = stream != self._stream
            self._stream = stream
        if changed:
            self.request_rebuild()

    def _symbols_snapshot(self) -> list[str]:
        with self._subscription_lock:
            return [self._stream] if self._stream else []

    def _build_url(self) -> str:
        # 官方示例: wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m
        stream = self._symbols_snapshot()[0]
        return f"{get_websocket_base_url(self._use_testnet)}/market/stream?streams={stream}"
