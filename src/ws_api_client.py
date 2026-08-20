"""Binance USDⓈ-M WebSocket API(WS API)账户查询客户端。

官方文档(AGENTS.md 登记):
- USDⓈ-M WebSocket API General Info:
  https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-api-general-info
  - 实盘 base endpoint: wss://ws-fapi.binance.com/ws-fapi/v1
  - 测试网 base endpoint: wss://testnet.binancefuture.com/ws-fapi/v1
- v2/account.status:仅返回有持仓/挂单的 symbol,性能更好(change-log 2024-07-24)

签名方式与 REST 一致:params(除 signature)按参数名升序排列,
序列化为 query string 后以 secretKey 做 HMAC-SHA256;apiKey/timestamp/recvWindow 置于 params。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any
from urllib.parse import quote, urlencode

from websocket import create_connection

from logging_utils import get_logger

LIVE_WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
TESTNET_WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
OFFICIAL_DOC_URL = (
    "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-api-general-info"
)
RECV_WINDOW = 5000
REQUEST_TIMEOUT = 15

LOGGER = get_logger("ws_api")


class WsApiError(RuntimeError):
    """WebSocket API 请求失败。"""


class WsApiClient:
    """USDⓈ-M WebSocket API 客户端(短连接请求-响应,代理经 URL 参数传入)。"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        use_testnet: bool,
        proxy_url: str | None = None,
        time_offset_ms: int = 0,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError("必须提供 Binance API Key 与 Secret。")
        self._api_key = api_key
        self._api_secret = api_secret
        self._use_testnet = use_testnet
        self._base_url = TESTNET_WS_API_URL if use_testnet else LIVE_WS_API_URL
        self._proxy_url = proxy_url
        # 本地时钟与服务器时钟的偏移(毫秒):可由外部已校准客户端传入,
        # 签名请求遇 -1021 时自动重新校准
        self._time_offset_ms = time_offset_ms

    def account_status(self) -> dict[str, Any]:
        """查询账户状态(官方权威盈亏字段:totalUnrealizedProfit、各仓 unrealizedProfit)。

        WS API 的账户状态端点为 account.status(v2 前缀在 ws-fapi 上实测
        生产网/测试网均不可用,直接使用旧版,避免反复回退产生噪音)。
        """
        payload = self._request("account.status")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise WsApiError("account.status 响应缺少 result。")
        return result

    def _request(self, method: str) -> dict[str, Any]:
        """请求一次;遇时间戳超窗(-1021)自动重新校准时钟后重试一次。"""
        try:
            return self._request_once(method)
        except WsApiError as exc:
            if "-1021" not in str(exc) and "Timestamp" not in str(exc):
                raise
            # 本地时钟漂移:通过公开端点重新校准服务器时间后重试
            self._time_offset_ms = self._fetch_server_time_offset()
            return self._request_once(method)

    def _request_once(self, method: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "recvWindow": RECV_WINDOW,
            "timestamp": int(time.time() * 1000) + self._time_offset_ms,
            "apiKey": self._api_key,
        }
        query = urlencode(sorted(params.items()), quote_via=quote)
        params["signature"] = hmac.new(
            self._api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        request = {"id": str(uuid.uuid4()), "method": method, "params": params}
        proxy_host, proxy_port = None, None
        if self._proxy_url:
            from urllib.parse import urlparse

            parsed = urlparse(self._proxy_url)
            if parsed.hostname:
                proxy_host = parsed.hostname
                proxy_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            ws = create_connection(
                self._base_url,
                timeout=REQUEST_TIMEOUT,
                http_proxy_host=proxy_host,
                http_proxy_port=proxy_port,
                proxy_type="http",
            )
            ws.settimeout(REQUEST_TIMEOUT)
            ws.send(json.dumps(request))
            response = json.loads(ws.recv())
            ws.close()
        except Exception as exc:
            raise WsApiError(f"WebSocket API 请求失败:{exc}") from exc
        if not isinstance(response, dict) or response.get("status") != 200:
            error = response.get("error", {}) if isinstance(response, dict) else {}
            raise WsApiError(f"{method} 返回 {response.get('status')} {error.get('msg', error)}")
        return response

    def _fetch_server_time_offset(self) -> int:
        """通过 REST /fapi/v1/time 查询服务器时间,计算本地时钟偏移(公开端点,无需签名)。

        走 urllib 默认 opener(全局代理已在调度器配置),与 WS API 短连接解耦。
        """
        from urllib.request import urlopen

        base = "https://demo-fapi.binance.com" if self._use_testnet else "https://fapi.binance.com"
        try:
            with urlopen(f"{base}/fapi/v1/time", timeout=REQUEST_TIMEOUT) as response:
                payload = json.load(response)
            return int(payload["serverTime"]) - int(time.time() * 1000)
        except Exception as exc:
            raise WsApiError(f"服务器时间校准失败:{exc}") from exc
