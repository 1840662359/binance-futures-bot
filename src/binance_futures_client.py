"""Binance USDⓈ-M 永续合约签名请求客户端(仅标准库 urllib)。

本模块负责私有端点的 HMAC-SHA256 签名请求与错误归一化,供交易执行器调用。
官方文档依据(AGENTS.md 要求登记):
- 签名流程(totalParams = query string + request body、X-MBX-APIKEY、timestamp/recvWindow):
  https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info
- filters 语义(LOT_SIZE/MARKET_LOT_SIZE/MIN_NOTIONAL/PRICE_FILTER):
  https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition
- 各端点(New Order / order/test / leverage / balance / positionRisk / positionSide/dual / ticker/price / time):
  已对照 https://developers.binance.com/en/docs/llms-full.txt 中 USDⓈ-M 部分核实存在。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


LIVE_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://demo-fapi.binance.com"
SERVER_TIME_PATH = "/fapi/v1/time"
# v1 将被弃用,官方建议 v2(更低延迟、更省 IP 权重)
LAST_PRICE_PATH = "/fapi/v2/ticker/price"
# v2 将弃用,官方建议 v3(仅返回有持仓/挂单的 symbol)
BALANCE_PATH = "/fapi/v3/balance"
ACCOUNT_PATH = "/fapi/v3/account"
POSITION_RISK_PATH = "/fapi/v3/positionRisk"
MARK_PRICE_PATH = "/fapi/v1/premiumIndex"
POSITION_SIDE_DUAL_PATH = "/fapi/v1/positionSide/dual"
LEVERAGE_PATH = "/fapi/v1/leverage"
LEVERAGE_BRACKET_PATH = "/fapi/v1/leverageBracket"
# v3 移除了配置字段(如 leverage),由 symbolConfig 查询
SYMBOL_CONFIG_PATH = "/fapi/v1/symbolConfig"
ORDER_PATH = "/fapi/v1/order"
# 收入历史(已实现盈亏/手续费),供平仓盈亏统计查询
# 官方文档: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/account/rest-api/Get-Income-History
INCOME_PATH = "/fapi/v1/income"
# 成交明细用于按订单归因程序仓位的已实现盈亏与手续费。
USER_TRADES_PATH = "/fapi/v1/userTrades"
# 2025-12-09 起条件单(STOP_MARKET 等)强制迁移到 Algo Order API,旧端点返回 -4120
# 官方文档:https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order
ALGO_ORDER_PATH = "/fapi/v1/algoOrder"
# 用户数据流 listenKey 管理(官方文档:该组端点仅需 API Key,无需 HMAC 签名)
LISTEN_KEY_PATH = "/fapi/v1/listenKey"
# 官方文档:recvWindow 缺省 5000 毫秒,最大 60000
RECV_WINDOW = 5000


class BinanceFuturesError(RuntimeError):
    """Binance API 返回错误(含交易所错误码与消息)。"""

    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class BinanceFuturesClient:
    """封装 USDⓈ-M 永续合约私有端点的签名请求。

    构造函数会校准服务器时钟并缓存账户持仓模式:
    - 服务器允许 ±1 秒时钟偏差,校准本地时钟可避免 -1021 时间戳错误;
    - 对冲模式下单必须携带 positionSide,单向模式不得携带(官方文档 general-info)。
    """

    def __init__(self, api_key: str, api_secret: str, use_testnet: bool, timeout: float) -> None:
        if not api_key or not api_secret:
            raise ValueError("必须提供 Binance API Key 与 Secret。")
        if timeout <= 0:
            raise ValueError("timeout 必须大于 0。")
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = TESTNET_BASE_URL if use_testnet else LIVE_BASE_URL
        self._timeout = timeout
        self._time_offset_ms = self._calibrate_clock()
        self._dual_side = self._load_position_mode()

    def _calibrate_clock(self) -> int:
        """请求服务器时间并返回本地时钟偏移(毫秒)。"""
        payload = self._public_request(SERVER_TIME_PATH)
        try:
            server_time = int(payload["serverTime"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("服务器时间响应格式无效。") from exc
        return server_time - int(time.time() * 1000)

    def _load_position_mode(self) -> bool:
        """查询账户持仓模式并缓存(对冲模式须带 positionSide 下单)。"""
        payload = self._signed_request("GET", POSITION_SIDE_DUAL_PATH, {})
        if not isinstance(payload, dict) or not isinstance(payload.get("dualSidePosition"), bool):
            raise RuntimeError("持仓模式响应格式无效。")
        return bool(payload["dualSidePosition"])

    # ---- 公开接口 ----

    @property
    def time_offset_ms(self) -> int:
        """本地时钟与服务器时钟的偏移(毫秒),供 WS API 客户端复用初始校准。"""
        return self._time_offset_ms

    def get_last_price(self, symbol: str) -> float:
        """查询交易对最新价格(公开接口)。"""
        payload = self._public_request(LAST_PRICE_PATH, {"symbol": symbol})
        try:
            price = float(payload["price"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"最新价响应格式无效:{symbol}") from exc
        if not math.isfinite(price) or price <= 0:
            raise RuntimeError(f"最新价无效:{symbol}")
        return price

    # ---- 账户与订单 ----

    def get_balance_usdt(self) -> float:
        """查询 USDT 可用余额(/fapi/v3/balance 的 availableBalance)。"""
        payload = self._signed_request("GET", BALANCE_PATH, {})
        if not isinstance(payload, list):
            raise RuntimeError("余额响应格式无效。")
        for asset in payload:
            if not isinstance(asset, dict) or asset.get("asset") != "USDT":
                continue
            try:
                balance = float(asset.get("availableBalance"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError("USDT 可用余额格式无效。") from exc
            if not math.isfinite(balance) or balance < 0:
                raise RuntimeError("USDT 可用余额无效。")
            return balance
        return 0.0

    def get_wallet_balance_usdt(self) -> float:
        """查询 USDT 钱包余额(/fapi/v3/balance 的 balance 字段)。

        钱包余额会反映已结算的资金变动，但不以保证金余额
        (margin balance)作为风险百分比分母。
        """
        payload = self._signed_request("GET", BALANCE_PATH, {})
        if not isinstance(payload, list):
            raise RuntimeError("余额响应格式无效。")
        for asset in payload:
            if not isinstance(asset, dict) or asset.get("asset") != "USDT":
                continue
            try:
                wallet = float(asset.get("balance"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError("USDT 钱包余额格式无效。") from exc
            if not math.isfinite(wallet) or wallet < 0:
                raise RuntimeError("USDT 钱包余额无效。")
            return wallet
        return 0.0

    def get_account_overview(self) -> dict[str, Any]:
        """查询账户汇总(/fapi/v3/account)。

        监控模块用其中的 totalWalletBalance 作为程序组合未实现盈亏
        百分比的分母；禁止改用 totalMarginBalance，后者包含未实现盈亏。
        官方文档：
        https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/rest-api
        """
        payload = self._signed_request("GET", ACCOUNT_PATH, {})
        if not isinstance(payload, dict):
            raise RuntimeError("账户汇总响应格式无效。")
        return payload

    def get_mark_prices(self) -> dict[str, float]:
        """查询全市场标记价格(/fapi/v1/premiumIndex)，供行情流失效时兜底。"""
        payload = self._public_request(MARK_PRICE_PATH)
        items = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
        marks: dict[str, float] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            try:
                mark = float(item.get("markPrice"))
            except (TypeError, ValueError):
                continue
            if isinstance(symbol, str) and math.isfinite(mark) and mark > 0:
                marks[symbol] = mark
        return marks

    def get_position_risk(self) -> dict[str, float]:
        """查询全部持仓数量(/fapi/v3/positionRisk),返回 symbol → 持仓绝对量。

        对冲模式下同一 symbol 可能同时有 LONG/SHORT；执行器只需判断是否已有
        任一方向仓位，因此累计绝对量，禁止多空抵消后误判为无仓。
        """
        payload = self._signed_request("GET", POSITION_RISK_PATH, {})
        if not isinstance(payload, list):
            raise RuntimeError("持仓信息响应格式无效。")
        positions: dict[str, float] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            try:
                amount = float(item.get("positionAmt"))
            except (TypeError, ValueError):
                continue
            if isinstance(symbol, str) and math.isfinite(amount):
                positions[symbol] = positions.get(symbol, 0.0) + abs(amount)
        return positions

    def get_positions_detail(self) -> list[dict[str, Any]]:
        """查询全部持仓明细(/fapi/v3/positionRisk,仅含持仓/挂单 symbol),供账户快照使用。"""
        payload = self._signed_request("GET", POSITION_RISK_PATH, {})
        return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []

    def get_leverage_map(self) -> dict[str, int]:
        """查询全部 symbol 的杠杆配置(/fapi/v1/symbolConfig,无参数全量)。

        v3 positionRisk 移除了 leverage 配置字段,杠杆需从 symbolConfig 获取。
        """
        payload = self._signed_request("GET", SYMBOL_CONFIG_PATH, {})
        leverage_map: dict[str, int] = {}
        if not isinstance(payload, list):
            return leverage_map
        for item in payload:
            if not isinstance(item, dict) or not isinstance(item.get("symbol"), str):
                continue
            try:
                leverage = int(item.get("leverage"))
            except (TypeError, ValueError):
                continue
            if leverage > 0:
                leverage_map[item["symbol"]] = leverage
        return leverage_map

    def get_income(
        self, symbol: str, start_time: int, end_time: int, income_type: str, page: int = 1,
    ) -> list[dict[str, Any]]:
        """查询指定交易对时间窗内的收入历史(/fapi/v1/income),返回明细列表。

        incomeType 支持 REALIZED_PNL(已实现盈亏)、COMMISSION(手续费,负值)等;
        供平仓后盈亏统计使用。
        """
        payload = self._signed_request("GET", INCOME_PATH, {
            "symbol": symbol,
            "incomeType": income_type,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000,
            "page": page,
        })
        return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []

    def get_user_trades(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
        order_id: int | None = None,
        from_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """查询成交明细(/fapi/v1/userTrades)，用于程序仓位生命周期对账。

        仅在本地已登记的程序仓位关闭时使用；不会扫描或归档人工仓位。
        官方文档：
        https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/rest-api
        """
        params: dict[str, Any] = {
            "symbol": symbol, "startTime": start_time, "endTime": end_time, "limit": 1000,
        }
        if order_id is not None:
            params["orderId"] = order_id
        if from_id is not None:
            params["fromId"] = from_id
        payload = self._signed_request("GET", USER_TRADES_PATH, params)
        return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []

    def get_order_status(
        self, symbol: str, order_id: int | None = None, client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """查询普通订单状态(GET /fapi/v1/order)。

        orderId 与 origClientOrderId 二选一；开仓请求结果未知时用稳定的
        newClientOrderId 回查交易所，避免把实际成交误判成失败。
        官方文档：https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/trade/rest-api/Query-Order
        """
        if order_id is None and not client_order_id:
            raise ValueError("查询订单状态必须提供 order_id 或 client_order_id。")
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        payload = self._signed_request("GET", ORDER_PATH, params)
        return payload if isinstance(payload, dict) else {}

    def get_algo_order_status(self, symbol: str, algo_id: int) -> dict[str, Any]:
        """查询条件单状态(GET /fapi/v1/algoOrder,返回 algoStatus 等)。

        algoStatus 为 TRIGGERED 表示止损单已触发,用于平仓原因推断
        (区分止损触发与手动平仓联动取消)。
        """
        payload = self._signed_request("GET", ALGO_ORDER_PATH, {"symbol": symbol, "algoId": algo_id})
        return payload if isinstance(payload, dict) else {}

    def get_position(self, symbol: str, position_side: str | None = None) -> dict[str, Any] | None:
        """查询指定交易对当前持仓记录(/fapi/v2/positionRisk?symbol=X)。

        对冲模式下同一 symbol 可能返回多空两条记录,取非零持仓那条;
        全零或查询不到时返回 None。
        """
        payload = self._signed_request("GET", POSITION_RISK_PATH, {"symbol": symbol})
        if not isinstance(payload, list):
            raise RuntimeError("持仓信息响应格式无效。")
        for item in payload:
            if not isinstance(item, dict) or item.get("symbol") != symbol:
                continue
            if position_side is not None and item.get("positionSide") != position_side:
                continue
            try:
                amount = float(item.get("positionAmt"))
            except (TypeError, ValueError):
                continue
            if abs(amount) > 0:
                return item
        return None

    def is_dual_side_position(self) -> bool:
        """返回账户持仓模式:True=对冲模式,False=单向模式。"""
        return self._dual_side

    def set_leverage(self, symbol: str, leverage: int) -> None:
        """设置交易对杠杆倍数(/fapi/v1/leverage)。"""
        self._signed_request("POST", LEVERAGE_PATH, {"symbol": symbol, "leverage": leverage})

    def get_leverage_info(self, symbol: str) -> tuple[int, Decimal | None] | None:
        """查询交易对最大杠杆及其档位的最大名义金额 notionalCap(/fapi/v1/leverageBracket)。

        名义金额超过 notionalCap 时,交易所会在该杠杆下拒绝订单(-2027),
        开仓数量计算需以 notionalCap 为上限(与 杠杆 × 余额 取较小者)。
        """
        payload = self._signed_request("GET", LEVERAGE_BRACKET_PATH, {"symbol": symbol})
        if not isinstance(payload, list):
            return None
        for item in payload:
            if not isinstance(item, dict) or item.get("symbol") != symbol:
                continue
            brackets = item.get("brackets")
            if not isinstance(brackets, list):
                return None
            best: tuple[int, Decimal | None] | None = None
            for bracket in brackets:
                if not isinstance(bracket, dict):
                    continue
                try:
                    leverage = int(bracket.get("initialLeverage"))
                except (TypeError, ValueError):
                    continue
                if leverage <= 0:
                    continue
                cap: Decimal | None = None
                try:
                    cap = Decimal(str(bracket.get("notionalCap")))
                except (InvalidOperation, TypeError, ValueError):
                    cap = None
                if cap is not None and not cap.is_finite():
                    cap = None
                if best is None or leverage > best[0]:
                    best = (leverage, cap)
            return best
        return None

    def get_leverage_for_notional(self, symbol: str, notional: Decimal) -> tuple[int, Decimal | None] | None:
        """按名义价值匹配杠杆档位,返回 (该档位可用杠杆, 该档位名义上限 notionalCap)。

        逻辑:风险制先确定仓位名义价值(与杠杆无关),再按 notionalFloor ≤ 名义 < notionalCap
        找到对应档位,使用该档位的可用最大杠杆(杠杆不扩大名义,仅影响保证金占用);
        名义超过全部档位时按最后一档处理。
        """
        payload = self._signed_request("GET", LEVERAGE_BRACKET_PATH, {"symbol": symbol})
        if not isinstance(payload, list):
            return None
        for item in payload:
            if not isinstance(item, dict) or item.get("symbol") != symbol:
                continue
            brackets = item.get("brackets")
            if not isinstance(brackets, list) or not brackets:
                return None
            fallback: tuple[int, Decimal | None] | None = None
            for bracket in brackets:
                if not isinstance(bracket, dict):
                    continue
                try:
                    leverage = int(bracket.get("initialLeverage"))
                    cap = Decimal(str(bracket.get("notionalCap")))
                    floor = Decimal(str(bracket.get("notionalFloor")))
                except (TypeError, ValueError, InvalidOperation):
                    continue
                if leverage <= 0:
                    continue
                if notional >= floor and notional < cap:
                    return leverage, cap
                fallback = (leverage, cap)
            # 名义超过全部档位上限:按最后一档处理(名义会被截断到该档上限)
            return fallback
        return None

    def get_max_leverage(self, symbol: str) -> int | None:
        """查询交易对支持的最大杠杆(/fapi/v1/leverageBracket,签名端点)。"""
        info = self.get_leverage_info(symbol)
        return info[0] if info is not None else None

    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: str,
        position_side: str | None = None,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """市价单(/fapi/v1/order,newOrderRespType=RESULT 返回成交均价与数量)。

        reduce_only 仅单向模式可用:对冲模式下交易所拒收 reduceOnly(-1106),
        反向 positionSide 单本身即为减仓单。
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
            "newOrderRespType": "RESULT",
        }
        if position_side is not None:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        payload = self._signed_request("POST", ORDER_PATH, params)
        return payload if isinstance(payload, dict) else {}

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        price: str,
        quantity: str,
        position_side: str | None = None,
        reduce_only: bool = True,
    ) -> dict[str, Any]:
        """挂限价单(/fapi/v1/order,普通单不受条件单 Algo 迁移影响)。

        用于箱体内高抛低吸仓位的箱体中轨限价止盈:
        单向模式带 reduceOnly=true,防止止损触发后止盈单反向开仓;
        对冲模式带与持仓方向一致的 positionSide(反向单天然减仓,不带 reduceOnly)。
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "quantity": quantity,
            "price": price,
            "timeInForce": "GTC",
        }
        if position_side is not None:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        payload = self._signed_request("POST", ORDER_PATH, params)
        return payload if isinstance(payload, dict) else {}

    def place_stop_market_close_position(
        self,
        symbol: str,
        side: str,
        stop_price: str,
        position_side: str | None = None,
    ) -> dict[str, Any]:
        """挂全平止损单(STOP_MARKET + closePosition=true,经 /fapi/v1/algoOrder)。

        官方文档:closePosition=true 表示触发时平掉整个仓位,不能与 quantity
        或 reduceOnly 组合,因此双向/对冲模式统一适用,不存在 reduceOnly
        拒收问题(-1106);对冲模式需带与持仓方向一致的 positionSide。
        条件单自 2025-12-09 起强制使用 Algo Order API(algoType=CONDITIONAL),
        触发价参数名为 triggerPrice(非旧版 stopPrice),响应返回 algoId/algoStatus。
        workingType 取 MARK_PRICE(官方文档默认值):按标记价格触发,
        避免现货价格异常插针导致止损过早触发。
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "algoType": "CONDITIONAL",
            "type": "STOP_MARKET",
            "triggerPrice": stop_price,
            "closePosition": "true",
            "workingType": "MARK_PRICE",
        }
        if position_side is not None:
            params["positionSide"] = position_side
        payload = self._signed_request("POST", ALGO_ORDER_PATH, params)
        return payload if isinstance(payload, dict) else {}

    # ---- 用户数据流 listenKey 与撤单 ----

    def create_listen_key(self) -> str:
        """创建用户数据流 listenKey(POST /fapi/v1/listenKey,仅需 API Key)。"""
        payload = self._api_key_request("POST", LISTEN_KEY_PATH, {})
        listen_key = payload.get("listenKey") if isinstance(payload, dict) else None
        if not isinstance(listen_key, str) or not listen_key:
            raise RuntimeError("listenKey 创建响应格式无效。")
        return listen_key

    def renew_listen_key(self, listen_key: str) -> None:
        """续期用户数据流 listenKey(PUT,官方文档:60 分钟 TTL,建议每 30 分钟续期)。"""
        self._api_key_request("PUT", LISTEN_KEY_PATH, {"listenKey": listen_key})

    def close_listen_key(self, listen_key: str) -> None:
        """关闭用户数据流 listenKey(DELETE)。"""
        self._api_key_request("DELETE", LISTEN_KEY_PATH, {"listenKey": listen_key})

    def cancel_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        """取消普通挂单(DELETE /fapi/v1/order)。"""
        payload = self._signed_request("DELETE", ORDER_PATH, {"symbol": symbol, "orderId": order_id})
        return payload if isinstance(payload, dict) else {}

    def cancel_algo_order(self, symbol: str, algo_id: int) -> dict[str, Any]:
        """取消条件单(DELETE /fapi/v1/algoOrder,官方要求必须携带 symbol)。"""
        payload = self._signed_request("DELETE", ALGO_ORDER_PATH, {"symbol": symbol, "algoId": algo_id})
        return payload if isinstance(payload, dict) else {}

    # ---- 请求基础 ----

    def _public_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """公开端点 GET 请求。"""
        url = self._base_url + path
        if params:
            url = f"{url}?{urlencode(params, quote_via=quote)}"
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=self._timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            raise self._build_error(exc) from exc
        except URLError as exc:
            raise RuntimeError(f"无法连接 Binance API:{exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError("Binance API 响应不是有效 JSON。") from exc

    def _api_key_request(self, method: str, path: str, params: dict[str, Any]) -> Any:
        """仅需 API Key 的请求(listenKey 管理端点,官方文档:无需 HMAC 签名)。"""
        query = urlencode(params, quote_via=quote)
        url = f"{self._base_url}{path}" + (f"?{query}" if query else "")
        headers = {"Accept": "application/json", "X-MBX-APIKEY": self._api_key}
        request = Request(url, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self._timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            raise self._build_error(exc) from exc
        except URLError as exc:
            raise RuntimeError(f"无法连接 Binance API:{exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError("Binance API 响应不是有效 JSON。") from exc

    def _signed_request(self, method: str, path: str, params: dict[str, Any]) -> Any:
        """签名端点请求;遇 -1021(时间戳超窗)自动重新校准时钟后重试一次。

        构造时校准过一次服务器时钟,长期运行后本地时钟/网络延迟漂移仍可能
        触发 -1021;重新校准后重试可自愈,避免持仓快照/盈亏统计等链路中断。
        """
        try:
            return self._signed_request_once(method, path, params)
        except BinanceFuturesError as exc:
            if exc.code != -1021:
                raise
            self._time_offset_ms = self._calibrate_clock()
            return self._signed_request_once(method, path, params)

    def _signed_request_once(self, method: str, path: str, params: dict[str, Any]) -> Any:
        """单次签名端点请求。

        官方文档:totalParams = query string + request body;本模块参数全部放入
        query string(body 为空),签名串即完整 query string,并置于 URL 末尾的
        signature 参数;API Key 经 X-MBX-APIKEY 请求头传递。
        """
        request_params = dict(params)
        request_params["timestamp"] = int(time.time() * 1000) + self._time_offset_ms
        request_params["recvWindow"] = RECV_WINDOW
        query = urlencode(request_params, quote_via=quote)
        signature = hmac.new(
            self._api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        url = f"{self._base_url}{path}?{query}&signature={signature}"
        headers = {"Accept": "application/json", "X-MBX-APIKEY": self._api_key}
        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        request = Request(url, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self._timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            raise self._build_error(exc) from exc
        except URLError as exc:
            raise RuntimeError(f"无法连接 Binance API:{exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError("Binance API 响应不是有效 JSON。") from exc

    @staticmethod
    def _build_error(exc: HTTPError) -> BinanceFuturesError:
        """从 HTTPError 提取交易所错误码与消息(不含任何密钥信息)。

        错误码解析为 int 存入异常 code 属性,供调用方按码分级处理
        (如 -2011 Unknown order sent = 订单已不存在)。
        """
        detail = f"HTTP {exc.code}"
        code: int | None = None
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except (OSError, ValueError):
            body = None
        if isinstance(body, dict):
            raw_code = body.get("code")
            message = body.get("msg")
            if raw_code is not None or message:
                detail = f"{detail} {raw_code} {message}".rstrip()
            try:
                code = int(raw_code)
            except (TypeError, ValueError):
                code = None
        return BinanceFuturesError(f"Binance API 返回 {detail}。", code)
