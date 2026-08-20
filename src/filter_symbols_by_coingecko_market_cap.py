"""保留 CoinGecko 市值已匹配且低于 1 亿美元的 Binance 永续合约。"""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app_paths import runtime_directory
from logging_utils import configure_logging
from secret_utils import get_secret


COINGECKO_API_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_API_KEY_ENVIRONMENT_VARIABLE = "CG_DEMO_API_KEY"
DERIVATIVE_EXCHANGES_PATH = "/derivatives/exchanges/list"
DERIVATIVE_EXCHANGE_DETAILS_PATH = "/derivatives/exchanges/{exchange_id}"
COINS_MARKETS_PATH = "/coins/markets"
OFFICIAL_COINGECKO_DOC_URL = (
    "https://docs.coingecko.com/reference/derivatives-exchanges-id"
)

MARKET_CAP_THRESHOLD_USD = 100_000_000
COIN_IDS_PER_REQUEST = 100
OUTPUT_FILENAME = "usdt_coin_perpetual_exchange_info_under_100m.json"
LIVE_INPUT_PATH = runtime_directory("production") / "usdt_coin_perpetual_exchange_info.json"
TESTNET_INPUT_PATH = runtime_directory("testnet") / "usdt_coin_perpetual_exchange_info.json"
LIVE_OUTPUT_PATH = LIVE_INPUT_PATH.parent / OUTPUT_FILENAME
TESTNET_OUTPUT_PATH = TESTNET_INPUT_PATH.parent / OUTPUT_FILENAME


def fetch_json(url: str, api_key: str, timeout: float) -> Any:
    """调用 CoinGecko Demo API 并将响应解析为 JSON。"""
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "x-cg-demo-api-key": api_key,
        },
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"CoinGecko API 返回 HTTP {exc.code}。") from exc
    except URLError as exc:
        raise RuntimeError(f"无法连接 CoinGecko API：{exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("CoinGecko API 响应不是有效 JSON。") from exc


def find_binance_futures_exchange_id(api_key: str, timeout: float) -> str:
    """从 CoinGecko 衍生品交易所列表自动发现 Binance Futures 的 ID。"""
    exchanges = fetch_json(
        f"{COINGECKO_API_BASE_URL}{DERIVATIVE_EXCHANGES_PATH}", api_key, timeout
    )
    if not isinstance(exchanges, list):
        raise RuntimeError("CoinGecko 衍生品交易所列表格式无效。")

    matching_ids = [
        exchange.get("id")
        for exchange in exchanges
        if isinstance(exchange, dict)
        and exchange.get("name") == "Binance (Futures)"
        and isinstance(exchange.get("id"), str)
    ]
    if len(matching_ids) != 1:
        raise RuntimeError("无法唯一确定 CoinGecko 的 Binance Futures 交易所 ID。")
    return matching_ids[0]


def fetch_binance_futures_tickers(
    exchange_id: str, api_key: str, timeout: float
) -> list[dict[str, Any]]:
    """获取 CoinGecko 已收录且未到期的 Binance Futures ticker。"""
    query = urlencode({"include_tickers": "unexpired"})
    url = (
        f"{COINGECKO_API_BASE_URL}"
        f"{DERIVATIVE_EXCHANGE_DETAILS_PATH.format(exchange_id=exchange_id)}?{query}"
    )
    payload = fetch_json(url, api_key, timeout)
    tickers = payload.get("tickers") if isinstance(payload, dict) else None
    if not isinstance(tickers, list):
        raise RuntimeError("CoinGecko 响应缺少 Binance Futures ticker 列表。")
    return [ticker for ticker in tickers if isinstance(ticker, dict)]


def build_coin_id_index(tickers: list[dict[str, Any]]) -> dict[tuple[str, str, str], set[str]]:
    """以合约 symbol、基础资产和计价资产建立 CoinGecko ID 索引。"""
    index: dict[tuple[str, str, str], set[str]] = {}
    for ticker in tickers:
        if ticker.get("contract_type") != "perpetual":
            continue

        symbol = ticker.get("symbol")
        base = ticker.get("base")
        target = ticker.get("target")
        coin_id = ticker.get("coin_id")
        if not all(isinstance(value, str) and value for value in (symbol, base, target, coin_id)):
            continue

        index.setdefault((symbol, base, target), set()).add(coin_id)
    return index


def get_symbol_coin_id(
    symbol: dict[str, Any], coin_id_index: dict[tuple[str, str, str], set[str]]
) -> str | None:
    """仅在 CoinGecko ticker 给出唯一 coin_id 时返回匹配结果。"""
    exchange_symbol = symbol.get("symbol")
    base_asset = symbol.get("baseAsset")
    quote_asset = symbol.get("quoteAsset")
    if not all(isinstance(value, str) for value in (exchange_symbol, base_asset, quote_asset)):
        return None

    coin_ids = coin_id_index.get((exchange_symbol, base_asset, quote_asset), set())
    return next(iter(coin_ids)) if len(coin_ids) == 1 else None


def chunked(items: list[str], size: int) -> list[list[str]]:
    """将 CoinGecko ID 切分为固定大小的请求批次。"""
    return [items[index : index + size] for index in range(0, len(items), size)]


def fetch_market_data(
    coin_ids: set[str], api_key: str, timeout: float
) -> dict[str, dict[str, Any]]:
    """按 CoinGecko ID 批量获取美元市值和市场数据更新时间。"""
    market_data: dict[str, dict[str, Any]] = {}
    for coin_id_batch in chunked(sorted(coin_ids), COIN_IDS_PER_REQUEST):
        query = urlencode(
            {
                "vs_currency": "usd",
                "ids": ",".join(coin_id_batch),
                "sparkline": "false",
            }
        )
        payload = fetch_json(
            f"{COINGECKO_API_BASE_URL}{COINS_MARKETS_PATH}?{query}",
            api_key,
            timeout,
        )
        if not isinstance(payload, list):
            raise RuntimeError("CoinGecko 市场数据响应格式无效。")

        for coin in payload:
            if isinstance(coin, dict) and isinstance(coin.get("id"), str):
                market_data[coin["id"]] = coin
    return market_data


def is_valid_market_cap(value: Any) -> bool:
    """判断市值是否为可用于筛选的有效数值。"""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def build_filtered_exchange_info(
    exchange_info: dict[str, Any],
    coin_id_index: dict[tuple[str, str, str], set[str]],
    market_data: dict[str, dict[str, Any]],
    environment: str,
    market_cap_threshold_usd: int = MARKET_CAP_THRESHOLD_USD,
    market_cap_floor_usd: int = 0,
) -> dict[str, Any]:
    """保留市值匹配成功且落在 [下限, 上限] 闭区间的完整 Binance 交易规则。

    market_cap_floor_usd=0 表示不设下限;
    market_cap_threshold_usd=0 表示不设上限(仍查询并附带市值数据,不过滤)。
    闭区间:恰好等于上限的市值保留(与成交量过滤口径一致)。
    """
    retained_symbols: list[dict[str, Any]] = []
    for symbol in exchange_info["symbols"]:
        if not isinstance(symbol, dict):
            continue

        coin_id = get_symbol_coin_id(symbol, coin_id_index)
        market = market_data.get(coin_id) if coin_id else None
        market_cap = market.get("market_cap") if isinstance(market, dict) else None
        if (
            not is_valid_market_cap(market_cap)
            or market_cap < market_cap_floor_usd
            or (market_cap_threshold_usd > 0 and market_cap > market_cap_threshold_usd)
        ):
            continue

        # 复制交易对后仅追加市场数据，原始 Binance 交易规则不作修改。
        retained_symbol = dict(symbol)
        retained_symbol["coingecko"] = {
            "match_status": "matched",
            "match_source": "binance_futures_ticker",
            "coin_id": coin_id,
            "market_cap_usd": market_cap,
            "market_data_updated_at": market.get("last_updated"),
        }
        retained_symbols.append(retained_symbol)

    result = {key: value for key, value in exchange_info.items() if key != "symbols"}
    result["symbols"] = retained_symbols
    if market_cap_threshold_usd > 0 and market_cap_floor_usd > 0:
        comparison = f"{market_cap_floor_usd} <= market_cap_usd <= {market_cap_threshold_usd}"
    elif market_cap_threshold_usd > 0:
        comparison = f"market_cap_usd <= {market_cap_threshold_usd}"
    elif market_cap_floor_usd > 0:
        comparison = f"market_cap_usd >= {market_cap_floor_usd}"
    else:
        comparison = "market_cap_usd 不设过滤(全部保留)"
    result["marketCapFilter"] = {
        "environment": environment,
        "comparison": comparison,
        "thresholdUsd": market_cap_threshold_usd,
        "floorUsd": market_cap_floor_usd,
        "coingeckoDerivativeExchange": "Binance (Futures)",
        "officialDocumentation": OFFICIAL_COINGECKO_DOC_URL,
        "downloadedAt": datetime.now(timezone.utc).isoformat(),
    }
    return result


def write_json_atomically(payload: dict[str, Any], output_path: Path) -> None:
    """将 JSON 写入临时文件后原子替换，避免生成半截文件。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary_file:
        json.dump(payload, temporary_file, ensure_ascii=False, indent=2)
        temporary_file.write("\n")
        temporary_path = Path(temporary_file.name)

    try:
        os.replace(temporary_path, output_path)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise


def parse_arguments() -> argparse.Namespace:
    """解析命令行选项。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="读取并输出 Binance USD-M Futures 测试网数据。",
    )
    parser.add_argument("--input", type=Path, help="Binance 交易规则 JSON 输入路径。")
    parser.add_argument("--output", type=Path, help="筛选结果 JSON 输出路径。")
    parser.add_argument(
        "--timeout", type=float, default=15.0, help="每次 HTTP 请求超时秒数（默认：15）。"
    )
    return parser.parse_args()


def main() -> None:
    """执行 CoinGecko 匹配、市值查询和筛选。"""
    arguments = parse_arguments()
    if arguments.timeout <= 0:
        raise ValueError("--timeout 必须大于 0。")

    # 优先环境变量,缺失时回退旧程序配置文件(兼容旧程序迁移)
    api_key = get_secret(COINGECKO_API_KEY_ENVIRONMENT_VARIABLE)
    if not api_key:
        raise RuntimeError(
            f"请先设置环境变量或旧程序配置中的 {COINGECKO_API_KEY_ENVIRONMENT_VARIABLE}。"
        )

    environment = "testnet" if arguments.testnet else "production"
    logger = configure_logging(environment)
    input_path = arguments.input or (
        TESTNET_INPUT_PATH if arguments.testnet else LIVE_INPUT_PATH
    )
    output_path = arguments.output or (
        TESTNET_OUTPUT_PATH if arguments.testnet else LIVE_OUTPUT_PATH
    )
    if not input_path.is_file():
        raise FileNotFoundError(f"找不到 Binance 交易规则文件：{input_path}")

    exchange_info = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(exchange_info, dict) or not isinstance(exchange_info.get("symbols"), list):
        raise RuntimeError("Binance 交易规则文件缺少 symbols 列表。")

    logger.info("开始CoinGecko市值筛选 input=%s", input_path)
    exchange_id = find_binance_futures_exchange_id(api_key, arguments.timeout)
    tickers = fetch_binance_futures_tickers(exchange_id, api_key, arguments.timeout)
    coin_id_index = build_coin_id_index(tickers)
    matched_coin_ids = {
        coin_id
        for symbol in exchange_info["symbols"]
        if isinstance(symbol, dict)
        for coin_id in [get_symbol_coin_id(symbol, coin_id_index)]
        if coin_id is not None
    }
    market_data = fetch_market_data(matched_coin_ids, api_key, arguments.timeout)
    filtered_exchange_info = build_filtered_exchange_info(
        exchange_info, coin_id_index, market_data, environment
    )
    write_json_atomically(filtered_exchange_info, output_path)
    logger.info("市值交易池已发布 path=%s symbols=%s", output_path, len(filtered_exchange_info["symbols"]))

    print(
        f"已保存 {len(filtered_exchange_info['symbols'])} 个市值低于 "
        f"{MARKET_CAP_THRESHOLD_USD:,} 美元的交易对至 {output_path}"
    )


if __name__ == "__main__":
    main()
