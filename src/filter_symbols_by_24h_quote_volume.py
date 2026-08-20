"""保留 24 小时 USDT 成交额不少于 100 万的低市值 Binance 永续合约。"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app_paths import runtime_directory
from logging_utils import configure_logging


LIVE_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://demo-fapi.binance.com"
TICKER_24H_PATH = "/fapi/v1/ticker/24hr"
OFFICIAL_BINANCE_DOC_URL = (
    "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures"
)
MINIMUM_QUOTE_VOLUME_USDT = Decimal("1000000")
INPUT_FILENAME = "usdt_coin_perpetual_exchange_info_under_100m.json"
OUTPUT_FILENAME = "usdt_coin_perpetual_exchange_info_under_100m_volume_1m.json"
LIVE_INPUT_PATH = runtime_directory("production") / INPUT_FILENAME
TESTNET_INPUT_PATH = runtime_directory("testnet") / INPUT_FILENAME
LIVE_OUTPUT_PATH = LIVE_INPUT_PATH.parent / OUTPUT_FILENAME
TESTNET_OUTPUT_PATH = TESTNET_INPUT_PATH.parent / OUTPUT_FILENAME


def get_ticker_url(use_testnet: bool) -> str:
    """根据环境开关返回实盘或测试网的 24 小时 ticker 接口地址。"""
    base_url = TESTNET_BASE_URL if use_testnet else LIVE_BASE_URL
    return f"{base_url}{TICKER_24H_PATH}"


def fetch_24h_tickers(ticker_url: str, timeout: float) -> list[dict[str, Any]]:
    """请求 Binance 全量 24 小时价格变动统计并校验响应结构。"""
    request = Request(ticker_url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"Binance API 返回 HTTP {exc.code}。") from exc
    except URLError as exc:
        raise RuntimeError(f"无法连接 Binance API：{exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("Binance API 响应不是有效 JSON。") from exc

    if not isinstance(payload, list):
        raise RuntimeError("Binance 24 小时 ticker 响应不是列表。")
    return [ticker for ticker in payload if isinstance(ticker, dict)]


def build_ticker_index(tickers: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """按 symbol 建立唯一 ticker 索引，重复记录不参与自动筛选。"""
    index: dict[str, dict[str, Any]] = {}
    duplicate_symbols: set[str] = set()
    for ticker in tickers:
        symbol = ticker.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        if symbol in index:
            duplicate_symbols.add(symbol)
        else:
            index[symbol] = ticker

    for symbol in duplicate_symbols:
        del index[symbol]
    return index


def parse_quote_volume(value: Any) -> Decimal | None:
    """将 Binance quoteVolume 解析为可精确比较的十进制数。"""
    if not isinstance(value, str):
        return None
    try:
        quote_volume = Decimal(value)
    except InvalidOperation:
        return None
    return quote_volume if quote_volume.is_finite() and quote_volume >= 0 else None


def build_filtered_exchange_info(
    exchange_info: dict[str, Any], ticker_index: dict[str, dict[str, Any]], environment: str,
    minimum_quote_volume_usdt: Decimal = MINIMUM_QUOTE_VOLUME_USDT,
    maximum_quote_volume_usdt: Decimal = Decimal("0"),
) -> dict[str, Any]:
    """保留 24h 成交额落在 [下限, 上限] 区间内的交易对，并追加 Binance 24 小时成交量信息。

    maximum_quote_volume_usdt=0 表示不设上限(与旧行为一致)。
    """
    retained_symbols: list[dict[str, Any]] = []
    for symbol in exchange_info["symbols"]:
        if not isinstance(symbol, dict):
            continue

        symbol_name = symbol.get("symbol")
        ticker = ticker_index.get(symbol_name) if isinstance(symbol_name, str) else None
        quote_volume = parse_quote_volume(ticker.get("quoteVolume")) if ticker else None
        if (
            quote_volume is None
            or quote_volume < minimum_quote_volume_usdt
            or (maximum_quote_volume_usdt > 0 and quote_volume > maximum_quote_volume_usdt)
        ):
            continue

        # 保留已有 Binance 规则和 CoinGecko 数据，仅追加本次 24 小时行情数据。
        retained_symbol = dict(symbol)
        retained_symbol["binance24hr"] = {
            "quote_volume_usdt": ticker["quoteVolume"],
            "base_volume": ticker.get("volume"),
            "price_change": ticker.get("priceChange"),
            "price_change_percent": ticker.get("priceChangePercent"),
            "last_price": ticker.get("lastPrice"),
            "open_time": ticker.get("openTime"),
            "close_time": ticker.get("closeTime"),
            "trade_count": ticker.get("count"),
        }
        retained_symbols.append(retained_symbol)

    result = {key: value for key, value in exchange_info.items() if key != "symbols"}
    result["symbols"] = retained_symbols
    if maximum_quote_volume_usdt > 0:
        comparison = f"{minimum_quote_volume_usdt} <= quote_volume_usdt <= {maximum_quote_volume_usdt}"
    else:
        comparison = f"quote_volume_usdt >= {minimum_quote_volume_usdt}"
    result["volume24hFilter"] = {
        "environment": environment,
        "comparison": comparison,
        "minimumQuoteVolumeUsdt": str(minimum_quote_volume_usdt),
        "maximumQuoteVolumeUsdt": str(maximum_quote_volume_usdt),
        "endpoint": get_ticker_url(environment == "testnet"),
        "officialDocumentation": OFFICIAL_BINANCE_DOC_URL,
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
    parser.add_argument("--input", type=Path, help="市值筛选结果 JSON 输入路径。")
    parser.add_argument("--output", type=Path, help="成交量筛选结果 JSON 输出路径。")
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="HTTP 请求超时秒数（默认：10）。"
    )
    return parser.parse_args()


def main() -> None:
    """执行 Binance 24 小时成交额查询和筛选。"""
    arguments = parse_arguments()
    if arguments.timeout <= 0:
        raise ValueError("--timeout 必须大于 0。")

    environment = "testnet" if arguments.testnet else "production"
    logger = configure_logging(environment)
    input_path = arguments.input or (
        TESTNET_INPUT_PATH if arguments.testnet else LIVE_INPUT_PATH
    )
    output_path = arguments.output or (
        TESTNET_OUTPUT_PATH if arguments.testnet else LIVE_OUTPUT_PATH
    )
    if not input_path.is_file():
        raise FileNotFoundError(f"找不到市值筛选结果文件：{input_path}")

    exchange_info = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(exchange_info, dict) or not isinstance(exchange_info.get("symbols"), list):
        raise RuntimeError("市值筛选结果文件缺少 symbols 列表。")

    logger.info("开始24小时成交额筛选 input=%s", input_path)
    ticker_url = get_ticker_url(arguments.testnet)
    ticker_index = build_ticker_index(fetch_24h_tickers(ticker_url, arguments.timeout))
    filtered_exchange_info = build_filtered_exchange_info(
        exchange_info, ticker_index, environment
    )
    write_json_atomically(filtered_exchange_info, output_path)
    logger.info("成交量交易池已发布 path=%s symbols=%s", output_path, len(filtered_exchange_info["symbols"]))

    print(
        f"已保存 {len(filtered_exchange_info['symbols'])} 个 24 小时成交额不少于 "
        f"{MINIMUM_QUOTE_VOLUME_USDT} USDT 的交易对至 {output_path}"
    )


if __name__ == "__main__":
    main()
