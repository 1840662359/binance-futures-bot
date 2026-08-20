"""并发下载指定 Binance Futures 交易对的 K 线数据。"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app_paths import runtime_directory
from logging_utils import configure_logging, get_logger


LIVE_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://demo-fapi.binance.com"
KLINES_PATH = "/fapi/v1/klines"
OFFICIAL_BINANCE_DOC_URL = (
    "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures"
)
SUPPORTED_INTERVALS = {
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
}
DEFAULT_INTERVAL = "1h"
DEFAULT_LIMIT = 100
MAX_KLINE_LIMIT = 1500
REQUEST_RETRIES = 3
INPUT_FILENAME = "usdt_coin_perpetual_exchange_info_under_100m_volume_1m.json"
LIVE_INPUT_PATH = runtime_directory("production") / INPUT_FILENAME
TESTNET_INPUT_PATH = runtime_directory("testnet") / INPUT_FILENAME
LOGGER = get_logger("klines")


def determine_worker_count(symbol_count: int) -> int:
    """根据交易对总数自动选择受控并发数，避免无界并发触发限频。"""
    if symbol_count <= 0:
        return 0
    if symbol_count <= 10:
        return symbol_count
    if symbol_count <= 50:
        return 10
    if symbol_count <= 200:
        return 20
    return 30


def get_klines_url(use_testnet: bool) -> str:
    """根据环境开关返回实盘或测试网 K 线接口地址。"""
    base_url = TESTNET_BASE_URL if use_testnet else LIVE_BASE_URL
    return f"{base_url}{KLINES_PATH}"


def validate_request(interval: str, limit: int) -> None:
    """校验 Binance K 线周期和条数。"""
    if interval not in SUPPORTED_INTERVALS:
        raise ValueError(f"不支持的 K 线周期：{interval}")
    if not 1 <= limit <= MAX_KLINE_LIMIT:
        raise ValueError(f"K 线条数必须在 1 到 {MAX_KLINE_LIMIT} 之间。")


def fetch_symbol_klines(
    symbol: str, interval: str, limit: int, kline_url: str, timeout: float
) -> list[list[Any]]:
    """获取单个交易对的 K 线，并在短暂瞬态错误后重试。"""
    url = f"{kline_url}?{urlencode({'symbol': symbol, 'interval': interval, 'limit': limit})}"
    last_error: Exception | None = None

    for attempt in range(REQUEST_RETRIES):
        try:
            request = Request(url, headers={"Accept": "application/json"})
            with urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
            if not isinstance(payload, list) or not all(isinstance(item, list) for item in payload):
                raise RuntimeError(f"{symbol} 的 K 线响应格式无效。")
            return payload
        except (HTTPError, URLError, OSError, json.JSONDecodeError, RuntimeError) as exc:
            # OSError 覆盖 TimeoutError/ConnectionError 等网络层异常:
            # 漏捕会穿透到调度器导致整个程序崩溃(实测 2026-08-19 01:30 事故)
            last_error = exc
            if attempt < REQUEST_RETRIES - 1:
                time.sleep(2**attempt)

    raise RuntimeError(f"获取 {symbol} 的 K 线失败：{last_error}") from last_error


def fetch_klines_for_symbols(
    symbols: Iterable[str], interval: str, limit: int, use_testnet: bool = False, timeout: float = 10.0
) -> dict[str, list[list[Any]]]:
    """按交易对数量自动并发获取完整 K 线数据，任一失败即报错。"""
    klines_by_symbol, failures = fetch_klines_with_failures(symbols, interval, limit, use_testnet, timeout)
    if failures:
        raise RuntimeError("部分 K 线请求失败：\n" + "\n".join(failures.values()))
    return klines_by_symbol


def fetch_klines_with_failures(
    symbols: Iterable[str], interval: str, limit: int, use_testnet: bool = False, timeout: float = 10.0
) -> tuple[dict[str, list[list[Any]]], dict[str, str]]:
    """并发获取 K 线，并将单个交易对失败作为可剔除结果返回。"""
    validate_request(interval, limit)
    if timeout <= 0:
        raise ValueError("timeout 必须大于 0。")

    unique_symbols = sorted({symbol for symbol in symbols if isinstance(symbol, str) and symbol})
    worker_count = determine_worker_count(len(unique_symbols))
    if worker_count == 0:
        return {}, {}

    kline_url = get_klines_url(use_testnet)
    klines_by_symbol: dict[str, list[list[Any]]] = {}
    failures: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(fetch_symbol_klines, symbol, interval, limit, kline_url, timeout): symbol
            for symbol in unique_symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                klines_by_symbol[symbol] = future.result()
            except Exception as exc:
                # 单币失败只剔除该交易对,任何异常都不允许搞崩整个调度器
                failures[symbol] = str(exc)
                LOGGER.warning("K线下载失败 symbol=%s interval=%s reason=%s", symbol, interval, exc)

    return (
        {symbol: klines_by_symbol[symbol] for symbol in unique_symbols if symbol in klines_by_symbol},
        failures,
    )


def build_output(
    klines_by_symbol: dict[str, list[list[Any]]],
    interval: str,
    limit: int,
    environment: str,
) -> dict[str, Any]:
    """构建包含请求元数据和完整 K 线数组的输出 JSON。"""
    return {
        "source": {
            "environment": environment,
            "endpoint": get_klines_url(environment == "testnet"),
            "officialDocumentation": OFFICIAL_BINANCE_DOC_URL,
            "downloadedAt": datetime.now(timezone.utc).isoformat(),
        },
        "request": {
            "interval": interval,
            "limit": limit,
            "symbolCount": len(klines_by_symbol),
            "concurrency": determine_worker_count(len(klines_by_symbol)),
        },
        "klines": klines_by_symbol,
    }


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


def get_default_output_path(output_directory: Path, interval: str, limit: int) -> Path:
    """根据输出目录、周期和条数生成默认输出路径。"""
    return output_directory / f"klines_{interval}_{limit}.json"


def get_output_path(
    output_directory: Path, interval: str, limit: int, output: Path | None, output_filename: str | None
) -> Path:
    """根据完整路径或指定文件名生成最终输出路径。"""
    if output is not None:
        return output
    if output_filename is None:
        return get_default_output_path(output_directory, interval, limit)

    filename_path = Path(output_filename)
    if filename_path.name != output_filename or filename_path.suffix.lower() != ".json":
        raise ValueError("--output-filename 必须是仅包含文件名的 .json 文件。")
    return output_directory / filename_path


def parse_arguments() -> argparse.Namespace:
    """解析命令行选项。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--testnet", action="store_true", help="使用 Binance USD-M Futures 测试网。")
    parser.add_argument("--input", type=Path, help="含 symbols 或 boxes 列表的 JSON 输入路径。")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--output", type=Path, help="K 线 JSON 完整输出路径。")
    output_group.add_argument(
        "--output-filename",
        help="输出 JSON 文件名，将写入当前环境的 runtime 目录。",
    )
    parser.add_argument("--interval", default=DEFAULT_INTERVAL, help="K 线周期，默认：1h。")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="每个交易对的 K 线条数，默认：100。")
    parser.add_argument("--timeout", type=float, default=10.0, help="单次 HTTP 请求超时秒数，默认：10。")
    return parser.parse_args()


def main() -> None:
    """从成交量筛选结果提取交易对并批量下载 K 线。"""
    arguments = parse_arguments()
    validate_request(arguments.interval, arguments.limit)
    environment = "testnet" if arguments.testnet else "production"
    logger = configure_logging(environment)
    logger.info("K线脚本启动 input=%s interval=%s limit=%s", arguments.input, arguments.interval, arguments.limit)
    input_path = arguments.input or (
        TESTNET_INPUT_PATH if arguments.testnet else LIVE_INPUT_PATH
    )
    if not input_path.is_file():
        raise FileNotFoundError(f"找不到成交量筛选结果文件：{input_path}")

    exchange_info = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(exchange_info, dict):
        raise RuntimeError("输入文件必须是 JSON 对象。")
    source_symbols = exchange_info.get("symbols")
    source_boxes = exchange_info.get("boxes")
    if isinstance(source_symbols, list):
        symbols = [symbol.get("symbol") for symbol in source_symbols if isinstance(symbol, dict)]
    elif isinstance(source_boxes, list):
        symbols = [box.get("symbol") for box in source_boxes if isinstance(box, dict)]
    else:
        raise RuntimeError("输入文件缺少 symbols 或 boxes 列表。")

    output_path = get_output_path(
        input_path.parent,
        arguments.interval,
        arguments.limit,
        arguments.output,
        arguments.output_filename,
    )
    klines_by_symbol = fetch_klines_for_symbols(
        symbols, arguments.interval, arguments.limit, arguments.testnet, arguments.timeout
    )
    write_json_atomically(
        build_output(klines_by_symbol, arguments.interval, arguments.limit, environment),
        output_path,
    )
    logger.info("K线文件已发布 path=%s symbols=%s", output_path, len(klines_by_symbol))
    print(f"已保存 {len(klines_by_symbol)} 个交易对的 K 线数据至 {output_path}")


if __name__ == "__main__":
    main()
