"""为箱体池或交易对池并发下载 Binance Futures 持仓量历史数据。"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from logging_utils import configure_logging, get_logger


LIVE_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://demo-fapi.binance.com"
OPEN_INTEREST_HISTORY_PATH = "/futures/data/openInterestHist"
SUPPORTED_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
DEFAULT_PERIOD = "15m"
DEFAULT_LIMIT = 150
OFFICIAL_BINANCE_DOC_URL = "https://developers.binance.com/en/docs/llms.txt"
LOGGER = get_logger("open_interest")


def determine_worker_count(symbol_count: int) -> int:
    """按交易对数量自动选择受控并发数。"""
    if symbol_count <= 0:
        return 0
    if symbol_count <= 10:
        return symbol_count
    if symbol_count <= 50:
        return 10
    return 20


def fetch_symbol_open_interest(
    symbol: str, period: str, limit: int, base_url: str, timeout: float
) -> list[dict[str, Any]]:
    """获取单个交易对指定周期的 OI 历史数据。"""
    query = urlencode(
        {"symbol": symbol, "period": period, "limit": limit, "contractType": "PERPETUAL"}
    )
    request = Request(f"{base_url}{OPEN_INTEREST_HISTORY_PATH}?{query}", headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"{symbol} 的 Binance API 返回 HTTP {exc.code}。") from exc
    except URLError as exc:
        raise RuntimeError(f"无法获取 {symbol} 的 OI：{exc.reason}") from exc
    except TimeoutError as exc:
        # 漏捕会穿透到调度器导致整个程序崩溃(与 K 线拉取同因,2026-08-19 01:30 事故)
        raise RuntimeError(f"获取 {symbol} 的 OI 超时。") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{symbol} 的 OI 响应不是有效 JSON。") from exc

    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise RuntimeError(f"{symbol} 的 OI 响应格式无效。")
    return payload


def fetch_open_interest(
    symbols: list[str], period: str, limit: int, use_testnet: bool, timeout: float
) -> dict[str, list[dict[str, Any]]]:
    """根据交易对数量自动并发下载全部 OI 序列，任一失败即报错。"""
    result, failures = fetch_open_interest_with_failures(symbols, period, limit, use_testnet, timeout)
    if failures:
        raise RuntimeError("部分 OI 请求失败：\n" + "\n".join(failures.values()))
    return result


def fetch_open_interest_with_failures(
    symbols: list[str], period: str, limit: int, use_testnet: bool, timeout: float
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    """并发下载 OI，并将单个交易对失败作为可剔除结果返回。"""
    if period not in SUPPORTED_PERIODS:
        raise ValueError(f"不支持的 OI 周期：{period}")
    if not 1 <= limit <= 500:
        raise ValueError("OI 条数必须在 1 到 500 之间。")
    if timeout <= 0:
        raise ValueError("timeout 必须大于 0。")

    unique_symbols = sorted(set(symbol for symbol in symbols if isinstance(symbol, str) and symbol))
    worker_count = determine_worker_count(len(unique_symbols))
    if worker_count == 0:
        return {}, {}
    base_url = TESTNET_BASE_URL if use_testnet else LIVE_BASE_URL
    result: dict[str, list[dict[str, Any]]] = {}
    failures: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=worker_count or 1) as executor:
        futures = {
            executor.submit(fetch_symbol_open_interest, symbol, period, limit, base_url, timeout): symbol
            for symbol in unique_symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result[symbol] = future.result()
            except Exception as exc:
                # 单币失败只剔除该交易对,任何异常都不允许搞崩整个调度器
                failures[symbol] = str(exc)
                LOGGER.warning("OI下载失败 symbol=%s period=%s reason=%s", symbol, period, exc)
    return (
        {symbol: result[symbol] for symbol in unique_symbols if symbol in result},
        failures,
    )


def write_json_atomically(payload: dict[str, Any], output_path: Path) -> None:
    """以原子替换方式保存 JSON。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output_path.parent, delete=False) as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.write("\n")
        temporary_path = Path(file.name)
    try:
        os.replace(temporary_path, output_path)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise


def parse_arguments() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="含 symbols 或 boxes 列表的 JSON 输入路径。")
    parser.add_argument("--output", type=Path, help="OI JSON 输出路径。")
    parser.add_argument("--testnet", action="store_true", help="使用 Binance USD-M Futures 测试网。")
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="OI 周期，默认：15m。")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="每个交易对的 OI 条数，默认：150。")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP 超时秒数，默认：10。")
    return parser.parse_args()


def main() -> None:
    """从输入交易对池提取 symbol 并下载 OI 历史数据。"""
    arguments = parse_arguments()
    if not arguments.input.is_file():
        raise FileNotFoundError(f"找不到输入文件：{arguments.input}")
    input_data = json.loads(arguments.input.read_text(encoding="utf-8"))
    items = input_data.get("symbols") if isinstance(input_data, dict) else None
    if not isinstance(items, list):
        items = input_data.get("boxes") if isinstance(input_data, dict) else None
    if not isinstance(items, list):
        raise RuntimeError("输入文件缺少 symbols 或 boxes 列表。")
    symbols = [item.get("symbol") for item in items if isinstance(item, dict)]
    output_path = arguments.output or arguments.input.parent / f"open_interest_{arguments.period}_{arguments.limit}.json"
    open_interest = fetch_open_interest(symbols, arguments.period, arguments.limit, arguments.testnet, arguments.timeout)
    environment = "testnet" if arguments.testnet else "production"
    logger = configure_logging(environment)
    logger.info("OI脚本启动 input=%s period=%s limit=%s", arguments.input, arguments.period, arguments.limit)
    payload = {
        "source": {
            "environment": environment,
            "endpoint": f"{TESTNET_BASE_URL if arguments.testnet else LIVE_BASE_URL}{OPEN_INTEREST_HISTORY_PATH}",
            "officialDocumentation": OFFICIAL_BINANCE_DOC_URL,
            "downloadedAt": datetime.now(timezone.utc).isoformat(),
        },
        "request": {"period": arguments.period, "limit": arguments.limit, "symbolCount": len(open_interest)},
        "openInterest": open_interest,
    }
    write_json_atomically(payload, output_path)
    logger.info("OI文件已发布 path=%s symbols=%s", output_path, len(open_interest))
    print(f"已保存 {len(open_interest)} 个交易对的 OI 数据至 {output_path}")


if __name__ == "__main__":
    main()
