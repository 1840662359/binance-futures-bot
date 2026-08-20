"""下载并筛选 USDⓈ-M COIN 标的永续合约的交易规则。"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app_paths import runtime_directory
from logging_utils import configure_logging


LIVE_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://demo-fapi.binance.com"
EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
OFFICIAL_API_DOC_URL = (
    "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures"
)
OUTPUT_FILENAME = "usdt_coin_perpetual_exchange_info.json"
LIVE_OUTPUT_PATH = runtime_directory("production") / OUTPUT_FILENAME
TESTNET_OUTPUT_PATH = runtime_directory("testnet") / OUTPUT_FILENAME

REQUIRED_SYMBOL_FIELDS = {
    "contractType": "PERPETUAL",
    "status": "TRADING",
    "quoteAsset": "USDT",
    "underlyingType": "COIN",
}


def get_api_url(use_testnet: bool) -> str:
    """根据环境开关返回实盘或测试网的 exchangeInfo URL。"""
    base_url = TESTNET_BASE_URL if use_testnet else LIVE_BASE_URL
    return f"{base_url}{EXCHANGE_INFO_PATH}"


def fetch_exchange_info(api_url: str, timeout: float) -> dict[str, Any]:
    """请求 Binance 公开 exchangeInfo 接口并验证响应结构。"""
    request = Request(api_url, headers={"Accept": "application/json"})

    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"Binance API 返回 HTTP {exc.code}。") from exc
    except URLError as exc:
        raise RuntimeError(f"无法连接 Binance API：{exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("Binance API 响应不是有效 JSON。") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise RuntimeError("Binance API 响应缺少 symbols 列表。")

    return payload


def matches_required_fields(symbol: dict[str, Any]) -> bool:
    """判断交易对是否同时满足指定的四项筛选条件。"""
    return all(symbol.get(field) == value for field, value in REQUIRED_SYMBOL_FIELDS.items())


def build_filtered_exchange_info(
    exchange_info: dict[str, Any], api_url: str, environment: str
) -> dict[str, Any]:
    """保留交易所级交易规则及符合条件的交易对完整交易规则。"""
    filtered_symbols = [
        symbol
        for symbol in exchange_info["symbols"]
        if isinstance(symbol, dict) and matches_required_fields(symbol)
    ]

    # 顶层字段包含 rateLimits、exchangeFilters 等交易所级规则，原样保留。
    result = {key: value for key, value in exchange_info.items() if key != "symbols"}
    result["symbols"] = filtered_symbols
    result["filter"] = REQUIRED_SYMBOL_FIELDS
    result["source"] = {
        "environment": environment,
        "endpoint": api_url,
        "officialDocumentation": OFFICIAL_API_DOC_URL,
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
        "--output",
        type=Path,
        help=(
            "输出 JSON 文件路径。默认会根据环境分别使用 "
            f"{LIVE_OUTPUT_PATH} 或 {TESTNET_OUTPUT_PATH}。"
        ),
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="使用 Binance USDⓈ-M Futures 测试网，而不是默认的实盘环境。",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP 请求超时秒数（默认：10）。",
    )
    return parser.parse_args()


def main() -> None:
    """执行下载、筛选和保存。"""
    arguments = parse_arguments()
    if arguments.timeout <= 0:
        raise ValueError("--timeout 必须大于 0。")

    api_url = get_api_url(arguments.testnet)
    environment = "testnet" if arguments.testnet else "live"
    logger = configure_logging("testnet" if arguments.testnet else "production")
    output_path = arguments.output or (
        TESTNET_OUTPUT_PATH if arguments.testnet else LIVE_OUTPUT_PATH
    )

    logger.info("开始下载合约信息 endpoint=%s timeout=%s", api_url, arguments.timeout)
    exchange_info = fetch_exchange_info(api_url, arguments.timeout)
    filtered_exchange_info = build_filtered_exchange_info(
        exchange_info, api_url, environment
    )
    write_json_atomically(filtered_exchange_info, output_path)
    logger.info("合约信息已发布 path=%s symbols=%s", output_path, len(filtered_exchange_info["symbols"]))

    print(
        f"已保存 {len(filtered_exchange_info['symbols'])} 个交易对至 "
        f"{output_path}（{environment}）"
    )


if __name__ == "__main__":
    main()
