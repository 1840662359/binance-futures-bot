"""基于 1 小时箱体与执行周期 K 线生成反转或突破执行信号。"""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from logging_utils import configure_logging


VOLUME_LOOKBACK = 20
VOLATILITY_LOOKBACK = 96
VOLUME_MULTIPLIER = 1.25
# 止损缓冲用 ATR 周期:真实波幅直接度量含影线的插针深度(1×ATR 容纳单根 K 线插针)
ATR_PERIOD = 14
OFFICIAL_BINANCE_DOC_URL = "https://developers.binance.com/en/docs/llms.txt"


def parse_kline(kline: list[Any]) -> tuple[int, float, float, float, float, int, float] | None:
    """提取判断信号所需的 15 分钟 K 线字段。"""
    if not isinstance(kline, list) or len(kline) < 8:
        return None
    try:
        open_time = int(kline[0])
        open_price = float(kline[1])
        high_price = float(kline[2])
        low_price = float(kline[3])
        close_price = float(kline[4])
        close_time = int(kline[6])
        quote_volume = float(kline[7])
    except (TypeError, ValueError):
        return None
    values = (open_price, high_price, low_price, close_price, quote_volume)
    if not all(math.isfinite(value) and value >= 0 for value in values) or close_price <= 0:
        return None
    return open_time, open_price, high_price, low_price, close_price, close_time, quote_volume


def calculate_sigma(candles: list[tuple[int, float, float, float, float, int, float]]) -> float | None:
    """按已收盘 15 分钟 K 线的收盘价计算稳健波动率。"""
    closes = [candle[4] for candle in candles]
    if len(closes) < 2:
        return None
    returns = [math.log(current / previous) for previous, current in zip(closes, closes[1:])]
    median_return = median(returns)
    sigma = 1.4826 * median(abs(value - median_return) for value in returns)
    return sigma if math.isfinite(sigma) and sigma > 0 else None


def calculate_atr(
    candles: list[tuple[int, float, float, float, float, int, float]], period: int = 14
) -> float | None:
    """按最近已收盘 K 线(含信号 K 线)的真实波幅计算 ATR。

    TR = max(高−低, |高−前收|, |低−前收|),由高低价直接度量含影线的插针深度;
    收盘价 σ 对插针不敏感,止损缓冲须以 ATR 为参考(用户确认:缓冲取 1×ATR14)。
    简单平均,与持仓监控移动止损的 ATR(14) 口径一致。
    """
    if len(candles) < period + 1:
        return None
    true_ranges = [
        max(candle[2] - candle[3], abs(candle[2] - previous[4]), abs(candle[3] - previous[4]))
        for candle, previous in zip(candles[-period:], candles[-period - 1 : -1])
    ]
    atr = sum(true_ranges) / period
    return atr if math.isfinite(atr) and atr > 0 else None


def signal_type_allowed(signal_type: str, breakout_mode: str, range_mode: str) -> bool:
    """按策略模式判断信号类型是否允许产出。

    breakout_mode:none=都不做,up=只追向上突破,down=只追向下突破,both=都做;
    range_mode:none=都不做,buy=只低吸,sell=只高抛,both=都做。
    未知信号类型放行,避免误伤后续新增类型。
    """
    if signal_type == "上破箱体上沿":
        return breakout_mode in ("up", "both")
    if signal_type == "下破箱体下沿":
        return breakout_mode in ("down", "both")
    if signal_type == "箱体内低吸":
        return range_mode in ("buy", "both")
    if signal_type == "箱体内高抛":
        return range_mode in ("sell", "both")
    return True


def find_latest_aligned_signal_index(
    candles: list[tuple[int, float, float, float, float, int, float]],
    required_open_time: int | None = None,
) -> int | None:
    """找出可判定信号的最新已收盘执行周期 K 线(直接取最后一根)。"""
    for index in range(len(candles) - 1, VOLATILITY_LOOKBACK - 1, -1):
        open_time = candles[index][0]
        if required_open_time is not None and open_time != required_open_time:
            continue
        return index
    return None


def generate_signal_for_box(
    box: dict[str, Any],
    raw_klines: list[list[Any]],
    required_open_time: int | None = None,
) -> dict[str, Any] | None:
    """对单个箱体生成最新一根可判定执行周期 K 线的信号。

    信号判定仅基于 K 线与成交量(不依赖 OI 数据)。
    """
    candles = [parsed for kline in raw_klines if (parsed := parse_kline(kline)) is not None]
    signal_index = find_latest_aligned_signal_index(candles, required_open_time)
    if signal_index is None or signal_index < max(VOLUME_LOOKBACK, VOLATILITY_LOOKBACK):
        return None

    signal_candle = candles[signal_index]
    open_time, open_price, high_price, low_price, close_price, close_time, quote_volume = signal_candle
    prior_candles = candles[signal_index - VOLATILITY_LOOKBACK : signal_index]
    sigma = calculate_sigma(prior_candles)
    if sigma is None:
        return None
    median_quote_volume = median(candle[6] for candle in candles[signal_index - VOLUME_LOOKBACK : signal_index])
    if median_quote_volume <= 0:
        return None

    try:
        upper = float(box["upper"])
        lower = float(box["lower"])
        mid = float(box["mid"])
        edge_zone_pct = float(box["edgeZonePct"])
    except (KeyError, TypeError, ValueError):
        return None

    # 突破确认/止损缓冲:1σ×收盘价(较 0.5σ 加大缓冲,降低插针扫损概率)
    breakout_buffer = max(1.0 * sigma * close_price, 0.15 * edge_zone_pct * mid)
    # 真实波幅 ATR(14):供执行器做止损缓冲(含信号 K 线的插针,缓冲至少容纳一次典型插针)
    atr14 = calculate_atr(candles[: signal_index + 1])
    volume_ratio = quote_volume / median_quote_volume

    signal_type: str | None = None
    direction: str | None = None
    # 上破蜡烛必须为阳线且上影线不超实体(收盘贴近高点,排除冲高回落的诱多形态)
    if (
        close_price > upper + breakout_buffer
        and close_price > open_price
        and high_price - close_price <= close_price - open_price
        and volume_ratio >= VOLUME_MULTIPLIER
    ):
        signal_type, direction = "上破箱体上沿", "多"
    # 下破与上破镜像:阴线且下影线不超实体(收盘贴近低点,排除砸破回收的诱空形态)
    elif (
        close_price < lower - breakout_buffer
        and close_price < open_price
        and close_price - low_price <= open_price - close_price
        and volume_ratio >= VOLUME_MULTIPLIER
    ):
        signal_type, direction = "下破箱体下沿", "空"
    # 高抛低吸:刺破边沿且收盘落回箱体,影线至少占整根 K 线 30%(插针形态确认,
    # 加 1e-9 容差消除浮点边界误差);刺破深度不超过突破确认缓冲
    # (插针太深说明可能是真突破,不按假插针处理)
    elif (
        high_price > upper
        and close_price < upper
        and high_price - upper <= breakout_buffer
        and (high_price - max(open_price, close_price)) + 1e-9 >= (high_price - low_price) * 0.3
    ):
        signal_type, direction = "箱体内高抛", "空"
    elif (
        low_price < lower
        and close_price > lower
        and lower - low_price <= breakout_buffer
        and (min(open_price, close_price) - low_price) + 1e-9 >= (high_price - low_price) * 0.3
    ):
        signal_type, direction = "箱体内低吸", "多"
    if signal_type is None:
        return None

    return {
        "symbol": box["symbol"],
        "signalType": signal_type,
        "direction": direction,
        # 含影线极值随信号下发,供执行器做高抛低吸止损参考(旧箱体缺字段时回退 upper/lower)
        "box": {
            "window": box["window"],
            "upper": upper,
            "lower": lower,
            "extremeHigh": float(box["extremeHigh"]) if isinstance(box.get("extremeHigh"), (int, float)) else upper,
            "extremeLow": float(box["extremeLow"]) if isinstance(box.get("extremeLow"), (int, float)) else lower,
            "boxScore": box["boxScore"],
        },
        "signalKline": {
            "openTime": open_time,
            "closeTime": close_time,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "quoteVolumeUsdt": quote_volume,
        },
        "executionMetrics": {
            "sigma15m": sigma,
            "atr14": atr14,
            "breakoutBuffer": breakout_buffer,
            "volumeMedianLookback": VOLUME_LOOKBACK,
            "medianQuoteVolumeUsdt": median_quote_volume,
            "volumeRatio": volume_ratio,
        },
    }


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
    parser.add_argument("--boxes", type=Path, required=True, help="1 小时箱体池 JSON 路径。")
    parser.add_argument("--klines", type=Path, required=True, help="15 分钟 K 线 JSON 路径。")
    parser.add_argument("--output", type=Path, help="信号 JSON 输出路径。")
    return parser.parse_args()


def main() -> None:
    """加载箱体与执行数据，生成当前可执行信号。"""
    arguments = parse_arguments()
    for path in (arguments.boxes, arguments.klines):
        if not path.is_file():
            raise FileNotFoundError(f"找不到输入文件：{path}")
    box_data = json.loads(arguments.boxes.read_text(encoding="utf-8"))
    kline_data = json.loads(arguments.klines.read_text(encoding="utf-8"))
    environment = kline_data.get("source", {}).get("environment", "production") if isinstance(kline_data, dict) else "production"
    logger = configure_logging(environment if environment in {"production", "testnet"} else "production")
    boxes = box_data.get("boxes") if isinstance(box_data, dict) else None
    klines_by_symbol = kline_data.get("klines") if isinstance(kline_data, dict) else None
    if not isinstance(boxes, list) or not isinstance(klines_by_symbol, dict):
        raise RuntimeError("输入文件格式不符合箱体或 K 线数据要求。")

    signals = [
        signal
        for box in boxes
        if isinstance(box, dict)
        and isinstance(box.get("symbol"), str)
        and isinstance(klines_by_symbol.get(box["symbol"]), list)
        and (signal := generate_signal_for_box(box, klines_by_symbol[box["symbol"]])) is not None
    ]
    output_path = arguments.output or arguments.boxes.parent / "box_execution_signals_15m.json"
    payload = {
        "source": {
            "boxPool": str(arguments.boxes),
            "klines": str(arguments.klines),
            "officialDocumentation": OFFICIAL_BINANCE_DOC_URL,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        },
        "strategy": {
            "structureInterval": box_data.get("strategy", {}).get("klineInterval"),
            "executionInterval": kline_data.get("request", {}).get("interval"),
            "volumeMultiplier": VOLUME_MULTIPLIER,
            "volumeLookback": VOLUME_LOOKBACK,
            "volatilityLookback": VOLATILITY_LOOKBACK,
            "priority": "上破/下破箱体边界优先于箱体内高抛低吸",
            "breakoutConfirmation": "上破:收盘越过上沿+1σ缓冲 且 阳线 且 上影线不超实体 且 放量1.25倍",
            "breakdownConfirmation": "下破:收盘跌破下沿+1σ缓冲 且 阴线 且 下影线不超实体 且 放量1.25倍",
            "rangeConfirmation": "高抛低吸:刺破边沿且收盘落回箱体,影线至少占整根K线30%,刺破深度不超过突破缓冲",
        },
        "signals": sorted(signals, key=lambda signal: signal["symbol"]),
    }
    write_json_atomically(payload, output_path)
    logger.info("信号识别完成 boxes=%s signals=%s path=%s", len(boxes), len(signals), output_path)
    print(f"已生成 {len(signals)} 个 15 分钟执行信号至 {output_path}")


if __name__ == "__main__":
    main()
