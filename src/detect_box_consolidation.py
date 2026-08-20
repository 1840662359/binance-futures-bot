"""并发识别 Binance K 线中的动态波动率箱体震荡。"""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from logging_utils import configure_logging


# 窗口从 24 到 72 每 4 根一档(13 档):默认 1h 结构周期下最短箱体周期 24 小时,
# 覆盖 24h~72h 震荡时长,中间长度(如 28/40/56)也能命中;保持离散分档,选箱稳定
WINDOWS = tuple(range(24, 73, 4))
MINIMUM_REFERENCE_BARS = 120
# 箱体宽度上下限锚点(按窗口线性插值):24 档箱体窄、72 档箱体宽,中间档平滑过渡
WIDTH_FLOOR_ANCHORS: tuple[tuple[int, float], ...] = (
    (24, 0.015), (36, 0.018), (52, 0.022), (72, 0.025),
)
WIDTH_CEILING_ANCHORS: tuple[tuple[int, float], ...] = (
    (24, 0.060), (36, 0.075), (52, 0.090), (72, 0.110),
)


def width_bounds_for(window: int) -> tuple[float, float]:
    """返回指定窗口的箱体宽度 (下限, 上限):锚点之间线性插值,锚点处取原值。

    原实现按 6 档窗口查字典,窗口加密后中间档(如 20)无字典参数,
    改为在相邻锚点间线性插值,保持 12 窄→72 宽的单调平滑过渡。
    """
    def interpolate(anchors: tuple[tuple[int, float], ...]) -> float:
        for (w0, v0), (w1, v1) in zip(anchors, anchors[1:]):
            if w0 <= window <= w1:
                if w1 == w0:
                    return v0
                return v0 + (v1 - v0) * (window - w0) / (w1 - w0)
        # 超出锚点范围(窗口恒在 12~72 内,防御性兜底):取端点值
        return anchors[-1][1]
    return interpolate(WIDTH_FLOOR_ANCHORS), interpolate(WIDTH_CEILING_ANCHORS)


def determine_worker_count(symbol_count: int) -> int:
    """根据交易对数量选择受控并发数。"""
    if symbol_count <= 0:
        return 0
    if symbol_count <= 10:
        return symbol_count
    if symbol_count <= 50:
        return 10
    if symbol_count <= 200:
        return 20
    return 30


def clamp(value: float, lower: float, upper: float) -> float:
    """将数值限制在闭区间内。"""
    return max(lower, min(value, upper))


def parse_candles(raw_klines: list[list[Any]]) -> list[tuple[int, float, float, float, float, int]]:
    """从 Binance 12 字段 K 线数组提取时间、OHLC 和收盘时间。"""
    candles: list[tuple[int, float, float, float, float, int]] = []
    for kline in raw_klines:
        if not isinstance(kline, list) or len(kline) < 7:
            continue
        try:
            open_time = int(kline[0])
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            close_time = int(kline[6])
        except (TypeError, ValueError):
            continue
        if (
            not math.isfinite(open_price)
            or not math.isfinite(high_price)
            or not math.isfinite(low_price)
            or not math.isfinite(close_price)
            or open_price <= 0
            or high_price <= 0
            or low_price <= 0
            or close_price <= 0
        ):
            continue
        candles.append((open_time, open_price, high_price, low_price, close_price, close_time))
    return candles


def calculate_robust_volatility(
    reference_candles: list[tuple[int, float, float, float, float, int]]
) -> float | None:
    """以历史收盘价对数收益率的 MAD 计算单根 K 线稳健波动率。"""
    closes = [candle[4] for candle in reference_candles]
    if len(closes) < 2:
        return None
    returns = [math.log(current / previous) for previous, current in zip(closes, closes[1:])]
    if not returns:
        return None
    median_return = median(returns)
    sigma = 1.4826 * median(abs(value - median_return) for value in returns)
    return sigma if math.isfinite(sigma) and sigma > 0 else None


def count_mid_crosses(closes: list[float], mid: float) -> int:
    """统计收盘价穿越箱体中轴的次数，正好位于中轴的 K 线不改变状态。"""
    previous_side = 0
    cross_count = 0
    for close_price in closes:
        current_side = 1 if close_price > mid else -1 if close_price < mid else 0
        if current_side == 0:
            continue
        if previous_side and current_side != previous_side:
            cross_count += 1
        previous_side = current_side
    return cross_count


def has_single_candle_boundary_anomaly(
    candidate_candles: list[tuple[int, float, float, float, float, int]], upper: float, lower: float
) -> bool:
    """排除单根大实体独自决定箱体边界的异常情形。"""
    box_height = upper - lower
    if box_height <= 0:
        return True
    tops = [max(open_price, close_price) for _, open_price, _, _, close_price, _ in candidate_candles]
    bottoms = [min(open_price, close_price) for _, open_price, _, _, close_price, _ in candidate_candles]
    upper_setter_count = sum(top == upper for top in tops)
    lower_setter_count = sum(bottom == lower for bottom in bottoms)

    for candle, top, bottom in zip(candidate_candles, tops, bottoms):
        _, open_price, _, _, close_price, _ = candle
        body_ratio = abs(close_price - open_price) / box_height
        if body_ratio <= 0.45:
            continue
        if (top == upper and upper_setter_count == 1) or (
            bottom == lower and lower_setter_count == 1
        ):
            return True
    return False


def count_touch_events(touch_strengths: list[float]) -> tuple[float, int]:
    """将连续触达合并为一个事件，并返回加权触达分与事件数量。"""
    touch_score = 0.0
    event_count = 0
    current_event_strength = 0.0
    for strength in touch_strengths:
        if strength > 0:
            current_event_strength = max(current_event_strength, strength)
            continue
        if current_event_strength > 0:
            touch_score += current_event_strength
            event_count += 1
            current_event_strength = 0.0
    if current_event_strength > 0:
        touch_score += current_event_strength
        event_count += 1
    return touch_score, event_count


def detect_window_box(
    candles: list[tuple[int, float, float, float, float, int]], window: int
) -> dict[str, Any] | None:
    """识别最新指定窗口是否满足动态波动率箱体震荡条件。"""
    reference_count = max(3 * window, MINIMUM_REFERENCE_BARS)
    required_candle_count = reference_count + window
    if len(candles) < required_candle_count:
        return None

    candidate_candles = candles[-window:]
    reference_candles = candles[-required_candle_count:-window]
    sigma = calculate_robust_volatility(reference_candles)
    if sigma is None:
        return None

    tops = [max(open_price, close_price) for _, open_price, _, _, close_price, _ in candidate_candles]
    bottoms = [min(open_price, close_price) for _, open_price, _, _, close_price, _ in candidate_candles]
    closes = [close_price for _, _, _, _, close_price, _ in candidate_candles]
    upper = max(tops)
    lower = min(bottoms)
    # 箱体窗口内含影线的真实极值(高抛低吸止损参考价;边界 upper/lower 仅按开收盘计算)
    extreme_high = max(candle[2] for candle in candidate_candles)
    extreme_low = min(candle[3] for candle in candidate_candles)
    mid = (upper + lower) / 2
    if mid <= 0 or upper <= lower:
        return None

    width_pct = (upper - lower) / mid
    width_floor, width_ceiling = width_bounds_for(window)
    max_width_pct = clamp(2.2 * sigma * math.sqrt(window), width_floor, width_ceiling)
    drift_pct = abs(closes[-1] - closes[0]) / mid
    max_drift_pct = min(0.35 * width_pct, 1.5 * sigma * math.sqrt(window))
    edge_zone_pct = clamp(1.5 * sigma, 0.12 * width_pct, 0.30 * width_pct)
    edge_zone_price = edge_zone_pct * mid
    upper_touch_strengths = [
        1.0 if top >= upper - edge_zone_price else 0.5 if high_price >= upper - edge_zone_price else 0.0
        for top, (_, _, high_price, _, _, _) in zip(tops, candidate_candles)
    ]
    lower_touch_strengths = [
        1.0 if bottom <= lower + edge_zone_price else 0.5 if low_price <= lower + edge_zone_price else 0.0
        for bottom, (_, _, _, low_price, _, _) in zip(bottoms, candidate_candles)
    ]
    upper_touch_score, upper_touch_events = count_touch_events(upper_touch_strengths)
    lower_touch_score, lower_touch_events = count_touch_events(lower_touch_strengths)
    # 触达按独立事件计数而非按 K 线计数，门槛相应收敛至 2 到 3 次。
    required_touch_score = clamp(math.ceil(0.25 * window * sigma / width_pct), 2, 3)
    required_crosses = max(2, window // 12)
    mid_cross_count = count_mid_crosses(closes, mid)
    boundary_anomaly = has_single_candle_boundary_anomaly(candidate_candles, upper, lower)

    if not (
        width_pct <= max_width_pct
        and drift_pct <= max_drift_pct
        and upper_touch_score >= required_touch_score
        and lower_touch_score >= required_touch_score
        and mid_cross_count >= required_crosses
        and not boundary_anomaly
    ):
        return None

    return {
        "window": window,
        "startTime": candidate_candles[0][0],
        "endTime": candidate_candles[-1][5],
        "upper": upper,
        "lower": lower,
        "extremeHigh": extreme_high,
        "extremeLow": extreme_low,
        "mid": mid,
        "widthPct": width_pct,
        "maxWidthPct": max_width_pct,
        "sigma": sigma,
        "driftPct": drift_pct,
        "maxDriftPct": max_drift_pct,
        "edgeZonePct": edge_zone_pct,
        "upperTouchScore": upper_touch_score,
        "lowerTouchScore": lower_touch_score,
        "requiredTouchScore": required_touch_score,
        "upperTouchEvents": upper_touch_events,
        "lowerTouchEvents": lower_touch_events,
        "midCrossCount": mid_cross_count,
        "requiredCrosses": required_crosses,
    }


def detect_symbol_boxes(symbol: str, raw_klines: list[list[Any]]) -> list[dict[str, Any]]:
    """识别单个交易对在全部目标窗口中的箱体。"""
    candles = parse_candles(raw_klines)
    return [
        {"symbol": symbol, **box}
        for window in WINDOWS
        if (box := detect_window_box(candles, window)) is not None
    ]


def calculate_box_score(box: dict[str, Any]) -> float:
    """按紧凑度、无趋势、双边触达和中轴往返计算箱体质量分。"""
    width_score = clamp(1 - box["widthPct"] / box["maxWidthPct"], 0, 1)
    trend_score = clamp(1 - box["driftPct"] / box["maxDriftPct"], 0, 1)

    minimum_touches = min(box["upperTouchScore"], box["lowerTouchScore"])
    touch_excess_score = clamp(minimum_touches / box["requiredTouchScore"] - 1, 0, 1)
    touch_balance_score = 1 - (
        abs(box["upperTouchScore"] - box["lowerTouchScore"])
        / max(box["upperTouchScore"], box["lowerTouchScore"])
    )
    touch_score = 0.7 * touch_excess_score + 0.3 * touch_balance_score

    cross_score = clamp(box["midCrossCount"] / box["requiredCrosses"] - 1, 0, 1)
    return round(
        100 * (0.30 * width_score + 0.25 * trend_score + 0.25 * touch_score + 0.20 * cross_score),
        4,
    )


def select_best_box(symbol_boxes: list[dict[str, Any]]) -> dict[str, Any]:
    """选择最高分附近窗口中周期最长的箱体，作为交易对的最优箱体。"""
    for box in symbol_boxes:
        box["boxScore"] = calculate_box_score(box)

    highest_score = max(box["boxScore"] for box in symbol_boxes)
    similar_score_boxes = [
        box for box in symbol_boxes if box["boxScore"] >= highest_score - 5
    ]
    selected_box = max(similar_score_boxes, key=lambda box: (box["window"], box["boxScore"]))
    selected_box["selection"] = {
        "highestScoreForSymbol": highest_score,
        "scoreTolerance": 5,
        "rule": "within_score_tolerance_choose_longer_window",
    }
    return selected_box


def detect_boxes_concurrently(klines_by_symbol: dict[str, list[list[Any]]]) -> list[dict[str, Any]]:
    """按交易对数量自动并发识别箱体，并为每个交易对保留最优箱体。"""
    worker_count = determine_worker_count(len(klines_by_symbol))
    if worker_count == 0:
        return []

    boxes_by_symbol: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(detect_symbol_boxes, symbol, raw_klines): symbol
            for symbol, raw_klines in klines_by_symbol.items()
            if isinstance(symbol, str) and isinstance(raw_klines, list)
        }
        for future in as_completed(futures):
            symbol_boxes = future.result()
            if symbol_boxes:
                boxes_by_symbol[symbol_boxes[0]["symbol"]] = symbol_boxes

    selected_boxes = [select_best_box(symbol_boxes) for symbol_boxes in boxes_by_symbol.values()]
    return sorted(selected_boxes, key=lambda box: box["symbol"])


def build_output(input_data: dict[str, Any], boxes: list[dict[str, Any]]) -> dict[str, Any]:
    """仅构建命中箱体及其参数的输出结构。"""
    request = input_data.get("request", {})
    source = input_data.get("source", {})
    return {
        "source": {
            "klineFile": source,
            "analyzedAt": datetime.now(timezone.utc).isoformat(),
        },
        "strategy": {
            "windows": list(WINDOWS),
            "minimumReferenceBars": MINIMUM_REFERENCE_BARS,
            "volatility": "1.4826 * MAD(log(close_t / close_t_minus_1))",
            "boundary": "max/min of open and close only; wick-assisted buffered touches",
            "stopReference": "extremeHigh/extremeLow of the box window (wick-inclusive) for in-box signal stops",
            "klineInterval": request.get("interval"),
            "score": {
                "formula": "30% width + 25% trend + 25% touches + 20% mid crosses",
                "selection": "highest score; within 5 points select longer window",
            },
        },
        "boxes": boxes,
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


def parse_arguments() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="K 线 JSON 输入路径。")
    parser.add_argument("--output", type=Path, help="箱体识别结果 JSON 输出路径。")
    return parser.parse_args()


def main() -> None:
    """读取 K 线文件、并发识别箱体并保存结果。"""
    arguments = parse_arguments()
    if not arguments.input.is_file():
        raise FileNotFoundError(f"找不到 K 线文件：{arguments.input}")

    input_data = json.loads(arguments.input.read_text(encoding="utf-8"))
    environment = input_data.get("source", {}).get("environment", "production") if isinstance(input_data, dict) else "production"
    logger = configure_logging(environment if environment in {"production", "testnet"} else "production")
    klines_by_symbol = input_data.get("klines") if isinstance(input_data, dict) else None
    if not isinstance(klines_by_symbol, dict):
        raise RuntimeError("K 线文件缺少 klines 对象。")

    output_path = arguments.output or arguments.input.with_name(
        f"{arguments.input.stem}_boxes.json"
    )
    boxes = detect_boxes_concurrently(klines_by_symbol)
    write_json_atomically(build_output(input_data, boxes), output_path)
    logger.info("箱体识别完成 input=%s boxes=%s path=%s", arguments.input, len(boxes), output_path)
    print(f"已识别 {len(boxes)} 个箱体并保存至 {output_path}")


if __name__ == "__main__":
    main()
