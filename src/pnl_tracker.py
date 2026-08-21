"""平仓盈亏统计:按日分文件记录程序开出的单被平仓后的盈亏情况。

目录结构:runtime/{环境}/pnl/{YYYY-MM-DD}.json —— 文件按 UTC 日期命名
(程序内部时间约定为 UTC),每条记录含 closeTime 毫秒时间戳,
GUI 展示层按北京时间日期聚合(见 beijing_date_key)。

记录字段:
- symbol / signalType / direction / entryPrice / quantity / leverage:开仓信息
- openTime / closeTime:开仓与平仓时间(毫秒)
- realizedPnlUsdt / commissionUsdt / fundingFeeUsdt / netPnlUsdt:成交盈亏、手续费、资金费与净盈亏
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app_paths import runtime_directory

CHINA_TIMEZONE = timezone(timedelta(hours=8))

# 追加写入互斥(持仓监控线程唯一写者,原子替换保证读者不读到半截文件)
_LOCK = threading.Lock()


def pnl_directory(environment: str) -> Path:
    """返回指定环境的盈亏记录目录。"""
    return runtime_directory(environment) / "pnl"


def beijing_date_key(close_time_ms: Any) -> str:
    """将平仓毫秒时间戳转换为北京时间日期键(YYYY-MM-DD),非法值返回空串。"""
    try:
        return datetime.fromtimestamp(int(close_time_ms) / 1000, CHINA_TIMEZONE).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return ""


def read_pnl_records(environment: str) -> list[dict[str, Any]]:
    """合并读取全部按日文件中的盈亏记录(按文件时间序)。"""
    directory = pnl_directory(environment)
    if not directory.is_dir():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        items = payload.get("records") if isinstance(payload, dict) else None
        if isinstance(items, list):
            records.extend(item for item in items if isinstance(item, dict))
    return records


def aggregate_days(records: list[dict[str, Any]]) -> dict[str, dict[str, float | int]]:
    """按北京时间日期聚合:返回 {日期: {"net": 净盈亏合计, "count": 平仓笔数}}。"""
    days: dict[str, dict[str, float | int]] = {}
    for record in records:
        key = beijing_date_key(record.get("closeTime"))
        if not key:
            continue
        entry = days.setdefault(key, {"net": 0.0, "count": 0})
        entry["net"] += float(record.get("netPnlUsdt") or 0.0)
        entry["count"] += 1
    return days


def append_pnl_record(environment: str, record: dict[str, Any]) -> None:
    """将一条平仓盈亏记录追加到对应 UTC 日期的文件(原子替换)。"""
    with _LOCK:
        try:
            close_time = int(record["closeTime"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"盈亏记录缺少有效 closeTime:{record.get('closeTime')}") from exc
        day = datetime.fromtimestamp(close_time / 1000, timezone.utc).strftime("%Y-%m-%d")
        path = pnl_directory(environment) / f"{day}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {}
        if path.is_file():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
        records = payload.get("records") if isinstance(payload.get("records"), list) else []
        # 进程在“PnL 写入成功、程序持仓删除前”异常重启时，避免同一程序仓位重复记账。
        # positionKey 只区分 symbol|side，同日再次开同方向仓位会复用；
        # 因此优先使用 positionKey+实际 openTime 组成的生命周期唯一记录 ID。
        key = record.get("recordId") or record.get("positionKey")
        key_field = "recordId" if record.get("recordId") else "positionKey"
        if key and any(isinstance(item, dict) and item.get(key_field) == key for item in records):
            return
        records.append(record)
        payload = {
            "source": {"environment": environment, "updatedAt": datetime.now(timezone.utc).isoformat()},
            "records": records,
        }
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
