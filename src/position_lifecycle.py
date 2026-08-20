"""程序仓位生命周期账本。

该文件只记录 program_positions.json 已登记仓位的订单事实，用于在仓位
消失后精确归因平仓原因并恢复统计；不扫描或记录人工/外部仓位。
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app_paths import runtime_directory


FILENAME = "position_lifecycle.json"
_LOCK = threading.Lock()


def _path(environment: str) -> Path:
    return runtime_directory(environment) / FILENAME


def _read(path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    except (OSError, json.JSONDecodeError):
        return {}
    items = payload.get("lifecycles") if isinstance(payload, dict) else None
    return {str(key): dict(value) for key, value in items.items()} if isinstance(items, dict) else {}


def _write(environment: str, lifecycles: dict[str, dict[str, Any]]) -> None:
    path = _path(environment)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": {"environment": environment, "updatedAt": datetime.now(timezone.utc).isoformat()},
        "lifecycles": lifecycles,
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def create_lifecycle(environment: str, record: dict[str, Any]) -> None:
    """登记程序开仓的固定身份与保护订单 ID；同 key 仅首次创建。"""
    key = str(record.get("positionKey") or "")
    if not key:
        return
    with _LOCK:
        lifecycles = _read(_path(environment))
        if key not in lifecycles:
            lifecycles[key] = {
                "positionKey": key,
                "symbol": record.get("symbol"),
                "positionSide": record.get("positionSide"),
                "openTime": record.get("openTime"),
                "entryOrderId": record.get("entryOrderId"),
                "takeProfitOrderId": record.get("takeProfitOrderId"),
                "stopAlgoId": record.get("stopAlgoId"),
                "events": [],
                "createdAt": datetime.now(timezone.utc).isoformat(),
            }
            _write(environment, lifecycles)


def add_event(environment: str, key: str, event: dict[str, Any]) -> None:
    """追加去重后的订单/条件单事实，保留最近一次确定的平仓原因。"""
    if not key:
        return
    with _LOCK:
        lifecycles = _read(_path(environment))
        lifecycle = lifecycles.get(key)
        if lifecycle is None:
            return
        events = lifecycle.setdefault("events", [])
        event_id = str(event.get("eventId") or "")
        if event_id and any(str(item.get("eventId") or "") == event_id for item in events if isinstance(item, dict)):
            return
        events.append(dict(event))
        if event.get("closeReason"):
            # 已确认的程序止盈/止损/全平优先级高于随后无法归因的普通成交。
            current = str(lifecycle.get("closeReason") or "")
            incoming = str(event["closeReason"])
            priority = {"": 0, "外部平仓": 1, "程序止盈": 2, "程序止损": 2, "程序盈亏全平": 2}
            if priority.get(incoming, 0) >= priority.get(current, 0):
                lifecycle["closeReason"] = incoming
                lifecycle["closeReasonEvidence"] = event.get("evidence")
        lifecycle["updatedAt"] = datetime.now(timezone.utc).isoformat()
        _write(environment, lifecycles)


def read_lifecycle(environment: str, key: str) -> dict[str, Any] | None:
    with _LOCK:
        lifecycle = _read(_path(environment)).get(key)
        return dict(lifecycle) if isinstance(lifecycle, dict) else None


def remove_lifecycle(environment: str, key: str) -> None:
    with _LOCK:
        lifecycles = _read(_path(environment))
        if key in lifecycles:
            del lifecycles[key]
            _write(environment, lifecycles)
