"""密钥读取:优先系统环境变量,回退旧程序配置文件(兼容旧程序迁移)。

旧程序把密钥写入 ~/.futures-bot/trading_gui_config.json,顶层键即环境变量名
(BINANCE_API_KEY / BINANCE_TESTNET_API_KEY / CG_DEMO_API_KEY 等,另有 proxy
对象)。本模块提供统一读取入口:环境变量缺失或为空时回退该文件,
方便旧程序用户不重设系统环境变量即可无感迁移。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


LEGACY_SECRETS_PATH = Path.home() / ".futures-bot" / "trading_gui_config.json"


def _legacy_config() -> dict[str, Any]:
    """读取旧程序配置文件;不存在、损坏或非对象时返回空字典。"""
    try:
        value = json.loads(LEGACY_SECRETS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def get_secret(name: str) -> str:
    """读取密钥:优先系统环境变量,缺失或为空时回退旧程序配置文件。

    返回空字符串表示该密钥未配置。
    """
    value = os.environ.get(name, "")
    if value:
        return value
    value = _legacy_config().get(name)
    return value if isinstance(value, str) else ""
