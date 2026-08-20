"""PushDeer 通知推送:开仓/平仓等仓位操作即时推送到手机。

密钥读取优先环境变量 PUSHDEER_PUSHKEY,缺失时回退旧程序配置文件
(~/.futures-bot/trading_gui_config.json 顶层同名键,见 secret_utils)。
推送失败仅记日志,不影响交易主流程;异步推送不阻塞调用线程。
官方 API:https://www.pushdeer.com (POST message/push,参数 pushkey/text/desp/type)
"""

from __future__ import annotations

import json
import threading
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from logging_utils import get_logger
from secret_utils import get_secret


PUSHDEER_API_URL = "https://api2.pushdeer.com/message/push"
PUSHDEER_KEY_ENVIRONMENT_VARIABLE = "PUSHDEER_PUSHKEY"
PUSHDEER_TIMEOUT = 10

LOGGER = get_logger("pushdeer")


def push_message(text: str, desp: str = "") -> None:
    """同步推送一条 PushDeer 通知;无密钥或失败仅记日志,不抛出。"""
    push_key = get_secret(PUSHDEER_KEY_ENVIRONMENT_VARIABLE)
    if not push_key:
        LOGGER.info("未配置 PUSHDEER_PUSHKEY,跳过推送: %s", text)
        return
    params: dict[str, Any] = {"pushkey": push_key, "text": text, "type": "markdown"}
    if desp:
        params["desp"] = desp
    url = f"{PUSHDEER_API_URL}?{urlencode(params)}"
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=PUSHDEER_TIMEOUT) as response:
            payload = json.load(response)
        if not isinstance(payload, dict) or payload.get("code") != 0:
            LOGGER.warning("PushDeer 推送失败: %s", payload.get("error", payload))
    except (HTTPError, URLError, OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("PushDeer 推送异常: %s", exc)


def push_message_async(text: str, desp: str = "") -> None:
    """异步推送(后台线程,不阻塞交易主流程);无密钥时直接跳过。"""
    if not get_secret(PUSHDEER_KEY_ENVIRONMENT_VARIABLE):
        return
    threading.Thread(target=push_message, args=(text, desp), daemon=True).start()
