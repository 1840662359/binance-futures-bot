"""统一配置运行日志，按 Binance 环境隔离保存。"""

from __future__ import annotations

import logging
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from app_paths import runtime_directory


LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)-7s %(name)s %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure_logging(environment: str) -> logging.Logger:
    """配置 UTC 日滚动日志文件，并返回项目根日志器。"""
    if environment not in {"production", "testnet"}:
        raise ValueError("日志环境必须是 production 或 testnet。")

    logger = logging.getLogger("binance_futures_bot")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    log_path = runtime_directory(environment) / "logs" / "application.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # 同一 GUI 进程可在停止后切换 production/testnet 再启动；移除旧环境
    # handler，避免日志被双写或写入错误环境。
    expected_path = str(log_path.resolve())
    for handler in tuple(logger.handlers):
        if isinstance(handler, TimedRotatingFileHandler) and handler.baseFilename != expected_path:
            logger.removeHandler(handler)
            handler.close()

    if not any(getattr(handler, "baseFilename", None) == expected_path for handler in logger.handlers):
        handler = TimedRotatingFileHandler(
            log_path, when="midnight", interval=1, backupCount=30, encoding="utf-8", utc=True
        )
        formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def get_logger(name: str) -> logging.Logger:
    """获取项目命名空间内的日志器。"""
    return logging.getLogger(f"binance_futures_bot.{name}")
