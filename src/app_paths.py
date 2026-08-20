"""应用数据目录:所有运行文件(配置、数据池、日志、台账)统一存放。

运行文件位于 <用户主目录>/.BinanceFuturesBot 下:
- config/scheduler.json          运行时配置(首次启动从项目模板复制)
- runtime/{production,testnet}/  数据池、日志、订单台账、本地持仓等运行产物

项目仓库中的 config/ 目录保留为 Git 跟踪的配置模板,供首次启动初始化。
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path


APP_DATA_DIRECTORY = Path.home() / ".BinanceFuturesBot"
CONFIG_DIRECTORY = APP_DATA_DIRECTORY / "config"
CONFIG_PATH = CONFIG_DIRECTORY / "scheduler.json"


def _project_root() -> Path:
    """项目根目录:开发时为仓库根,打包(exe)后为 PyInstaller 解压目录(_MEIPASS)。

    onefile 打包后模块 __file__ 指向临时解压目录,仓库内的 config 模板
    经 --add-data 一并解压到 _MEIPASS/config,因此此处按 frozen 状态区分。
    """
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path.cwd()))
    return Path(__file__).resolve().parent.parent


# 项目仓库内的配置模板(Git 跟踪),首次启动时复制到用户数据目录
PROJECT_CONFIG_TEMPLATE = _project_root() / "config" / "scheduler.json"


def runtime_directory(environment: str) -> Path:
    """返回指定环境的运行产物目录。"""
    return APP_DATA_DIRECTORY / "runtime" / environment


def ensure_app_config(config_path: Path = CONFIG_PATH) -> None:
    """首次启动初始化:创建用户数据目录,并把项目配置模板复制为运行时配置。

    仅当用户数据目录中尚无配置时执行;已有配置(含用户修改)不会被覆盖。
    自定义 --config 路径不参与模板复制,缺失时直接报错。
    """
    if config_path.is_file():
        return
    if config_path != CONFIG_PATH:
        raise FileNotFoundError(f"找不到配置:{config_path}")
    if not PROJECT_CONFIG_TEMPLATE.is_file():
        raise FileNotFoundError(f"找不到配置模板:{PROJECT_CONFIG_TEMPLATE}")
    config_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_CONFIG_TEMPLATE, config_path)
