"""桌面程序入口：运行此文件打开 Binance Futures Bot 图形界面。"""

from __future__ import annotations

import sys
from pathlib import Path


SOURCE_DIRECTORY = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(SOURCE_DIRECTORY))

from app import main


if __name__ == "__main__":
    main()
