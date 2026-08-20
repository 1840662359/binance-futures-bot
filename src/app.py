"""Binance Futures Bot 的桌面程序入口。"""

from __future__ import annotations

import calendar
import json
import logging
import math
import os
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

try:
    from PySide6.QtCore import QDate, QObject, Qt, QThread, QTimer, Signal
    from PySide6.QtGui import QColor, QFont
    from PySide6.QtWidgets import (
        QApplication, QComboBox, QDateEdit, QDialog, QFormLayout, QFrame,
        QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QMainWindow,
        QMessageBox, QPlainTextEdit, QPushButton, QScrollArea, QSizePolicy,
        QStackedWidget, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget,
        QHeaderView, QButtonGroup,
    )
except ImportError as exc:
    raise SystemExit("缺少 PySide6。请先运行：python -m pip install -r requirements.txt") from exc

from app_paths import ensure_app_config, runtime_directory
from kline_chart import KlineChartWidget, apply_color_style
from logging_utils import configure_logging
from pnl_tracker import aggregate_days, beijing_date_key, pnl_directory, read_pnl_records
from position_monitor import TRAILED_SIGNALS, get_active_monitor, load_positions, position_key
from scheduler import CONFIG_PATH, PoolScheduler, _history_timestamp, bucket_start, get_current_activity, load_config, run_scheduler
from secret_utils import get_secret


CARD_DEFINITIONS = (
    ("contract", "合约池", "contract_pool.json", "symbols", "#4F8CFF"),
    ("market", "市值池", "market_cap_pool.json", "symbols", "#8A6CFF"),
    ("trading", "24h 成交量池", "trading_pool.json", "symbols", "#16B8A6"),
    ("boxes", "箱体池", "box_pool_{structure_interval}.json", "boxes", "#F59E0B"),
    ("signals", "信号池", "signal_pool_{execution_interval}.json", "signals", "#EF5A78"),
)
CHINA_TIMEZONE = timezone(timedelta(hours=8))


def read_json_safely(path: Path) -> dict[str, Any]:
    """读取运行产物；文件尚未生成或损坏时返回空对象。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_config_atomically(data: dict[str, Any]) -> None:
    """原子保存受 Git 跟踪的配置文件。"""
    temporary = CONFIG_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, CONFIG_PATH)


# ---- 交易所字段枚举翻译(核对 Binance exchangeInfo 文档字段含义) ----
_STATUS_TRANSLATIONS = {"TRADING": "交易中", "BREAK": "熔断", "HALT": "暂停"}
_CONTRACT_TYPE_TRANSLATIONS = {"PERPETUAL": "永续合约", "CURRENT_QUARTER": "当季合约", "NEXT_QUARTER": "次季合约"}
_UNDERLYING_TYPE_TRANSLATIONS = {"COIN": "加密货币", "EQUITY": "股票", "COMMODITY": "大宗商品"}
_UNDERLYING_SUBTYPE_TRANSLATIONS = {
    "DEFI": "DeFi 板块", "HOT": "热门板块", "BSC": "BSC 链", "NFT": "NFT 板块", "PreMarket": "上市前 (PreMarket)",
}
_TIME_IN_FORCE_TRANSLATIONS = {
    "GTC": "一直有效", "IOC": "立即成交或取消", "FOK": "全部成交或取消", "GTX": "被动委托", "GTD": "指定日期有效",
}
_ORDER_TYPE_TRANSLATIONS = {
    "LIMIT": "限价单", "MARKET": "市价单", "STOP": "止损限价", "STOP_MARKET": "止损市价",
    "TAKE_PROFIT": "止盈限价", "TAKE_PROFIT_MARKET": "止盈市价", "TRAILING_STOP_MARKET": "移动止损",
}
_PERMISSION_TRANSLATIONS = {"GRID": "网格策略", "COPY": "复制交易", "RPI": "RPI 订单", "DCA": "定投策略"}
_MATCH_STATUS_TRANSLATIONS = {"matched": "已匹配", "unmatched": "未匹配"}
_MATCH_SOURCE_TRANSLATIONS = {"binance_futures_ticker": "币安合约 Ticker"}
LABEL_TRANSLATIONS: dict[str, dict[str, str]] = {
    "状态": _STATUS_TRANSLATIONS, "status": _STATUS_TRANSLATIONS,
    "合约状态": _STATUS_TRANSLATIONS,
    "合约类型": _CONTRACT_TYPE_TRANSLATIONS, "contractType": _CONTRACT_TYPE_TRANSLATIONS,
    "标的类型": _UNDERLYING_TYPE_TRANSLATIONS, "underlyingType": _UNDERLYING_TYPE_TRANSLATIONS,
    "标的子类型": _UNDERLYING_SUBTYPE_TRANSLATIONS, "underlyingSubType": _UNDERLYING_SUBTYPE_TRANSLATIONS,
    "支持有效期": _TIME_IN_FORCE_TRANSLATIONS, "timeInForce": _TIME_IN_FORCE_TRANSLATIONS,
    "支持订单类型": _ORDER_TYPE_TRANSLATIONS, "orderTypes": _ORDER_TYPE_TRANSLATIONS,
    "权限集合": _PERMISSION_TRANSLATIONS, "permissionSets": _PERMISSION_TRANSLATIONS,
    "匹配状态": _MATCH_STATUS_TRANSLATIONS,
    "匹配来源": _MATCH_SOURCE_TRANSLATIONS,
}


def format_value(value: Any) -> str:
    """将常见数值与时间戳格式化为适合界面阅读的文本。"""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int) and value > 1_000_000_000_000:
        return datetime.fromtimestamp(value / 1000, CHINA_TIMEZONE).strftime("%Y-%m-%d %H:%M")
    if isinstance(value, str) and "T" in value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(CHINA_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    if isinstance(value, str):
        # 交易所 ticker 等接口返回数字字符串(如 "665877042.965430"),解析后按数值美化
        try:
            parsed = float(value)
        except ValueError:
            parsed = None
        if parsed is not None and math.isfinite(parsed):
            value = parsed
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        absolute = abs(value)
        if absolute >= 100_000_000:
            return f"{value / 100_000_000:.2f}亿"
        if absolute >= 10_000:
            return f"{value / 10_000:.2f}万"
        if isinstance(value, float):
            return f"{value:.8g}"
        return f"{value:,}"
    return str(value)


def display_signal_type(signal_type: Any, direction: Any) -> str:
    """兼容历史英文信号并显示为限定的中文交易信号。"""
    if signal_type == "range_reversal":
        return "箱体内低吸" if direction == "long" else "箱体内高抛"
    if signal_type == "breakout_follow":
        return "上破箱体上沿" if direction == "long" else "—"
    return str(signal_type or "—")


def display_direction(direction: Any) -> str:
    """兼容历史英文方向并统一显示为中文。"""
    return {"long": "多", "short": "空"}.get(direction, str(direction or "—"))


def _sort_key_for(value: Any) -> Any:
    """计算单元格的排序键:数值列用原始数值,文本列用字符串,空值排最后。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return value
        if math.isfinite(parsed):
            return parsed
        return value
    return str(value)


class SortableTableItem(QTableWidgetItem):
    """携带原始排序键的表格项,数值列按数值而非显示文本排序。"""

    def __init__(self, text: str, sort_key: Any = None) -> None:
        super().__init__(text)
        self.sort_key = sort_key

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, SortableTableItem):
            left, right = self.sort_key, other.sort_key
            if left is None and right is None:
                return False
            if left is None:
                return False  # 空值排最后
            if right is None:
                return True
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                return left < right
            return str(left) < str(right)
        return super().__lt__(other)


# 当前已应用的涨跌配色风格(与界面控件/配置即时同步)
_CURRENT_COLOR_STYLE = "cn"


def profit_colors() -> tuple[str, str]:
    """按当前配色风格返回 (盈利色, 亏损色):cn=红盈绿亏,intl=绿盈红亏。"""
    if _CURRENT_COLOR_STYLE == "intl":
        return "#2FA36B", "#E04A4A"
    return "#E04A4A", "#2FA36B"


def format_metric(label: str, value: Any) -> str:
    """依据指标含义翻译枚举并格式化百分比、倍率、金额等数据。"""
    if value is None:
        return "—"
    translations = LABEL_TRANSLATIONS.get(label)
    if translations:
        translated = translations.get(str(value))
        if translated is not None:
            return translated
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, str):
        # 交易所 ticker 等接口返回数字字符串(如 "1.004" = 1.004%),解析后按数值处理
        try:
            parsed = float(value)
        except ValueError:
            parsed = None
        if parsed is not None and math.isfinite(parsed):
            value = parsed
    if isinstance(value, (int, float)):
        # Binance/CoinGecko 的涨跌幅等字段值已是百分数(如 1.004 = 1.004%),直接加 %
        if any(token in label for token in ("涨跌幅", "回撤率", "涨幅", "变动率")):
            return f"{value:g}%"
        # 项目自身生成的指标为小数比例(如 OI 变化 0.012766 = 1.28%),乘 100 显示
        if any(token in label for token in ("OI 变化", "宽度", "漂移", "边沿缓冲", "触发保护", "保护", "强平费率")):
            return f"{value * 100:g}%"
        if any(token in label for token in ("倍率", "倍数")):
            return f"{value:.2f}x"
    return format_value(value)


class CloseWorkerBridge(QObject):
    """平仓 worker 线程 → GUI 线程的信号桥。

    worker 是纯 Python 线程(无 Qt 事件循环),QTimer.singleShot 不会触发,
    必须通过 Qt 信号跨线程投递(Qt 自动 QueuedConnection 到 GUI 线程)。
    """

    finished = Signal(list)


class LiveKlineBridge(QObject):
    """实时看盘页 worker/WS 线程 → GUI 线程的信号桥。

    REST 历史加载与 K 线流回调均在后台线程,经 Qt 信号投递到 GUI 线程
    更新图表(与 CloseWorkerBridge 同模式)。
    """

    fetched = Signal(object)  # (symbol, interval, raw_klines | None):REST 历史加载完成
    kline = Signal(object)    # 实时 K 线 k 字典:WebSocket 推送


class LogMessageBridge(QObject):
    """日志 handler → GUI 线程的信号桥(日志由后台线程写入,跨线程投递)。"""

    message = Signal(str)


class GuiLogHandler(logging.Handler):
    """将 binance_futures_bot 日志实时转发到主界面日志台。"""

    def __init__(self, bridge: LogMessageBridge) -> None:
        super().__init__()
        self._bridge = bridge
        # GUI 日志台统一显示北京时间(界面口径),文件日志仍为 UTC
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._bridge.message.emit(self.format(record))
        except Exception:
            pass  # 日志转发失败不影响业务


class SchedulerWorker(QObject):
    """在后台线程运行循环调度，保证图形界面流畅。"""

    completed = Signal()
    failed = Signal(str)

    def run(self) -> None:
        try:
            scheduler = PoolScheduler(load_config(CONFIG_PATH))
            run_scheduler(
                scheduler,
                once=False,
                should_stop=QThread.currentThread().isInterruptionRequested,
            )
        except BaseException as exc:
            # 捕获全部异常(含 SystemExit 等),防止异常传播到 Qt C++ 层触发 abort 崩溃
            environment = read_json_safely(CONFIG_PATH).get("environment", "production")
            configure_logging(environment).exception("界面触发初始化刷新失败")
            self.failed.emit(str(exc))
            return
        self.completed.emit()


class PoolCard(QFrame):
    """概览页中的可点击统计卡片。"""

    clicked = Signal(str)

    def __init__(self, key: str, title: str, color: str) -> None:
        super().__init__()
        self.key = key
        self.setObjectName("poolCard")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setStyleSheet(f"QFrame#poolCard {{ border-top: 5px solid {color}; }}")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        self.title = QLabel(title, objectName="cardTitle")
        self.count = QLabel("—", objectName="cardCount")
        self.detail = QLabel("尚未生成", objectName="cardDetail")
        # 下一调度行默认隐藏,仅数据池卡片显示(账户持仓卡片不展示)
        self.schedule = QLabel("下一调度：—", objectName="cardDetail")
        self.schedule.hide()
        # 文本不参与水平 sizeHint:卡片宽度完全由布局分配决定,
        # 避免标题长短不一导致各分类卡片宽度不一致
        for label in (self.title, self.count, self.detail, self.schedule):
            label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        layout.addWidget(self.title)
        layout.addWidget(self.count)
        layout.addWidget(self.detail)
        layout.addWidget(self.schedule)

    def update_data(self, count: int, timestamp: str | None) -> None:
        self.count.setText(f"{count:,}")
        self.detail.setText(f"数据时点：{format_value(timestamp)}")

    def set_schedule(self, text: str) -> None:
        """设置并显示卡片调度状态文本(下一调度时刻或当前正在运行)。"""
        self.schedule.setText(text)
        self.schedule.show()

    def mousePressEvent(self, event: Any) -> None:
        self.clicked.emit(self.key)
        super().mousePressEvent(event)


class MainWindow(QMainWindow):
    """配置、概览和数据池详情页。"""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Binance Futures Bot")
        self.resize(1420, 900)
        self.cards: dict[str, PoolCard] = {}
        self.pool_data: dict[str, tuple[str, list[dict[str, Any]]]] = {}
        self.pool_file_fingerprints: tuple[tuple[str, int], ...] = ()
        self.utc_clocks: list[QLabel] = []
        self.worker_thread: QThread | None = None
        self.close_requested = False
        self._history_retention_days = 7
        self.viewing_history = False
        self._history_entries: list = []
        # 历史快照独立保存(不覆盖实时 pool_data):正常返回即可回到实时数据
        self._history_snapshot_key = ""
        self._history_snapshot_items: list[dict[str, Any]] = []
        # 盈亏统计:记录缓存(按文件指纹增量刷新)与当前选中日期
        self._pnl_records: list[dict[str, Any]] = []
        self._pnl_day_totals: dict[str, dict[str, float | int]] = {}
        # None 哨兵:与「无文件」指纹 () 区分,保证首次刷新必定加载
        self._pnl_fingerprint: tuple | None = None
        self._pnl_selected_day: date | None = None
        # 账户快照由监控线程提供；保留最后一份结构完整的快照，瞬时读取异常时不清空界面。
        self._account_snapshot_cache: dict[str, Any] | None = None
        self._last_account_content_signature: tuple | None = None
        # 实时看盘页状态
        self.live_view_open = False
        self.live_symbol = ""
        self.live_position_key = ""
        self.live_position_closed = False
        self.live_loading_initial = False
        self.live_return_page = 3
        self.live_kline_stream: Any = None
        self._live_kline_bridge = LiveKlineBridge()
        self._live_kline_bridge.fetched.connect(self._on_live_kline_fetched)
        self._live_kline_bridge.kline.connect(self._apply_live_kline)
        self.allow_close = False
        self._build_ui()
        self.load_configuration()
        self.refresh_dashboard()
        self.refresh_schedule_cards()
        self.clock_timer = QTimer(self)
        self.clock_timer.timeout.connect(self.refresh_clock)
        self.clock_timer.start(1000)
        self.data_timer = QTimer(self)
        self.data_timer.timeout.connect(self.refresh_data_if_changed)
        self.data_timer.start(2000)
        # 账户持仓页:1 秒级流数据刷新(仅页面可见时执行,退出即停止)
        self.account_timer = QTimer(self)
        self.account_timer.timeout.connect(self.refresh_account_view)
        self.account_timer.start(1000)
        self._last_account_snapshot_at: float | None = None
        self.refresh_clock()

    def _build_ui(self) -> None:
        root = QWidget(objectName="root")
        self.setCentralWidget(root)
        layout = QHBoxLayout(root)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self._build_sidebar())

        # 主内容区:顶部状态条 + 页面栈
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(0)
        status_bar = QHBoxLayout()
        status_bar.setContentsMargins(20, 10, 20, 0)
        status_bar.addStretch(1)
        self.program_status = QLabel("● 已停止", objectName="programStatus")
        status_bar.addWidget(self.program_status)
        content_layout.addLayout(status_bar)
        self.pages = QStackedWidget()
        self.pages.addWidget(self._build_overview_page())
        self.pages.addWidget(self._build_detail_page())
        self.pages.addWidget(self._build_symbol_page())
        self.pages.addWidget(self._build_account_page())
        self.pages.addWidget(self._build_history_page())
        self.pages.addWidget(self._build_live_page())
        self.pages.addWidget(self._build_pnl_page())
        content_layout.addWidget(self.pages, 1)
        # 主界面右下方日志输出台:实时显示程序运行日志
        content_layout.addWidget(self._build_log_panel())
        layout.addWidget(content, 1)
        self._apply_style()
        # 日志桥:后台线程的日志经 Qt 信号实时投递到日志台
        self._log_bridge = LogMessageBridge()
        self._log_bridge.message.connect(self._append_log_line)
        logging.getLogger("binance_futures_bot").addHandler(GuiLogHandler(self._log_bridge))

    def _build_log_panel(self) -> QWidget:
        """日志输出台:主界面右下方,实时显示程序运行日志(后台线程经信号桥投递)。"""
        panel = QFrame(objectName="logPanel")
        panel.setFixedHeight(210)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 8, 12, 10)
        layout.setSpacing(6)
        header = QHBoxLayout()
        header.addWidget(QLabel("运行日志", objectName="logTitle"))
        header.addStretch(1)
        self.log_clear_button = QPushButton("清空", objectName="lightActionButton")
        self.log_clear_button.setFixedWidth(64)
        self.log_clear_button.setFixedHeight(26)
        header.addWidget(self.log_clear_button)
        layout.addLayout(header)
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        # 最多保留 1000 行,自动丢弃最早日志,避免长时间运行内存膨胀
        self.log_text.setMaximumBlockCount(1000)
        self.log_clear_button.clicked.connect(self.log_text.clear)
        layout.addWidget(self.log_text, 1)
        return panel

    def _append_log_line(self, line: str) -> None:
        """将日志行追加到日志台并自动滚动到底部。"""
        self.log_text.appendPlainText(line)
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())

    def _build_sidebar(self) -> QScrollArea:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        # 侧边栏宽度自适应:小屏收窄、大屏放宽,避免固定宽度导致布局错乱
        scroll.setMinimumWidth(320)
        scroll.setMaximumWidth(480)
        panel = QWidget(objectName="sidebar")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(24, 26, 24, 26)
        layout.setSpacing(16)
        layout.addWidget(QLabel("策略控制台", objectName="sidebarTitle"))

        self.environment_buttons = QButtonGroup(self)
        environment_switch = QFrame(objectName="environmentSwitch")
        environment_layout = QHBoxLayout(environment_switch)
        environment_layout.setContentsMargins(3, 3, 3, 3)
        environment_layout.setSpacing(3)
        self.testnet_button = QPushButton("模拟盘")
        self.live_button = QPushButton("实盘")
        for button, environment in ((self.testnet_button, "testnet"), (self.live_button, "production")):
            button.setCheckable(True)
            button.setProperty("environment", environment)
            button.toggled.connect(self.update_api_key_status)
            self.environment_buttons.addButton(button)
            environment_layout.addWidget(button)
        self.proxy_buttons = QButtonGroup(self)
        proxy_switch = QFrame(objectName="environmentSwitch")
        proxy_layout = QHBoxLayout(proxy_switch)
        proxy_layout.setContentsMargins(3, 3, 3, 3)
        proxy_layout.setSpacing(3)
        self.proxy_off_button = QPushButton("关闭")
        self.proxy_on_button = QPushButton("开启")
        for button, enabled in ((self.proxy_off_button, False), (self.proxy_on_button, True)):
            button.setCheckable(True)
            button.setProperty("proxy_enabled", enabled)
            self.proxy_buttons.addButton(button)
            proxy_layout.addWidget(button)
        self.proxy_url = QLineEdit()
        self.proxy_url.setPlaceholderText("http://127.0.0.1:7892")
        layout.addWidget(self._form_group("连接", [
            ("运行环境", environment_switch), ("网络代理", proxy_switch), ("代理地址", self.proxy_url),
        ]))

        # 市值/成交量改为范围过滤:下限 ~ 上限(上限/下限填 0 表示不限)
        self.market_cap_floor = self._million_input("0")
        self.market_cap = self._million_input("100")
        self.market_cap.setPlaceholderText("0=不限")
        self.volume = self._million_input("1")
        self.volume_cap = self._million_input("0")
        self.volume_cap.setPlaceholderText("0=不限")
        self.structure_interval = QComboBox()
        self.execution_interval = QComboBox()
        self.structure_interval.addItems(["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"])
        self.execution_interval.addItems(["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"])
        layout.addWidget(self._form_group("筛选与策略", [
            ("市值范围 (百万 USD)", self._range_input(self.market_cap_floor, self.market_cap)),
            ("成交量范围 (百万 USDT)", self._range_input(self.volume, self.volume_cap)),
            ("箱体 K 线周期", self.structure_interval), ("信号 K 线周期", self.execution_interval),
        ]))
        self.trading_buttons = QButtonGroup(self)
        trading_switch = QFrame(objectName="environmentSwitch")
        trading_layout = QHBoxLayout(trading_switch)
        trading_layout.setContentsMargins(3, 3, 3, 3)
        trading_layout.setSpacing(3)
        self.trading_off_button = QPushButton("关闭")
        self.trading_on_button = QPushButton("开启")
        for button, enabled in ((self.trading_off_button, False), (self.trading_on_button, True)):
            button.setCheckable(True)
            button.setProperty("trading_enabled", enabled)
            self.trading_buttons.addButton(button)
            trading_layout.addWidget(button)
        # 方向模式切换:只做多/只做空/多空都做(样式与交易开关一致),
        # 决定下单模块处理信号时是否实际开仓
        self.direction_buttons = QButtonGroup(self)
        direction_switch = QFrame(objectName="environmentSwitch")
        direction_layout = QHBoxLayout(direction_switch)
        direction_layout.setContentsMargins(3, 3, 3, 3)
        direction_layout.setSpacing(3)
        self.direction_long_button = QPushButton("只做多")
        self.direction_short_button = QPushButton("只做空")
        self.direction_both_button = QPushButton("多空都做")
        for button, mode in (
            (self.direction_long_button, "long"),
            (self.direction_short_button, "short"),
            (self.direction_both_button, "both"),
        ):
            button.setCheckable(True)
            button.setProperty("direction_mode", mode)
            self.direction_buttons.addButton(button)
            direction_layout.addWidget(button)
        # 突破策略模式:不做/向上突破/向下突破/都做(样式与交易开关一致),
        # 决定信号池是否产出突破类信号(上破/下破箱体);仅影响信号产生,不随交易开关隐藏
        self.breakout_buttons = QButtonGroup(self)
        breakout_switch = QFrame(objectName="environmentSwitch")
        breakout_layout = QHBoxLayout(breakout_switch)
        breakout_layout.setContentsMargins(3, 3, 3, 3)
        breakout_layout.setSpacing(3)
        self.breakout_none_button = QPushButton("都不产生")
        self.breakout_up_button = QPushButton("向上突破")
        self.breakout_down_button = QPushButton("向下突破")
        self.breakout_both_button = QPushButton("都产生")
        for button, mode in (
            (self.breakout_none_button, "none"),
            (self.breakout_up_button, "up"),
            (self.breakout_down_button, "down"),
            (self.breakout_both_button, "both"),
        ):
            button.setCheckable(True)
            button.setProperty("breakout_mode", mode)
            self.breakout_buttons.addButton(button)
            breakout_layout.addWidget(button)
        # 震荡策略模式:不做/低吸/高抛/都做(样式与交易开关一致),
        # 决定信号池是否产出震荡类信号(箱体内低吸/高抛);仅影响信号产生,不随交易开关隐藏
        self.range_buttons = QButtonGroup(self)
        range_switch = QFrame(objectName="environmentSwitch")
        range_layout = QHBoxLayout(range_switch)
        range_layout.setContentsMargins(3, 3, 3, 3)
        range_layout.setSpacing(3)
        self.range_none_button = QPushButton("都不产生")
        self.range_buy_button = QPushButton("低吸")
        self.range_sell_button = QPushButton("高抛")
        self.range_both_button = QPushButton("都产生")
        for button, mode in (
            (self.range_none_button, "none"),
            (self.range_buy_button, "buy"),
            (self.range_sell_button, "sell"),
            (self.range_both_button, "both"),
        ):
            button.setCheckable(True)
            button.setProperty("range_mode", mode)
            self.range_buttons.addButton(button)
            range_layout.addWidget(button)
        self.risk_input = self._million_input("1")
        self.risk_input.setPlaceholderText("0-5")
        self.max_positions_input = self._million_input("30")
        self.max_positions_input.setPlaceholderText("1-200")
        # 盈亏全平开关:账户未实现盈亏(以钱包余额为基准)达到阈值时自动全平全部仓位
        self.auto_close_buttons = QButtonGroup(self)
        auto_close_switch = QFrame(objectName="environmentSwitch")
        auto_close_layout = QHBoxLayout(auto_close_switch)
        auto_close_layout.setContentsMargins(3, 3, 3, 3)
        auto_close_layout.setSpacing(3)
        self.auto_close_off_button = QPushButton("关闭")
        self.auto_close_on_button = QPushButton("开启")
        for button, enabled in ((self.auto_close_off_button, False), (self.auto_close_on_button, True)):
            button.setCheckable(True)
            button.setProperty("auto_close_enabled", enabled)
            self.auto_close_buttons.addButton(button)
            auto_close_layout.addWidget(button)
        self.auto_close_profit_input = self._million_input("10")
        self.auto_close_profit_input.setPlaceholderText("例如：10")
        self.auto_close_loss_input = self._million_input("5")
        self.auto_close_loss_input.setPlaceholderText("例如：5")
        # 交易开关关闭时隐藏的交易执行配置(涨跌配色为显示偏好,始终可见);
        # 盈亏全平开关关闭时隐藏 % 阈值输入,两者都开启时才展示并生效。
        # 突破/震荡策略模式只决定信号产出(与是否下单无关),不随交易开关隐藏;
        # 交易开关关闭时程序仍按模式生成信号池,只是不调用下单模块。
        self._trading_config_widgets = [
            direction_switch,
            self.risk_input, self.max_positions_input, auto_close_switch,
        ]
        self._auto_close_pct_widgets = [self.auto_close_profit_input, self.auto_close_loss_input]
        self.trading_off_button.toggled.connect(self._update_trading_config_visibility)
        self.auto_close_off_button.toggled.connect(self._update_auto_close_visibility)
        # 涨跌配色开关(样式与交易开关一致)
        self.color_style_buttons = QButtonGroup(self)
        color_style_switch = QFrame(objectName="environmentSwitch")
        color_style_layout = QHBoxLayout(color_style_switch)
        color_style_layout.setContentsMargins(3, 3, 3, 3)
        color_style_layout.setSpacing(3)
        self.color_cn_button = QPushButton("红涨绿跌")
        self.color_intl_button = QPushButton("红跌绿涨")
        for button, style in ((self.color_cn_button, "cn"), (self.color_intl_button, "intl")):
            button.setCheckable(True)
            button.setProperty("color_style", style)
            self.color_style_buttons.addButton(button)
            color_style_layout.addWidget(button)
        # 信号产生:突破/震荡策略模式决定信号池产出哪些信号(与是否交易解耦),
        # 排版为「一行标题 + 一行开关」,不随交易开关隐藏
        signal_group = QGroupBox("信号产生")
        signal_layout = QVBoxLayout(signal_group)
        signal_layout.setSpacing(8)
        for title, switch in (
            ("突破信号", breakout_switch),
            ("震荡信号", range_switch),
        ):
            title_label = QLabel(title)
            title_label.setStyleSheet("color: #AFC3E2; font-size: 12px; font-weight: 600;")
            signal_layout.addWidget(title_label)
            signal_layout.addWidget(switch)
        layout.addWidget(signal_group)

        layout.addWidget(self._form_group("交易执行", [
            ("交易开关", trading_switch),
            ("方向模式", direction_switch),
            ("单笔风险 (%)", self.risk_input),
            ("最大仓位数 (1-200)", self.max_positions_input),
            ("盈亏全平", auto_close_switch),
            ("盈利平仓 (%)", self.auto_close_profit_input),
            ("亏损平仓 (%)", self.auto_close_loss_input),
        ]))
        # 个性化配置:界面显示偏好(涨跌配色),与交易执行分离,不受交易开关控制,始终可见
        layout.addWidget(self._form_group("个性化配置", [
            ("涨跌配色", color_style_switch),
        ]))
        # 历史数据配置
        self.history_retention_input = self._million_input("7")
        self.history_retention_input.setPlaceholderText("1-365")
        self.history_dir_input = QLineEdit()
        self.history_dir_input.setPlaceholderText("留空 = runtime/环境/history")
        self.clean_history_button = QPushButton("清理历史数据", objectName="lightActionButton")
        self.clean_history_button.clicked.connect(self.open_history_cleaner)
        history_cleaner_row = QHBoxLayout()
        history_cleaner_row.addWidget(self.clean_history_button)
        layout.addWidget(self._form_group("历史数据", [
            ("保留时长 (天)", self.history_retention_input),
            ("保存位置", self.history_dir_input),
        ]))
        layout.addLayout(history_cleaner_row)
        self.api_key_status = QLabel("API 密钥：未设置", objectName="muted")
        layout.addWidget(self.api_key_status)
        self.start_button = QPushButton("启动程序", objectName="primaryButton")
        self.start_button.clicked.connect(self.start_program)
        self.stop_button = QPushButton("停止程序", objectName="stopButton")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_program)
        layout.addWidget(self.start_button)
        layout.addWidget(self.stop_button)
        layout.addStretch(1)
        scroll.setWidget(panel)
        return scroll

    def _build_overview_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(22)
        layout.addLayout(self._header("数据概览", "点击可查看详细数据", False))

        # 市场行情:各数据池卡片(合约/市值/交易/箱体/信号)
        layout.addWidget(QLabel("市场行情", objectName="sectionTitle"))
        grid = QGridLayout()
        grid.setSpacing(14)
        for index, (key, title, _, _, color) in enumerate(CARD_DEFINITIONS):
            card = PoolCard(key, title, color)
            card.clicked.connect(self.open_pool_page)
            self.cards[key] = card
            grid.addWidget(card, index // 3, index % 3)

        # 资产信息:标题横跨整行,下方为账户持仓与盈亏统计卡片;
        # 与池卡片同一网格(同列宽,大小精确一致)
        asset_title = QLabel("资产信息", objectName="sectionTitle")
        grid.addWidget(asset_title, 2, 0, 1, 3)
        account_card = PoolCard("account", "账户持仓", "#2FA36B")
        account_card.clicked.connect(self.open_account_page)
        self.cards["account"] = account_card
        pnl_card = PoolCard("pnl", "盈亏统计", "#B45309")
        pnl_card.clicked.connect(self.open_pnl_page)
        self.cards["pnl"] = pnl_card
        grid.addWidget(account_card, 3, 0)
        grid.addWidget(pnl_card, 3, 1)
        layout.addLayout(grid)

        layout.addStretch(1)
        return page

    def _build_detail_page(self) -> QWidget:
        """页 1:数据池列表(点击行进入币详情页)。"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(18)
        header = self._header("数据池详情", "选择交易对后查看 K 线与指标", True)
        self.back_button.clicked.connect(self.back_to_overview)
        self.history_button = QPushButton("历史", objectName="backButton")
        self.history_button.clicked.connect(self.toggle_history_mode)
        header.addWidget(self.history_button)
        layout.addLayout(header)
        self.pool_summary = QLabel(objectName="poolSummary")
        layout.addWidget(self.pool_summary)
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.cellClicked.connect(self.open_symbol_page)
        layout.addWidget(self.table, 1)
        return page

    def _build_symbol_page(self) -> QWidget:
        """页 2:币详情(K 线图 + 结构化指标)。"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(18)
        header = QHBoxLayout()
        self.symbol_back_button = QPushButton("← 返回列表", objectName="backButton")
        self.symbol_back_button.clicked.connect(self.close_symbol_page)
        header.addWidget(self.symbol_back_button)
        # 持仓中的交易对可跳转实时看盘(仅持仓时显示)
        self.live_jump_button = QPushButton("实时看盘", objectName="lightActionButton")
        self.live_jump_button.clicked.connect(lambda: self.open_live_symbol_view(self.current_symbol, 2))
        self.live_jump_button.setVisible(False)
        header.addWidget(self.live_jump_button)
        labels = QVBoxLayout()
        self.symbol_page_title = QLabel(objectName="pageTitle")
        self.symbol_page_subtitle = QLabel(objectName="muted")
        labels.addWidget(self.symbol_page_title)
        labels.addWidget(self.symbol_page_subtitle)
        header.addLayout(labels)
        header.addStretch(1)
        utc_clock = QLabel(objectName="utcClock")
        self.utc_clocks.append(utc_clock)
        header.addWidget(utc_clock)
        layout.addLayout(header)

        self.kline_chart = KlineChartWidget()
        # K 线图高度弹性:小屏压缩、大屏扩展,布局随窗口尺寸自适应
        self.kline_chart.setMinimumHeight(240)
        self.kline_chart.setMaximumHeight(460)
        layout.addWidget(self.kline_chart)

        self.metrics_scroll = QScrollArea()
        self.metrics_scroll.setWidgetResizable(True)
        self.metrics_scroll.setMinimumHeight(240)
        self.metrics_content = QWidget()
        self.metrics_layout = QGridLayout(self.metrics_content)
        self.metrics_layout.setContentsMargins(6, 6, 6, 6)
        self.metrics_layout.setSpacing(12)
        self.metrics_scroll.setWidget(self.metrics_content)
        layout.addWidget(self.metrics_scroll, 1)
        return page

    def _header(self, title: str, subtitle: str, back: bool) -> QHBoxLayout:
        header = QHBoxLayout()
        if back:
            self.back_button = QPushButton("← 返回概览", objectName="backButton")
            header.addWidget(self.back_button)
        labels = QVBoxLayout()
        title_label = QLabel(title, objectName="pageTitle")
        subtitle_label = QLabel(subtitle, objectName="muted")
        labels.addWidget(title_label)
        labels.addWidget(subtitle_label)
        if back:
            self.detail_page_title = title_label
            self.detail_page_subtitle = subtitle_label
        header.addLayout(labels)
        header.addStretch(1)
        utc_clock = QLabel(objectName="utcClock")
        self.utc_clocks.append(utc_clock)
        header.addWidget(utc_clock)
        return header

    @staticmethod
    def _million_input(value: str) -> QLineEdit:
        """创建以百万为单位的纯数字阈值输入框。"""
        field = QLineEdit(value)
        field.setPlaceholderText("例如：100")
        return field

    @staticmethod
    def _range_input(lower: QLineEdit, upper: QLineEdit) -> QWidget:
        """组合「下限 ~ 上限」范围输入行(两个输入框 + 波浪号分隔)。"""
        box = QWidget()
        layout = QHBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        layout.addWidget(lower, 1)
        layout.addWidget(QLabel("~"), 0)
        layout.addWidget(upper, 1)
        return box

    @staticmethod
    def _form_group(title: str, rows: list[tuple[str, QWidget]]) -> QGroupBox:
        group = QGroupBox(title)
        form = QFormLayout(group)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)
        for label, widget in rows:
            form.addRow(label, widget)
        return group

    def _apply_style(self) -> None:
        self.setStyleSheet("""
            QWidget#root { background: #F3F6FC; color: #182742; font-family: 'Microsoft YaHei UI'; }
            QWidget#sidebar { background: #10213D; color: #DCE8FA; }
            QWidget#sidebar QLabel, QWidget#sidebar QCheckBox { color: #DCE8FA; }
            QWidget#sidebar QLabel#muted { color: #90A4C4; }
            QLabel#sidebarTitle { color: #FFFFFF; font-size: 25px; font-weight: 700; }
            QLabel#pageTitle { font-size: 27px; font-weight: 700; } QLabel#muted { color: #71809A; font-size: 12px; }
            QGroupBox { border: 1px solid #355274; border-radius: 10px; margin-top: 12px; padding: 13px 10px 10px; color: #E7EEFC; font-weight: 600; }
            QGroupBox::title { left: 11px; padding: 0 4px; }
            QLineEdit, QComboBox, QDoubleSpinBox { background: #F9FBFF; border: 1px solid #C4D2E6; border-radius: 7px; min-height: 32px; padding: 0 8px; color: #182742; }
            QFrame#environmentSwitch { background: #09182E; border: 1px solid #355274; border-radius: 9px; }
            QFrame#environmentSwitch QPushButton { min-height: 30px; padding: 0 10px; background: transparent; border: 0; color: #AFC3E2; border-radius: 7px; }
            QFrame#environmentSwitch QPushButton:checked { background: #3D7CFF; color: #FFFFFF; font-weight: 700; }
            QWidget#sidebar QPushButton { min-height: 40px; border-radius: 10px; padding: 0 14px; font-weight: 600; }
            QFrame#environmentSwitch QPushButton { min-height: 30px; padding: 0 10px; background: transparent; border: 0; color: #AFC3E2; border-radius: 7px; }
            QFrame#environmentSwitch QPushButton:checked { background: #3D7CFF; color: #FFFFFF; font-weight: 700; }
            QPushButton#secondaryButton { background: transparent; border: 1px solid #40678F; color: #DCE8FA; }
            QPushButton#secondaryButton:hover { background: #193457; border-color: #6C98C7; }
            QPushButton#primaryButton { background: #3D7CFF; border: 1px solid #5E95FF; color: white; }
            QPushButton#primaryButton:hover { background: #2466ED; border-color: #89B1FF; }
            QPushButton#stopButton { background: transparent; border: 1px solid #C66172; color: #FFB9C4; }
            QPushButton#stopButton:hover { background: #54263A; border-color: #EF7E92; }
            QPushButton:disabled { background: #263A59; border-color: #263A59; color: #7184A0; }
            QLabel#programStatus { background: #0B1930; border: 1px solid #355274; border-radius: 9px; color: #B9CBE7; padding: 11px 13px; font-weight: 600; }
            QPushButton#backButton { color: #2459D7; background: #E8EFFD; border: 0; padding: 8px 12px; }
            QFrame#poolCard { background: #FFFFFF; border-radius: 12px; min-height: 118px; } QFrame#poolCard:hover { background: #F5F8FF; }
            QLabel#cardTitle { color: #6D7890; font-size: 14px; } QLabel#cardCount { font-size: 31px; font-weight: 700; } QLabel#cardDetail { color: #74829A; font-size: 12px; }
            QLabel#utcClock { color: #1C5CDC; background: #E7F0FF; border-radius: 9px; padding: 10px 14px; font-weight: 600; }
            QLabel#poolSummary { color: #52627D; background: #FFFFFF; border-radius: 9px; padding: 10px 14px; }
            QTableWidget { background: white; border: 1px solid #E0E6F0; border-radius: 10px; gridline-color: #EEF2F8; } QHeaderView::section { background: #F6F8FC; border: 0; padding: 9px; color: #586984; font-weight: 600; }
            QTableWidget::item { padding: 7px; } QTableWidget::item:hover { background: #EEF3FB; } QTableWidget::item:selected { background: #E8EFFD; color: #15223B; }
            QScrollArea { border: 0; } QFrame#metricPanel { background: #FFFFFF; border: 1px solid #DDE7F5; border-radius: 10px; }
            QFrame#quotePanel { background: #10213D; border-radius: 12px; } QLabel#quoteSymbol { color: #FFFFFF; font-size: 27px; font-weight: 700; } QLabel#quotePrice { color: #75A7FF; font-size: 30px; font-weight: 700; } QLabel#quoteMeta { color: #B7CAE8; font-size: 12px; }
            QLabel#metricTitle { color: #61708B; font-size: 12px; } QLabel#metricValue { color: #172642; font-size: 17px; font-weight: 700; }
            QLabel#sectionTitle { color: #274C7F; font-size: 15px; font-weight: 700; padding: 4px 1px; }
            QPushButton#lightActionButton { background: #3D7CFF; color: #FFFFFF; border: 0; border-radius: 7px; padding: 6px 18px; font-weight: 600; min-height: 22px; }
            QPushButton#lightActionButton:hover { background: #2466ED; }
            QPushButton#lightActionButton:disabled { background: #C4D2E6; color: #F0F5FF; }
            QFrame#logPanel { background: #F7F9FD; border: 1px solid #DDE7F5; border-radius: 10px; }
            QLabel#logTitle { color: #274C7F; font-size: 13px; font-weight: 700; }
            QFrame#logPanel QPlainTextEdit { background: #FFFFFF; border: 1px solid #E0E6F0; border-radius: 8px; color: #33415C; font-family: 'Consolas'; font-size: 12px; }
        """)

    def load_configuration(self) -> None:
        # 首次运行初始化用户数据目录(从项目模板复制配置),防止读到空配置
        ensure_app_config()
        data = read_json_safely(CONFIG_PATH)
        selected_environment = data.get("environment", "testnet")
        self.testnet_button.setChecked(selected_environment == "testnet")
        self.live_button.setChecked(selected_environment == "production")
        self.proxy_off_button.setChecked(not bool(data.get("proxy_enabled", False)))
        self.proxy_on_button.setChecked(bool(data.get("proxy_enabled", False)))
        self.proxy_url.setText(str(data.get("proxy_url", "http://127.0.0.1:7892")))
        self.market_cap.setText(str(data.get("market_cap_threshold_usd", 100_000_000) / 1_000_000).rstrip("0").rstrip("."))
        self.market_cap_floor.setText(str(data.get("market_cap_floor_usd", 0) / 1_000_000).rstrip("0").rstrip("."))
        self.volume.setText(str(data.get("minimum_quote_volume_usdt", 1_000_000) / 1_000_000).rstrip("0").rstrip("."))
        self.volume_cap.setText(str(data.get("maximum_quote_volume_usdt", 0) / 1_000_000).rstrip("0").rstrip("."))
        self.structure_interval.setCurrentText(str(data.get("structure_interval", "1h")))
        self.execution_interval.setCurrentText(str(data.get("execution_interval", "15m")))
        self.trading_off_button.setChecked(not bool(data.get("trading_enabled", False)))
        self.trading_on_button.setChecked(bool(data.get("trading_enabled", False)))
        # 方向模式:旧配置缺键时按默认「多空都做」处理
        selected_direction_mode = str(data.get("direction_mode", "both"))
        self.direction_long_button.setChecked(selected_direction_mode == "long")
        self.direction_short_button.setChecked(selected_direction_mode == "short")
        self.direction_both_button.setChecked(selected_direction_mode == "both")
        # 突破/震荡策略模式:旧配置缺键时按默认「都做」处理
        selected_breakout_mode = str(data.get("breakout_mode", "both"))
        self.breakout_none_button.setChecked(selected_breakout_mode == "none")
        self.breakout_up_button.setChecked(selected_breakout_mode == "up")
        self.breakout_down_button.setChecked(selected_breakout_mode == "down")
        self.breakout_both_button.setChecked(selected_breakout_mode == "both")
        selected_range_mode = str(data.get("range_mode", "both"))
        self.range_none_button.setChecked(selected_range_mode == "none")
        self.range_buy_button.setChecked(selected_range_mode == "buy")
        self.range_sell_button.setChecked(selected_range_mode == "sell")
        self.range_both_button.setChecked(selected_range_mode == "both")
        # 盈亏自动全平:旧配置缺键时开关默认关闭,阈值取默认值
        self.auto_close_off_button.setChecked(not bool(data.get("auto_close_enabled", False)))
        self.auto_close_on_button.setChecked(bool(data.get("auto_close_enabled", False)))
        try:
            self.auto_close_profit_input.setText(f"{float(data.get('auto_close_profit_pct', 10)):g}")
            self.auto_close_loss_input.setText(f"{float(data.get('auto_close_loss_pct', 5)):g}")
        except (TypeError, ValueError):
            pass
        try:
            self.risk_input.setText(f"{float(data.get('risk_per_trade_pct', 0.01)) * 100:g}")
        except (TypeError, ValueError):
            self.risk_input.setText("1")
        self.max_positions_input.setText(str(data.get("max_positions", 30)))
        selected_style = str(data.get("color_style", "cn"))
        self.color_cn_button.setChecked(selected_style == "cn")
        self.color_intl_button.setChecked(selected_style != "cn")
        try:
            self._history_retention_days = int(data.get("history_retention_days", 7))
        except (TypeError, ValueError):
            self._history_retention_days = 7
        self.history_retention_input.setText(str(self._history_retention_days))
        self.history_dir_input.setText(str(data.get("history_directory", "")))
        self.update_api_key_status()
        self._apply_color_style()
        # 按配置恢复交易执行配置的显示状态(开关关闭时隐藏)
        self._update_trading_config_visibility()

    def save_configuration(self) -> bool:
        data = read_json_safely(CONFIG_PATH)
        try:
            market_cap_millions = float(self.market_cap.text().strip())
            market_cap_floor_millions = float(self.market_cap_floor.text().strip())
            volume_millions = float(self.volume.text().strip())
            volume_cap_millions = float(self.volume_cap.text().strip())
            risk_percent = float(self.risk_input.text().strip())
            max_positions_value = float(self.max_positions_input.text().strip())
            retention_days_value = float(self.history_retention_input.text().strip())
            auto_close_profit_pct = float(self.auto_close_profit_input.text().strip())
            auto_close_loss_pct = float(self.auto_close_loss_input.text().strip())
            if volume_millions <= 0:
                raise ValueError("成交量下限必须大于 0。")
            if market_cap_millions < 0 or market_cap_floor_millions < 0 or volume_cap_millions < 0:
                raise ValueError("范围下限与上限不得小于 0(0=不限)。")
            if market_cap_millions > 0 and market_cap_floor_millions > market_cap_millions:
                raise ValueError("市值下限不得高于上限(上限为 0 时不设上限)。")
            if volume_cap_millions > 0 and volume_cap_millions < volume_millions:
                raise ValueError("成交量上限(>0 时)不得低于下限。")
            if not 0 < risk_percent <= 5:
                raise ValueError("单笔风险必须大于 0 且不超过 5%。")
            if auto_close_profit_pct <= 0 or auto_close_loss_pct <= 0:
                raise ValueError("盈亏平仓阈值必须大于 0。")
            if max_positions_value != int(max_positions_value):
                raise ValueError("最大仓位数必须是整数。")
            max_positions = int(max_positions_value)
            if not 1 <= max_positions <= 200:
                raise ValueError("最大仓位数必须在 1 到 200 之间。")
            if retention_days_value != int(retention_days_value):
                raise ValueError("历史保留时长必须是整数。")
            if not 1 <= int(retention_days_value) <= 365:
                raise ValueError("历史保留时长必须在 1 到 365 天之间。")
            self._history_retention_days = int(retention_days_value)
        except ValueError as exc:
            QMessageBox.critical(self, "输入无效", f"配置输入无效：{exc}")
            return False
        data.update({
            "environment": self.selected_environment(), "proxy_enabled": self.selected_proxy_enabled(),
            "proxy_url": self.proxy_url.text().strip(), "market_cap_threshold_usd": int(market_cap_millions * 1_000_000),
            "market_cap_floor_usd": int(market_cap_floor_millions * 1_000_000),
            "minimum_quote_volume_usdt": int(volume_millions * 1_000_000),
            "maximum_quote_volume_usdt": int(volume_cap_millions * 1_000_000), "structure_interval": self.structure_interval.currentText(),
            "execution_interval": self.execution_interval.currentText(),
            "trading_enabled": self.selected_trading_enabled(),
            "direction_mode": self.selected_direction_mode(),
            "breakout_mode": self.selected_breakout_mode(),
            "range_mode": self.selected_range_mode(),
            "auto_close_enabled": self.selected_auto_close_enabled(),
            "auto_close_profit_pct": auto_close_profit_pct,
            "auto_close_loss_pct": auto_close_loss_pct,
            "risk_per_trade_pct": risk_percent / 100,
            "max_positions": max_positions,
            "color_style": self.selected_color_style(),
            "history_retention_days": self._history_retention_days,
            "history_directory": self.history_dir_input.text().strip(),
        })
        try:
            write_config_atomically(data)
            load_config(CONFIG_PATH)
        except Exception as exc:
            QMessageBox.critical(self, "配置无效", str(exc))
            return False
        # 保存后立即应用涨跌配色(K 线重绘)
        self._apply_color_style()
        configure_logging(data["environment"]).info("界面保存配置 environment=%s proxy=%s", data["environment"], data["proxy_enabled"])
        return True

    def selected_environment(self) -> str:
        """返回界面中高亮的运行环境。"""
        button = self.environment_buttons.checkedButton()
        return str(button.property("environment")) if button is not None else "testnet"

    def selected_proxy_enabled(self) -> bool:
        """返回界面中高亮的代理状态。"""
        button = self.proxy_buttons.checkedButton()
        return bool(button.property("proxy_enabled")) if button is not None else False

    def selected_trading_enabled(self) -> bool:
        """返回界面中高亮的交易开关状态。"""
        button = self.trading_buttons.checkedButton()
        return bool(button.property("trading_enabled")) if button is not None else False

    def selected_direction_mode(self) -> str:
        """返回界面中高亮的方向模式:long=只做多,short=只做空,both=多空都做。"""
        button = self.direction_buttons.checkedButton()
        return str(button.property("direction_mode")) if button is not None else "both"

    def selected_auto_close_enabled(self) -> bool:
        """返回界面中高亮的盈亏全平开关状态。"""
        button = self.auto_close_buttons.checkedButton()
        return bool(button.property("auto_close_enabled")) if button is not None else False

    def selected_breakout_mode(self) -> str:
        """返回界面中高亮的突破策略模式:none=都不做,up=只向上突破,down=只向下突破,both=都做。"""
        button = self.breakout_buttons.checkedButton()
        return str(button.property("breakout_mode")) if button is not None else "both"

    def selected_range_mode(self) -> str:
        """返回界面中高亮的震荡策略模式:none=都不做,buy=只低吸,sell=只高抛,both=都做。"""
        button = self.range_buttons.checkedButton()
        return str(button.property("range_mode")) if button is not None else "both"

    def _set_form_row_visible(self, widget: QWidget, visible: bool) -> None:
        """显示/隐藏 QFormLayout 整行:控件与其标签一同隐藏。

        仅隐藏控件时标签列仍会显示,须用 labelForField 找到行标签一并处理。
        """
        widget.setVisible(visible)
        parent = widget.parentWidget()
        form = parent.layout() if parent is not None else None
        if isinstance(form, QFormLayout):
            label = form.labelForField(widget)
            if label is not None:
                label.setVisible(visible)

    def _update_trading_config_visibility(self) -> None:
        """交易开关关闭时隐藏交易执行配置(方向模式/风险/仓位/盈亏全平),开启时展示。

        涨跌配色为显示偏好,不受交易开关影响,始终可见。
        % 阈值行由 _update_auto_close_visibility 统一处理:交易开关或盈亏全平
        任一关闭即隐藏,避免交易开关关闭后 % 行残留显示。
        """
        enabled = self.selected_trading_enabled()
        for widget in self._trading_config_widgets:
            self._set_form_row_visible(widget, enabled)
        self._update_auto_close_visibility()

    def _update_auto_close_visibility(self) -> None:
        """盈亏全平开关关闭时隐藏 % 阈值输入(含行标签);交易开关关闭时同样隐藏。"""
        enabled = self.selected_trading_enabled() and self.selected_auto_close_enabled()
        for widget in self._auto_close_pct_widgets:
            self._set_form_row_visible(widget, enabled)

    def _apply_color_style(self) -> None:
        """按配置应用涨跌配色(K 线与盈亏高亮),并触发 K 线重绘。"""
        global _CURRENT_COLOR_STYLE
        _CURRENT_COLOR_STYLE = self.selected_color_style()
        apply_color_style(_CURRENT_COLOR_STYLE)
        if hasattr(self, "kline_chart"):
            self.kline_chart.update()

    def selected_color_style(self) -> str:
        """返回界面中高亮的涨跌配色风格。"""
        button = self.color_style_buttons.checkedButton()
        return str(button.property("color_style")) if button is not None else "cn"

    def update_api_key_status(self) -> None:
        """按当前运行环境检查对应的 API 密钥环境变量是否已设置。"""
        if self.selected_environment() == "testnet":
            key_env, secret_env = "BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET"
        else:
            key_env, secret_env = "BINANCE_API_KEY", "BINANCE_API_SECRET"
        key_present = bool(get_secret(key_env))
        secret_present = bool(get_secret(secret_env))
        self.api_key_status.setText(
            "API 密钥：已设置" if key_present and secret_present else "API 密钥：未设置"
        )

    def start_program(self) -> None:
        if not self.save_configuration() or self.worker_thread is not None:
            return
        self.set_controls_running(True)
        self.worker_thread = QThread(self)
        self.worker = SchedulerWorker()
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.completed.connect(self.on_scheduler_completed)
        self.worker.failed.connect(self.on_scheduler_failed)
        self.worker.completed.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        # 官方安全清理模式:finished 后由 Qt 负责 deleteLater,
        # Python 引用延迟释放,避免 GC 与 Qt 清理竞争导致 0xC0000409
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.finished.connect(self.on_worker_finished)
        self.worker_thread.start()

    def stop_program(self) -> None:
        """请求后台调度循环在当前安全点退出。"""
        if self.worker_thread is None:
            return
        self.stop_button.setEnabled(False)
        self.stop_button.setText("正在停止…")
        self.program_status.setText("● 运行中（正在停止）")
        self.worker_thread.requestInterruption()

    def on_scheduler_completed(self) -> None:
        self.refresh_dashboard()

    def on_scheduler_failed(self, message: str) -> None:
        QMessageBox.critical(self, "程序异常停止", message)

    def on_worker_finished(self) -> None:
        self.set_controls_running(False)
        self.refresh_dashboard()
        if self.close_requested:
            self.allow_close = True
            self.close()
        # 延迟到 Qt 完成 finished/deleteLater 清理后再释放 Python 引用,避免 GC 竞争崩溃
        QTimer.singleShot(0, self._release_worker)

    def _release_worker(self) -> None:
        self.worker_thread = None
        self.worker = None

    def set_controls_running(self, running: bool) -> None:
        """运行时冻结配置，防止配置与执行中的调度不一致。"""
        controls = [
            self.testnet_button, self.live_button, self.proxy_off_button, self.proxy_on_button, self.proxy_url,
            self.market_cap_floor, self.market_cap, self.volume, self.volume_cap,
            self.structure_interval, self.execution_interval,
            self.trading_off_button, self.trading_on_button,
            self.direction_long_button, self.direction_short_button, self.direction_both_button,
            self.breakout_none_button, self.breakout_up_button, self.breakout_down_button, self.breakout_both_button,
            self.range_none_button, self.range_buy_button, self.range_sell_button, self.range_both_button,
            self.risk_input, self.max_positions_input,
            self.auto_close_off_button, self.auto_close_on_button,
            self.auto_close_profit_input, self.auto_close_loss_input,
            self.color_cn_button, self.color_intl_button,
            self.history_retention_input, self.history_dir_input,
        ]
        for control in controls:
            control.setEnabled(not running)
        self.start_button.setEnabled(not running)
        self.stop_button.setEnabled(running)
        self.stop_button.setText("停止程序")
        self.program_status.setText("● 运行中" if running else "● 已停止")

    def closeEvent(self, event: Any) -> None:
        """关闭窗口前二次确认，运行中的调度器会先安全退出。"""
        if self.allow_close:
            event.accept()
            return
        running = self.worker_thread is not None
        prompt = "程序正在运行。确认停止调度器并关闭窗口吗？" if running else "确认关闭程序吗？"
        answer = QMessageBox.question(
            self,
            "确认关闭",
            prompt,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            event.ignore()
            return
        if running:
            self.close_requested = True
            self.stop_program()
            event.ignore()
            return
        event.accept()

    def refresh_clock(self) -> None:
        # GUI 时钟统一显示北京时间(UTC+8);内部计算仍用 UTC,见 _next_schedule_text
        text = datetime.now(CHINA_TIMEZONE).strftime("%Y-%m-%d  %H:%M:%S")
        for clock in self.utc_clocks:
            clock.setText(text)

    def refresh_dashboard(self) -> None:
        config = read_json_safely(CONFIG_PATH)
        base = runtime_directory(config.get("environment", "production"))
        for key, title, template, list_key, _ in CARD_DEFINITIONS:
            payload = read_json_safely(base / template.format(**config))
            raw_items = payload.get(list_key, [])
            items = [item for item in raw_items if isinstance(item, dict)] if isinstance(raw_items, list) else []
            timestamp = self._pool_timestamp(payload)
            self.cards[key].update_data(len(items), timestamp)
            self.pool_data[key] = (title, items)

    def _next_schedule_text(self, key: str) -> str:
        """计算卡片对应数据池的下一次调度时刻(按调度器 UTC 周期边界)。"""
        config = read_json_safely(CONFIG_PATH)
        if key in ("contract", "market"):
            interval = "1d"  # 合约池/市值池在 UTC 日边界刷新
        elif key in ("trading", "boxes"):
            interval = str(config.get("structure_interval", "1h"))
        elif key == "signals":
            interval = str(config.get("execution_interval", "15m"))
        else:
            return "实时刷新"
        try:
            now = datetime.now(timezone.utc)
            next_bucket = bucket_start(interval, now) + self._interval_delta(interval)
            return format_value(int(next_bucket.timestamp() * 1000))
        except ValueError:
            return "—"

    @staticmethod
    def _interval_delta(interval: str) -> timedelta:
        """将 K 线周期文本转换为时长(与调度器 bucket 对齐)。"""
        if interval.endswith("m"):
            return timedelta(minutes=int(interval[:-1]))
        if interval.endswith("h"):
            return timedelta(hours=int(interval[:-1]))
        if interval.endswith("d"):
            return timedelta(days=int(interval[:-1]))
        if interval == "1w":
            return timedelta(weeks=1)
        raise ValueError(f"不支持的调度周期:{interval}")

    @staticmethod
    def _pool_timestamp(payload: dict[str, Any]) -> Any:
        """兼容各历史产物的元数据位置，获取最近一次有效数据时点。"""
        source = payload.get("source", {}) if isinstance(payload.get("source"), dict) else {}
        snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
        for value in (
            snapshot.get("dataAsOf"), payload.get("volume24hFilter", {}).get("downloadedAt"),
            payload.get("marketCapFilter", {}).get("downloadedAt"), source.get("generatedAt"),
            source.get("downloadedAt"), source.get("analyzedAt"),
        ):
            if value:
                return value
        return None

    def refresh_data_if_changed(self) -> None:
        """检测调度器原子发布的数据池文件，并在变化后刷新界面。"""
        if self.viewing_history:
            # 历史快照查看模式:暂停自动刷新,避免被最新数据覆盖
            return
        config = read_json_safely(CONFIG_PATH)
        base = runtime_directory(config.get("environment", "testnet"))
        paths = [base / template.format(**config) for _, _, template, _, _ in CARD_DEFINITIONS]
        fingerprints = tuple(
            (str(path), path.stat().st_mtime_ns if path.is_file() else 0)
            for path in paths
        )
        if fingerprints == self.pool_file_fingerprints:
            return
        self.pool_file_fingerprints = fingerprints
        active_pool = getattr(self, "current_pool_key", None)
        self.refresh_dashboard()
        if active_pool:
            if self.pages.currentIndex() == 1:
                self.open_pool_page(active_pool)
            elif self.pages.currentIndex() == 2:
                # 币详情页:数据更新后按当前交易对重新加载(排序后行号不可靠,以 symbol 定位)
                title, items = self.pool_data.get(active_pool, ("", []))
                symbol = getattr(self, "current_symbol", None)
                record = next(
                    (item for item in items if str(item.get("symbol", "")) == symbol),
                    None,
                ) if symbol else None
                if record is not None:
                    self.open_symbol_record(record, title)

    def _build_history_page(self) -> QWidget:
        """页 4:历史数据节点列表(按当前池类型过滤,倒序展示过去每个节点的快照)。"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(18)
        header = QHBoxLayout()
        self.history_back_button = QPushButton("← 返回列表", objectName="backButton")
        self.history_back_button.clicked.connect(self.close_history_page)
        header.addWidget(self.history_back_button)
        labels = QVBoxLayout()
        self.history_page_title = QLabel("历史数据", objectName="pageTitle")
        self.history_page_subtitle = QLabel("按时间倒序展示过去每个节点的数据快照,点击查看", objectName="muted")
        labels.addWidget(self.history_page_title)
        labels.addWidget(self.history_page_subtitle)
        header.addLayout(labels)
        header.addStretch(1)
        utc_clock = QLabel(objectName="utcClock")
        self.utc_clocks.append(utc_clock)
        header.addWidget(utc_clock)
        layout.addLayout(header)
        self.history_table = QTableWidget()
        self.history_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.history_table.setAlternatingRowColors(True)
        self.history_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.history_table.cellClicked.connect(self.open_history_snapshot)
        layout.addWidget(self.history_table, 1)
        return page

    def _build_live_page(self) -> QWidget:
        """页 5:实时看盘(实时 K 线 + 开仓/止损/止盈标注,交易所看盘体验)。"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(14)
        header = QHBoxLayout()
        self.live_back_button = QPushButton("← 返回", objectName="backButton")
        self.live_back_button.clicked.connect(self.close_live_page)
        header.addWidget(self.live_back_button)
        labels = QVBoxLayout()
        self.live_page_title = QLabel("实时看盘", objectName="pageTitle")
        self.live_page_subtitle = QLabel(objectName="muted")
        labels.addWidget(self.live_page_title)
        labels.addWidget(self.live_page_subtitle)
        header.addLayout(labels)
        header.addStretch(1)
        self.live_interval = QComboBox()
        self.live_interval.addItems(["1m", "5m", "15m", "1h", "4h", "1d"])
        self.live_interval.currentTextChanged.connect(self._on_live_interval_changed)
        header.addWidget(self.live_interval)
        utc_clock = QLabel(objectName="utcClock")
        self.utc_clocks.append(utc_clock)
        header.addWidget(utc_clock)
        layout.addLayout(header)

        # 价格头部(交易所报价样式):左=交易对+持仓信息(固定两行),右=最新价
        self.live_quote = QFrame(objectName="quotePanel")
        quote_layout = QHBoxLayout(self.live_quote)
        quote_layout.setContentsMargins(22, 14, 22, 14)
        quote_layout.setSpacing(18)
        symbol_box = QVBoxLayout()
        symbol_box.setSpacing(4)
        self.live_quote_symbol = QLabel("—", objectName="quoteSymbol")
        self.live_quote_meta = QLabel("", objectName="quoteMeta")
        symbol_box.addWidget(self.live_quote_symbol)
        symbol_box.addWidget(self.live_quote_meta)
        quote_layout.addLayout(symbol_box)
        quote_layout.addStretch(1)
        price_box = QVBoxLayout()
        price_box.setSpacing(4)
        price_box.addWidget(QLabel("最新价", objectName="quoteMeta"))
        self.live_quote_price = QLabel("—", objectName="quotePrice")
        self.live_quote_price.setAlignment(Qt.AlignmentFlag.AlignRight)
        price_box.addWidget(self.live_quote_price)
        quote_layout.addLayout(price_box)
        layout.addWidget(self.live_quote)

        # K 线大图
        self.live_chart = KlineChartWidget()
        self.live_chart.setMinimumHeight(420)
        layout.addWidget(self.live_chart, 1)

        # 持仓信息条 + 市价平仓
        info_row = QHBoxLayout()
        self.live_position_info = QLabel("", objectName="poolSummary")
        info_row.addWidget(self.live_position_info, 1)
        self.live_close_button = QPushButton("市价平仓", objectName="lightActionButton")
        self.live_close_button.clicked.connect(self.confirm_close_live_position)
        info_row.addWidget(self.live_close_button)
        layout.addLayout(info_row)
        # 状态横幅(加载失败/已平仓提示)
        self.live_status = QLabel("", objectName="muted")
        self.live_status.hide()
        layout.addWidget(self.live_status)
        return page

    def _build_pnl_page(self) -> QWidget:
        """页 6:盈亏统计(月历卡 + 按天筛选 + 当日平仓明细)。"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(14)
        header = QHBoxLayout()
        self.pnl_back_button = QPushButton("← 返回概览", objectName="backButton")
        self.pnl_back_button.clicked.connect(self.close_pnl_page)
        header.addWidget(self.pnl_back_button)
        labels = QVBoxLayout()
        pnl_title = QLabel("盈亏统计", objectName="pageTitle")
        pnl_subtitle = QLabel("程序开出的单平仓后按日统计", objectName="muted")
        labels.addWidget(pnl_title)
        labels.addWidget(pnl_subtitle)
        header.addLayout(labels)
        header.addStretch(1)
        utc_clock = QLabel(objectName="utcClock")
        self.utc_clocks.append(utc_clock)
        header.addWidget(utc_clock)
        layout.addLayout(header)

        # 汇总条
        self.pnl_summary = QLabel(objectName="poolSummary")
        layout.addWidget(self.pnl_summary)

        # 左:月历卡(近三个月横向排列,按日盈亏着色,窄窗可横向滚动)  右:日期筛选
        middle = QHBoxLayout()
        middle.setSpacing(14)
        calendar_frame = QFrame(objectName="metricPanel")
        calendar_layout = QVBoxLayout(calendar_frame)
        calendar_layout.setContentsMargins(14, 12, 14, 12)
        months_scroll = QScrollArea()
        months_scroll.setWidgetResizable(True)
        months_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # 固定高度容纳单个月份块(标题+星期行+最多 6 周),表格获得更多纵向空间
        months_scroll.setFixedHeight(240)
        months_container = QWidget()
        self.pnl_calendar_layout = QHBoxLayout(months_container)
        self.pnl_calendar_layout.setContentsMargins(0, 0, 0, 0)
        self.pnl_calendar_layout.setSpacing(20)
        # 贴左排列:容器宽于内容时剩余空间固定留右侧,避免月份块被整体偏移产生不对称边距
        self.pnl_calendar_layout.setAlignment(Qt.AlignmentFlag.AlignLeft)
        months_scroll.setWidget(months_container)
        calendar_layout.addWidget(months_scroll)
        calendar_layout.addStretch(1)
        middle.addWidget(calendar_frame, 3)
        side = QVBoxLayout()
        side.setSpacing(8)
        side.addWidget(QLabel("选择日期", objectName="sectionTitle"))
        self.pnl_day_combo = QComboBox()
        self.pnl_day_combo.currentTextChanged.connect(self._on_pnl_day_changed)
        side.addWidget(self.pnl_day_combo)
        side.addWidget(QLabel("点击月历日期切换筛选", objectName="muted"))
        side.addStretch(1)
        middle.addLayout(side, 1)
        layout.addLayout(middle)

        # 当日平仓明细(标题紧随表格,不再悬于右侧栏)
        layout.addWidget(QLabel("当日平仓明细", objectName="sectionTitle"))
        self.pnl_table = QTableWidget()
        self.pnl_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.pnl_table.setAlternatingRowColors(True)
        self.pnl_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        layout.addWidget(self.pnl_table, 1)
        return page

    # ---- 盈亏统计 ----

    def open_pnl_page(self) -> None:
        """进入盈亏统计页:加载记录并重建月历与明细。"""
        self._pnl_fingerprint = ()
        self.refresh_pnl_data()
        self._pnl_selected_day = None
        self.pages.setCurrentIndex(6)
        self._rebuild_pnl_view()

    @staticmethod
    def _clear_layout_items(layout: Any) -> None:
        """递归销毁布局内的全部子布局与控件(离开即销毁,避免重建叠加残留)。"""
        while layout.count():
            child = layout.takeAt(0)
            if child.layout() is not None:
                MainWindow._clear_layout_items(child.layout())
            elif child.widget() is not None:
                child.widget().deleteLater()

    def close_pnl_page(self) -> None:
        """离开盈亏统计页:清空明细表与月历(离开即销毁)。"""
        self.pnl_table.setRowCount(0)
        self._clear_layout_items(self.pnl_calendar_layout)
        self.pages.setCurrentIndex(0)

    def refresh_pnl_data(self) -> bool:
        """按文件指纹增量加载盈亏记录;返回是否有更新。"""
        directory = pnl_directory(self._current_environment())
        fingerprint = (
            tuple(sorted((str(path), path.stat().st_mtime_ns) for path in directory.glob("*.json")))
            if directory.is_dir()
            else ()
        )
        if fingerprint == self._pnl_fingerprint:
            return False
        self._pnl_fingerprint = fingerprint
        self._pnl_records = read_pnl_records(self._current_environment())
        self._pnl_day_totals = aggregate_days(self._pnl_records)
        return True

    def refresh_pnl_card(self) -> None:
        """概览页盈亏卡片:大数字为今日盈亏相对账户(钱包余额)的百分比,小字为平仓笔数与盈亏额。"""
        card = self.cards.get("pnl")
        if card is None:
            return
        # 指纹缓存仅避免重复读盘(数据未变化时沿用缓存记录);
        # UI 每次调用都需按实时钱包余额重新计算,不能因数据未变化而跳过刷新,
        # 否则监控器就绪前算出的「—」会一直卡住,百分比永不出现
        self.refresh_pnl_data()
        today = beijing_date_key(int(datetime.now(CHINA_TIMEZONE).timestamp() * 1000))
        today_info = self._pnl_day_totals.get(today, {})
        today_net = float(today_info.get("net", 0.0))
        today_count = int(today_info.get("count", 0))
        if today_count == 0:
            card.count.setText("—")
            card.detail.setText("今日暂无平仓")
            return
        profit_color, loss_color = profit_colors()
        color = profit_color if today_net >= 0 else loss_color
        # 账户基准:钱包余额(与账户持仓页/盈亏自动全平同一口径);
        # 监控器未运行拿不到余额时,百分比无法计算,大数字显示占位
        wallet = 0.0
        monitor = get_active_monitor()
        if monitor is not None:
            wallet = float(monitor.get_account_snapshot()["balances"].get("USDT", {}).get("balance", 0.0))
        if wallet > 0:
            pnl_pct = today_net / wallet * 100
            card.count.setText(f"<span style='color:{color}'>{pnl_pct:+.2f}%</span>")
        else:
            card.count.setText("—")
        card.detail.setText(
            f"今日平仓 {today_count} 笔 · 今日盈亏 <span style='color:{color}'>{today_net:+,.2f}</span>"
        )

    def refresh_pnl_page_if_changed(self) -> None:
        """盈亏统计页可见时的增量刷新:记录文件变化后重建月历与明细。"""
        if not self.refresh_pnl_data():
            return
        self._rebuild_pnl_view()

    def _rebuild_pnl_view(self) -> None:
        """重建月历卡、日期筛选与当日明细(保持当前选中日期)。"""
        days = list(self._pnl_day_totals.keys())
        selected = self._pnl_selected_day
        if selected is None or selected.isoformat() not in self._pnl_day_totals:
            # 默认选中今天,无今日记录时选最近的记录日
            today = beijing_date_key(int(datetime.now(CHINA_TIMEZONE).timestamp() * 1000))
            selected = date.fromisoformat(today) if today in self._pnl_day_totals else (
                date.fromisoformat(sorted(days)[-1]) if days else None
            )
        self._pnl_selected_day = selected
        self._pnl_summary_update()
        # 日期下拉(倒序)
        self.pnl_day_combo.blockSignals(True)
        self.pnl_day_combo.clear()
        self.pnl_day_combo.addItems(sorted(days, reverse=True))
        if selected is not None:
            self.pnl_day_combo.setCurrentText(selected.isoformat())
        self.pnl_day_combo.blockSignals(False)
        self._rebuild_pnl_calendar()
        self._fill_pnl_table(self._pnl_records_for(selected))

    def _pnl_summary_update(self) -> None:
        """汇总条:累计净盈亏 / 今日净盈亏 / 平仓笔数。"""
        total_net = sum(float(record.get("netPnlUsdt") or 0.0) for record in self._pnl_records)
        today = beijing_date_key(int(datetime.now(CHINA_TIMEZONE).timestamp() * 1000))
        today_info = self._pnl_day_totals.get(today, {})
        today_net = float(today_info.get("net", 0.0))
        profit_color, loss_color = profit_colors()
        total_color = profit_color if total_net >= 0 else loss_color
        today_color = profit_color if today_net >= 0 else loss_color
        self.pnl_summary.setText(
            f"累计净盈亏 <b style='color:{total_color}'>{total_net:+,.2f}</b> · 平仓 {len(self._pnl_records)} 笔"
            f"&nbsp;&nbsp;|&nbsp;&nbsp;今日净盈亏 <b style='color:{today_color}'>{today_net:+,.2f}</b> · "
            f"今日平仓 {int(today_info.get('count', 0))} 笔"
        )

    def _rebuild_pnl_calendar(self) -> None:
        """重建近三个月的月历卡(横向排列):每日格子按当日净盈亏着色,点击筛选当日明细。"""
        self._clear_layout_items(self.pnl_calendar_layout)
        today = datetime.now(CHINA_TIMEZONE).date()
        max_abs = max((abs(float(info.get("net", 0.0))) for info in self._pnl_day_totals.values()), default=0.0)
        profit_color, loss_color = profit_colors()
        for offset in range(2, -1, -1):
            total_months = today.year * 12 + (today.month - 1) - offset
            year, month = total_months // 12, total_months % 12 + 1
            month_block = QVBoxLayout()
            month_block.setSpacing(4)
            grid_width = 7 * 30 + 6 * 3  # 与网格列宽一致,保证标题与网格严格对齐
            title_label = QLabel(f"{year}-{month:02d}", objectName="sectionTitle")
            title_label.setFixedWidth(grid_width)
            title_label.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            month_block.addWidget(title_label)
            grid = QGridLayout()
            grid.setSpacing(3)
            for column, name in enumerate(["一", "二", "三", "四", "五", "六", "日"]):
                grid.addWidget(QLabel(name, objectName="muted"), 0, column)
            first_weekday = datetime(year, month, 1).weekday()
            for day in range(1, calendar.monthrange(year, month)[1] + 1):
                day_date = date(year, month, day)
                cell = QPushButton(str(day))
                cell.setFixedSize(30, 26)
                if day_date > today:
                    cell.setEnabled(False)
                    cell.setStyleSheet("QPushButton { color: #C4D2E6; background: transparent; border: 0; }")
                else:
                    info = self._pnl_day_totals.get(day_date.isoformat())
                    selected = self._pnl_selected_day == day_date
                    if info is None:
                        cell.setStyleSheet(
                            "QPushButton { background: #F2F6FC; color: #71809A; border: 1px solid #E0E6F0; border-radius: 6px; }"
                        )
                        cell.setToolTip(day_date.isoformat())
                    else:
                        net = float(info.get("net", 0.0))
                        color = QColor(profit_color if net >= 0 else loss_color)
                        alpha = 70 + int(185 * min(1.0, abs(net) / max_abs)) if max_abs > 0 else 70
                        color.setAlpha(alpha)
                        border = "2px solid #3D7CFF" if selected else "1px solid #C9D6EA"
                        cell.setStyleSheet(
                            f"QPushButton {{ background: rgba({color.red()},{color.green()},{color.blue()},{alpha}); "
                            f"color: #1A2B4A; border: {border}; border-radius: 6px; }}"
                        )
                        cell.setToolTip(f"{day_date.isoformat()} 净盈亏 {net:+,.2f} · {int(info.get('count', 0))} 笔")
                        cell.clicked.connect(lambda _=False, d=day_date: self._on_pnl_calendar_click(d))
                grid.addWidget(cell, 1 + (day - 1 + first_weekday) // 7, (day - 1 + first_weekday) % 7)
            month_block.addLayout(grid)
            self.pnl_calendar_layout.addLayout(month_block)
        if not self._pnl_day_totals:
            self.pnl_calendar_layout.addWidget(QLabel("暂无平仓记录", objectName="muted"))

    def _on_pnl_calendar_click(self, day: date) -> None:
        """月历格子点击:通过日期下拉联动筛选(触发 _on_pnl_day_changed)。"""
        self.pnl_day_combo.setCurrentText(day.isoformat())

    def _on_pnl_day_changed(self, text: str) -> None:
        """日期筛选变化:更新选中日并重建月历高亮与当日明细。"""
        if not text:
            return
        try:
            selected = date.fromisoformat(text)
        except ValueError:
            return
        self._pnl_selected_day = selected
        self._rebuild_pnl_calendar()
        self._fill_pnl_table(self._pnl_records_for(selected))

    def _pnl_records_for(self, day: date | None) -> list[dict[str, Any]]:
        """按北京时间日期筛选盈亏记录(按平仓时间倒序)。"""
        if day is None:
            return []
        key = day.isoformat()
        records = [record for record in self._pnl_records if beijing_date_key(record.get("closeTime")) == key]
        records.sort(key=lambda record: float(record.get("closeTime") or 0.0), reverse=True)
        return records

    @staticmethod
    def _implied_exit_price(record: dict[str, Any]) -> float | None:
        """由已实现盈亏与开仓信息反推平仓价(仅数量已知时)。"""
        entry = float(record.get("entryPrice") or 0.0)
        quantity = float(record.get("quantity") or 0.0)
        realized = float(record.get("realizedPnlUsdt") or 0.0)
        sign = 1.0 if record.get("direction") == "多" else -1.0
        if entry > 0 and quantity > 0:
            return entry + realized / (quantity * sign)
        return None

    def _fill_pnl_table(self, records: list[dict[str, Any]]) -> None:
        """填充当日平仓明细表。"""
        columns = [
            ("交易对", "symbol"), ("方向", "direction"), ("数量", "quantity"),
            ("开仓价", "entryPrice"), ("平仓价", "exit"), ("净盈亏", "net"),
            ("手续费", "commission"), ("平仓原因", "closeReason"), ("平仓时间", "closeTime"),
        ]
        self.pnl_table.setColumnCount(len(columns))
        self.pnl_table.setHorizontalHeaderLabels([label for label, _ in columns])
        self.pnl_table.setRowCount(len(records))
        profit_color, loss_color = profit_colors()
        for row, record in enumerate(records):
            net = float(record.get("netPnlUsdt") or 0.0)
            commission = float(record.get("commissionUsdt") or 0.0)
            exit_price = self._implied_exit_price(record)
            quantity = record.get("quantity")
            try:
                close_time_text = datetime.fromtimestamp(
                    int(record.get("closeTime")) / 1000, CHINA_TIMEZONE
                ).strftime("%Y-%m-%d %H:%M")
            except (TypeError, ValueError, OverflowError):
                close_time_text = "—"
            values: dict[str, str] = {
                "symbol": str(record.get("symbol", "—")),
                "direction": display_direction(record.get("direction")),
                "quantity": f"{float(quantity):g}" if quantity is not None else "—",
                "entryPrice": format_metric("价格", record.get("entryPrice")),
                "exit": f"{exit_price:.8g}" if exit_price is not None else "—",
                "net": f"{net:+,.2f}",
                "commission": f"{commission:+,.2f}",
                "closeReason": str(record.get("closeReason") or "—"),
                "closeTime": close_time_text,
            }
            for column, (_, field) in enumerate(columns):
                item = SortableTableItem(values[field], _sort_key_for(record.get(field)))
                if field == "net":
                    item.setForeground(QColor(profit_color if net >= 0 else loss_color))
                self.pnl_table.setItem(row, column, item)
        self.pnl_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        # 平仓时间列按内容自适应宽度,避免被均分宽度截断
        self.pnl_table.horizontalHeader().setSectionResizeMode(
            len(columns) - 1, QHeaderView.ResizeMode.ResizeToContents
        )

    # ---- 历史数据 ----

    @staticmethod
    def _history_prefix(key: str, config: dict[str, Any]) -> str:
        """池 key → 历史文件名前缀。"""
        return {
            "contract": "contract_pool",
            "market": "market_cap_pool",
            "trading": "trading_pool",
            "boxes": f"box_pool_{config.get('structure_interval', '1h')}",
            "signals": f"signal_pool_{config.get('execution_interval', '15m')}",
        }.get(key, key)

    def _history_dir(self, config: dict[str, Any]) -> Path:
        """历史保存位置:自定义路径或默认 runtime/{环境}/history。"""
        configured = str(config.get("history_directory", "") or "")
        if configured:
            return Path(configured)
        return runtime_directory(config.get("environment", "production")) / "history"

    def toggle_history_mode(self) -> None:
        """「历史」按钮:先退出历史查看模式(恢复正常实时数据),再打开历史节点列表。"""
        if self.viewing_history:
            self.viewing_history = False
            self._history_snapshot_items = []
            self._history_snapshot_key = ""
        self.open_history_page()

    def open_history_page(self) -> None:
        """打开当前池的历史节点列表(每行显示快照时间与对应条目数量)。"""
        config = read_json_safely(CONFIG_PATH)
        prefix = self._history_prefix(self.current_pool_key, config)
        history_dir = self._history_dir(config)
        title = next((t for k, t, _, _, _ in CARD_DEFINITIONS if k == self.current_pool_key), "数据池")
        list_key = next((lk for k, _, _, lk, _ in CARD_DEFINITIONS if k == self.current_pool_key), "symbols")
        self.history_page_title.setText(f"{title} · 历史数据")
        entries: list = []
        if history_dir.is_dir():
            for file in history_dir.glob(f"{prefix}_*.json"):
                timestamp = _history_timestamp(file.stem)
                if timestamp is not None:
                    entries.append((timestamp, file))
        entries.sort(key=lambda entry: entry[0], reverse=True)
        self._history_entries = entries
        self.history_table.setColumnCount(2)
        self.history_table.setHorizontalHeaderLabels(["快照时间", "数量"])
        self.history_table.setRowCount(len(entries))
        for row, (timestamp, file) in enumerate(entries):
            self.history_table.setItem(
                row, 0, QTableWidgetItem(timestamp.astimezone(CHINA_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"))
            )
            count = self._history_entry_count(file, list_key)
            self.history_table.setItem(
                row, 1, SortableTableItem(f"{count:,}" if count is not None else "—", count)
            )
        # 时间列占满,数量列按内容自适应
        self.history_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.history_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.pages.setCurrentIndex(4)

    @staticmethod
    def _history_entry_count(file: Path, list_key: str) -> int | None:
        """读取历史快照文件中的条目数量;文件缺失或 JSON 损坏时返回 None。

        直接解析文件以区分「损坏」与「合法空对象」(read_json_safely 会将
        损坏文件静默转为空对象,无法区分)。
        """
        try:
            payload = json.loads(file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        items = payload.get(list_key, []) if isinstance(payload, dict) else None
        return len(items) if isinstance(items, list) else None

    def open_history_snapshot(self, row: int, _: int = 0) -> None:
        """点击历史节点:载入该快照为当前查看数据(历史查看模式)。

        快照独立保存在 _history_snapshot_*,不覆盖实时 pool_data;
        因此正常返回(back_to_overview)即可回到实时数据,无需再点「返回实时」。
        """
        if not 0 <= row < len(self._history_entries):
            return
        timestamp, file = self._history_entries[row]
        payload = read_json_safely(file)
        key = self.current_pool_key
        list_key = next((lk for k, _, _, lk, _ in CARD_DEFINITIONS if k == key), "symbols")
        raw_items = payload.get(list_key, [])
        items = [item for item in raw_items if isinstance(item, dict)] if isinstance(raw_items, list) else []
        title = next((t for k, t, _, _, _ in CARD_DEFINITIONS if k == key), "数据池")
        self._history_snapshot_key = key
        self._history_snapshot_items = items
        self.open_pool_page(key, mode="history")
        self.pool_summary.setText(
            f"{title} · 历史快照 {timestamp.astimezone(CHINA_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} · {len(items):,} 条"
        )

    def back_to_overview(self) -> None:
        """详情页返回概览:历史模式下正常返回即自动回到实时数据。"""
        if self.viewing_history:
            self.viewing_history = False
            self._history_snapshot_items = []
            self._history_snapshot_key = ""
        self.table.setRowCount(0)  # 离开即销毁:清空表格,下次进入时重建
        self.pages.setCurrentIndex(0)

    def close_history_page(self) -> None:
        """离开历史列表页:清空列表与条目(离开即销毁)。"""
        self.history_table.setRowCount(0)
        self._history_entries = []
        self.pages.setCurrentIndex(1)

    def close_symbol_page(self) -> None:
        """离开币详情页:清空图表与指标面板(离开即销毁)。"""
        self.kline_chart.clear()
        self.clear_metrics("")
        self.pages.setCurrentIndex(1)

    def _current_pool_items(self) -> tuple[str, list[dict[str, Any]]]:
        """返回当前页面展示的数据集:历史快照模式用快照,否则用最新数据。"""
        if self.viewing_history and self._history_snapshot_key == self.current_pool_key:
            title = next((t for k, t, _, _, _ in CARD_DEFINITIONS if k == self.current_pool_key), "数据池")
            return title, self._history_snapshot_items
        return self.pool_data.get(self.current_pool_key, ("数据池", []))

    def open_history_cleaner(self) -> None:
        """清理指定时间段内的历史数据文件。"""
        config = read_json_safely(CONFIG_PATH)
        history_dir = self._history_dir(config)
        if not history_dir.is_dir():
            QMessageBox.information(self, "清理历史数据", "历史目录不存在。")
            return
        dialog = QDialog(self)
        dialog.setWindowTitle("清理历史数据")
        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        start_edit = QDateEdit(QDate.currentDate().addDays(-7))
        end_edit = QDateEdit(QDate.currentDate())
        for edit in (start_edit, end_edit):
            edit.setCalendarPopup(True)
            edit.setDisplayFormat("yyyy-MM-dd")
        form.addRow("开始日期", start_edit)
        form.addRow("结束日期", end_edit)
        layout.addLayout(form)
        buttons = QHBoxLayout()
        confirm = QPushButton("确认清理", objectName="lightActionButton")
        cancel = QPushButton("取消")
        confirm.clicked.connect(dialog.accept)
        cancel.clicked.connect(dialog.reject)
        buttons.addStretch(1)
        buttons.addWidget(cancel)
        buttons.addWidget(confirm)
        layout.addLayout(buttons)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        start_date = start_edit.date().toPython()
        end_date = end_edit.date().toPython()
        removed = 0
        for file in history_dir.glob("*.json"):
            timestamp = _history_timestamp(file.stem)
            if timestamp is None:
                continue
            if start_date <= timestamp.date() <= end_date:
                try:
                    file.unlink()
                    removed += 1
                except OSError:
                    pass
        QMessageBox.information(self, "清理历史数据", f"已清理 {removed} 个历史文件。")

    def open_pool_page(self, key: str, mode: str = "live") -> None:
        """打开数据池详情页;mode=live 显示最新数据,mode=history 显示历史快照。

        进入页面时以参数区分查看模式:历史模式只读快照且暂停自动刷新,
        实时模式读取最新数据池并恢复自动刷新。
        """
        self.current_pool_key = key
        if mode == "history":
            title = next((t for k, t, _, _, _ in CARD_DEFINITIONS if k == key), "数据池")
            items = self._history_snapshot_items if self._history_snapshot_key == key else []
            self.viewing_history = True
        else:
            title, items = self.pool_data.get(key, ("数据池", []))
            self.viewing_history = False
        self.detail_page_title.setText(title)
        self.detail_page_subtitle.setText("选择交易对后查看结构化指标")
        self.pool_summary.setText(f"{title} · 共 {len(items):,} 个交易对 · 点击行查看结构化指标")
        columns = self._columns_for_pool(key)
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels([label for label, _ in columns])
        # 填充期间关闭排序,避免边填边排;填完再启用(点击列头升降序)
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(items))
        for row, item in enumerate(items):
            for column, (_, field) in enumerate(columns):
                value = self._lookup(item, field)
                if field == "signalType":
                    rendered = display_signal_type(value, item.get("direction"))
                elif field == "direction":
                    rendered = display_direction(value)
                elif field == "consumed":
                    status = item.get("consumedStatus")
                    if status == "filled":
                        rendered = "已按信号开仓"
                    elif status == "abandoned":
                        # consumedReason 存在 = 交易对规则限制(数量/名义超限);否则为预估盈利不足
                        if item.get("consumedReason"):
                            rendered = "开仓数量不满足交易限制,放弃开仓"
                        else:
                            rendered = "预估盈利空间不足已放弃开仓"
                    elif value is True:
                        # 兼容历史格式:consumed=true 表示已消费(原语义即已开仓)
                        rendered = "已按信号开仓"
                    else:
                        rendered = "未处理"
                else:
                    rendered = format_metric(field, value)
                self.table.setItem(row, column, SortableTableItem(rendered, _sort_key_for(value)))
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.pages.setCurrentIndex(1)

    @staticmethod
    def _columns_for_pool(key: str) -> list[tuple[str, str]]:
        common = [("交易对", "symbol"), ("基础币", "baseAsset"), ("状态", "status")]
        if key == "market":
            return [("交易对", "symbol"), ("基础币", "baseAsset"), ("市值 (USD)", "coingecko.market_cap_usd")]
        if key == "trading":
            return [("交易对", "symbol"), ("基础币", "baseAsset"), ("24h 成交额 (USDT)", "binance24hr.quote_volume_usdt")]
        if key == "boxes":
            return [("交易对", "symbol"), ("窗口", "window"), ("箱体评分", "boxScore"), ("宽度", "widthPct")]
        if key == "signals":
            return [("交易对", "symbol"), ("类型", "signalType"), ("方向", "direction"), ("收盘价", "signalKline.close"), ("处理状态", "consumed")]
        return common

    @staticmethod
    def _lookup(data: dict[str, Any], dotted_key: str) -> Any:
        value: Any = data
        for key in dotted_key.split("."):
            value = value.get(key) if isinstance(value, dict) else None
        return value

    def _build_account_page(self) -> QWidget:
        """页 3:账户持仓(余额信息 + 受监管/未受监管双列表,基于账户流数据实时刷新)。"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(34, 28, 34, 28)
        layout.setSpacing(14)
        header = QHBoxLayout()
        self.account_back_button = QPushButton("← 返回概览", objectName="backButton")
        self.account_back_button.clicked.connect(self.close_account_page)
        header.addWidget(self.account_back_button)
        labels = QVBoxLayout()
        account_title = QLabel("账户持仓", objectName="pageTitle")
        account_subtitle = QLabel("基于用户数据流 (ACCOUNT_UPDATE) 实时刷新", objectName="muted")
        labels.addWidget(account_title)
        labels.addWidget(account_subtitle)
        header.addLayout(labels)
        header.addStretch(1)
        utc_clock = QLabel(objectName="utcClock")
        self.utc_clocks.append(utc_clock)
        header.addWidget(utc_clock)
        layout.addLayout(header)

        self.account_balance_label = QLabel(objectName="poolSummary")
        self.close_all_button = QPushButton("一键全平", objectName="lightActionButton")
        self.close_all_button.clicked.connect(self.confirm_close_all)
        balance_row = QHBoxLayout()
        balance_row.addWidget(self.account_balance_label, 1)
        balance_row.addWidget(self.close_all_button)
        layout.addLayout(balance_row)

        self.trailed_title = QLabel("移动止损监管中 (0)", objectName="sectionTitle")
        self.close_trailed_button = QPushButton("一键平仓", objectName="lightActionButton")
        self.close_trailed_button.clicked.connect(lambda: self.confirm_close_list(True))
        trailed_header = QHBoxLayout()
        trailed_header.addWidget(self.trailed_title, 1)
        trailed_header.addWidget(self.close_trailed_button)
        layout.addLayout(trailed_header)
        self.trailed_table = self._make_position_table()
        self.trailed_table.cellClicked.connect(self._handle_position_cell_click)
        layout.addWidget(self.trailed_table, 1)

        self.other_title = QLabel("其他持仓 (0)", objectName="sectionTitle")
        self.close_other_button = QPushButton("一键平仓", objectName="lightActionButton")
        self.close_other_button.clicked.connect(lambda: self.confirm_close_list(False))
        other_header = QHBoxLayout()
        other_header.addWidget(self.other_title, 1)
        other_header.addWidget(self.close_other_button)
        layout.addLayout(other_header)
        self.other_table = self._make_position_table()
        self.other_table.cellClicked.connect(self._handle_position_cell_click)
        layout.addWidget(self.other_table, 1)
        # 平仓完成信号桥:worker 线程 emit,GUI 线程接收
        self._close_bridge = CloseWorkerBridge()
        self._close_bridge.finished.connect(self._on_close_finished)
        return page

    @staticmethod
    def _make_position_table() -> QTableWidget:
        """构建账户持仓列表表格(只读,行高容纳平仓按钮)。"""
        table = QTableWidget()
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        # 行高容纳「平仓」按钮,避免按钮溢出单行范围
        table.verticalHeader().setDefaultSectionSize(36)
        return table

    # ---- 账户持仓页 ----

    def open_account_page(self) -> None:
        """进入账户持仓页并强制刷新首帧。"""
        self.pages.setCurrentIndex(3)
        self._last_account_snapshot_at = None
        self._last_account_content_signature = None
        self.refresh_account_page()

    def close_account_page(self) -> None:
        """退出账户持仓页:清空表格数据并返回概览(退出即销毁界面内容)。"""
        self.trailed_table.setRowCount(0)
        self.other_table.setRowCount(0)
        self.trailed_title.setText("移动止损监管中 (0)")
        self.other_title.setText("其他持仓 (0)")
        self._last_account_content_signature = None
        self.pages.setCurrentIndex(0)

    def refresh_account_view(self) -> None:
        """账户 1 秒刷新:概览卡片计数、账户页表格、实时看盘页与盈亏统计页,仅当前页面可见时执行。"""
        index = self.pages.currentIndex()
        if index == 0:
            self.refresh_schedule_cards()
            self.refresh_account_card()
            self.refresh_pnl_card()
        elif index == 3:
            self.refresh_account_page()
        elif index == 5 and self.live_view_open:
            self.refresh_live_view()
        elif index == 6:
            self.refresh_pnl_page_if_changed()

    def refresh_schedule_cards(self) -> None:
        """每秒更新数据池卡片状态:调度器正在刷新某池时显示「当前正在运行」,否则显示下一调度时刻。"""
        activity = get_current_activity()
        for key, title, _, _, _ in CARD_DEFINITIONS:
            if activity == title:
                self.cards[key].set_schedule("当前正在运行")
            else:
                self.cards[key].set_schedule(f"下一调度：{self._next_schedule_text(key)}")

    def refresh_account_card(self) -> None:
        """概览页「账户持仓」卡片:总未实现盈亏值与百分比 + 持仓监管分类计数。

        盈亏口径与账户持仓页一致:逐仓实时标记价计算,百分比以钱包余额为分母。
        """
        card = self.cards.get("account")
        if card is None:
            return
        monitor = get_active_monitor()
        if monitor is None:
            card.count.setText("监控器未运行")
            card.detail.setText("启动程序后自动监控")
            return
        snapshot, trailed, other = self._account_view_data(monitor)
        # 展示盈亏 = 仅程序仓(本地台账记录)的实时浮盈合计;人工仓不纳入统计口径
        total_unrealized = sum(
            self._display_pnl(symbol, info)
            for symbol, info, local in [*trailed, *other]
            if local is not None
        )
        usdt = snapshot["balances"].get("USDT", {})
        wallet = float(usdt.get("balance", 0.0))
        pnl_pct = total_unrealized / wallet * 100 if wallet > 0 else None
        profit_color, loss_color = profit_colors()
        pnl_color = profit_color if total_unrealized >= 0 else loss_color
        pct_color = profit_color if pnl_pct is not None and pnl_pct >= 0 else loss_color
        # 大数字显示未实现盈亏 %,小字显示盈亏值与持仓个数(均为程序仓口径)
        card.count.setText(
            f"<span style='color:{pct_color}'>{pnl_pct:+.2f}%</span>" if pnl_pct is not None else "—"
        )
        program_count = sum(1 for _, _, local in [*trailed, *other] if local is not None)
        card.detail.setText(
            f"未实现盈亏 <span style='color:{pnl_color}'>{total_unrealized:+,.2f}</span> · "
            f"持仓 {program_count} 个"
        )

    def refresh_account_page(self) -> None:
        """刷新账户持仓页:余额信息条 + 受监管/未受监管双列表。"""
        monitor = get_active_monitor()
        if monitor is None:
            self.account_balance_label.setText("持仓监控器未运行(请开启交易开关后启动程序)")
            self.trailed_table.setRowCount(0)
            self.other_table.setRowCount(0)
            return
        snapshot, trailed, other = self._account_view_data(monitor)
        # 全市场标记价格流会持续刷新快照时间，即使程序仓位的数据完全没有变化。
        # 因此用实际展示字段的签名决定是否重绘，避免每秒清空并重建整张表。
        content_signature = self._account_content_signature(snapshot, trailed, other)
        if content_signature == self._last_account_content_signature:
            return
        self._last_account_snapshot_at = snapshot["updatedAt"]
        self._last_account_content_signature = content_signature

        usdt = snapshot["balances"].get("USDT", {})
        wallet = float(usdt.get("balance", 0.0))
        available = float(usdt.get("availableBalance", 0.0))
        # 展示盈亏 = 仅程序仓(本地台账记录)的实时浮盈合计,逐秒跳动;
        # mark 缺失时回退官方 unrealizedProfit(WS API 每 30 秒校准);
        # 人工仓不纳入统计口径(表格中仍单独展示其行盈亏)
        total_unrealized = sum(
            self._display_pnl(symbol, info)
            for symbol, info, local in [*trailed, *other]
            if local is not None
        )
        profit_color, loss_color = profit_colors()
        up_color = profit_color if total_unrealized >= 0 else loss_color
        # 未实现盈亏百分比:以钱包余额为基准,与盈亏自动全平触发口径一致
        pnl_pct = total_unrealized / wallet * 100 if wallet > 0 else None
        pnl_pct_text = f" (<b style='color:{up_color}'>{pnl_pct:+.2f}%</b>)" if pnl_pct is not None else ""
        stale_text = " · <b style='color:#B45309'>数据已过期</b>" if snapshot.get("stale") else ""
        self.account_balance_label.setText(
            f"USDT 钱包 <b>{wallet:,.2f}</b> · 可用 <b>{available:,.2f}</b> · "
            f"未实现盈亏 <b style='color:{up_color}'>{total_unrealized:+,.2f}</b>{pnl_pct_text}{stale_text}"
        )
        self.trailed_title.setText(f"移动止损监管中 ({len(trailed)})")
        self.other_title.setText(f"其他持仓 ({len(other)})")
        self._fill_position_table(self.trailed_table, trailed, show_stop=True)
        self._fill_position_table(self.other_table, other, show_stop=False)

    def _account_view_data(self, monitor: Any) -> tuple[dict, list, list]:
        """从监控器快照计算程序持仓的两类展示列表。

        监控器已按程序持仓列表过滤并携带 local 字段；GUI 不再每秒重复读盘，
        从而避免原子替换文件时的瞬时空读影响展示。
        """
        try:
            candidate = monitor.get_account_snapshot()
        except Exception as exc:
            logging.getLogger(__name__).warning("读取持仓监控快照失败，沿用上次界面数据: %s", exc)
            candidate = None
        if (
            isinstance(candidate, dict)
            and isinstance(candidate.get("balances"), dict)
            and isinstance(candidate.get("positions"), dict)
            and "updatedAt" in candidate
        ):
            snapshot = candidate
            self._account_snapshot_cache = candidate
        elif self._account_snapshot_cache is not None:
            # 读取异常时保留上一帧，同时标记为陈旧，不能伪装成实时账户数据。
            snapshot = {**self._account_snapshot_cache, "stale": True}
        else:
            snapshot = {"balances": {}, "positions": {}, "updatedAt": 0.0}
        local_positions = {
            key: dict(info["local"])
            for key, info in snapshot["positions"].items()
            if isinstance(info, dict) and isinstance(info.get("local"), dict)
        }
        trailed_symbols = {
            key for key, position in local_positions.items()
            if position.get("signalType") in TRAILED_SIGNALS
        }
        active = {}
        for key, info in snapshot["positions"].items():
            if key not in local_positions or not isinstance(info, dict):
                continue
            try:
                if abs(float(info.get("positionAmt", 0.0))) > 0:
                    active[key] = info
            except (TypeError, ValueError):
                continue
        trailed = [
            (str(info.get("symbol") or key), info, local_positions.get(key))
            for key, info in active.items() if key in trailed_symbols
        ]
        other = [
            (str(info.get("symbol") or key), info, local_positions.get(key))
            for key, info in active.items() if key not in trailed_symbols
        ]
        return snapshot, trailed, other

    @staticmethod
    def _account_content_signature(snapshot: dict, trailed: list, other: list) -> tuple:
        """返回所有已展示字段的稳定签名，供 GUI 跳过无关的流更新。"""
        usdt = snapshot.get("balances", {}).get("USDT", {})
        balances = (usdt.get("balance"), usdt.get("availableBalance")) if isinstance(usdt, dict) else ()

        def row_signature(row: tuple) -> tuple:
            symbol, info, local = row
            return (
                symbol, info.get("positionSide"), info.get("positionAmt"), info.get("positionAmtStr"),
                info.get("entryPrice"), info.get("markPrice"), info.get("unrealizedProfit"), info.get("leverage"),
                local.get("signalType") if isinstance(local, dict) else None,
                local.get("currentStop") if isinstance(local, dict) else None,
                local.get("trailLevel") if isinstance(local, dict) else None,
            )

        return balances, tuple(row_signature(row) for row in trailed), tuple(row_signature(row) for row in other)

    @staticmethod
    def _display_pnl(symbol: str, info: dict[str, Any]) -> float:
        """单仓展示盈亏:优先实时标记价按官方公式计算,mark 缺失回退官方 unrealizedProfit。

        公式 (mark − entry) × positionAmt 与交易所口径一致;markPrice 每秒由流更新,
        官方 unrealizedProfit 由 WS API 每 30 秒校准。
        """
        mark = float(info.get("markPrice", 0.0))
        if mark > 0:
            amount = float(info.get("positionAmt", 0.0))
            entry = float(info.get("entryPrice", 0.0))
            return (mark - entry) * amount
        return float(info.get("unrealizedProfit", 0.0))

    def _fill_position_table(self, table: QTableWidget, rows: list[tuple], show_stop: bool) -> None:
        """填充账户持仓表格;show_stop 时额外显示当前止损与移动档位,末列为平仓按钮。"""
        columns = [
            ("交易对", "symbol"), ("方向", "direction"), ("数量", "quantity"),
            ("开仓价", "entry"), ("标记价", "mark"), ("未实现盈亏", "unrealized"),
            ("盈亏 %", "pnl_pct"), ("杠杆", "leverage"),
        ]
        if show_stop:
            columns += [("当前止损", "stop"), ("档位", "level")]
        columns.append(("操作", "action"))
        # 保留用户当前选中仓位；整表重绘期间禁用更新，避免流刷新造成闪烁。
        selected_key = None
        current_item = table.item(table.currentRow(), 0) if table.currentRow() >= 0 else None
        if current_item is not None:
            selected_key = current_item.data(Qt.ItemDataRole.UserRole)
        table.setUpdatesEnabled(False)
        try:
            table.setColumnCount(len(columns))
            table.setHorizontalHeaderLabels([label for label, _ in columns])
            table.setRowCount(len(rows))
            for row, (symbol, info, local) in enumerate(rows):
                amount = float(info.get("positionAmt", 0.0))
            # 数量显示与平仓下单优先使用交易所原始数量字符串(大数量时避免科学计数法/截断)
                amount_str = info.get("positionAmtStr") if isinstance(info.get("positionAmtStr"), str) else None
                entry = float(info.get("entryPrice", 0.0))
                mark = float(info.get("markPrice", 0.0))
            # 展示盈亏 = 实时标记价按官方公式计算(markPrice 每秒由流更新),mark 缺失回退官方值
                unrealized = (mark - entry) * amount if mark > 0 else float(info.get("unrealizedProfit", 0.0))
                leverage = int(info.get("leverage", 0) or 0)
            # 杠杆未知(手动开仓且无 symbolConfig 记录)时显示占位,不误导为 1x
                margin = abs(amount) * entry / leverage if entry > 0 and leverage > 0 else 0.0
                pnl_pct = unrealized / margin * 100 if margin > 0 else 0.0
                values: dict[str, str] = {
                "symbol": symbol,
                "direction": "多" if amount > 0 else "空",
                "quantity": self._quantity_text(amount, amount_str),
                "entry": f"{entry:.8g}",
                "mark": f"{mark:.8g}",
                "unrealized": f"{unrealized:+,.2f}",
                "pnl_pct": f"{pnl_pct:+.2f}%" if margin > 0 else "—",
                "leverage": f"{leverage}x" if leverage > 0 else "—",
                }
                if show_stop:
                    current_stop = local.get("currentStop") if isinstance(local, dict) else None
                    level = local.get("trailLevel") if isinstance(local, dict) else None
                    values["stop"] = f"{current_stop:.8g}" if current_stop is not None else "—"
                    values["level"] = str(level) if level is not None else "—"
                profit_color, loss_color = profit_colors()
                row_key = position_key(symbol, info.get("positionSide"))
                for column, (_, field) in enumerate(columns):
                    if field == "action":
                    # 操作列:点击单元格即平仓(无额外按钮),深蓝加粗居中文本,悬停浅色背景下仍清晰
                        item = QTableWidgetItem("平仓")
                        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        item.setForeground(QColor("#1D4ED8"))
                        action_font = item.font()
                        action_font.setBold(True)
                        item.setFont(action_font)
                        item.setData(Qt.ItemDataRole.UserRole, (symbol, amount, amount_str))
                    else:
                        item = QTableWidgetItem(values[field])
                        if field == "unrealized":
                            item.setForeground(QColor(profit_color if unrealized >= 0 else loss_color))
                        if column == 0:
                            item.setData(Qt.ItemDataRole.UserRole, row_key)
                    table.setItem(row, column, item)
                    if row_key == selected_key:
                        table.setCurrentCell(row, 0)
            table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        finally:
            table.setUpdatesEnabled(True)
            table.viewport().update()

    # ---- 平仓操作 ----

    def _handle_position_cell_click(self, row: int, column: int) -> None:
        """账户持仓表格点击:操作列触发平仓确认,其余列进入实时看盘。"""
        table = self.sender()
        if not isinstance(table, QTableWidget):
            return
        if column == table.columnCount() - 1:
            item = table.item(row, column)
            if item is None:
                return
            data = item.data(Qt.ItemDataRole.UserRole)
            if not isinstance(data, tuple) or len(data) != 3:
                return
            amount_str = data[2] if isinstance(data[2], str) and data[2] else None
            self.confirm_close_single(str(data[0]), float(data[1]), amount_str)
            return
        item = table.item(row, 0)
        if item is None:
            return
        symbol = item.text()
        if symbol:
            key = item.data(Qt.ItemDataRole.UserRole)
            self.open_live_symbol_view(symbol, 3, key if isinstance(key, str) else None)

    def confirm_close_single(self, symbol: str, amount: float, amount_str: str | None = None) -> None:
        """单个仓位平仓:二次确认后市价反向平仓。

        amount_str 为交易所原始数量字符串(持仓快照携带),下单时原样传递保证精度。
        """
        direction = "多" if amount > 0 else "空"
        answer = QMessageBox.question(
            self,
            "确认平仓",
            f"市价平仓 {symbol} {direction} {self._quantity_text(amount, amount_str)}?\n\n"
            "平仓后本地记录与残留挂单将由持仓监控自动清理。",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer == QMessageBox.StandardButton.Yes:
            self._close_positions_async([(symbol, amount, amount_str)])

    def confirm_close_list(self, trailed: bool) -> None:
        """一键平仓:平掉指定列表(受监管/其他)内的全部仓位。"""
        monitor = get_active_monitor()
        if monitor is None:
            return
        _, trailed_rows, other_rows = self._account_view_data(monitor)
        rows = trailed_rows if trailed else other_rows
        if not rows:
            QMessageBox.information(self, "一键平仓", "该列表暂无持仓。")
            return
        self._confirm_close_targets(rows, "列表一键平仓")

    def confirm_close_all(self) -> None:
        """一键全平:平掉账户全部持仓。"""
        monitor = get_active_monitor()
        if monitor is None:
            return
        _, trailed_rows, other_rows = self._account_view_data(monitor)
        rows = trailed_rows + other_rows
        if not rows:
            QMessageBox.information(self, "一键全平", "账户暂无持仓。")
            return
        self._confirm_close_targets(rows, "一键全平")

    def _confirm_close_targets(self, rows: list[tuple], title: str) -> None:
        """对一组仓位做二次确认并执行平仓。"""
        names = "、".join(symbol for symbol, _, _ in rows[:8])
        if len(rows) > 8:
            names += f" 等 {len(rows)} 个仓位"
        answer = QMessageBox.question(
            self,
            f"确认{title}",
            f"市价平仓以下仓位:\n{names}\n\n共 {len(rows)} 个,确认执行?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer == QMessageBox.StandardButton.Yes:
            self._close_positions_async([
                (symbol, float(info.get("positionAmt", 0.0)), info.get("positionAmtStr"))
                for symbol, info, _ in rows
            ])

    @staticmethod
    def _quantity_text(amount: float, amount_str: str | None) -> str:
        """生成数量文本:优先交易所原始数量字符串,回退 float 最短表示。

        曾用 f"{abs(amount):g}" 格式化,持仓量超过百万时输出科学计数法
        (如 1.23457e+07)并截断,导致下单报 -1111 精度错误、平仓数量与
        实际持仓不符;现改为 Decimal 原文传递,数量原样送交易所。
        """
        if amount_str:
            try:
                quantity = Decimal(amount_str)
            except (InvalidOperation, TypeError, ValueError):
                quantity = None
            if quantity is not None and quantity.is_finite() and quantity != 0:
                return format(abs(quantity), "f")
        return format(Decimal(str(abs(amount))), "f")

    def _close_positions_async(self, targets: list[tuple[str, float, str | None]]) -> None:
        """后台线程逐仓市价平仓,完成后回 GUI 线程刷新与提示,避免阻塞界面。

        targets 元素为 (symbol, 净持仓 float, 交易所原始数量字符串)。
        """
        self.close_all_button.setEnabled(False)
        self.close_trailed_button.setEnabled(False)
        self.close_other_button.setEnabled(False)

        def worker() -> None:
            results: list[str] = []
            try:
                client = self._build_trading_client()
                dual_side = client.is_dual_side_position()
                for symbol, amount, amount_str in targets:
                    if amount == 0:
                        continue
                    side = "SELL" if amount > 0 else "BUY"
                    position_side = "LONG" if amount > 0 else "SHORT"
                    try:
                        # 单向模式带 reduceOnly 防反向开仓;对冲模式反向 positionSide 天然减仓
                        quantity_text = self._quantity_text(amount, amount_str)
                        if dual_side:
                            client.place_market_order(symbol, side, quantity_text, position_side)
                        else:
                            client.place_market_order(symbol, side, quantity_text, None, reduce_only=True)
                        results.append(f"{symbol} 已平仓")
                    except Exception as exc:
                        results.append(f"{symbol} 失败:{exc}")
            except Exception as exc:
                results.append(f"平仓客户端初始化失败:{exc}")
            # worker 线程无 Qt 事件循环,经信号桥投递到 GUI 线程
            self._close_bridge.finished.emit(results)

        threading.Thread(target=worker, daemon=True).start()

    def _on_close_finished(self, results: list[str]) -> None:
        """平仓完成回调(GUI 线程):恢复按钮、提示结果、请求快照刷新。"""
        self.close_all_button.setEnabled(True)
        self.close_trailed_button.setEnabled(True)
        self.close_other_button.setEnabled(True)
        QMessageBox.information(self, "平仓结果", "\n".join(results) if results else "无可平仓仓位。")
        monitor = get_active_monitor()
        if monitor is not None:
            # 事件流可能不推送归零仓位,请求监控线程立即全量快照对齐
            monitor.request_full_snapshot()
        # 快照刷新异步完成,延迟后强制刷新账户页
        QTimer.singleShot(1500, self._refresh_account_after_close)

    def _refresh_account_after_close(self) -> None:
        self._last_account_snapshot_at = None
        self.refresh_account_page()
        self.refresh_account_card()
        if self.pages.currentIndex() == 5 and self.live_view_open:
            self.refresh_live_view()

    # ---- 实时看盘页 ----

    def open_live_symbol_view(
        self, symbol: str, return_page: int = 3, position_identity: str | None = None,
    ) -> None:
        """进入实时看盘页:加载历史 K 线并订阅实时流(交易所看盘体验)。"""
        if not symbol:
            return
        resolved_key = position_identity or self._live_position_key_for_symbol(symbol)
        if self.live_view_open and self.live_symbol == symbol and self.live_position_key == resolved_key:
            self.pages.setCurrentIndex(5)
            return
        self.live_symbol = symbol
        self.live_position_key = resolved_key
        self.live_view_open = True
        self.live_position_closed = False
        self.live_loading_initial = False
        self.live_return_page = return_page
        self.live_page_title.setText(symbol)
        self.live_page_subtitle.setText("实时 K 线 · 开仓/止损/止盈标注")
        self.live_close_button.setEnabled(True)
        self.live_status.hide()
        self.live_quote_symbol.setText(symbol)
        self.live_quote_price.setText("加载中…")
        self.live_chart.clear()
        self.pages.setCurrentIndex(5)
        self._load_live_history()

    def close_live_page(self) -> None:
        """退出实时看盘页:关闭 K 线流并销毁图表内容(返回进入前页面)。"""
        self.live_view_open = False
        self.live_symbol = ""
        self.live_position_key = ""
        if self.live_kline_stream is not None:
            self.live_kline_stream.stop()
            self.live_kline_stream = None
        self.live_chart.clear()
        self.live_status.hide()
        self.pages.setCurrentIndex(self.live_return_page)

    def _load_live_history(self) -> None:
        """后台线程拉取当前交易对/周期的初始 K 线,完成后经信号桥回调 GUI 线程。"""
        symbol = self.live_symbol
        interval = self.live_interval.currentText()
        self.live_loading_initial = True
        config = read_json_safely(CONFIG_PATH)
        use_testnet = config.get("environment", "production") == "testnet"
        timeout = float(config.get("http_timeout_seconds", 15))

        def worker() -> None:
            try:
                from fetch_klines_for_symbols import fetch_symbol_klines, get_klines_url
                klines = fetch_symbol_klines(symbol, interval, 300, get_klines_url(use_testnet), timeout)
            except Exception:
                klines = None
            self._live_kline_bridge.fetched.emit((symbol, interval, klines))

        threading.Thread(target=worker, daemon=True).start()

    def _on_live_kline_fetched(self, payload: Any) -> None:
        """初始 K 线加载完成(GUI 线程):填充图表后订阅实时流。"""
        if not isinstance(payload, tuple) or len(payload) != 3:
            return
        symbol, interval, klines = payload
        if not self.live_view_open or symbol != self.live_symbol:
            return
        self.live_loading_initial = False
        if not klines:
            self.live_status.setText("K 线加载失败,请稍后重试")
            self.live_status.show()
            return
        self.live_status.hide()
        # 实时看盘页绘制最新价标注线;箱体池/信号池静态图不展示
        self.live_chart.set_data(symbol, klines, None, None, show_latest=True)
        if self.live_kline_stream is None:
            from websocket_streams import KlineStream
            config = read_json_safely(CONFIG_PATH)
            proxy = config.get("proxy_url") if config.get("proxy_enabled") else None
            use_testnet = config.get("environment", "production") == "testnet"
            self.live_kline_stream = KlineStream(use_testnet, proxy, self._on_live_kline_event)
            self.live_kline_stream.start()
        self.live_kline_stream.set_subscription(symbol, interval)
        local = self._live_position_info()[1]
        self._update_live_annotations(local)

    def _on_live_kline_event(self, payload: dict[str, Any]) -> None:
        """K 线流回调(接收线程):组合流解包后经信号桥投递 GUI 线程。"""
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            payload = payload["data"]
        if not isinstance(payload, dict) or payload.get("e") != "kline":
            return
        k = payload.get("k")
        if isinstance(k, dict):
            self._live_kline_bridge.kline.emit(k)

    def _apply_live_kline(self, k: dict[str, Any]) -> None:
        """实时 K 线更新(GUI 线程):按周期边界校验后原地更新图表,不重置视野。"""
        if not self.live_view_open or self.live_loading_initial:
            return
        interval = self.live_interval.currentText()
        seconds = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}.get(interval)
        try:
            open_time = int(k.get("t"))
        except (TypeError, ValueError):
            return
        if seconds is None or open_time % (seconds * 1000) != 0:
            return  # 旧周期残留推送,忽略
        self.live_chart.update_live_candle(k)

    def _on_live_interval_changed(self, interval: str) -> None:
        """切换 K 线周期:重新拉取历史并重建实时流订阅。"""
        if self.live_view_open and self.live_symbol:
            self._load_live_history()

    def refresh_live_view(self) -> None:
        """实时看盘页 1 秒刷新:价格头部、标注线(移动止损抬升)、平仓检测。"""
        if not self.live_view_open or not self.live_symbol:
            return
        info, local = self._live_position_info()
        amount = float(info.get("positionAmt", 0.0)) if isinstance(info, dict) else 0.0
        if abs(amount) <= 0:
            self._mark_live_position_closed()
            return
        mark = float(info.get("markPrice", 0.0) or 0.0)
        entry = float(info.get("entryPrice", 0.0) or 0.0)
        leverage = int(info.get("leverage", 0) or 0)
        unrealized = self._display_pnl(self.live_symbol, info)
        profit_color, loss_color = profit_colors()
        up_color = profit_color if unrealized >= 0 else loss_color
        change_pct = (mark / entry - 1) * 100 if mark > 0 and entry > 0 else 0.0
        change_color = profit_color if change_pct >= 0 else loss_color
        direction = "多" if amount > 0 else "空"
        self.live_quote_price.setText(f"{mark:.8g}")
        # 固定两行排版:第一行持仓要素,第二行盈亏与涨跌(着色),避免自动换行错乱
        self.live_quote_meta.setText(
            f"{direction} 单 · 数量 {abs(amount):g} · 杠杆 {leverage if leverage else '—'}x · 开仓 {entry:.8g}"
            f"<br/>浮动盈亏 <span style='color:{up_color}'>{unrealized:+,.2f}</span> · "
            f"涨跌 <span style='color:{change_color}'>{change_pct:+.2f}%</span>"
        )
        signal_type = str((local or {}).get("signalType") or "—")
        stop_text = "—"
        if local:
            stop = local.get("currentStop") or local.get("initialStop")
            if stop is not None:
                stop_text = f"{float(stop):.8g}"
                # R阶仅受监管仓位展示(上破/下破;高抛低吸无移动止损)
                if local.get("signalType") in TRAILED_SIGNALS:
                    stop_text += f" | R阶 {local.get('trailLevel', 0)}"
        # 止盈仅展示实际存在止盈价的仓位;突破仓位设计上无止盈,不显示「止盈: —」占位
        info_parts = [f"信号: {signal_type}", f"止损: {stop_text}"]
        if local and local.get("takeProfitPrice") is not None:
            info_parts.append(f"止盈: {float(local['takeProfitPrice']):.8g}")
        self.live_position_info.setText(" · ".join(info_parts))
        self._update_live_annotations(local)
        self.live_close_button.setEnabled(True)

    def _update_live_annotations(self, local: dict[str, Any] | None) -> None:
        """刷新标注线:开仓(琥珀实线)/止损(亏损色虚线)/止盈(盈利色虚线)。

        止损取 currentStop(移动止损抬升后实时更新);止盈仅箱体内高抛低吸存在。
        颜色随涨跌配色开关:cn=红盈绿亏,intl=绿盈红亏。
        """
        annotations: list[dict[str, Any]] = []
        profit_color, loss_color = profit_colors()
        info, _ = self._live_position_info()
        entry_price = float((local or {}).get("entryPrice") or (info or {}).get("entryPrice") or 0.0)
        if entry_price > 0:
            annotations.append({"price": entry_price, "label": "开仓", "color": QColor("#F59E0B"), "dashed": False})
        if local:
            stop = local.get("currentStop") or local.get("initialStop")
            if stop is not None:
                # R阶(移动止损档位)仅受监管仓位(上破/下破)存在;高抛低吸无移动止损,不展示
                stop_text = f"止损：{float(stop):.8g}"
                if local.get("signalType") in TRAILED_SIGNALS:
                    stop_text += f" | R阶：{local.get('trailLevel', 0)}"
                annotations.append({
                    "price": float(stop),
                    "label": "止损",
                    "text": stop_text,
                    "color": QColor(loss_color),
                    "dashed": True,
                })
            take_profit = local.get("takeProfitPrice")
            if take_profit is not None:
                annotations.append({
                    "price": float(take_profit),
                    "label": "止盈",
                    "color": QColor(profit_color),
                    "dashed": True,
                })
        self.live_chart.set_position_lines(annotations)

    def _mark_live_position_closed(self) -> None:
        """持仓已平仓:清除标注、禁用平仓按钮、提示横幅(图表保留历史行情)。"""
        if self.live_position_closed:
            return
        self.live_position_closed = True
        self.live_close_button.setEnabled(False)
        self.live_quote_meta.setText("仓位已平仓")
        self.live_chart.set_position_lines([])
        self.live_status.setText("该仓位已平仓,图表保留历史行情。可返回持仓列表查看其他仓位。")
        self.live_status.show()

    def confirm_close_live_position(self) -> None:
        """看盘页市价平仓:复用持仓页的单仓平仓确认流程。"""
        info, _ = self._live_position_info()
        if not isinstance(info, dict):
            return
        amount = float(info.get("positionAmt", 0.0) or 0.0)
        if amount != 0:
            amount_str = info.get("positionAmtStr") if isinstance(info.get("positionAmtStr"), str) else None
            self.confirm_close_single(self.live_symbol, amount, amount_str)

    def _live_position_key_for_symbol(self, symbol: str) -> str:
        """按交易对解析唯一的程序仓位键；对冲双向同时存在时不猜测方向。"""
        monitor = get_active_monitor()
        if monitor is None:
            return ""
        snapshot, _, _ = self._account_view_data(monitor)
        keys = [
            key for key, info in snapshot["positions"].items()
            if isinstance(info, dict) and info.get("symbol") == symbol
            and abs(float(info.get("positionAmt", 0.0) or 0.0)) > 0
        ]
        return keys[0] if len(keys) == 1 else ""

    def _live_position_info(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """读取实时看盘目标的程序仓位；使用持仓键以兼容对冲模式。"""
        monitor = get_active_monitor()
        if monitor is None:
            return None, None
        snapshot, _, _ = self._account_view_data(monitor)
        info = snapshot["positions"].get(self.live_position_key)
        if not isinstance(info, dict) and not self.live_position_key:
            candidates = [
                value for value in snapshot["positions"].values()
                if isinstance(value, dict) and value.get("symbol") == self.live_symbol
            ]
            info = candidates[0] if len(candidates) == 1 else None
        return info if isinstance(info, dict) else None, (
            dict(info["local"]) if isinstance(info, dict) and isinstance(info.get("local"), dict) else None
        )

    def _symbol_has_position(self, symbol: str) -> bool:
        """判断交易对当前是否有持仓(本地台账记录或交易所快照)。"""
        if any(p.get("symbol") == symbol for p in load_positions(self._current_environment())):
            return True
        monitor = get_active_monitor()
        if monitor is None:
            return False
        snapshot, _, _ = self._account_view_data(monitor)
        return any(
            isinstance(info, dict) and info.get("symbol") == symbol
            and abs(float(info.get("positionAmt", 0.0) or 0.0)) > 0
            for info in snapshot["positions"].values()
        )

    def _current_environment(self) -> str:
        """返回配置中的当前运行环境。"""
        return str(read_json_safely(CONFIG_PATH).get("environment", "production"))

    def _build_trading_client(self) -> Any:
        """按当前运行环境构建平仓签名客户端(与调度器/监控器独立)。"""
        from binance_futures_client import BinanceFuturesClient

        environment = self._current_environment()
        if environment == "testnet":
            api_key = get_secret("BINANCE_TESTNET_API_KEY")
            api_secret = get_secret("BINANCE_TESTNET_API_SECRET")
        else:
            api_key = get_secret("BINANCE_API_KEY")
            api_secret = get_secret("BINANCE_API_SECRET")
        if not api_key or not api_secret:
            raise RuntimeError("缺少 Binance API 密钥,无法平仓。")
        return BinanceFuturesClient(api_key, api_secret, environment == "testnet", 15)

    def open_symbol_page(self, row: int, _: int = 0) -> None:
        """列表页点击行进入币详情页。

        表格支持排序后行号不再对应原始顺序,以首列交易对 symbol 在数据中定位。
        """
        cell = self.table.item(row, 0)
        if cell is None:
            return
        symbol = cell.text()
        # 历史模式下列表展示的是快照数据,交易对须在快照中定位(而非实时数据)
        title, items = self._current_pool_items()
        item = next((entry for entry in items if str(entry.get("symbol", "")) == symbol), None)
        if item is None:
            return
        self.open_symbol_record(item, title)

    def open_symbol_record(self, item: dict[str, Any], title: str) -> None:
        """打开指定交易对记录的币详情页。"""
        self.current_symbol = str(item.get("symbol", "—"))
        # 持仓中的交易对显示实时看盘入口
        self.live_jump_button.setVisible(self._symbol_has_position(self.current_symbol))
        self.symbol_page_title.setText(self.current_symbol)
        # 仅箱体池与信号池展示 K 线图(其余池只展示结构化指标)
        if self.current_pool_key in ("boxes", "signals"):
            self.symbol_page_subtitle.setText(f"{title} · 结构化指标与 K 线")
            self.show_kline(item)
        else:
            self.symbol_page_subtitle.setText(f"{title} · 结构化指标")
            self.kline_chart.hide()
        self.render_metrics(title, item)
        self.pages.setCurrentIndex(2)

    def show_kline(self, item: dict[str, Any]) -> None:
        """加载 K 线并绘制箱体/信号标注(仅箱体池与信号池调用)。

        箱体池:结构周期 K 线 + 识别箱体;信号池:执行周期 K 线(信号所在周期)
        + 信号箱体 + 信号点策略信息卡。
        """
        config = read_json_safely(CONFIG_PATH)
        base = runtime_directory(config.get("environment", "production"))
        symbol = str(item.get("symbol", ""))
        signal: dict[str, Any] | None = None
        if self.current_pool_key == "signals":
            kline_file = base / (
                f"execution_klines_{config.get('execution_interval', '15m')}"
                f"_{config.get('execution_kline_limit', 150)}.json"
            )
            box = item.get("box") if isinstance(item.get("box"), dict) else None
            signal = item
        else:
            kline_file = base / (
                f"structure_klines_{config.get('structure_interval', '1h')}"
                f"_{config.get('structure_kline_limit', 300)}.json"
            )
            box = item
        payload = self._load_cached_json(kline_file)
        klines = payload.get("klines", {}) if isinstance(payload, dict) else {}
        raw_klines = klines.get(symbol)
        if not isinstance(raw_klines, list):
            raw_klines = []
        self.kline_chart.set_data(symbol, raw_klines, box, signal)
        self.kline_chart.show()

    def _load_cached_json(self, path: Path) -> dict[str, Any]:
        """按文件指纹缓存 JSON 数据池,避免每次点击重复解析大文件。"""
        fingerprint = (str(path), path.stat().st_mtime_ns if path.is_file() else 0)
        cache = getattr(self, "_json_cache", None)
        if cache is not None and cache[0] == fingerprint:
            return cache[1]
        payload = read_json_safely(path)
        self._json_cache = (fingerprint, payload)
        return payload

    def clear_metrics(self, message: str) -> None:
        while self.metrics_layout.count():
            child = self.metrics_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        if message:
            self.metrics_layout.addWidget(QLabel(message, objectName="muted"), 0, 0)

    def render_metrics(self, pool_title: str, item: dict[str, Any]) -> None:
        self.clear_metrics("")
        # 统一使用 4 列网格:双排分组(columns=2)的面板各跨 2 列,均分填满宽度不留白
        total_columns = 4
        for column in range(total_columns):
            self.metrics_layout.setColumnStretch(column, 1)
        summary = self._quote_panel(item) if self.current_pool_key == "signals" else self._summary_panel(pool_title, item)
        self.metrics_layout.addWidget(summary, 0, 0, 1, total_columns)
        row = 1
        for section_title, fields, columns in self._metric_groups(self.current_pool_key, item):
            span = total_columns // columns
            self.metrics_layout.addWidget(QLabel(section_title, objectName="sectionTitle"), row, 0, 1, total_columns)
            row += 1
            for index, (label, value) in enumerate(fields):
                panel = QFrame(objectName="metricPanel")
                panel_layout = QVBoxLayout(panel)
                panel_layout.setContentsMargins(14, 12, 14, 12)
                panel_layout.addWidget(QLabel(label, objectName="metricTitle"))
                value_label = QLabel(format_metric(label, value), objectName="metricValue")
                # 长文本自动换行,信息流只上下滚动、不横向超出界面
                value_label.setWordWrap(True)
                panel_layout.addWidget(value_label)
                self.metrics_layout.addWidget(
                    panel, row + index // columns, (index % columns) * span, 1, span
                )
            row += (len(fields) + columns - 1) // columns

    def _quote_panel(self, item: dict[str, Any]) -> QFrame:
        """构建类似交易所报价头部的交易对摘要。"""
        panel = QFrame(objectName="quotePanel")
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(22, 17, 22, 17)
        symbol_box = QVBoxLayout()
        symbol_box.addWidget(QLabel(str(item.get("symbol", "—")), objectName="quoteSymbol"))
        status = display_signal_type(item.get("signalType"), item.get("direction")) if item.get("signalType") else item.get("status") or "已纳入数据池"
        direction = display_direction(item.get("direction")) if item.get("direction") else ""
        symbol_box.addWidget(QLabel(f"{status}{' · ' + direction if direction else ''}", objectName="quoteMeta"))
        layout.addLayout(symbol_box)
        layout.addStretch(1)
        price = self._lookup(item, "signalKline.close") or self._lookup(item, "binance24hr.last_price") or item.get("mid") or item.get("upper")
        price_box = QVBoxLayout()
        price_label = QLabel("最新/信号价格", objectName="quoteMeta")
        price_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        value_label = QLabel(format_metric("价格", price), objectName="quotePrice")
        value_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        price_box.addWidget(price_label)
        price_box.addWidget(value_label)
        layout.addLayout(price_box)
        return panel

    def _summary_panel(self, pool_title: str, item: dict[str, Any]) -> QFrame:
        """为非信号池提供交易对与数据用途摘要，不展示价格。"""
        panel = QFrame(objectName="quotePanel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(22, 17, 22, 17)
        layout.addWidget(QLabel(str(item.get("symbol", "—")), objectName="quoteSymbol"))
        descriptions = {
            "合约池": "Binance 永续合约交易规则与交易对参数",
            "市值池": "CoinGecko 市值匹配成功且符合市值门槛的交易对",
            "24h 成交量池": "通过 24 小时 USDT 成交额门槛的交易对",
            "箱体池": "当前结构周期识别出的动态波动率箱体",
        }
        layout.addWidget(QLabel(descriptions.get(pool_title, "交易对详细信息"), objectName="quoteMeta"))
        return panel

    def _metric_fields(self, key: str, item: dict[str, Any]) -> list[tuple[str, Any]]:
        fields = [("交易对", item.get("symbol")), ("基础币", item.get("baseAsset")), ("报价资产", item.get("quoteAsset")), ("状态", item.get("status"))]
        if key == "market":
            fields += [("CoinGecko ID", self._lookup(item, "coingecko.coin_id")), ("市值 (USD)", self._lookup(item, "coingecko.market_cap_usd"))]
        elif key == "trading":
            fields += [("24h 成交额 (USDT)", self._lookup(item, "binance24hr.quote_volume_usdt")), ("24h 涨跌幅", self._lookup(item, "binance24hr.price_change_percent")), ("最新价", self._lookup(item, "binance24hr.last_price"))]
        elif key == "boxes":
            fields = [("交易对", item.get("symbol")), ("箱体窗口", item.get("window")), ("箱体评分", item.get("boxScore")), ("上沿", item.get("upper")), ("下沿", item.get("lower")), ("中轴", item.get("mid")), ("宽度", item.get("widthPct")), ("波动率 σ", item.get("sigma")), ("上沿触达", item.get("upperTouchScore")), ("下沿触达", item.get("lowerTouchScore")), ("中轴穿越", item.get("midCrossCount")), ("开始时间", item.get("startTime")), ("结束时间", item.get("endTime"))]
        elif key == "signals":
            fields = [("交易对", item.get("symbol")), ("信号类型", item.get("signalType")), ("交易方向", item.get("direction")), ("箱体窗口", self._lookup(item, "box.window")), ("箱体评分", self._lookup(item, "box.boxScore")), ("箱体上沿", self._lookup(item, "box.upper")), ("箱体下沿", self._lookup(item, "box.lower")), ("信号收盘价", self._lookup(item, "signalKline.close")), ("信号成交额", self._lookup(item, "signalKline.quoteVolumeUsdt")), ("成交量倍率", self._lookup(item, "executionMetrics.volumeRatio")), ("OI 变化", self._lookup(item, "executionMetrics.oiChangePct")), ("突破缓冲", self._lookup(item, "executionMetrics.breakoutBuffer"))]
        return fields

    def _metric_groups(self, key: str, item: dict[str, Any]) -> list[tuple[str, list[tuple[str, Any]], int]]:
        """按交易所阅读习惯组织详情页指标。

        返回 (分组标题, 字段列表, 每行列数);交易规则字段较多使用双排信息流,
        其余分组保持四列;信息流只上下滚动,不横向超出界面。
        """
        basic = [("基础资产", item.get("baseAsset")), ("报价资产", item.get("quoteAsset")), ("合约状态", item.get("status")), ("合约类型", item.get("contractType"))]
        if key == "market":
            return [
                ("合约概要", basic, 2),
                ("市值筛选数据", [
                    ("CoinGecko 币种 ID", self._lookup(item, "coingecko.coin_id")),
                    ("市值 (USD)", self._lookup(item, "coingecko.market_cap_usd")),
                    ("匹配来源", self._lookup(item, "coingecko.match_source")),
                    ("匹配状态", self._lookup(item, "coingecko.match_status")),
                    ("市场数据更新时间", self._lookup(item, "coingecko.market_data_updated_at")),
                ], 2),
                ("交易规则", self._contract_rule_fields(item), 2),
            ]
        if key == "trading":
            return [
                ("合约概要", basic, 2),
                ("24 小时流动性", [
                    ("成交额 (USDT)", self._lookup(item, "binance24hr.quote_volume_usdt")),
                    ("成交量", self._lookup(item, "binance24hr.base_volume")),
                    ("价格变动", self._lookup(item, "binance24hr.price_change")),
                    ("涨跌幅", self._lookup(item, "binance24hr.price_change_percent")),
                    ("最新价", self._lookup(item, "binance24hr.last_price")),
                    ("成交笔数", self._lookup(item, "binance24hr.trade_count")),
                    ("统计开始时间", self._lookup(item, "binance24hr.open_time")),
                    ("统计结束时间", self._lookup(item, "binance24hr.close_time")),
                ], 2),
                ("交易规则", self._contract_rule_fields(item), 2),
            ]
        if key == "boxes":
            return [
                ("箱体结构", [
                    ("识别窗口", item.get("window")), ("箱体评分", item.get("boxScore")),
                    ("上沿", item.get("upper")), ("下沿", item.get("lower")),
                    ("窗口内最高价", item.get("extremeHigh")), ("窗口内最低价", item.get("extremeLow")),
                    ("中轴", item.get("mid")), ("箱体宽度", item.get("widthPct")),
                    ("允许最大宽度", item.get("maxWidthPct")), ("价格漂移", item.get("driftPct")),
                    ("允许最大漂移", item.get("maxDriftPct")), ("边沿缓冲区", item.get("edgeZonePct")),
                ], 4),
                ("震荡质量", [
                    ("波动率 σ", item.get("sigma")), ("上沿触达评分", item.get("upperTouchScore")),
                    ("下沿触达评分", item.get("lowerTouchScore")), ("要求触达评分", item.get("requiredTouchScore")),
                    ("上沿独立触达", item.get("upperTouchEvents")), ("下沿独立触达", item.get("lowerTouchEvents")),
                    ("中轴穿越次数", item.get("midCrossCount")), ("要求穿越次数", item.get("requiredCrosses")),
                    ("最高候选评分", self._lookup(item, "selection.highestScoreForSymbol")),
                    ("评分容差", self._lookup(item, "selection.scoreTolerance")),
                    ("开始时间", item.get("startTime")), ("结束时间", item.get("endTime")),
                ], 4),
            ]
        if key == "signals":
            return [("交易信号", [("信号类型", display_signal_type(item.get("signalType"), item.get("direction"))), ("交易方向", display_direction(item.get("direction"))), ("信号时间", self._lookup(item, "signalKline.closeTime")), ("信号收盘价", self._lookup(item, "signalKline.close")), ("信号成交额 (USDT)", self._lookup(item, "signalKline.quoteVolumeUsdt")), ("成交量倍率", self._lookup(item, "executionMetrics.volumeRatio"))], 4), ("箱体与持仓", [("箱体窗口", self._lookup(item, "box.window")), ("箱体评分", self._lookup(item, "box.boxScore")), ("箱体上沿", self._lookup(item, "box.upper")), ("箱体下沿", self._lookup(item, "box.lower")), ("OI 变化", self._lookup(item, "executionMetrics.oiChangePct")), ("突破缓冲", self._lookup(item, "executionMetrics.breakoutBuffer"))], 4)]
        return [("合约信息", basic, 2), ("交易规则", self._contract_rule_fields(item), 2)]

    @staticmethod
    def _contract_rule_fields(item: dict[str, Any]) -> list[tuple[str, Any]]:
        """完整展开 Binance 交易对规则，包括 filters 内的每一项（界面翻译为中文易读样式）。"""
        labels = {
            "symbol": "交易对", "pair": "交易对简称", "baseAsset": "基础资产", "quoteAsset": "报价资产",
            "marginAsset": "保证金资产", "pricePrecision": "价格精度", "quantityPrecision": "数量精度",
            "baseAssetPrecision": "基础资产精度", "quotePrecision": "报价精度", "triggerProtect": "触发保护",
            "liquidationFee": "强平费率", "marketTakeBound": "市价单价格保护", "maxMoveOrderLimit": "最大订单限制",
            "underlyingType": "标的类型", "underlyingSubType": "标的子类型", "settlePlan": "结算计划",
            "deliveryDate": "交割时间", "onboardDate": "上线时间", "orderTypes": "支持订单类型",
            "timeInForce": "支持有效期", "permissionSets": "权限集合",
        }
        filter_type_names = {
            "PRICE_FILTER": "价格过滤", "LOT_SIZE": "数量规则", "MARKET_LOT_SIZE": "市价数量规则",
            "MAX_NUM_ORDERS": "最大挂单数", "MIN_NOTIONAL": "最小名义金额", "PERCENT_PRICE": "价格百分比限制",
            "POSITION_RISK_CONTROL": "仓位风险控制",
        }
        filter_field_names = {
            "tickSize": "最小价格精度", "minPrice": "最低价", "maxPrice": "最高价",
            "minQty": "最小数量", "maxQty": "最大数量", "stepSize": "数量步进",
            "notional": "名义金额 (USDT)", "multiplierUp": "向上倍数", "multiplierDown": "向下倍数",
            "multiplierDecimal": "倍数小数位", "limit": "上限", "positionControlSide": "持仓控制方向",
        }
        excluded = {"filters", "symbol", "baseAsset", "quoteAsset", "status", "contractType"}
        fields: list[tuple[str, Any]] = []
        for key, value in item.items():
            if key in excluded or isinstance(value, dict):
                continue
            label = labels.get(key, key)
            if key == "deliveryDate":
                # 当前程序目标均为永续合约,交割时间统一显示为永续合约
                value = "永续合约"
            if isinstance(value, list):
                parts: list[str] = []
                for element in value:
                    if isinstance(element, list):
                        parts.extend(str(part) for part in element)
                    else:
                        parts.append(str(element))
                translations = LABEL_TRANSLATIONS.get(label)
                if translations:
                    parts = [translations.get(part, part) for part in parts]
                value = "、".join(parts)
            fields.append((label, value))
        filters = item.get("filters")
        if isinstance(filters, list):
            for rule in filters:
                if not isinstance(rule, dict):
                    continue
                rule_type = str(rule.get("filterType", "规则"))
                rule_label = filter_type_names.get(rule_type, rule_type)
                for key, value in rule.items():
                    if key != "filterType":
                        field_label = filter_field_names.get(key, key)
                        fields.append((f"{rule_label} · {field_label}", value))
        return fields


def main() -> None:
    """启动图形界面。"""
    import faulthandler
    import os
    import sys

    # windowed 打包(exe)后控制台被隐藏,sys.stdout/stderr 为 None:
    # faulthandler.enable() 默认写 stderr 会抛 RuntimeError,print/logging 兜底同样会崩,
    # 重定向到 devnull 保住崩溃转储能力并让程序正常启动
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w", encoding="utf-8")
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")

    # 崩溃诊断:程序异常退出时转储各线程 Python 栈,便于定位原生崩溃点
    faulthandler.enable()
    application = QApplication(sys.argv)
    application.setFont(QFont("Microsoft YaHei UI", 10))
    window = MainWindow()
    window.show()
    raise SystemExit(application.exec())


if __name__ == "__main__":
    main()
