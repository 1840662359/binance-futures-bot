"""自绘 K 线蜡烛图组件:绘制结构周期 K 线,并把识别到的箱体圈出来。

基于 PySide6 QPainter 手绘(无第三方绘图依赖),支持:
- 蜡烛图(阳线红/阴线绿,中国习惯);
- 箱体可视化:上下沿实线 + 半透明区域 + 中轴虚线;
- 滚轮以鼠标为锚点缩放、拖拽平移、双击复位。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from PySide6.QtCore import QRectF, Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QWidget

# GUI 展示层统一使用北京时间(UTC+8);程序内部时间仍为 UTC,互不干扰
CHINA_TIMEZONE = timezone(timedelta(hours=8))

UP_COLOR = QColor("#E04A4A")      # 涨(默认 cn 风格:红涨)
DOWN_COLOR = QColor("#2FA36B")    # 跌(默认 cn 风格:绿跌)
BOX_COLOR = QColor("#F59E0B")     # 箱体标注(与箱体池卡片同色系)
BOX_FILL = QColor(245, 158, 11, 36)
GRID_COLOR = QColor("#E8EDF5")
TEXT_COLOR = QColor("#52627D")
BACKGROUND = QColor("#FFFFFF")
CARD_BACKGROUND = QColor(255, 255, 255, 235)
CARD_BORDER = QColor("#DDE7F5")

MIN_VISIBLE_COUNT = 20


def apply_color_style(style: str) -> None:
    """按配色风格设置涨跌颜色:cn=红涨绿跌(中国习惯),intl=绿涨红跌(国际习惯)。"""
    global UP_COLOR, DOWN_COLOR
    if style == "intl":
        UP_COLOR = QColor("#2FA36B")
        DOWN_COLOR = QColor("#E04A4A")
    else:
        UP_COLOR = QColor("#E04A4A")
        DOWN_COLOR = QColor("#2FA36B")

# 信号类型的易读解释(翻译成人能直接理解的策略含义)
SIGNAL_EXPLANATIONS = {
    "上破箱体上沿": "收盘突破上沿+缓冲确认,追多",
    "下破箱体下沿": "收盘跌破下沿+缓冲确认,追空",
    "箱体内高抛": "上影刺穿上沿后收回,逢高做空",
    "箱体内低吸": "下影刺穿下沿后收回,逢低做多",
}


class KlineChartWidget(QWidget):
    """K 线蜡烛图 + 箱体可视化组件。"""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.symbol = ""
        # 每根蜡烛: (open_time, open, high, low, close)
        self.candles: list[tuple[int, float, float, float, float]] = []
        self.box: dict[str, Any] | None = None
        # 信号信息(信号池):含 signalType/direction/signalKline/executionMetrics/box
        self.signal: dict[str, Any] | None = None
        # 持仓标注线(实时看盘页): [{"price", "label", "color", "dashed"}, ...]
        self.position_lines: list[dict[str, Any]] = []
        # 最新价标注线仅实时看盘页展示;箱体池/信号池静态图不绘制
        self.show_latest = False
        self.visible_count = 80
        self.end_index = 0
        self._plot_rect = QRectF()
        self._drag_start_x: int | None = None
        self._drag_start_end = 0
        self.setMinimumHeight(280)

    # ---- 数据接口 ----

    def set_data(
        self,
        symbol: str,
        raw_klines: list[Any],
        box: dict[str, Any] | None,
        signal: dict[str, Any] | None = None,
        show_latest: bool = False,
    ) -> None:
        """设置交易对、原始 K 线数组(Binance 12 字段格式)、箱体参数与可选信号信息。

        show_latest=True 时绘制最新价标注线(仅实时看盘页使用)。
        """
        candles: list[tuple[int, float, float, float, float]] = []
        for kline in raw_klines:
            if not isinstance(kline, list) or len(kline) < 5:
                continue
            try:
                candles.append((int(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])))
            except (TypeError, ValueError):
                continue
        self.symbol = symbol
        self.candles = candles
        self.box = box
        self.signal = signal
        self.show_latest = show_latest
        self.position_lines = []  # 换数据源时清空持仓标注
        self.end_index = len(candles)
        self.visible_count = min(80, max(MIN_VISIBLE_COUNT, len(candles)))
        self.update()

    def set_position_lines(self, annotations: list[dict[str, Any]]) -> None:
        """设置持仓标注线(开仓/止损/止盈),每项: {"price", "label", "color", "dashed"}。"""
        self.position_lines = [annotation for annotation in annotations if isinstance(annotation, dict)]
        self.update()

    def update_live_candle(self, kline: dict[str, Any]) -> None:
        """实时更新最后一根 K 线:未收盘原地更新,新周期追加;不重置缩放平移视野。"""
        try:
            open_time = int(kline["t"])
            open_price = float(kline["o"])
            high_price = float(kline["h"])
            low_price = float(kline["l"])
            close_price = float(kline["c"])
        except (KeyError, TypeError, ValueError):
            return
        if close_price <= 0:
            return
        if self.candles and self.candles[-1][0] == open_time:
            self.candles[-1] = (open_time, open_price, high_price, low_price, close_price)
        else:
            # 新周期:追加蜡烛并让视野跟随最新
            self.candles.append((open_time, open_price, high_price, low_price, close_price))
            self.end_index = len(self.candles)
        self.update()

    def clear(self) -> None:
        """清空数据。"""
        self.symbol = ""
        self.candles = []
        self.box = None
        self.signal = None
        self.position_lines = []
        self.show_latest = False
        self.update()

    # ---- 绘制 ----

    def paintEvent(self, event: Any) -> None:
        painter = QPainter(self)
        painter.fillRect(self.rect(), BACKGROUND)
        if not self.candles:
            painter.setPen(TEXT_COLOR)
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "无 K 线数据")
            return
        self._draw_title(painter)
        plot = self._compute_plot_rect()
        self._plot_rect = plot
        self._draw_grid(painter, plot)
        self._draw_box(painter, plot)
        self._draw_candles(painter, plot)
        self._draw_position_lines(painter, plot)
        self._draw_time_axis(painter, plot)
        if self.signal is not None:
            self._draw_signal(painter, plot)
        elif self.show_latest:
            # 最新价标注线仅实时看盘页绘制;箱体池/信号池静态图不展示
            self._draw_latest_price_line(painter, plot)

    def _draw_title(self, painter: QPainter) -> None:
        """绘制标题行:交易对与箱体参数摘要。"""
        painter.setPen(TEXT_COLOR)
        font = painter.font()
        font.setBold(True)
        font.setPointSize(9)
        painter.setFont(font)
        text = self.symbol or "K 线"
        if self.box is not None:
            window = self.box.get("window")
            score = self.box.get("boxScore")
            upper = self.box.get("upper")
            lower = self.box.get("lower")
            mid = self.box.get("mid")
            width = self.box.get("widthPct")
            # 信号携带的箱体不含 mid,按上下沿推算
            if mid is None and isinstance(upper, (int, float)) and isinstance(lower, (int, float)):
                mid = (upper + lower) / 2
            text += (
                f" · 箱体 窗口{window} · 评分{score:g} · "
                f"上沿 {upper:g} / 下沿 {lower:g} / 中轴 {mid:g}"
                + (f" · 宽度 {width * 100:.2f}%" if width is not None else "")
            )
        painter.drawText(QRectF(6, 2, self.width() - 12, 20), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, text)
        font.setBold(False)
        painter.setFont(font)

    def _compute_plot_rect(self) -> QRectF:
        # 底部预留 22px 时间坐标轴
        return QRectF(8, 26, self.width() - 60, self.height() - 26 - 22)

    def _draw_time_axis(self, painter: QPainter, plot: QRectF) -> None:
        """横轴时间坐标:均匀刻度 + 竖虚线对齐 + 北京时间文本。"""
        start, end = self._visible_range()
        if end <= start:
            return
        axis_top = plot.bottom()
        axis_height = max(8.0, self.height() - axis_top - 2)
        tick_count = 5
        for tick in range(tick_count + 1):
            index = start + (end - start) * tick / tick_count
            index = min(end - 1, int(index))
            x = self._x_for_index(index, start, end, plot)
            painter.setPen(QPen(GRID_COLOR, 1, Qt.PenStyle.DashLine))
            painter.drawLine(int(x), int(plot.top()), int(x), int(axis_top))
            painter.setPen(TEXT_COLOR)
            painter.drawText(
                QRectF(x - 42, axis_top + 2, 84, axis_height),
                Qt.AlignmentFlag.AlignCenter,
                format_kline_time(self.candles[index][0]),
            )
        painter.setPen(QPen(GRID_COLOR, 1))
        painter.drawLine(int(plot.left()), int(axis_top), int(plot.right()), int(axis_top))

    def _visible_range(self) -> tuple[int, int]:
        """返回可见窗口 [start, end)。"""
        start = max(0, self.end_index - self.visible_count)
        end = min(len(self.candles), self.end_index)
        if end - start < MIN_VISIBLE_COUNT:
            start = max(0, end - MIN_VISIBLE_COUNT)
        return start, end

    def _price_range(self, start: int, end: int) -> tuple[float, float]:
        low = min(candle[3] for candle in self.candles[start:end])
        high = max(candle[2] for candle in self.candles[start:end])
        if self.box is not None:
            upper = self.box.get("upper")
            lower = self.box.get("lower")
            if isinstance(upper, (int, float)):
                high = max(high, float(upper))
            if isinstance(lower, (int, float)):
                low = min(low, float(lower))
        # 持仓标注线(开仓/止损/止盈)纳入价格范围,保证线始终可见
        for annotation in self.position_lines:
            price = annotation.get("price")
            if isinstance(price, (int, float)):
                high = max(high, float(price))
                low = min(low, float(price))
        span = high - low
        if span <= 0:
            span = high * 0.01 or 1.0
        return low - span * 0.05, high + span * 0.05

    def _x_for_index(self, index: int, start: int, end: int, plot: QRectF) -> float:
        total = max(1, end - start)
        # 右侧预留约 10% 空白,最新一根蜡烛不被右边界贴住(交易所看盘习惯)
        padded = total + max(1, total // 10)
        return plot.left() + (index - start + 0.5) / padded * plot.width()

    def _y_for_price(self, price: float, low: float, high: float, plot: QRectF) -> float:
        span = high - low
        return plot.bottom() - (price - low) / span * plot.height()

    def _draw_grid(self, painter: QPainter, plot: QRectF) -> None:
        start, end = self._visible_range()
        low, high = self._price_range(start, end)
        painter.setPen(QPen(GRID_COLOR, 1))
        for tick in range(5):
            price = low + (high - low) * tick / 4
            y = self._y_for_price(price, low, high, plot)
            painter.drawLine(int(plot.left()), int(y), int(plot.right()), int(y))
            painter.setPen(TEXT_COLOR)
            painter.drawText(
                QRectF(plot.right() + 4, y - 8, 44, 16),
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                f"{price:.4g}",
            )
            painter.setPen(QPen(GRID_COLOR, 1))

    def _draw_box(self, painter: QPainter, plot: QRectF) -> None:
        if self.box is None:
            return
        upper = self.box.get("upper")
        lower = self.box.get("lower")
        mid = self.box.get("mid")
        if not all(isinstance(value, (int, float)) for value in (upper, lower)):
            return
        start, end = self._visible_range()
        low, high = self._price_range(start, end)
        y_upper = self._y_for_price(float(upper), low, high, plot)
        y_lower = self._y_for_price(float(lower), low, high, plot)
        y_mid = self._y_for_price(float(mid), low, high, plot) if isinstance(mid, (int, float)) else None
        # 箱体时间范围(以 K 线索引近似:箱体位于序列末尾的 window 根)
        window = int(self.box.get("window") or 0)
        box_start = max(start, len(self.candles) - window)
        x_left = self._x_for_index(box_start, start, end, plot)
        x_right = plot.right()
        # 半透明箱体区域
        painter.fillRect(QRectF(x_left, y_upper, x_right - x_left, y_lower - y_upper), BOX_FILL)
        # 上下沿实线 + 中轴虚线
        pen = QPen(BOX_COLOR, 1.6)
        painter.setPen(pen)
        painter.drawLine(int(x_left), int(y_upper), int(x_right), int(y_upper))
        painter.drawLine(int(x_left), int(y_lower), int(x_right), int(y_lower))
        if y_mid is not None:
            dash = QPen(BOX_COLOR, 1, Qt.PenStyle.DashLine)
            painter.setPen(dash)
            painter.drawLine(int(x_left), int(y_mid), int(x_right), int(y_mid))

    def _draw_position_lines(self, painter: QPainter, plot: QRectF) -> None:
        """绘制持仓标注线(开仓/止损/止盈):横线 + 左侧名称标签,线随涨跌配色。"""
        if not self.position_lines:
            return
        start, end = self._visible_range()
        low, high = self._price_range(start, end)
        font = painter.font()
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        for annotation in self.position_lines:
            price = annotation.get("price")
            if not isinstance(price, (int, float)):
                continue
            color = annotation.get("color")
            if not isinstance(color, QColor):
                continue
            y = self._y_for_price(float(price), low, high, plot)
            if y < plot.top() or y > plot.bottom():
                continue
            pen = QPen(color, 1.4, Qt.PenStyle.DashLine if annotation.get("dashed") else Qt.PenStyle.SolidLine)
            painter.setPen(pen)
            painter.drawLine(int(plot.left()), int(y), int(plot.right()), int(y))
            # 优先使用调用方提供的完整文本(如「止损：0.03281 | R阶：2」),否则默认「标签 价格」
            text = annotation.get("text") or f"{annotation.get('label', '')} {price:g}"
            label_width = max(60, min(painter.fontMetrics().horizontalAdvance(text) + 12, int(plot.width() * 0.45)))
            label_rect = QRectF(plot.left() + 4, y - 8, label_width, 16)
            painter.fillRect(label_rect, CARD_BACKGROUND)
            painter.drawRect(label_rect)
            painter.setPen(color)
            painter.drawText(label_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, text)
        font.setBold(False)
        painter.setFont(font)

    def _draw_latest_price_line(self, painter: QPainter, plot: QRectF) -> None:
        """绘制最新价标注线:与开仓价同样式(横线 + 左侧标签),颜色随当根蜡烛涨跌。

        最新价取可见区域内最后一根蜡烛的收盘价;实时流更新时随蜡烛原地重绘。
        """
        start, end = self._visible_range()
        if end <= start:
            return
        low, high = self._price_range(start, end)
        last = self.candles[end - 1]
        price = last[4]
        y = self._y_for_price(price, low, high, plot)
        if y < plot.top() or y > plot.bottom():
            return
        color = UP_COLOR if last[4] >= last[1] else DOWN_COLOR
        pen = QPen(color, 1.4)
        painter.setPen(pen)
        painter.drawLine(int(plot.left()), int(y), int(plot.right()), int(y))
        label_rect = QRectF(plot.left() + 4, y - 8, 120, 16)
        painter.fillRect(label_rect, CARD_BACKGROUND)
        painter.drawRect(label_rect)
        painter.setPen(color)
        font = painter.font()
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        painter.drawText(
            label_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, f"最新 {price:g}"
        )
        font.setBold(False)
        painter.setFont(font)

    def _draw_candles(self, painter: QPainter, plot: QRectF) -> None:
        start, end = self._visible_range()
        low, high = self._price_range(start, end)
        step = plot.width() / max(1, end - start)
        body_width = max(1.0, step * 0.62)
        for index in range(start, end):
            _, open_price, high_price, low_price, close_price = self.candles[index]
            x = self._x_for_index(index, start, end, plot)
            color = UP_COLOR if close_price >= open_price else DOWN_COLOR
            painter.setPen(QPen(color, 1.2))
            painter.drawLine(int(x), int(self._y_for_price(high_price, low, high, plot)),
                             int(x), int(self._y_for_price(low_price, low, high, plot)))
            y_open = self._y_for_price(open_price, low, high, plot)
            y_close = self._y_for_price(close_price, low, high, plot)
            top = min(y_open, y_close)
            height = max(1.0, abs(y_open - y_close))
            painter.fillRect(QRectF(x - body_width / 2, top, body_width, height), color)

    def _find_signal_index(self) -> int | None:
        """在蜡烛序列中定位信号 K 线(按 signalKline.openTime 匹配)。"""
        signal_kline = self.signal.get("signalKline") if isinstance(self.signal, dict) else None
        if not isinstance(signal_kline, dict):
            return None
        open_time = signal_kline.get("openTime")
        if not isinstance(open_time, int):
            return None
        for index, candle in enumerate(self.candles):
            if candle[0] == open_time:
                return index
        return None

    def _draw_signal(self, painter: QPainter, plot: QRectF) -> None:
        """信号池:高亮信号 K 线 + 方向箭头 + 左上角策略信息卡。"""
        start, end = self._visible_range()
        low, high = self._price_range(start, end)
        index = self._find_signal_index()
        direction = str(self.signal.get("direction", ""))
        if index is not None and start <= index < end:
            _, open_price, high_price, low_price, close_price = self.candles[index]
            x = self._x_for_index(index, start, end, plot)
            color = UP_COLOR if close_price >= open_price else DOWN_COLOR
            y_high = self._y_for_price(high_price, low, high, plot)
            y_low = self._y_for_price(low_price, low, high, plot)
            # 信号蜡烛高亮边框
            painter.setPen(QPen(color, 1.8))
            painter.drawRect(QRectF(x - 5, y_high, 10, max(2.0, y_low - y_high)))
            # 方向箭头:多单红▲(蜡烛上方),空单绿▼(蜡烛下方)
            if direction == "多":
                arrow_y = y_high - 18
                if arrow_y >= plot.top():
                    painter.setPen(QPen(UP_COLOR, 2.2))
                    painter.drawText(QRectF(x - 30, arrow_y, 60, 14), Qt.AlignmentFlag.AlignCenter, "▲")
            elif direction == "空":
                arrow_y = y_low + 4
                if arrow_y + 14 <= plot.bottom():
                    painter.setPen(QPen(DOWN_COLOR, 2.2))
                    painter.drawText(QRectF(x - 30, arrow_y, 60, 14), Qt.AlignmentFlag.AlignCenter, "▼")
        self._draw_signal_card(painter, plot)

    def _signal_card_lines(self) -> list[str]:
        """将信号池运行数据翻译为人易读的策略信息行。"""
        signal_type = str(self.signal.get("signalType", ""))
        direction = str(self.signal.get("direction", ""))
        explanation = SIGNAL_EXPLANATIONS.get(signal_type, "")
        signal_kline = self.signal.get("signalKline") if isinstance(self.signal.get("signalKline"), dict) else {}
        metrics = self.signal.get("executionMetrics") if isinstance(self.signal.get("executionMetrics"), dict) else {}
        box = self.signal.get("box") if isinstance(self.signal.get("box"), dict) else {}
        lines = [f"信号: {signal_type} · {explanation or direction}"]
        open_time = signal_kline.get("openTime")
        if isinstance(open_time, int):
            lines.append(f"时间: {format_kline_time(open_time)}")
        close = signal_kline.get("close")
        if isinstance(close, (int, float)):
            lines.append(f"信号收盘: {close:g}")
        ratio = metrics.get("volumeRatio")
        if isinstance(ratio, (int, float)):
            lines.append(f"成交量: {ratio:.2f}× 中位数")
        oi = metrics.get("oiChangePct")
        if isinstance(oi, (int, float)):
            lines.append(f"持仓量变化: {oi * 100:+.2f}%")
        buffer_value = metrics.get("breakoutBuffer")
        if isinstance(buffer_value, (int, float)):
            lines.append(f"突破缓冲: {buffer_value:g}")
        atr = metrics.get("atr14")
        if isinstance(atr, (int, float)):
            lines.append(f"止损缓冲 ATR: {atr:g}")
        window = box.get("window")
        if window is not None:
            lines.append(f"箱体窗口: {window}")
        return lines

    def _draw_signal_card(self, painter: QPainter, plot: QRectF) -> None:
        """左上角绘制半透明策略信息卡。"""
        lines = self._signal_card_lines()
        if not lines:
            return
        card_width = 380.0
        line_height = 20.0
        margin = 8.0
        card_height = len(lines) * line_height + 2 * margin
        rect = QRectF(plot.left() + 6, plot.top() + 6, card_width, card_height)
        painter.fillRect(rect, CARD_BACKGROUND)
        painter.setPen(QPen(CARD_BORDER, 1))
        painter.drawRect(rect)
        painter.setPen(TEXT_COLOR)
        font = painter.font()
        font.setPointSize(8)
        painter.setFont(font)
        for index, line in enumerate(lines):
            painter.drawText(
                QRectF(rect.left() + margin, rect.top() + margin + index * line_height,
                       card_width - 2 * margin, line_height),
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                line,
            )
        font.setPointSize(9)
        painter.setFont(font)

    # ---- 交互 ----

    def wheelEvent(self, event: Any) -> None:
        """滚轮缩放:以鼠标位置为锚点。"""
        if not self.candles:
            return
        delta = event.angleDelta().y()
        if delta == 0:
            return
        old_count = self.visible_count
        new_count = int(old_count * 0.8) if delta > 0 else int(old_count * 1.25)
        new_count = max(MIN_VISIBLE_COUNT, min(len(self.candles), new_count))
        if new_count == old_count:
            return
        ratio = (event.position().x() - self._plot_rect.left()) / max(1.0, self._plot_rect.width())
        ratio = max(0.0, min(1.0, ratio))
        start, end = self._visible_range()
        anchor_index = start + ratio * (end - start)
        self.visible_count = new_count
        self.end_index = int(anchor_index + (1 - ratio) * new_count)
        self.end_index = max(new_count, min(len(self.candles), self.end_index))
        self.update()

    def mousePressEvent(self, event: Any) -> None:
        if event.button() == Qt.MouseButton.LeftButton and self.candles:
            self._drag_start_x = int(event.position().x())
            self._drag_start_end = self.end_index

    def mouseMoveEvent(self, event: Any) -> None:
        if self._drag_start_x is None:
            return
        delta_x = int(event.position().x()) - self._drag_start_x
        if abs(delta_x) < 2:
            return
        step = max(1, self.visible_count // 40)
        shift = delta_x // max(1, int(self._plot_rect.width() / max(1, self.visible_count)))
        if shift == 0:
            shift = 1 if delta_x > 0 else -1
        self.end_index = max(
            self.visible_count,
            min(len(self.candles), self._drag_start_end - shift * step),
        )
        self.update()

    def mouseReleaseEvent(self, event: Any) -> None:
        self._drag_start_x = None

    def mouseDoubleClickEvent(self, event: Any) -> None:
        """双击复位到默认视野。"""
        self.end_index = len(self.candles)
        self.visible_count = min(80, max(MIN_VISIBLE_COUNT, len(self.candles)))
        self.update()


def format_kline_time(open_time: int) -> str:
    """将 K 线起点毫秒时间戳格式化为北京时间文本(GUI 展示层)。"""
    return datetime.fromtimestamp(open_time / 1000, CHINA_TIMEZONE).strftime("%m-%d %H:%M")
