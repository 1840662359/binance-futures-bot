"""按 UTC K 线边界调度交易池、箱体池和信号池刷新。"""

from __future__ import annotations

import argparse
import functools
import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from detect_box_consolidation import build_output as build_box_output
from detect_box_consolidation import detect_boxes_concurrently
from filter_symbols_by_24h_quote_volume import (
    build_filtered_exchange_info as build_volume_filtered_exchange_info,
)
from filter_symbols_by_24h_quote_volume import build_ticker_index, fetch_24h_tickers, get_ticker_url
from filter_symbols_by_coingecko_market_cap import (
    build_coin_id_index,
    build_filtered_exchange_info as build_market_cap_filtered_exchange_info,
)
from filter_symbols_by_coingecko_market_cap import (
    fetch_binance_futures_tickers,
    fetch_market_data,
    find_binance_futures_exchange_id,
    get_symbol_coin_id,
)
from fetch_klines_for_symbols import SUPPORTED_INTERVALS as KLINE_INTERVALS
from fetch_klines_for_symbols import build_output as build_kline_output
from fetch_klines_for_symbols import fetch_klines_with_failures
from app_paths import CONFIG_PATH, ensure_app_config, runtime_directory
from fetch_usdt_coin_perpetual_exchange_info import (
    build_filtered_exchange_info as build_contract_exchange_info,
)
from fetch_usdt_coin_perpetual_exchange_info import fetch_exchange_info, get_api_url
from generate_box_execution_signals import generate_signal_for_box, signal_type_allowed
from logging_utils import configure_logging, get_logger
from network_utils import configure_network_proxy
from secret_utils import get_secret


COINGECKO_API_KEY_ENVIRONMENT_VARIABLE = "CG_DEMO_API_KEY"
COINGECKO_DOCUMENTATION_URL = "https://docs.coingecko.com/reference/derivatives-exchanges-id"
BINANCE_DOCUMENTATION_URL = "https://developers.binance.com/en/docs/llms.txt"

# 当前正在执行的调度活动(GUI 卡片「当前正在运行」展示,字符串赋值原子,无需锁)
_current_activity: str | None = None


def get_current_activity() -> str | None:
    """返回调度器当前正在执行的刷新活动名称,无则返回 None。"""
    return _current_activity


def _set_current_activity(activity: str | None) -> None:
    global _current_activity
    _current_activity = activity


def _with_activity(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """装饰器:方法运行期间标记当前调度活动,结束(含异常)后清除。"""

    def decorator(method: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            _set_current_activity(name)
            try:
                return method(self, *args, **kwargs)
            finally:
                _set_current_activity(None)
        return wrapper

    return decorator


class DataNotReadyError(RuntimeError):
    """本轮关键公共数据尚未就绪，保留上一份完整产物。"""


@dataclass(frozen=True)
class SchedulerConfig:
    """调度器运行参数。"""

    environment: str
    structure_interval: str
    structure_kline_limit: int
    execution_interval: str
    execution_kline_limit: int
    execution_oi_limit: int
    market_cap_threshold_usd: int
    minimum_quote_volume_usdt: int
    http_timeout_seconds: float
    data_settle_seconds: float
    poll_seconds: float
    proxy_enabled: bool
    proxy_url: str
    # 交易执行参数(带默认值,兼容旧配置文件缺键)
    trading_enabled: bool = False
    leverage: int = 3
    risk_per_trade_pct: float = 0.01
    max_positions: int = 30
    # 方向模式:long=只做多,short=只做空,both=多空都做;决定执行器是否按信号方向开仓
    direction_mode: str = "both"
    # 范围过滤:市值下限与成交量上限(0 = 不限,默认与旧行为一致)
    market_cap_floor_usd: int = 0
    maximum_quote_volume_usdt: int = 0
    # 信号策略模式:决定信号池产出哪些信号类型(信号生成层面过滤)
    # breakout_mode:none=都不做,up=只追向上突破,down=只追向下突破,both=都做
    # range_mode:none=都不做,buy=只低吸,sell=只高抛,both=都做
    breakout_mode: str = "both"
    range_mode: str = "both"
    # 盈亏自动全平:账户未实现盈亏(以钱包余额为基准)达到阈值时全平全部仓位
    auto_close_enabled: bool = False
    auto_close_profit_pct: float = 10.0  # 盈利阈值(%)
    auto_close_loss_pct: float = 5.0     # 亏损阈值(%)
    # 界面涨跌配色:cn=红涨绿跌(中国习惯),intl=绿涨红跌(国际习惯)
    color_style: str = "cn"
    # 历史数据:归档保留时长(天)与保存位置(空 = 默认 runtime/{环境}/history)
    history_retention_days: int = 7
    history_directory: str = ""

    @property
    def use_testnet(self) -> bool:
        return self.environment == "testnet"

    @property
    def runtime_directory(self) -> Path:
        return runtime_directory(self.environment)

    @property
    def history_dir(self) -> Path:
        """历史数据保存位置:自定义路径(相对路径按项目根解析)或默认 runtime/{环境}/history。"""
        if self.history_directory:
            return Path(self.history_directory)
        return self.runtime_directory / "history"


def load_config(config_path: Path) -> SchedulerConfig:
    """加载并校验调度配置(首次运行自动从项目模板初始化用户数据目录)。"""
    ensure_app_config(config_path)
    data = json.loads(config_path.read_text(encoding="utf-8"))
    config = SchedulerConfig(**data)
    if config.environment not in {"production", "testnet"}:
        raise ValueError("environment 必须是 production 或 testnet。")
    if config.http_timeout_seconds <= 0 or config.poll_seconds <= 0 or config.data_settle_seconds < 0:
        raise ValueError("HTTP 超时、轮询间隔必须大于 0，数据落库等待时间不得小于 0。")
    if config.structure_interval not in KLINE_INTERVALS:
        raise ValueError("structure_interval 必须是 Binance 支持的 K 线周期。")
    if config.execution_interval not in KLINE_INTERVALS:
        raise ValueError("execution_interval 必须是 Binance 支持的 K 线周期。")
    if config.minimum_quote_volume_usdt <= 0:
        raise ValueError("成交量下限必须大于 0。")
    if config.market_cap_threshold_usd < 0 or config.market_cap_floor_usd < 0 or config.maximum_quote_volume_usdt < 0:
        raise ValueError("市值下限/上限与成交量上限不得小于 0。")
    if config.market_cap_threshold_usd > 0 and config.market_cap_floor_usd > config.market_cap_threshold_usd:
        raise ValueError("市值下限不得高于上限(上限为 0 时不设上限)。")
    if config.maximum_quote_volume_usdt > 0 and config.maximum_quote_volume_usdt < config.minimum_quote_volume_usdt:
        raise ValueError("成交量上限(>0 时)不得低于下限。")
    if not 1 <= config.leverage <= 125:
        raise ValueError("杠杆必须位于 1 到 125 之间。")
    if not 0 < config.risk_per_trade_pct <= 0.05:
        raise ValueError("单笔风险比例必须位于 (0, 0.05] 区间。")
    if not 1 <= config.max_positions <= 200:
        raise ValueError("账户最大仓位数必须位于 1 到 200 之间。")
    if config.direction_mode not in {"long", "short", "both"}:
        raise ValueError("direction_mode 必须是 long、short 或 both。")
    if config.breakout_mode not in {"none", "up", "down", "both"}:
        raise ValueError("breakout_mode 必须是 none、up、down 或 both。")
    if config.range_mode not in {"none", "buy", "sell", "both"}:
        raise ValueError("range_mode 必须是 none、buy、sell 或 both。")
    if config.auto_close_profit_pct <= 0 or config.auto_close_loss_pct <= 0:
        raise ValueError("盈亏自动全平阈值必须大于 0。")
    if config.color_style not in {"cn", "intl"}:
        raise ValueError("color_style 必须是 cn 或 intl。")
    if not 1 <= config.history_retention_days <= 365:
        raise ValueError("历史数据保留时长必须位于 1 到 365 天之间。")
    return config


def write_json_atomically(payload: dict[str, Any], output_path: Path) -> None:
    """通过临时文件原子替换写入运行产物。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temporary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary_path, output_path)


def read_json(path: Path) -> dict[str, Any]:
    """读取调度器生成的数据池。"""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"数据池文件格式无效：{path}")
    return data


def get_symbols(pool: dict[str, Any], key: str) -> list[str]:
    """从 symbols 或 boxes 列表中提取交易对名称。"""
    items = pool.get(key)
    if not isinstance(items, list):
        raise RuntimeError(f"数据池缺少 {key} 列表。")
    return [item["symbol"] for item in items if isinstance(item, dict) and isinstance(item.get("symbol"), str)]


def has_kline_at_open_time(raw_klines: list[list[Any]], open_time: int) -> bool:
    """确认序列含有指定起点的完整目标 K 线。"""
    return any(isinstance(kline, list) and len(kline) >= 7 and kline[0] == open_time for kline in raw_klines)


def utc_milliseconds(value: datetime) -> int:
    """将 UTC 时间转换为 Unix 毫秒时间戳。"""
    return int(value.timestamp() * 1000)


def _interruptible_sleep(
    seconds: float, should_stop: Callable[[], bool] | None, chunk_seconds: float = 0.2
) -> None:
    """分段休眠并响应停止请求:长休眠期间停止请求可随时中断,避免退出延迟。

    调度循环的轮询/数据落库等待若整段休眠,停止请求最坏要等满整个休眠期
    (轮询 2s、落库 5s)才能被响应;按小块休眠逐段检查 should_stop。
    """
    deadline = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < deadline:
        if should_stop is not None and should_stop():
            return
        time.sleep(min(chunk_seconds, max(0.0, deadline - time.monotonic())))


def _history_timestamp(file_stem: str) -> datetime | None:
    """从历史文件名解析时间戳(格式:{类型}_{YYYYMMDDTHHMMSS})。"""
    parts = file_stem.rsplit("_", 1)
    if len(parts) != 2:
        return None
    try:
        return datetime.strptime(parts[1], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class PoolScheduler:
    """封装各数据池刷新操作及 UTC 周期调度。"""

    def __init__(self, config: SchedulerConfig) -> None:
        self.config = config
        self.logger = configure_logging(config.environment)
        configure_network_proxy(config.proxy_enabled, config.proxy_url)
        self.logger.info(
            "网络代理已配置 enabled=%s url=%s",
            config.proxy_enabled, config.proxy_url if config.proxy_enabled else "-",
        )
        directory = config.runtime_directory
        self.contract_pool_path = directory / "contract_pool.json"
        self.market_cap_pool_path = directory / "market_cap_pool.json"
        self.trading_pool_path = directory / "trading_pool.json"
        self.structure_kline_path = directory / f"structure_klines_{config.structure_interval}_{config.structure_kline_limit}.json"
        self.box_pool_path = directory / f"box_pool_{config.structure_interval}.json"
        self.execution_kline_path = directory / f"execution_klines_{config.execution_interval}_{config.execution_kline_limit}.json"
        self.signal_pool_path = directory / f"signal_pool_{config.execution_interval}.json"

    def add_snapshot_metadata(self, payload: dict[str, Any], run_id: str, data_as_of: datetime) -> dict[str, Any]:
        """为运行产物标记轮次与已验证的最新数据时点。"""
        payload["snapshot"] = {
            "runId": run_id,
            "dataAsOf": data_as_of.isoformat(),
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }
        return payload

    def publish_pool(self, payload: dict[str, Any], output_path: Path) -> None:
        """发布数据池:新版本覆盖前先归档旧文件到历史目录,并按保留时长清理过期历史。"""
        self.archive_pool_file(output_path)
        write_json_atomically(payload, output_path)

    def archive_pool_file(self, path: Path) -> None:
        """把旧数据池文件归档到历史目录(命名:{类型}_{UTC时间}),并清理超过保留时长的历史。"""
        if not path.is_file():
            return
        history_dir = self.config.history_dir
        history_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        shutil.copy2(path, history_dir / f"{path.stem}_{timestamp}.json")
        self.cleanup_history()

    def cleanup_history(self) -> int:
        """按配置保留时长清理过期历史文件,返回删除数量。"""
        history_dir = self.config.history_dir
        if not history_dir.is_dir():
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.history_retention_days)
        removed = 0
        for file in history_dir.glob("*.json"):
            timestamp = _history_timestamp(file.stem)
            if timestamp is not None and timestamp < cutoff:
                try:
                    file.unlink()
                    removed += 1
                except OSError:
                    self.logger.warning("历史文件清理失败: %s", file)
        if removed:
            self.logger.info("历史数据清理完成 removed=%s 保留天数=%s", removed, self.config.history_retention_days)
        return removed

    @_with_activity("合约池")
    def refresh_contract_pool(self) -> None:
        """下载并筛选 Binance 合约信息。"""
        self.logger.info("开始刷新合约池 environment=%s", self.config.environment)
        api_url = get_api_url(self.config.use_testnet)
        exchange_info = fetch_exchange_info(api_url, self.config.http_timeout_seconds)
        payload = build_contract_exchange_info(exchange_info, api_url, self.config.environment)
        self.publish_pool(payload, self.contract_pool_path)
        self.logger.info("合约池已发布 path=%s symbols=%s", self.contract_pool_path, len(payload["symbols"]))

    @_with_activity("市值池")
    def refresh_market_cap_pool(self) -> None:
        """基于合约池匹配 CoinGecko 市值并筛选低市值交易对。"""
        self.logger.info("开始刷新市值池 input=%s", self.contract_pool_path)
        api_key = get_secret(COINGECKO_API_KEY_ENVIRONMENT_VARIABLE)
        if not api_key:
            raise RuntimeError(f"请设置环境变量 {COINGECKO_API_KEY_ENVIRONMENT_VARIABLE}。")
        contract_pool = read_json(self.contract_pool_path)
        exchange_id = find_binance_futures_exchange_id(api_key, self.config.http_timeout_seconds)
        tickers = fetch_binance_futures_tickers(exchange_id, api_key, self.config.http_timeout_seconds)
        coin_id_index = build_coin_id_index(tickers)
        matched_coin_ids = {
            coin_id
            for symbol in contract_pool["symbols"]
            if isinstance(symbol, dict)
            for coin_id in [get_symbol_coin_id(symbol, coin_id_index)]
            if coin_id is not None
        }
        market_data = fetch_market_data(matched_coin_ids, api_key, self.config.http_timeout_seconds)
        payload = build_market_cap_filtered_exchange_info(
            contract_pool,
            coin_id_index,
            market_data,
            self.config.environment,
            self.config.market_cap_threshold_usd,
            self.config.market_cap_floor_usd,
        )
        payload["marketCapFilter"]["officialDocumentation"] = COINGECKO_DOCUMENTATION_URL
        self.publish_pool(payload, self.market_cap_pool_path)
        self.logger.info("市值池已发布 path=%s symbols=%s", self.market_cap_pool_path, len(payload["symbols"]))

    @_with_activity("交易池")
    def refresh_trading_pool(self) -> None:
        """以最新 24 小时成交额过滤市值池，刷新交易池。"""
        self.logger.info("开始刷新交易池 input=%s", self.market_cap_pool_path)
        market_cap_pool = read_json(self.market_cap_pool_path)
        ticker_url = get_ticker_url(self.config.use_testnet)
        ticker_index = build_ticker_index(fetch_24h_tickers(ticker_url, self.config.http_timeout_seconds))
        payload = build_volume_filtered_exchange_info(
            market_cap_pool,
            ticker_index,
            self.config.environment,
            Decimal(str(self.config.minimum_quote_volume_usdt)),
            Decimal(str(self.config.maximum_quote_volume_usdt)),
        )
        self.publish_pool(payload, self.trading_pool_path)
        self.logger.info("交易池已发布 path=%s symbols=%s", self.trading_pool_path, len(payload["symbols"]))

    def _query_held_symbols(self) -> set[str]:
        """返回当前已有持仓的交易对集合:优先查询交易所(含手动开仓),无密钥时回退本地台账。

        箱体识别与信号生成跳过已有持仓的交易对,避免对持仓中的币重复分析。
        """
        if self.config.use_testnet:
            api_key = get_secret("BINANCE_TESTNET_API_KEY")
            api_secret = get_secret("BINANCE_TESTNET_API_SECRET")
        else:
            api_key = get_secret("BINANCE_API_KEY")
            api_secret = get_secret("BINANCE_API_SECRET")
        if api_key and api_secret:
            try:
                from binance_futures_client import BinanceFuturesClient
                client = BinanceFuturesClient(api_key, api_secret, self.config.use_testnet, self.config.http_timeout_seconds)
                positions = client.get_position_risk()
                return {symbol for symbol, amount in positions.items() if abs(amount) > 0}
            except Exception as exc:
                self.logger.warning("查询交易所持仓失败,回退本地台账: %s", exc)
        # 本地台账仅含机器人开仓记录;交易所查询不可用时退而求其次
        from position_monitor import load_positions
        return {str(position.get("symbol")) for position in load_positions(self.config.environment)}

    @_with_activity("箱体池")
    def refresh_box_pool(self, scheduled_at: datetime | None = None, run_id: str | None = None) -> None:
        """下载结构周期 K 线并从交易池刷新箱体池(跳过已有持仓的交易对)。"""
        scheduled_at = scheduled_at or datetime.now(timezone.utc)
        run_id = run_id or str(uuid.uuid4())
        self.logger.info("开始刷新箱体池 run_id=%s scheduled_at=%s", run_id, scheduled_at.isoformat())
        structure_bucket = bucket_start(self.config.structure_interval, scheduled_at)
        required_open_time = utc_milliseconds(previous_bucket_start(self.config.structure_interval, structure_bucket))
        trading_pool = read_json(self.trading_pool_path)
        symbols = get_symbols(trading_pool, "symbols")
        held_symbols = self._query_held_symbols()
        excluded_held = {
            symbol: "已有持仓,跳过箱体识别"
            for symbol in symbols
            if symbol in held_symbols
        }
        symbols = [symbol for symbol in symbols if symbol not in held_symbols]
        klines, failures = fetch_klines_with_failures(
            symbols,
            self.config.structure_interval,
            self.config.structure_kline_limit,
            self.config.use_testnet,
            self.config.http_timeout_seconds,
        )
        # 箱体只允许使用在本结构周期开始前已经收盘的 K 线，排除正在形成的当前 K 线。
        structure_boundary_time = utc_milliseconds(structure_bucket)
        klines = {
            symbol: [
                kline
                for kline in raw_klines
                if isinstance(kline, list) and len(kline) >= 7 and int(kline[6]) < structure_boundary_time
            ]
            for symbol, raw_klines in klines.items()
        }
        stale_symbols = {
            symbol: "缺少本轮最新已收盘的结构 K 线"
            for symbol, raw_klines in klines.items()
            if not has_kline_at_open_time(raw_klines, required_open_time)
        }
        excluded = {**excluded_held, **failures, **stale_symbols}
        klines = {symbol: raw_klines for symbol, raw_klines in klines.items() if symbol not in excluded}
        if symbols and not klines:
            self.logger.error("箱体池未发布 run_id=%s 原因=所有结构K线均未就绪", run_id)
            raise DataNotReadyError("所有交易对均缺少最新结构 K 线，本轮箱体池不发布。")
        kline_payload = build_kline_output(
            klines,
            self.config.structure_interval,
            self.config.structure_kline_limit,
            self.config.environment,
        )
        self.add_snapshot_metadata(kline_payload, run_id, structure_bucket)
        kline_payload["excludedSymbols"] = [
            {"symbol": symbol, "reason": reason} for symbol, reason in sorted(excluded.items())
        ]
        self.publish_pool(kline_payload, self.structure_kline_path)
        box_payload = build_box_output(kline_payload, detect_boxes_concurrently(klines))
        self.add_snapshot_metadata(box_payload, run_id, structure_bucket)
        box_payload["excludedSymbols"] = kline_payload["excludedSymbols"]
        self.publish_pool(box_payload, self.box_pool_path)
        self.logger.info(
            "箱体池已发布 run_id=%s boxes=%s excluded=%s path=%s",
            run_id, len(box_payload["boxes"]), len(excluded), self.box_pool_path,
        )

    @_with_activity("信号池")
    def scan_signals(self, scheduled_at: datetime | None = None, run_id: str | None = None) -> None:
        """针对当前箱体池拉取执行数据，并完全覆盖信号池。"""
        scheduled_at = scheduled_at or datetime.now(timezone.utc)
        run_id = run_id or str(uuid.uuid4())
        self.logger.info("开始扫描信号 run_id=%s scheduled_at=%s", run_id, scheduled_at.isoformat())
        execution_bucket = bucket_start(self.config.execution_interval, scheduled_at)
        signal_open_time = utc_milliseconds(previous_bucket_start(self.config.execution_interval, execution_bucket))
        box_pool = read_json(self.box_pool_path)
        boxes = box_pool.get("boxes")
        if not isinstance(boxes, list):
            raise RuntimeError("箱体池缺少 boxes 列表。")
        expected_box_end_time = utc_milliseconds(bucket_start(self.config.structure_interval, scheduled_at)) - 1
        valid_boxes = [
            box for box in boxes
            if isinstance(box, dict) and isinstance(box.get("symbol"), str) and box.get("endTime") == expected_box_end_time
        ]
        stale_boxes = {
            box["symbol"]: "箱体未覆盖最新已完成的结构周期"
            for box in boxes
            if isinstance(box, dict) and isinstance(box.get("symbol"), str) and box not in valid_boxes
        }
        held_symbols = self._query_held_symbols()
        symbols = get_symbols({"boxes": valid_boxes}, "boxes")
        excluded_held = {
            symbol: "已有持仓,跳过信号生成"
            for symbol in symbols
            if symbol in held_symbols
        }
        symbols = [symbol for symbol in symbols if symbol not in held_symbols]
        valid_boxes = [box for box in valid_boxes if box["symbol"] not in held_symbols]
        klines, kline_failures = fetch_klines_with_failures(
            symbols,
            self.config.execution_interval,
            self.config.execution_kline_limit,
            self.config.use_testnet,
            self.config.http_timeout_seconds,
        )
        excluded: dict[str, str] = {**excluded_held, **stale_boxes, **kline_failures}
        for symbol in symbols:
            if symbol not in klines or not has_kline_at_open_time(klines[symbol], signal_open_time):
                excluded[symbol] = "缺少本轮最新已收盘的执行 K 线"
        valid_boxes = [box for box in valid_boxes if box["symbol"] not in excluded]
        klines = {symbol: raw_klines for symbol, raw_klines in klines.items() if symbol not in excluded}
        if symbols and not valid_boxes:
            self.logger.error("信号池未覆盖 run_id=%s 原因=所有箱体交易对执行数据未就绪", run_id)
            raise DataNotReadyError("所有箱体交易对均缺少本轮执行数据，信号池不覆盖。")
        kline_payload = build_kline_output(
            klines,
            self.config.execution_interval,
            self.config.execution_kline_limit,
            self.config.environment,
        )
        self.add_snapshot_metadata(kline_payload, run_id, execution_bucket)
        kline_payload["excludedSymbols"] = [
            {"symbol": symbol, "reason": reason} for symbol, reason in sorted(excluded.items())
        ]
        self.publish_pool(kline_payload, self.execution_kline_path)
        signals = [
            signal
            for box in valid_boxes
            if isinstance(box, dict)
            and isinstance(box.get("symbol"), str)
            and isinstance(klines.get(box["symbol"]), list)
            and (signal := generate_signal_for_box(
                box,
                klines[box["symbol"]],
                signal_open_time,
            )) is not None
            # 策略模式过滤:突破/震荡模式决定信号池产出哪些信号类型
            and signal_type_allowed(
                signal["signalType"], self.config.breakout_mode, self.config.range_mode
            )
        ]
        payload = {
            "source": {
                "boxPool": str(self.box_pool_path),
                "structureInterval": self.config.structure_interval,
                "executionInterval": self.config.execution_interval,
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "runId": run_id,
                "dataAsOf": execution_bucket.isoformat(),
                "strategyModes": {
                    "breakout": self.config.breakout_mode,
                    "range": self.config.range_mode,
                },
            },
            "excludedSymbols": kline_payload["excludedSymbols"],
            "signals": sorted(signals, key=lambda signal: signal["symbol"]),
        }
        self.publish_pool(payload, self.signal_pool_path)
        if self.config.trading_enabled:
            try:
                from trading_executor import execute_signal_pool  # 局部导入避免循环依赖

                summary = execute_signal_pool(self.signal_pool_path, self.config)
                self.logger.info(
                    "信号交易执行完成 executed=%s skipped=%s failed=%s",
                    summary.get("executed", 0), summary.get("skipped", 0),
                    summary.get("failed", 0),
                )
            except Exception:
                # 交易执行异常不中断调度主循环,仅记录日志
                self.logger.exception("信号交易执行异常,已记录,调度循环继续")
        self.logger.info(
            "信号池已覆盖 run_id=%s signals=%s excluded=%s path=%s",
            run_id, len(payload["signals"]), len(excluded), self.signal_pool_path,
        )

    @staticmethod
    def _pool_timestamp_value(payload: dict[str, Any]) -> datetime | None:
        """提取数据池元数据时点(ISO 字符串,无时区按 UTC 处理),用于新鲜度判断。"""
        snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        market_filter = payload.get("marketCapFilter") if isinstance(payload.get("marketCapFilter"), dict) else {}
        volume_filter = payload.get("volume24hFilter") if isinstance(payload.get("volume24hFilter"), dict) else {}
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        for value in (
            snapshot.get("dataAsOf"),
            market_filter.get("downloadedAt"),
            volume_filter.get("downloadedAt"),
            source.get("downloadedAt"),
        ):
            if not isinstance(value, str):
                continue
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        return None

    def _pool_is_fresh(self, path: Path, interval: str) -> bool:
        """判断数据池是否已覆盖当前调度周期(新鲜度依据各池的扫描调度规则)。

        合约池/市值池按 UTC 日刷新 → 当天发布即为新鲜;
        交易池/箱体池按结构周期刷新 → 当前周期桶内发布即为新鲜。
        """
        if not path.is_file():
            return False
        try:
            payload = read_json(path)
        except (OSError, RuntimeError, json.JSONDecodeError):
            return False
        downloaded_at = self._pool_timestamp_value(payload)
        if downloaded_at is None:
            return False
        return downloaded_at >= bucket_start(interval, datetime.now(timezone.utc))

    def _pool_matches_config(self, path: Path, expected: dict[str, Any]) -> bool:
        """核对池产物元数据中的生成参数与当前配置是否一致(启动复用前校验)。

        任一关键参数与当前配置不符即视为不一致,启动时强制刷新该池,
        避免旧参数生成的数据被复用(如修改市值/成交量范围后仍复用旧池)。
        文件缺失、损坏或字段缺失均视为不一致。
        """
        if not path.is_file():
            return False
        try:
            payload = read_json(path)
        except (OSError, RuntimeError, json.JSONDecodeError):
            return False
        for dotted_key, expected_value in expected.items():
            actual: Any = payload
            for key in dotted_key.split("."):
                actual = actual.get(key) if isinstance(actual, dict) else None
            if actual is None or str(actual) != str(expected_value):
                return False
        return True

    def startup_refresh(self) -> None:
        """启动时构建数据池，不执行信号扫描。

        各数据池若已覆盖当前调度周期(新鲜度足够)且生成参数与当前配置一致
        则跳过刷新;参数不一致(市值/成交量范围、K 线周期等)时强制刷新,
        避免旧参数产物被复用;上游池被刷新时下游输入已变化,连锁一并重刷。
        周期边界的常规刷新仍会无条件执行。
        """
        self.logger.info("调度器启动，开始初始化刷新")
        if self._pool_is_fresh(self.contract_pool_path, "1d"):
            self.logger.info("合约池已覆盖当前 UTC 日,启动跳过刷新")
        else:
            self.refresh_contract_pool()
        # 市值池:核对市值范围(下限/上限)与当前配置一致
        market_cap_ok = self._pool_is_fresh(self.market_cap_pool_path, "1d") and self._pool_matches_config(
            self.market_cap_pool_path, {
                "marketCapFilter.thresholdUsd": self.config.market_cap_threshold_usd,
                "marketCapFilter.floorUsd": self.config.market_cap_floor_usd,
            }
        )
        if market_cap_ok:
            self.logger.info("市值池已覆盖当前 UTC 日且参数一致,启动跳过刷新")
        else:
            self.logger.info("市值池缺失/过期或参数不一致,启动刷新")
            self.refresh_market_cap_pool()
        # 交易池:核对成交量范围一致,且上游市值池未被重刷(输入变化)
        trading_ok = (
            self._pool_is_fresh(self.trading_pool_path, self.config.structure_interval)
            and self._pool_matches_config(self.trading_pool_path, {
                "volume24hFilter.minimumQuoteVolumeUsdt": str(self.config.minimum_quote_volume_usdt),
                "volume24hFilter.maximumQuoteVolumeUsdt": str(self.config.maximum_quote_volume_usdt),
            })
            and market_cap_ok
        )
        if trading_ok:
            self.logger.info("交易池已覆盖当前结构周期且参数一致,启动跳过刷新")
        else:
            self.logger.info("交易池缺失/过期或参数/上游不一致,启动刷新")
            self.refresh_trading_pool()
        # 箱体池:核对结构周期一致,且上游交易池未被重刷(输入变化)
        box_ok = (
            self._box_pool_covers_current_cycle()
            and self._pool_matches_config(self.box_pool_path, {
                "strategy.klineInterval": self.config.structure_interval,
            })
            and trading_ok
        )
        if box_ok:
            self.logger.info("当前结构周期箱体池已存在且参数一致,启动跳过箱体重扫")
        else:
            self.logger.info("箱体池缺失/过期或参数/上游不一致,启动重扫")
            self.refresh_box_pool()
        self.logger.info("调度器初始化刷新完成")

    def _box_pool_covers_current_cycle(self) -> bool:
        """判断箱体池的 dataAsOf 是否覆盖当前结构周期(与 refresh_box_pool 的周期桶一致)。"""
        if not self.box_pool_path.is_file():
            return False
        try:
            payload = read_json(self.box_pool_path)
        except (OSError, RuntimeError, json.JSONDecodeError):
            return False
        snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        data_as_of = snapshot.get("dataAsOf")
        if not isinstance(data_as_of, str):
            return False
        try:
            covered_at = datetime.fromisoformat(data_as_of)
        except ValueError:
            return False
        current_bucket = bucket_start(self.config.structure_interval, datetime.now(timezone.utc))
        return covered_at == current_bucket

    def daily_refresh(self) -> None:
        """每日先扫描旧箱体池，再完整刷新交易池与箱体池。"""
        self.logger.info("开始每日刷新")
        self.scan_signals()
        self.refresh_contract_pool()
        self.refresh_market_cap_pool()
        self.refresh_trading_pool()
        self.refresh_box_pool()
        self.logger.info("每日刷新完成")

    def structure_refresh(self) -> None:
        """结构周期边界先扫描旧箱体池，再刷新成交量、交易池和箱体池。"""
        self.logger.info("开始结构周期刷新")
        self.scan_signals()
        self.refresh_trading_pool()
        self.refresh_box_pool()
        self.logger.info("结构周期刷新完成")


def bucket_start(interval: str, now: datetime) -> datetime:
    """返回当前 UTC 时刻所属 Binance K 线周期桶的起点。"""
    now = now.astimezone(timezone.utc).replace(microsecond=0)
    if interval.endswith("m"):
        minutes = int(interval[:-1])
        return now.replace(minute=now.minute - now.minute % minutes, second=0)
    if interval.endswith("h"):
        hours = int(interval[:-1])
        return now.replace(hour=now.hour - now.hour % hours, minute=0, second=0)
    if interval.endswith("d"):
        days = int(interval[:-1])
        epoch_day = datetime(1970, 1, 1, tzinfo=timezone.utc)
        elapsed_days = (now.date() - epoch_day.date()).days
        return epoch_day.replace(hour=0) + timedelta(days=elapsed_days - elapsed_days % days)
    if interval == "1w":
        return now.replace(hour=0, minute=0, second=0) - timedelta(days=now.weekday())
    if interval == "1M":
        return now.replace(day=1, hour=0, minute=0, second=0)
    raise ValueError(f"不支持调度周期：{interval}")


def previous_bucket_start(interval: str, current_bucket: datetime) -> datetime:
    """返回给定 UTC 周期桶的前一个起点。"""
    if interval == "1M":
        if current_bucket.month == 1:
            return current_bucket.replace(year=current_bucket.year - 1, month=12)
        return current_bucket.replace(month=current_bucket.month - 1)
    if interval == "1w":
        return current_bucket - timedelta(weeks=1)
    if interval.endswith("m"):
        return current_bucket - timedelta(minutes=int(interval[:-1]))
    if interval.endswith("h"):
        return current_bucket - timedelta(hours=int(interval[:-1]))
    if interval.endswith("d"):
        return current_bucket - timedelta(days=int(interval[:-1]))
    raise ValueError(f"不支持调度周期：{interval}")


# 持仓监控器启动失败后的重试间隔(秒):网络/代理瞬时抖动(如 SSL 断连)恢复后自动补启动
POSITION_MONITOR_RETRY_SECONDS = 60


def _start_position_monitor(scheduler: PoolScheduler, retry: bool = False) -> Any:
    """启动持仓监控器;trading_enabled 且密钥可用时运行,失败仅告警不中断调度。

    retry=True 表示启动失败后的定期重试:重试失败仅记 INFO 不刷屏,
    网络恢复后下一次重试即可成功补启动。
    """
    if not scheduler.config.trading_enabled:
        return None
    try:
        from position_monitor import PositionMonitor

        monitor = PositionMonitor(scheduler.config)
        monitor.start()
        return monitor
    except Exception as exc:
        if retry:
            scheduler.logger.info("持仓监控器重试启动失败,稍后再次尝试: %s", exc)
        else:
            scheduler.logger.warning("持仓监控器启动失败，仅运行调度: %s", exc)
        return None


def _stop_position_monitor(scheduler: PoolScheduler, monitor: Any) -> None:
    """优雅停止持仓监控器。"""
    if monitor is not None:
        try:
            monitor.stop()
        except Exception as exc:
            scheduler.logger.warning("持仓监控器停止异常: %s", exc)


def run_scheduler(
    scheduler: PoolScheduler, once: bool, should_stop: Callable[[], bool] | None = None
) -> None:
    """启动初始化后，根据 UTC 周期边界串行执行调度任务。

    持仓监控器与程序同步启动(点击启动即开始监控账户),
    数据池构建(startup_refresh)与监控并行进行;once 模式不启动监控器。
    """
    monitor = None if once else _start_position_monitor(scheduler)
    # 监控器启动失败(如网络瞬时抖动)后每 POSITION_MONITOR_RETRY_SECONDS 秒重试,
    # 网络恢复后自动补启动;成功启动后该分支不再触发
    monitor_retry_at = time.monotonic() + POSITION_MONITOR_RETRY_SECONDS
    try:
        scheduler.startup_refresh()
        if once:
            return
        now = datetime.now(timezone.utc)
        last_daily = bucket_start("1d", now)
        last_structure = bucket_start(scheduler.config.structure_interval, now)
        last_execution = bucket_start(scheduler.config.execution_interval, now)
        while not (should_stop and should_stop()):
            # 持仓监控器未就绪时定期重试补启动(不影响各池调度)
            if monitor is None and scheduler.config.trading_enabled and time.monotonic() >= monitor_retry_at:
                monitor = _start_position_monitor(scheduler, retry=True)
                monitor_retry_at = time.monotonic() + POSITION_MONITOR_RETRY_SECONDS
            now = datetime.now(timezone.utc)
            daily = bucket_start("1d", now)
            structure = bucket_start(scheduler.config.structure_interval, now)
            execution = bucket_start(scheduler.config.execution_interval, now)
            if daily != last_daily:
                _interruptible_sleep(scheduler.config.data_settle_seconds, should_stop)
                if should_stop is not None and should_stop():
                    break
                try:
                    scheduler.daily_refresh()
                except DataNotReadyError as exc:
                    scheduler.logger.warning("UTC %s 日刷新暂不发布：%s", daily.isoformat(), exc)
                except Exception:
                    # 任何未预期异常只跳过本轮,不允许搞崩整个调度器(2026-08-19 01:30 事故)
                    scheduler.logger.exception("UTC %s 日刷新异常,本轮跳过", daily.isoformat())
                else:
                    last_daily, last_structure, last_execution = daily, structure, execution
            elif structure != last_structure:
                _interruptible_sleep(scheduler.config.data_settle_seconds, should_stop)
                if should_stop is not None and should_stop():
                    break
                try:
                    scheduler.structure_refresh()
                except DataNotReadyError as exc:
                    scheduler.logger.warning("UTC %s 结构刷新暂不发布：%s", structure.isoformat(), exc)
                except Exception:
                    scheduler.logger.exception("UTC %s 结构刷新异常,本轮跳过", structure.isoformat())
                else:
                    last_structure, last_execution = structure, execution
            elif execution != last_execution:
                _interruptible_sleep(scheduler.config.data_settle_seconds, should_stop)
                if should_stop is not None and should_stop():
                    break
                try:
                    scheduler.scan_signals()
                except DataNotReadyError as exc:
                    scheduler.logger.warning("UTC %s 信号扫描暂不发布：%s", execution.isoformat(), exc)
                except Exception:
                    scheduler.logger.exception("UTC %s 信号扫描异常,本轮跳过", execution.isoformat())
                else:
                    last_execution = execution
            _interruptible_sleep(scheduler.config.poll_seconds, should_stop)
    finally:
        _stop_position_monitor(scheduler, monitor)
    scheduler.logger.info("收到停止请求，调度器已安全退出")


def main() -> None:
    """加载配置并启动调度器。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="调度配置 JSON 路径。")
    parser.add_argument("--once", action="store_true", help="仅执行启动初始化流程后退出。")
    arguments = parser.parse_args()
    scheduler = PoolScheduler(load_config(arguments.config))
    try:
        run_scheduler(scheduler, arguments.once)
    except Exception:
        scheduler.logger.exception("调度器异常退出")
        raise


if __name__ == "__main__":
    main()
