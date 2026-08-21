"""程序仓位身份与安全快照的回归测试。"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace


SOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE_DIRECTORY))

from binance_futures_client import BinanceFuturesClient
from pnl_tracker import append_pnl_record, read_pnl_records
from position_lifecycle import add_event, create_lifecycle, read_lifecycle
from position_monitor import PositionMonitor, add_position, load_positions, position_key, remove_position
from trading_executor import TradingExecutor
from websocket_streams import MarkPriceStream, UserDataStream


class PositionIdentityTests(unittest.TestCase):
    def test_hedge_sides_are_independent_ledger_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory)
            with patch("position_monitor.runtime_directory", return_value=runtime):
                add_position("production", {"symbol": "BTCUSDT", "positionSide": "LONG", "quantity": 1})
                add_position("production", {"symbol": "BTCUSDT", "positionSide": "SHORT", "quantity": 2})
                self.assertTrue((runtime / "program_positions.json").is_file())
                positions = load_positions("production")
                self.assertEqual({item["positionKey"] for item in positions}, {
                    position_key("BTCUSDT", "LONG"), position_key("BTCUSDT", "SHORT"),
                })
                remove_position("production", "BTCUSDT", "LONG")
                self.assertEqual(load_positions("production")[0]["positionSide"], "SHORT")

    def test_program_positions_reads_legacy_file_until_first_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory)
            legacy = runtime / "position_state.json"
            legacy.write_text(json.dumps({"positions": [{"symbol": "ETHUSDT"}]}), encoding="utf-8")
            with patch("position_monitor.runtime_directory", return_value=runtime):
                self.assertEqual(load_positions("production"), [{"symbol": "ETHUSDT"}])

    def test_entry_ledger_contains_only_initial_filled_facts(self) -> None:
        entry = TradingExecutor._entry_ledger_record({
            "fingerprint": "BTCUSDT|1", "symbol": "BTCUSDT", "status": "filled",
            "entryPrice": 100.0, "quantity": 1.0, "estimatedEntry": 99.0,
            "order": {"orderId": 7, "positionSide": "LONG", "stopOrderId": 9},
        })
        self.assertEqual(entry["entryPrice"], 100.0)
        self.assertNotIn("estimatedEntry", entry)
        self.assertEqual(entry["entryOrder"]["orderId"], 7)
        self.assertEqual(entry["initialProtection"]["stopOrderId"], 9)

    def test_position_presence_does_not_net_hedge_sides(self) -> None:
        client = object.__new__(BinanceFuturesClient)
        client._signed_request = lambda *_: [
            {"symbol": "BTCUSDT", "positionAmt": "2"},
            {"symbol": "BTCUSDT", "positionAmt": "-2"},
        ]
        self.assertEqual(client.get_position_risk(), {"BTCUSDT": 4.0})

    def test_failed_snapshot_preserves_last_successful_state(self) -> None:
        class FailingClient:
            def get_positions_detail(self):
                raise RuntimeError("network unavailable")

        monitor = object.__new__(PositionMonitor)
        monitor.config = SimpleNamespace(environment="production")
        monitor.logger = SimpleNamespace(warning=lambda *args, **kwargs: None)
        monitor._client = FailingClient()
        monitor._exchange_positions = {position_key("BTCUSDT", "LONG"): 1.0}
        monitor._exchange_position_strs = {}
        monitor._account_positions = {position_key("BTCUSDT", "LONG"): {"symbol": "BTCUSDT"}}
        monitor._account_balances = {"USDT": {"balance": 100.0}}
        monitor._last_successful_snapshot = 123.0
        monitor._snapshot_updated_at = 123.0
        monitor._last_snapshot = 0.0
        with patch("position_monitor.time.sleep"), patch("position_monitor.load_positions", return_value=[]):
            monitor._full_snapshot()
        self.assertEqual(monitor._exchange_positions, {position_key("BTCUSDT", "LONG"): 1.0})
        self.assertEqual(monitor._last_successful_snapshot, 123.0)

    def test_private_and_market_streams_use_private_and_all_market_endpoints(self) -> None:
        user_stream = UserDataStream(False, "listen-key", None, lambda _: None)
        self.assertEqual(user_stream._build_url(), "wss://fstream.binance.com/private/ws/listen-key")
        mark_stream = MarkPriceStream(False, None, lambda _: None)
        mark_stream.set_symbols({"BTCUSDT", "ETHUSDT"})
        self.assertEqual(mark_stream._symbols_snapshot(), ["!markPrice@arr@1s"])
        self.assertIn("!markPrice@arr@1s", mark_stream._build_url())

    def test_authoritative_program_pnl_uses_wallet_balance_and_exact_position_keys(self) -> None:
        monitor = object.__new__(PositionMonitor)
        monitor.config = SimpleNamespace(environment="production")
        monitor._state_lock = threading.RLock()
        monitor._last_successful_snapshot = time.monotonic()
        monitor._account_balances = {"USDT": {"balance": 1000.0}}
        monitor._account_positions = {
            position_key("BTCUSDT", "LONG"): {"unrealizedProfit": -120.0},
            position_key("BTCUSDT", "SHORT"): {"unrealizedProfit": 40.0},
            position_key("ETHUSDT", "BOTH"): {"unrealizedProfit": -900.0},
        }
        with patch("position_monitor.load_positions", return_value=[
            {"symbol": "BTCUSDT", "positionSide": "LONG", "status": "active"},
            {"symbol": "BTCUSDT", "positionSide": "SHORT", "status": "active"},
        ]):
            self.assertEqual(monitor._current_pnl_pct(), -8.0)

    def test_gui_snapshot_is_a_consistent_copy_of_program_positions(self) -> None:
        monitor = object.__new__(PositionMonitor)
        monitor.config = SimpleNamespace(environment="production")
        monitor._state_lock = threading.RLock()
        monitor._account_balances = {"USDT": {"balance": 1000.0}}
        key = position_key("BTCUSDT", "LONG")
        monitor._account_positions = {key: {"symbol": "BTCUSDT", "positionAmt": 1.0}}
        monitor._snapshot_updated_at = 12.0
        with patch("position_monitor.load_positions", return_value=[
            {"symbol": "BTCUSDT", "positionSide": "LONG", "signalType": "上破"},
        ]):
            snapshot = monitor.get_account_snapshot()
        self.assertEqual(set(snapshot["positions"]), {key})
        self.assertEqual(snapshot["positions"][key]["local"]["signalType"], "上破")
        snapshot["positions"][key]["positionAmt"] = 99.0
        self.assertEqual(monitor._account_positions[key]["positionAmt"], 1.0)

    def test_lifecycle_only_records_registered_program_position_and_keeps_strong_reason(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory)
            with patch("position_lifecycle.runtime_directory", return_value=runtime):
                key = position_key("BTCUSDT", "LONG")
                create_lifecycle("production", {"positionKey": key, "symbol": "BTCUSDT"})
                add_event("production", "ETHUSDT|BOTH", {"eventId": "ignored", "closeReason": "外部平仓"})
                add_event("production", key, {"eventId": "stop", "closeReason": "程序止损", "evidence": "algo"})
                add_event("production", key, {"eventId": "external", "closeReason": "外部平仓"})
                lifecycle = read_lifecycle("production", key)
                self.assertEqual(lifecycle["closeReason"], "程序止损")
                self.assertEqual(len(lifecycle["events"]), 2)

    def test_pnl_records_deduplicate_by_program_position_key(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory)
            record = {"positionKey": "BTCUSDT|LONG", "closeTime": 1_700_000_000_000, "netPnlUsdt": 1.5}
            with patch("pnl_tracker.runtime_directory", return_value=runtime):
                append_pnl_record("production", record)
                append_pnl_record("production", record)
                self.assertEqual(len(read_pnl_records("production")), 1)

    def test_lifecycle_pnl_filters_hedge_side_and_includes_funding(self) -> None:
        class Client:
            def get_user_trades(self, *_, **__):
                return [
                    {"positionSide": "LONG", "side": "BUY", "realizedPnl": "0", "commission": "-1", "commissionAsset": "USDT", "orderId": 1},
                    {"positionSide": "LONG", "side": "SELL", "realizedPnl": "10", "commission": "-1", "commissionAsset": "USDT", "orderId": 2},
                    {"positionSide": "SHORT", "side": "BUY", "realizedPnl": "999", "commission": "-9", "commissionAsset": "USDT", "orderId": 3},
                ]

            def get_income(self, *_, **__):
                return [{"income": "0.5"}]

        monitor = object.__new__(PositionMonitor)
        monitor._client = Client()
        realized, commission, funding, assets, exits = monitor._query_lifecycle_pnl(
            "BTCUSDT", 1, 2, "多", "LONG"
        )
        self.assertEqual((realized, commission, funding), (10.0, -2.0, 0.5))
        self.assertEqual(assets, {"USDT": -2.0})
        self.assertEqual(exits, [2])

    def test_positive_exchange_commission_is_normalized_to_negative_expense(self) -> None:
        class Client:
            def get_user_trades(self, *_, **__):
                return [
                    {"positionSide": "LONG", "side": "BUY", "realizedPnl": "0", "commission": "0.01", "commissionAsset": "USDT", "orderId": 1},
                    {"positionSide": "LONG", "side": "SELL", "realizedPnl": "1", "commission": "0.02", "commissionAsset": "USDT", "orderId": 2},
                ]

            def get_income(self, *_, **__):
                return []

        monitor = object.__new__(PositionMonitor)
        monitor._client = Client()
        realized, commission, funding, assets, _ = monitor._query_lifecycle_pnl("BTCUSDT", 1, 2, "多", "LONG")
        self.assertEqual((realized, commission, funding), (1.0, -0.03, 0.0))
        self.assertEqual(assets, {"USDT": -0.03})

    def test_pnl_includes_entry_fee_that_precedes_open_position_timestamp(self) -> None:
        class Client:
            def get_user_trades(self, *_, **__):
                return [
                    {"id": 1, "time": 950, "positionSide": "LONG", "side": "BUY", "realizedPnl": "0", "commission": "0.01", "commissionAsset": "USDT", "orderId": 101},
                    {"id": 2, "time": 960, "positionSide": "LONG", "side": "BUY", "realizedPnl": "0", "commission": "0.9", "commissionAsset": "USDT", "orderId": 999},
                    {"id": 3, "time": 1200, "positionSide": "LONG", "side": "SELL", "realizedPnl": "1", "commission": "0.02", "commissionAsset": "USDT", "orderId": 202},
                ]

            def get_income(self, *_, **__):
                return []

        monitor = object.__new__(PositionMonitor)
        monitor._client = Client()
        realized, commission, _, assets, exits = monitor._query_lifecycle_pnl(
            "BTCUSDT", 1000, 2000, "多", "LONG", entry_order_id=101,
        )
        self.assertEqual((realized, commission), (1.0, -0.03))
        self.assertEqual(assets, {"USDT": -0.03})
        self.assertEqual(exits, [202])

    def test_unmatched_close_event_is_rechecked_against_registered_stop(self) -> None:
        class Client:
            def get_algo_order_status(self, *_):
                return {"algoStatus": "TRIGGERED"}

        monitor = object.__new__(PositionMonitor)
        monitor._client = Client()
        monitor.logger = SimpleNamespace(warning=lambda *args, **kwargs: None)
        reason = monitor._resolve_close_reason(
            {"symbol": "BTCUSDT", "stopAlgoId": 123},
            {"closeReason": "平仓原因待核实"},
            tp_missing=False,
            stop_missing=True,
        )
        self.assertEqual(reason, "程序止损")

    def test_market_order_accepts_program_client_order_id(self) -> None:
        client = object.__new__(BinanceFuturesClient)
        captured = {}
        client._signed_request = lambda _method, _path, params: captured.update(params) or {}
        client.place_market_order("BTCUSDT", "SELL", "1", reduce_only=True, client_order_id="bot-autoclose-1")
        self.assertEqual(captured["newClientOrderId"], "bot-autoclose-1")

    def test_initial_stops_follow_box_boundary_and_range_one_to_one(self) -> None:
        executor = object.__new__(TradingExecutor)
        box = {"upper": 110, "lower": 90}
        breakout_long = executor._stop_for_signal(
            {"signalType": "上破箱体上沿", "direction": "多"}, box, Decimal("2"), Decimal("115")
        )
        breakout_short = executor._stop_for_signal(
            {"signalType": "下破箱体下沿", "direction": "空"}, box, Decimal("2"), Decimal("85")
        )
        range_long = executor._stop_for_signal(
            {"signalType": "箱体内低吸", "direction": "多"}, box, Decimal("0"), Decimal("96"), Decimal("100")
        )
        range_short = executor._stop_for_signal(
            {"signalType": "箱体内高抛", "direction": "空"}, box, Decimal("0"), Decimal("104"), Decimal("100")
        )
        self.assertEqual(breakout_long[0], Decimal("88"))
        self.assertEqual(breakout_short[0], Decimal("112"))
        self.assertEqual(range_long[0], Decimal("92"))
        self.assertEqual(range_short[0], Decimal("108"))

    def test_stop_rounding_keeps_stop_on_protective_side_of_tick(self) -> None:
        executor = object.__new__(TradingExecutor)
        filters = {"PRICE_FILTER": {"tickSize": "0.1", "minPrice": "0.1", "maxPrice": "1000"}}
        self.assertEqual(executor._round_stop_price(Decimal("99.99"), filters, "BUY"), Decimal("99.9"))
        self.assertEqual(executor._round_stop_price(Decimal("100.01"), filters, "SELL"), Decimal("100.1"))

    def test_risk_loss_includes_stop_slippage_and_two_sided_fees(self) -> None:
        # 多仓预估入场 100、止损 90：止损成交按 89.955，手续费按 100+89.955 的双边万五。
        loss = TradingExecutor._estimated_total_loss(Decimal("100"), Decimal("90"), "BUY", Decimal("2"))
        self.assertEqual(loss, Decimal("20.2799550"))

    def test_limit_order_accepts_program_client_order_id(self) -> None:
        client = object.__new__(BinanceFuturesClient)
        captured = {}
        client._signed_request = lambda _method, _path, params: captured.update(params) or {}
        client.place_limit_order("BTCUSDT", "SELL", "100", "1", client_order_id="bot-take-profit-1")
        self.assertEqual(captured["newClientOrderId"], "bot-take-profit-1")


if __name__ == "__main__":
    unittest.main()
