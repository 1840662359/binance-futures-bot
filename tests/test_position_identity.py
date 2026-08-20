"""程序仓位身份与安全快照的回归测试。"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
import json
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace


SOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE_DIRECTORY))

from binance_futures_client import BinanceFuturesClient
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


if __name__ == "__main__":
    unittest.main()
