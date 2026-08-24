from __future__ import annotations

import asyncio
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kalshi_live_trader import LiveEngine, deterministic_client_order_id, load_config
from live_state import default_state


ROOT = Path(__file__).resolve().parents[1]


class Orders:
    def __init__(self, orders): self._orders = orders
    async def get_orders(self, **_kwargs): return {"orders": self._orders}


class Portfolio:
    def __init__(self, positions): self._positions = positions
    async def get_positions(self, **_kwargs): return {"market_positions": self._positions}


class Rest:
    def __init__(self, orders=(), positions=(), fills=()):
        self.orders = Orders(orders)
        self.portfolio = Portfolio(positions)
        self.fills = fills
    async def balance_decimal(self): return 100
    async def get_raw_json(self, _path, _params): return {"fills": self.fills}


class ReconciliationTests(unittest.TestCase):
    def engine(self):
        config = load_config(ROOT / "live_strategy_config.json")
        directory = Path(tempfile.mkdtemp())
        return LiveEngine(config, default_state(config), directory / "state", directory / "ledger", dry_run=True)

    def test_unknown_strategy_prefixed_order_fails_closed(self) -> None:
        async def scenario():
            engine = self.engine()
            rest = Rest(orders=[{"ticker": "KXBTC15M-x", "client_order_id": "kxbtc15m-hybrid-v1-lost"}])
            self.assertFalse(await engine.reconcile_startup(rest))
            self.assertTrue(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())

    def test_authoritative_position_restores_known_stop_management(self) -> None:
        async def scenario():
            engine = self.engine()
            engine.state["markets"]["KXBTC15M-x"] = {"ticker": "KXBTC15M-x", "signal_side": "yes", "status": "ENTRY_PENDING"}
            rest = Rest(positions=[{"ticker": "KXBTC15M-x", "position_fp": "1.25"}])
            self.assertTrue(await engine.reconcile_startup(rest))
            record = engine.state["markets"]["KXBTC15M-x"]
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(record["actual_quantity"], "1.25")
        asyncio.run(scenario())

    def test_error_record_with_exchange_position_restores_stop_and_entry_cost_from_fills(self) -> None:
        async def scenario():
            engine = self.engine()
            ticker = "KXBTC15M-reconciled"
            client_id = deterministic_client_order_id(ticker, "yes", "entry", engine.config)
            engine.state["markets"][ticker] = {
                "ticker": ticker, "signal_side": "yes", "status": "ERROR_RECONCILIATION",
                "entry_orders": [{
                    "client_order_id": client_id, "entry_phase": "maker", "fill_count": "0",
                    "remaining_count": "1.25", "fees_paid": "0",
                }],
            }
            rest = Rest(
                positions=[{"ticker": ticker, "position_fp": "1.25"}],
                fills=[{
                    "fill_id": "late-entry-fill", "ticker": ticker, "order_id": "exchange-entry-1",
                    "client_order_id": client_id, "side": "yes", "action": "buy", "count_fp": "1.25",
                    "yes_price_dollars": "0.52", "fee_cost": "0.03",
                }],
            )
            self.assertTrue(await engine.reconcile_startup(rest))
            record = engine.state["markets"][ticker]
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(record["actual_quantity"], "1.25")
            self.assertTrue(record["entry_accounting_reconciled"])
            self.assertEqual(record["entry_orders"][0]["fill_count"], "1.25")
            self.assertEqual(record["entry_orders"][0]["average_fill_price"], "0.52")
            self.assertEqual(record["entry_orders"][0]["fees_paid"], "0.03")
            self.assertEqual(engine.entry_cost(record), Decimal("0.68"))
        asyncio.run(scenario())

    def test_unknown_response_with_resting_maker_recovers_exchange_order_id_and_blocks_new_entries(self) -> None:
        async def scenario():
            engine = self.engine()
            ticker = "KXBTC15M-resting-uncertain"
            client_id = deterministic_client_order_id(ticker, "yes", "entry", engine.config)
            engine.state["markets"][ticker] = {
                "ticker": ticker, "signal_side": "yes", "status": "RECONCILIATION_PENDING",
                "entry_orders": [{"client_order_id": client_id, "entry_phase": "maker", "fill_count": "0", "remaining_count": "1.00"}],
            }
            rest = Rest(orders=[{
                "ticker": ticker, "order_id": "exchange-resting-maker", "client_order_id": client_id,
                "fill_count_fp": "0", "remaining_count_fp": "1.00", "yes_price_dollars": "0.51", "fee_cost": "0",
            }])
            self.assertTrue(await engine.reconcile_startup(rest))
            record = engine.state["markets"][ticker]
            self.assertEqual(record["status"], "ENTRY_PENDING")
            self.assertEqual(record["entry_orders"][0]["order_id"], "exchange-resting-maker")
            self.assertEqual(Decimal(record["entry_orders"][0]["remaining_count"]), Decimal("1.00"))
            self.assertTrue(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())

    def test_unexplained_authoritative_position_blocks_realized_pnl_updates(self) -> None:
        async def scenario():
            engine = self.engine()
            ticker = "KXBTC15M-unexplained"
            engine.state["markets"][ticker] = {
                "ticker": ticker, "signal_side": "yes", "status": "ERROR_RECONCILIATION", "entry_orders": [],
            }
            self.assertTrue(await engine.reconcile_startup(Rest(positions=[{"ticker": ticker, "position_fp": "1.00"}])))
            record = engine.state["markets"][ticker]
            self.assertFalse(record["entry_accounting_reconciled"])
            engine.record_realized(record, 0, "settlement", f"{ticker}:settlement:yes")
            self.assertEqual(record["status"], "ACCOUNTING_RECONCILIATION_PENDING")
            self.assertFalse(engine.state["processed_settlements"])
            self.assertTrue(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())

    def test_non_kx_position_does_not_become_strategy_exposure(self) -> None:
        async def scenario():
            engine = self.engine()
            self.assertTrue(await engine.reconcile_startup(Rest(positions=[{"ticker": "OTHER", "position_fp": "5"}])))
        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
