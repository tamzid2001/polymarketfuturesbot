from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from kalshi_live_trader import LiveEngine, load_config
from live_state import default_state


ROOT = Path(__file__).resolve().parents[1]


class Orders:
    def __init__(self, orders): self._orders = orders
    async def get_orders(self, **_kwargs): return {"orders": self._orders}


class Portfolio:
    def __init__(self, positions): self._positions = positions
    async def get_positions(self, **_kwargs): return {"market_positions": self._positions}


class Rest:
    def __init__(self, orders=(), positions=()):
        self.orders = Orders(orders)
        self.portfolio = Portfolio(positions)
    async def balance_decimal(self): return 100
    async def get_raw_json(self, _path, _params): return {"fills": []}


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

    def test_non_kx_position_does_not_become_strategy_exposure(self) -> None:
        async def scenario():
            engine = self.engine()
            self.assertTrue(await engine.reconcile_startup(Rest(positions=[{"ticker": "OTHER", "position_fp": "5"}])))
        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
