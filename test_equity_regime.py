from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from bot.equity_regime import (
    EquityRegimeController,
    HistoricalSynchronizer,
    LadderOrder,
    RegimeConfig,
    ShadowExecutor,
    StrategyDecision,
    normalize_fill,
    normalize_settlement,
    reconstruct_realized_pnl,
)


class FakeHistoryAPI:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        request = dict(params or {})
        self.calls.append((path, request))
        cursor = request.get("cursor")
        if path == "/historical/cutoff":
            return {"cutoff_ts": "2026-07-25T00:00:00Z"}
        if path == "/historical/fills":
            if cursor is None:
                return {"fills": [{"fill_id": "historical-1", "order_id": "owned", "ticker": "KXBTC15M-A", "side": "yes", "action": "buy", "price": "0.40", "count_fp": "1", "fee_cost": "0.01"}], "cursor": "page-2"}
            return {"fills": [{"fill_id": "historical-2", "order_id": "owned", "ticker": "KXBTC15M-B", "side": "no", "action": "buy", "price": "0.30", "count_fp": "1", "fee_cost": "0.01"}]}
        if path == "/portfolio/fills":
            return {"fills": [{"fill_id": "historical-2", "order_id": "owned", "ticker": "KXBTC15M-B", "side": "no", "action": "buy", "price": "0.30", "count_fp": "1"}]}
        if path == "/portfolio/settlements":
            return {"settlements": [{"ticker": "KXBTC15M-A", "market_result": "yes", "settlement_time": "2026-07-25T00:01:00Z"}]}
        if path == "/portfolio/balance":
            return {"balance_dollars": "123.45"}
        raise AssertionError(path)


class FakeForecaster:
    def __init__(self, p10: str, p90: str) -> None:
        self.p10 = Decimal(p10)
        self.p90 = Decimal(p90)
        self.fit_number = 0

    def forecast(self, observations: list[Mapping[str, Any]], target: datetime) -> dict[str, Decimal]:
        self.fit_number += 1
        p25 = self.p10 + Decimal("1")
        p50 = (self.p10 + self.p90) / Decimal("2")
        p75 = self.p90 - Decimal("1")
        return {
            "p01": self.p10 - Decimal("5"), "p10": self.p10, "p25": p25, "p50": p50,
            "p75": p75, "p90": self.p90, "p99": self.p90 + Decimal("5"),
        }


def decision(ticker: str, generated: datetime) -> StrategyDecision:
    return StrategyDecision(
        target_ticker=ticker,
        source_ticker="KXBTC15M-SOURCE",
        selected_side="yes",
        eligible=True,
        skip_reason=None,
        ladder_orders=(LadderOrder(Decimal("0.40"), Decimal("1"), "initial"),),
        stop_price=Decimal("0.05"),
        trailing_activation_gain=Decimal("0.60"),
        trailing_retracement=Decimal("0.10"),
        generated_at=generated,
        target_close_time=generated + timedelta(minutes=15),
    )


class EquityRegimeTests(unittest.TestCase):
    def test_history_merge_paginates_routes_and_deduplicates_fill_ids(self) -> None:
        config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, history_max_markets=200)
        state = {"markets": {"KXBTC15M-A": {"orders": {"x": {"order_id": "owned"}}}}}
        api = FakeHistoryAPI()
        result = asyncio.run(HistoricalSynchronizer(config, state).sync(api))
        self.assertEqual([fill.fill_id for fill in result.fills], ["historical-1", "historical-2"])
        self.assertEqual(result.duplicate_fills_removed, 1)
        self.assertEqual(len(result.settlements), 1)
        self.assertIn("/historical/fills", [path for path, _ in api.calls])
        self.assertIn("/portfolio/fills", [path for path, _ in api.calls])
        historical_calls = [params for path, params in api.calls if path == "/historical/fills"]
        self.assertEqual(historical_calls[1]["cursor"], "page-2")

    def test_decimal_accounting_handles_partial_exit_and_settlement_without_double_counting(self) -> None:
        fills = [
            normalize_fill({"fill_id": "b1", "ticker": "KXBTC15M-X", "side": "yes", "action": "buy", "price": "0.40", "count_fp": "2", "fee_cost": "0.02"}, "portfolio"),
            normalize_fill({"fill_id": "b2", "ticker": "KXBTC15M-X", "side": "yes", "action": "buy", "price": "0.30", "count_fp": "1", "fee_cost": "0.01"}, "portfolio"),
            normalize_fill({"fill_id": "s1", "ticker": "KXBTC15M-X", "side": "yes", "action": "sell", "price": "0.50", "count_fp": "1", "fee_cost": "0.01"}, "portfolio"),
        ]
        settlements = [normalize_settlement({"ticker": "KXBTC15M-X", "market_result": "yes", "settlement_time": "2026-07-25T00:00:00Z"}, "portfolio")]
        result = reconstruct_realized_pnl(fills, settlements)[0]
        self.assertEqual(result.entry_cost, Decimal("1.10"))
        self.assertEqual(result.exit_proceeds, Decimal("0.50"))
        self.assertEqual(result.settlement_payout, Decimal("2"))
        self.assertEqual(result.fees, Decimal("0.04"))
        self.assertEqual(result.realized_pnl, Decimal("1.36"))
        self.assertEqual(result.contracts_bought, Decimal("3"))
        self.assertEqual(result.contracts_sold, Decimal("1"))

    def test_conservative_shadow_never_invents_a_trade_through_fill(self) -> None:
        config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, shadow_fill_model="conservative_trade_through")
        executor = ShadowExecutor(config)
        trade = executor.start(decision("KXBTC15M-SHADOW", datetime.now(tz=UTC)), True)
        changed = executor.observe_touch_quote(trade, lambda *_: ({"economic_price": "0.01", "displayed_depth": "100"}, "quote"))
        self.assertFalse(changed)
        executor.finalize(trade, "yes")
        self.assertEqual(Decimal(trade["shadow_realized_pnl"]), Decimal("0"))
        self.assertEqual(trade["shadow_simulation_quality"], "unavailable")

    def test_conservative_shadow_uses_only_post_order_public_trade_volume(self) -> None:
        config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, shadow_fill_model="conservative_trade_through")
        executor = ShadowExecutor(config)
        created = datetime.now(tz=UTC) - timedelta(seconds=2)
        trade = executor.start(decision("KXBTC15M-TRADE", created), True)
        events = [
            # A pre-order event is deliberately ignored even though its price
            # crosses the 40c rung.
            {"trade_id": "before", "count": "1", "yes_price": "0.20", "source_server_timestamp": (created - timedelta(seconds=1)).isoformat()},
            {"trade_id": "after", "count": "0.50", "yes_price": "0.39", "source_server_timestamp": (created + timedelta(seconds=1)).isoformat()},
        ]
        changed = executor.observe_trade_through(trade, lambda _ticker, cutoff: [item for item in events if datetime.fromisoformat(item["source_server_timestamp"]) > cutoff])
        self.assertTrue(changed)
        self.assertEqual(Decimal(trade["orders"][0]["filled"]), Decimal("0.50"))
        self.assertEqual(Decimal(trade["entry_cost"]), Decimal("0.20"))
        self.assertEqual(trade["shadow_simulation_quality"], "conservative_approximation")
        executor.finalize(trade, "yes")
        self.assertEqual(Decimal(trade["shadow_realized_pnl"]), Decimal("0.30"))

    def test_touch_shadow_is_explicit_approximation_and_uses_post_decision_quote(self) -> None:
        config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, shadow_fill_model="touch")
        executor = ShadowExecutor(config)
        trade = executor.start(decision("KXBTC15M-TOUCH", datetime.now(tz=UTC) - timedelta(seconds=1)), True)
        changed = executor.observe_touch_quote(trade, lambda *_: ({"economic_price": "0.39", "displayed_depth": "1"}, "quote"))
        self.assertTrue(changed)
        executor.finalize(trade, "yes")
        self.assertEqual(Decimal(trade["shadow_realized_pnl"]), Decimal("0.60"))
        self.assertEqual(trade["shadow_simulation_quality"], "conservative_approximation")

    def test_p90_and_p10_apply_only_to_next_market_and_restore_from_disk(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, prophet_training_window=None)
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(hours=2)).isoformat(), "completed_at": (now - timedelta(hours=2)).isoformat(), "shadow_equity": "100"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(hours=1)).isoformat(), "completed_at": (now - timedelta(hours=1)).isoformat(), "shadow_equity": "100"},
            ]
            controller.forecaster = FakeForecaster("95", "100.50")
            first = decision("KXBTC15M-T1", now)
            controller.start_market(first)
            controller.state["shadow_open"][first.target_ticker].update({"status": "finalized", "shadow_realized_pnl": "1", "contracts": "0"})
            controller.close_market(ticker=first.target_ticker, outcome="yes", market_close_time=first.target_close_time, actual_realized_pnl=Decimal("0"))
            # T1 was processed under the on state; P90 becomes off only for T2.
            self.assertEqual(controller.state["live_vs_shadow"][-1]["live_execution_enabled"], True)
            self.assertFalse(controller.execution_enabled_for_market())
            controller.forecaster = FakeForecaster("100", "110")
            second = decision("KXBTC15M-T2", now + timedelta(minutes=16))
            controller.start_market(second)
            self.assertEqual(controller.state["shadow_open"][second.target_ticker]["live_execution_enabled"], False)
            controller.state["shadow_open"][second.target_ticker].update({
                "status": "finalized", "shadow_realized_pnl": "-2", "contracts": "0",
                "shadow_simulation_quality": "conservative_approximation",
            })
            controller.close_market(ticker=second.target_ticker, outcome="no", market_close_time=second.target_close_time, actual_realized_pnl=Decimal("0"))
            # T2 was skipped live in the simulated regime; P10 restarts T3.
            self.assertEqual(controller.state["live_vs_shadow"][-1]["live_execution_enabled"], False)
            self.assertEqual(Decimal(controller.state["actual_equity"]), Decimal("100"))
            self.assertEqual(Decimal(controller.state["shadow_equity"]), Decimal("99"))
            self.assertTrue(controller.execution_enabled_for_market())
            self.assertFalse(controller.should_suppress_new_live_orders())  # dry-run is never an execution switch
            restored = EquityRegimeController(config, root / "state.json", root / "outputs")
            self.assertTrue(restored.execution_enabled_for_market())
            rows_before = len(restored.state["shadow_history"])
            restored.close_market(ticker=second.target_ticker, outcome="no", market_close_time=second.target_close_time, actual_realized_pnl=Decimal("0"))
            self.assertEqual(len(restored.state["shadow_history"]), rows_before)
            for forecast in restored.state["forecasts"]:
                self.assertLess(forecast["training_end"], forecast["forecast_target_time"])

    def test_live_market_is_exact_shadow_even_with_conservative_off_state_model(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, shadow_fill_model="conservative_trade_through")
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            now = datetime.now(tz=UTC)
            target = decision("KXBTC15M-LIVE", now)
            controller.start_market(target)
            controller.close_market(
                ticker=target.target_ticker,
                outcome="yes",
                market_close_time=target.target_close_time,
                actual_realized_pnl=Decimal("0.60"),
                actual_metadata={"contracts_bought": "1", "entry_cost": "0.40", "settlement_payout": "1", "fees": "0"},
                actual_was_live=True,
            )
            shadow = controller.state["shadow_history"][-1]
            self.assertEqual(Decimal(shadow["shadow_realized_pnl"]), Decimal("0.60"))
            self.assertEqual(shadow["shadow_simulation_quality"], "exact_replay")


if __name__ == "__main__":
    unittest.main()
