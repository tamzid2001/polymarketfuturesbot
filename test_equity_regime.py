from __future__ import annotations

import asyncio
import csv
import json
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
    ReconstructedMarket,
    RegimeConfig,
    ShadowExecutor,
    StrategyDecision,
    normalize_fill,
    normalize_settlement,
    ownership_evidence,
    reconstruct_realized_pnl,
)
import kalshi_btc15m_average_down as trader


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
    def test_handoff_restore_preserves_shadow_balance_and_ledger_exactly(self) -> None:
        """A fresh Actions checkout must retain the exact persisted shadow curve."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path = root / "data" / "kalshi_equity_regime_state.json"
            config = RegimeConfig(enabled=True, dry_run=False, prophet_min_history=2)
            controller = EquityRegimeController(config, state_path, root / "outputs")
            controller.state.update({
                "actual_balance": "127.3121",
                "shadow_balance": "156.650",
                "balance_reconciled": False,
                "actual_history": [{
                    "market_ticker": "KXBTC15M-26JUL272045-45",
                    "actual_balance_after": "127.3121",
                }],
                "shadow_history": [
                    {
                        "market_ticker": "KXBTC15M-26JUL272030-30",
                        "market_close_time": "2026-07-28T00:30:00+00:00",
                        "completed_at": "2026-07-28T00:30:00+00:00",
                        "shadow_balance_before": "130.85",
                        "shadow_balance_after": "154.85",
                    },
                    {
                        "market_ticker": "KXBTC15M-26JUL272045-45",
                        "market_close_time": "2026-07-28T00:45:00+00:00",
                        "completed_at": "2026-07-28T00:45:00+00:00",
                        "shadow_balance_before": "154.85",
                        "shadow_balance_after": "156.650",
                    },
                ],
            })
            controller.save()
            ledger = state_path.parent / "kalshi_shadow_equity_history.csv"
            persisted_ledger = ledger.read_bytes()

            # A new controller is the next GitHub Actions handoff: it only
            # receives the committed state and ledger files from main.
            successor = EquityRegimeController(config, state_path, root / "successor-outputs")
            self.assertEqual(successor.state["shadow_balance"], "156.650")
            self.assertEqual(successor.heartbeat()["shadow_balance"], "156.650")
            self.assertEqual(successor.state["shadow_history"], controller.state["shadow_history"])
            self.assertEqual(ledger.read_bytes(), persisted_ledger)
            with ledger.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[-1]["market_ticker"], "KXBTC15M-26JUL272045-45")
            self.assertEqual(rows[-1]["shadow_balance_after"], "156.650")

    def test_checkpoint_fingerprint_tracks_shadow_balance_ledger(self) -> None:
        """A regime-only balance update must trigger the in-run checkpoint."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = root / "kalshi_shadow_equity_history.csv"
            ledger.write_text("market_ticker,shadow_balance_after\nA,100\n", encoding="utf-8")
            state = {"format_version": 1, "markets": {}}
            config = {"prophet_refit_every_markets": 1}
            before = trader.checkpoint_fingerprint(state, config, (ledger,))
            ledger.write_text("market_ticker,shadow_balance_after\nA,101\n", encoding="utf-8")
            after = trader.checkpoint_fingerprint(state, config, (ledger,))

            self.assertNotEqual(before, after)

    def test_legacy_rebased_state_is_archived_and_reinitialized_at_api_balance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path = root / "state.json"
            state_path.write_text(json.dumps({
                "format_version": 1,
                "actual_equity": "662.8476",
                "shadow_equity": "662.8476",
                "forecasts": [{"p01": "600", "p10": "620", "p25": "640", "p50": "660", "p75": "680", "p90": "700", "p99": "720"}],
                "actual_history": [{"market_ticker": "legacy"}],
                "shadow_history": [{"market_ticker": "legacy"}],
            }), encoding="utf-8")
            controller = EquityRegimeController(
                RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2), state_path, root / "outputs",
            )
            controller.initialize_absolute_balances(Decimal("80.13"), reason="test_legacy_migration")
            self.assertEqual(Decimal(controller.state["actual_balance"]), Decimal("80.13"))
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("80.13"))
            self.assertEqual(controller.state["forecasts"], [])
            self.assertEqual(controller.state["actual_history"], [])
            self.assertEqual(controller.state["shadow_history"], [])
            self.assertTrue(controller.state["balance_reconciled"])
            self.assertEqual(controller.state["legacy_migration"]["legacy_shadow_value"], "662.8476")
            self.assertTrue(list(root.glob("state.legacy-rebased-*.json")))

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

    def test_current_kalshi_cutoff_and_subaccount_number_are_normalized(self) -> None:
        class CurrentCutoffAPI(FakeHistoryAPI):
            async def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
                if path == "/historical/cutoff":
                    return {"trades_created_ts": "2026-07-25T00:00:00Z"}
                return await super().get_json(path, params)

        config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2)
        result = asyncio.run(HistoricalSynchronizer(config, {"markets": {}}).sync(CurrentCutoffAPI()))
        self.assertEqual(result.cutoff, datetime(2026, 7, 25, tzinfo=UTC))
        fill = normalize_fill({
            "fill_id": "subaccount-fill", "ticker": "KXBTC15M-X", "outcome_side": "yes",
            "action": "buy", "yes_price_dollars": "0.40", "count_fp": "1", "subaccount_number": 0,
        }, "portfolio")
        self.assertEqual(fill.subaccount, 0)

    def test_shared_series_ticker_is_not_bot_ownership_without_explicit_legacy_opt_in(self) -> None:
        fill = normalize_fill({
            "fill_id": "manual-same-series", "ticker": "KXBTC15M-26JUL260000-00",
            "side": "yes", "action": "buy", "price": "0.40", "count_fp": "1", "subaccount": 0,
        }, "portfolio")
        strict = RegimeConfig(enabled=True, dry_run=True)
        self.assertIsNone(ownership_evidence(fill, strict, set(), {fill.ticker}))
        legacy = RegimeConfig(enabled=True, dry_run=True, allow_series_ticker_ownership_fallback=True)
        self.assertEqual(
            ownership_evidence(fill, legacy, set(), {fill.ticker}),
            "configured_series_ticker_explicit_legacy_fallback",
        )

    def test_explicit_historical_anchor_is_distinct_from_default_starting_balance(self) -> None:
        config = RegimeConfig.from_mapping({
            "starting_balance": "100.00",
            "historical_starting_balance": "100.00",
            "equity_regime_enabled": True,
            "equity_regime_dry_run": True,
        })
        self.assertEqual(config.starting_balance, Decimal("100.00"))
        self.assertEqual(config.historical_starting_balance, Decimal("100.00"))

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
            config = RegimeConfig(
                enabled=True, dry_run=True, prophet_min_history=2,
                prophet_training_window=None, prophet_refit_every_markets=1,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(hours=2)).isoformat(), "completed_at": (now - timedelta(hours=2)).isoformat(), "shadow_balance_after": "100"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(hours=1)).isoformat(), "completed_at": (now - timedelta(hours=1)).isoformat(), "shadow_balance_after": "100"},
            ]
            controller.forecaster = FakeForecaster("95", "100.50")
            first = decision("KXBTC15M-T1", now)
            controller.start_market(first)
            self.assertEqual(controller.state["last_p10"], "95")
            self.assertEqual(controller.state["forecast_target_ticker"], first.target_ticker)
            self.assertEqual(controller.state["prophet_training_rows"], 2)
            controller.state["shadow_open"][first.target_ticker].update({"status": "finalized", "shadow_realized_pnl": "1", "contracts": "0"})
            controller.close_market(ticker=first.target_ticker, outcome="yes", market_close_time=first.target_close_time, actual_realized_pnl=Decimal("0"), actual_balance_after=Decimal("100"))
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
            controller.close_market(ticker=second.target_ticker, outcome="no", market_close_time=second.target_close_time, actual_realized_pnl=Decimal("0"), actual_balance_after=Decimal("100"))
            # T2 was skipped live in the simulated regime; P10 restarts T3.
            self.assertEqual(controller.state["live_vs_shadow"][-1]["live_execution_enabled"], False)
            self.assertEqual(Decimal(controller.state["actual_balance"]), Decimal("100"))
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("99"))
            self.assertTrue(controller.execution_enabled_for_market())
            self.assertFalse(controller.should_suppress_new_live_orders())  # dry-run is never an execution switch
            restored = EquityRegimeController(config, root / "state.json", root / "outputs")
            self.assertTrue(restored.execution_enabled_for_market())
            rows_before = len(restored.state["shadow_history"])
            restored.close_market(ticker=second.target_ticker, outcome="no", market_close_time=second.target_close_time, actual_realized_pnl=Decimal("0"), actual_balance_after=Decimal("100"))
            self.assertEqual(len(restored.state["shadow_history"]), rows_before)
            for forecast in restored.state["forecasts"]:
                self.assertLess(forecast["training_end"], forecast["forecast_target_time"])

    def test_state_retains_only_configured_recent_market_history(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, history_max_markets=2)
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            close = datetime.now(tz=UTC).isoformat()
            controller.state["actual_history"] = [{"market_ticker": str(index), "market_close_time": close, "actual_balance_after": "100"} for index in range(3)]
            controller.state["shadow_history"] = [{"market_ticker": str(index), "market_close_time": close, "shadow_balance_after": "100"} for index in range(3)]
            controller.state["forecasts"] = []
            controller.state["live_vs_shadow"] = [{"market_ticker": str(index)} for index in range(3)]
            controller.state["processed_market_tickers"] = [str(index) for index in range(3)]
            controller.save()
            self.assertEqual([row["market_ticker"] for row in controller.state["actual_history"]], ["1", "2"])
            self.assertEqual(controller.state["processed_market_tickers"], ["1", "2"])

    def test_live_market_is_exact_shadow_even_with_conservative_off_state_model(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(enabled=True, dry_run=True, prophet_min_history=2, shadow_fill_model="conservative_trade_through")
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
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
                actual_balance_after=Decimal("100.60"),
            )
            shadow = controller.state["shadow_history"][-1]
            self.assertEqual(Decimal(shadow["shadow_realized_pnl"]), Decimal("0.60"))
            self.assertEqual(shadow["shadow_simulation_quality"], "exact_replay")

    def test_live_cash_timing_uses_authenticated_balance_change_without_rebasing_shadow(self) -> None:
        """A position opened before recovery debits cash before its P/L closes."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(enabled=True, dry_run=False, allow_live_state_transitions=True, prophet_min_history=2)
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("75.9868"), reason="test")
            target = decision("KXBTC15M-CARRY", datetime.now(tz=UTC))
            controller.start_market(target)
            controller.close_market(
                ticker=target.target_ticker,
                outcome="yes",
                market_close_time=target.target_close_time,
                actual_realized_pnl=Decimal("1.8"),
                actual_metadata={"contracts_bought": "3", "entry_cost": "1.2", "settlement_payout": "3"},
                actual_was_live=True,
                actual_balance_after=Decimal("78.9868"),
            )
            self.assertEqual(Decimal(controller.state["actual_balance"]), Decimal("78.9868"))
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("78.9868"))
            shadow = controller.state["shadow_history"][-1]
            self.assertEqual(Decimal(shadow["shadow_market_pnl"]), Decimal("1.8"))
            self.assertEqual(Decimal(shadow["shadow_balance_change"]), Decimal("3"))
            self.assertEqual(controller.state["balance_adjustments"][-1]["adjustment_type"], "entry_or_open_position_cash_timing")
            self.assertTrue(controller.balance_reconciled)

    def test_endpoint_anchored_ledger_bootstrap_preserves_absolute_balances(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True,
                dry_run=False,
                allow_live_state_transitions=True,
                prophet_min_history=2,
                history_max_markets=2,
                prophet_training_window=2,
                allow_endpoint_anchored_ledger_bootstrap=True,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            trader_state = {
                "markets": {
                    "KXBTC15M-A": {
                        "ticker": "KXBTC15M-A", "status": "finalized", "market_close_time": (now - timedelta(minutes=30)).isoformat(),
                        "net_profit_loss": "1.50", "locked_side": "yes", "contracts": "3", "total_cost": "1.50", "gross_payout": "3",
                        # Historical terminal records can retain a stale
                        # position snapshot and must not block the new
                        # endpoint-anchored 200-market bootstrap.
                        "exchange_position_contracts": "-3",
                    },
                    "KXBTC15M-B": {
                        "ticker": "KXBTC15M-B", "status": "exited_early", "market_close_time": (now - timedelta(minutes=15)).isoformat(),
                        "net_profit_loss": "-0.50", "locked_side": "no", "contracts": "2", "total_cost": "1", "gross_payout": "0.50",
                    },
                },
            }
            self.assertEqual(
                controller.bootstrap_from_live_ledger(trader_state, api_current_balance=Decimal("100")),
                2,
            )
            balances = [Decimal(row["shadow_balance_after"]) for row in controller.state["shadow_history"]]
            self.assertEqual(balances, [Decimal("100.50"), Decimal("100.00")])
            self.assertEqual(Decimal(controller.state["actual_balance"]), Decimal("100"))
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("100"))
            self.assertEqual(Decimal(controller.state["historical_starting_balance"]), Decimal("99.00"))
            self.assertTrue(controller.balance_reconciled)
            self.assertEqual(controller.state["balance_source"], "authenticated_endpoint_anchored_durable_live_bot_ledger")

    def test_colab_account_series_history_uses_starting_balance_without_api_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True, dry_run=False, allow_live_state_transitions=True,
                starting_balance=Decimal("100"), prophet_min_history=2,
                history_max_markets=2, prophet_training_window=3,
                prophet_history_source="account_series",
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")

            def row(ticker: str, pnl: str) -> ReconstructedMarket:
                return ReconstructedMarket(
                    market_ticker=ticker, market_close_time=datetime(2026, 7, 26, tzinfo=UTC), selected_side="yes",
                    contracts_bought=Decimal("1"), contracts_sold=Decimal("0"), average_entry=Decimal("0.4"),
                    entry_cost=Decimal("0.4"), exit_proceeds=Decimal("0"), settlement_payout=Decimal("1"),
                    fees=Decimal("0"), realized_pnl=Decimal(pnl), exit_method="settlement",
                    source="portfolio", reconciliation_status="reconstructed",
                )

            self.assertTrue(controller.rebuild_colab_reference_account_series_history(
                [row("KXBTC15M-26JUL260000-00", "1.50"), row("KXBTC15M-26JUL260015-15", "-0.50")],
                api_current_balance=Decimal("80.13"),
            ))
            self.assertEqual(
                [Decimal(item["shadow_balance_after"]) for item in controller.state["shadow_history"]],
                [Decimal("101.50"), Decimal("101.00")],
            )
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("101.00"))
            self.assertEqual(Decimal(controller.state["actual_balance"]), Decimal("80.13"))
            self.assertTrue(controller.prophet_history_ready)
            self.assertFalse(controller.balance_reconciled)

    def test_closed_position_csv_matches_colab_sort_deduplicate_and_cumulative_pnl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            export = root / "closed-positions.csv"
            export.write_text(
                "Ticker,Total return ($)\n"
                "KXBTC15M-26JUL260000-00,$1.00\n"
                "KXBTC15M-26JUL260015-15,-$2.00\n"
                "KXBTC15M-26JUL260015-15,-$1.50\n"
                "KXBTC15M-26JUL260030-30,$2.00\n"
                "KXMVESPORTSMULTIGAMEEXTENDED-X,-$9.00\n",
                encoding="utf-8",
            )
            config = RegimeConfig(
                enabled=True, dry_run=False, allow_live_state_transitions=True,
                starting_balance=Decimal("100"), prophet_min_history=2,
                history_max_markets=2, prophet_training_window=3,
                prophet_history_source="account_series", prophet_reference_closed_positions_path=export,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            self.assertTrue(controller.rebuild_colab_reference_closed_positions_csv(
                export, api_current_balance=Decimal("80.13"),
            ))
            self.assertEqual(
                [Decimal(item["shadow_balance_after"]) for item in controller.state["shadow_history"]],
                [Decimal("98.50"), Decimal("100.50")],
            )
            self.assertEqual(controller.state["balance_source"], "colab_reference_closed_positions_csv")
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("100.50"))

            # A later live settlement must not replace the exact Colab curve
            # with the unrelated endpoint-anchored bot-only reconstruction.
            self.assertEqual(
                controller.bootstrap_from_live_ledger(
                    {"markets": {}}, api_current_balance=Decimal("80.13"),
                ),
                0,
            )
            self.assertEqual(controller.state["balance_source"], "colab_reference_closed_positions_csv")
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("100.50"))

            controller.forecaster = FakeForecaster("95", "105")
            forecast = controller.prime_colab_reference_forecast()
            self.assertIsNotNone(forecast)
            self.assertEqual(forecast["forecast_target_ticker"], "KXBTC15M-26JUL260045-30")
            self.assertEqual(controller.state["last_p10"], "95")
            self.assertEqual(controller.state["last_p50"], "100")
            self.assertEqual(controller.state["last_p90"], "105")

    def test_hundred_row_diagnostic_forecast_uses_only_first_row_for_live_filter(self) -> None:
        class HorizonForecaster:
            fit_number = 0

            def forecast_horizon(self, observations, _target, horizon):
                self.fit_number += 1
                self.observed_balances = [row["shadow_balance_after"] for row in observations]
                return [
                    {
                        "p01": Decimal("90"), "p10": Decimal("95"), "p25": Decimal("97"),
                        "p50": Decimal("100"), "p75": Decimal("103"), "p90": Decimal("105"), "p99": Decimal("110"),
                    }
                    for _ in range(horizon)
                ]

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True, dry_run=False, allow_live_state_transitions=True,
                prophet_min_history=2, prophet_training_window=2, history_max_markets=2,
                prophet_future_horizon_markets=100,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(minutes=30)).isoformat(), "completed_at": (now - timedelta(minutes=30)).isoformat(), "shadow_balance_after": "98.50"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(minutes=15)).isoformat(), "completed_at": (now - timedelta(minutes=15)).isoformat(), "shadow_balance_after": "100.00"},
            ]
            forecaster = HorizonForecaster()
            controller.forecaster = forecaster
            forecast = controller.prepare_forecast(decision("KXBTC15M-HORIZON", now))
            self.assertEqual(forecaster.observed_balances, ["98.50", "100.00"])
            self.assertEqual(forecast["p10"], "95")
            self.assertEqual(len(controller.state["future_forecast_snapshot"]), 100)
            self.assertTrue(controller.state["future_forecast_snapshot"][0]["used_for_live_filter"])
            self.assertFalse(controller.state["future_forecast_snapshot"][1]["used_for_live_filter"])

    def test_refit_cadence_consumes_the_matching_future_horizon_row(self) -> None:
        """A 75-market cadence must not reuse row one on market two."""

        class DistinctHorizonForecaster:
            fit_number = 0

            def forecast_horizon(self, _observations, _target, horizon):
                self.fit_number += 1
                return [
                    {
                        "p01": Decimal(str(90 + index)), "p10": Decimal(str(95 + index)),
                        "p25": Decimal(str(100 + index)), "p50": Decimal(str(105 + index)),
                        "p75": Decimal(str(110 + index)), "p90": Decimal(str(115 + index)), "p99": Decimal(str(120 + index)),
                    }
                    for index in range(horizon)
                ]

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True, dry_run=True, prophet_min_history=2,
                prophet_training_window=2, history_max_markets=2,
                prophet_refit_every_markets=75, prophet_future_horizon_markets=100,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(minutes=30)).isoformat(), "completed_at": (now - timedelta(minutes=30)).isoformat(), "shadow_balance_after": "99"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(minutes=15)).isoformat(), "completed_at": (now - timedelta(minutes=15)).isoformat(), "shadow_balance_after": "100"},
            ]
            forecaster = DistinctHorizonForecaster()
            controller.forecaster = forecaster
            first = controller.prepare_forecast(decision("KXBTC15M-CADENCE-1", now))
            self.assertEqual(first["p10"], "95")
            self.assertEqual(forecaster.fit_number, 1)
            # Market one has closed, so market two must consume horizon row 2.
            controller.state["markets_since_refit"] = 1
            second = controller.prepare_forecast(decision("KXBTC15M-CADENCE-2", now + timedelta(minutes=15)))
            self.assertEqual(second["p10"], "96")
            self.assertEqual(second["model_fit_error"], "refit_deferred_reused_horizon_row_2")
            self.assertEqual(forecaster.fit_number, 1)
            self.assertTrue(controller.state["future_forecast_snapshot"][1]["used_for_live_filter"])
            self.assertEqual(controller.state["future_forecast_snapshot"][1]["consumed_target_ticker"], "KXBTC15M-CADENCE-2")

    def test_completed_balance_change_forces_the_next_live_forecast_to_refit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True,
                dry_run=True,
                prophet_min_history=2,
                prophet_training_window=None,
                prophet_refit_every_markets=1,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(hours=2)).isoformat(), "completed_at": (now - timedelta(hours=2)).isoformat(), "shadow_balance_after": "100"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(hours=1)).isoformat(), "completed_at": (now - timedelta(hours=1)).isoformat(), "shadow_balance_after": "100"},
            ]
            forecaster = FakeForecaster("95", "105")
            controller.forecaster = forecaster
            first = decision("KXBTC15M-REFIT-1", now)
            controller.start_market(first)
            self.assertEqual(forecaster.fit_number, 1)

            controller.state["shadow_open"][first.target_ticker].update({
                "status": "finalized", "shadow_realized_pnl": "1", "contracts": "0",
            })
            controller.close_market(
                ticker=first.target_ticker,
                outcome="yes",
                market_close_time=first.target_close_time,
                actual_realized_pnl=Decimal("0"),
                actual_balance_after=Decimal("100"),
            )
            self.assertEqual(controller.state["markets_since_refit"], 1)

            controller.start_market(decision("KXBTC15M-REFIT-2", now + timedelta(minutes=16)))
            self.assertEqual(forecaster.fit_number, 2)
            self.assertEqual(controller.state["markets_since_refit"], 0)

    def test_finalization_bootstraps_immediately_after_inherited_position_closes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True, dry_run=False, allow_live_state_transitions=True,
                prophet_min_history=2, history_max_markets=2, prophet_training_window=2,
                allow_endpoint_anchored_ledger_bootstrap=True,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            previous = {
                "ticker": "KXBTC15M-OLD", "status": "finalized", "market_close_time": (now - timedelta(minutes=30)).isoformat(),
                "net_profit_loss": "1", "locked_side": "yes", "contracts": "1", "total_cost": "0.4", "gross_payout": "1",
            }
            current = {
                "ticker": "KXBTC15M-CURRENT-BOOT", "status": "finalized", "settlement_outcome": "yes",
                "settled_at": (now - timedelta(minutes=15)).isoformat(), "market_close_time": (now - timedelta(minutes=15)).isoformat(),
                "net_profit_loss": "-0.5", "locked_side": "no", "execution_mode": "live", "contracts": "1",
                "total_cost": "0.5", "gross_payout": "0", "settlement_contracts": "0", "kalshi_fees": "0",
            }
            trader_state = {"markets": {previous["ticker"]: previous, current["ticker"]: current}}

            class FakeRest:
                async def balance_decimal(self) -> Decimal:
                    return Decimal("100.50")

            asyncio.run(trader.account_equity_regime_finalization(
                FakeRest(), controller, trader_state, current, None, dry_run=False,
            ))
            self.assertTrue(current["regime_accounted"])
            self.assertEqual(len(controller.state["shadow_history"]), 2)
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("100.50"))
            self.assertEqual(controller.state["state_reason"], "absolute_200_market_ledger_bootstrap")

    def test_live_control_suppresses_only_the_next_market_after_p90(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True,
                dry_run=False,
                allow_live_state_transitions=True,
                prophet_min_history=2,
                prophet_training_window=None,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(hours=2)).isoformat(), "completed_at": (now - timedelta(hours=2)).isoformat(), "shadow_balance_after": "100"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(hours=1)).isoformat(), "completed_at": (now - timedelta(hours=1)).isoformat(), "shadow_balance_after": "100"},
            ]
            controller.forecaster = FakeForecaster("95", "100.50")
            current = decision("KXBTC15M-CURRENT", now)
            controller.start_market(current)
            controller.state["shadow_open"][current.target_ticker].update({"status": "finalized", "shadow_realized_pnl": "1", "contracts": "0"})
            controller.close_market(
                ticker=current.target_ticker,
                outcome="yes",
                market_close_time=current.target_close_time,
                actual_realized_pnl=Decimal("1"),
                actual_was_live=True,
                actual_balance_after=Decimal("101"),
            )
            self.assertFalse(controller.execution_enabled_for_market())
            self.assertTrue(controller.should_suppress_new_live_orders())
            self.assertTrue(controller.state["live_vs_shadow"][-1]["live_execution_enabled"])

    def test_startup_gate_suppresses_first_new_market_when_restored_shadow_is_above_p90(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(
                enabled=True,
                dry_run=False,
                allow_live_state_transitions=True,
                prophet_history_source="account_series",
                prophet_min_history=2,
                prophet_training_window=None,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("101"), reason="test")
            now = datetime.now(tz=UTC)
            controller.state["shadow_history"] = [
                {"market_ticker": "old1", "market_close_time": (now - timedelta(hours=2)).isoformat(), "completed_at": (now - timedelta(hours=2)).isoformat(), "shadow_balance_after": "100"},
                {"market_ticker": "old2", "market_close_time": (now - timedelta(hours=1)).isoformat(), "completed_at": (now - timedelta(hours=1)).isoformat(), "shadow_balance_after": "101"},
            ]
            controller.state["shadow_balance"] = "101"
            controller.state["prophet_history_ready"] = True
            controller.forecaster = FakeForecaster("95", "100.50")
            forecast = controller.prepare_forecast(decision("KXBTC15M-BOOTSTRAP", now))

            self.assertTrue(controller.apply_startup_regime_gate())
            self.assertFalse(controller.execution_enabled_for_market())
            self.assertTrue(controller.should_suppress_new_live_orders())
            self.assertEqual(controller.state["state_reason"], "startup_shadow_balance_at_or_above_p90")
            self.assertTrue(forecast["startup_exit_signal"])

            _, execution_for_next_market = controller.start_market(
                decision("KXBTC15M-AFTER-BOOTSTRAP", now + timedelta(minutes=15)),
            )
            self.assertFalse(execution_for_next_market)

    def test_live_finalization_updates_actual_and_shadow_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = RegimeConfig(enabled=True, dry_run=False, allow_live_state_transitions=True, prophet_min_history=2)
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.initialize_absolute_balances(Decimal("100"), reason="test")
            generated = datetime.now(tz=UTC)
            target = decision("KXBTC15M-LIVE-FINAL", generated)
            controller.start_market(target)
            record = {
                "ticker": target.target_ticker,
                "settlement_outcome": "yes",
                "settled_at": target.target_close_time.isoformat(),
                "market_close_time": target.target_close_time.isoformat(),
                "locked_side": "yes",
                "execution_mode": "live",
                "contracts": "1",
                "total_cost": "0.40",
                "gross_payout": "1.00",
                "settlement_contracts": "1",
                "kalshi_fees": "0",
                "net_profit_loss": "0.60",
            }
            class FakeRest:
                async def balance_decimal(self) -> Decimal:
                    return Decimal("100.60")
            asyncio.run(trader.account_equity_regime_finalization(FakeRest(), controller, {"markets": {}}, record, None, dry_run=False))
            self.assertTrue(record["regime_accounted"])
            self.assertEqual(Decimal(controller.state["actual_balance"]), Decimal("100.60"))
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("100.60"))
            asyncio.run(trader.account_equity_regime_finalization(FakeRest(), controller, {"markets": {}}, record, None, dry_run=False))
            self.assertEqual(len(controller.state["actual_history"]), 1)


if __name__ == "__main__":
    unittest.main()
