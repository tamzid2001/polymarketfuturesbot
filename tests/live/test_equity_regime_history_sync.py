"""Read-only history-sync accounting evidence tests."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from bot.equity_regime import (
    EquityRegimeController,
    HistoricalSynchronizer,
    RegimeConfig,
    reconstruct_realized_pnl,
    synchronize_history,
)


class HistorySyncAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_colab_closed_positions_source_survives_startup_sync(self) -> None:
        """Startup must publish CSV-based—not endpoint-ledger—Prophet bands."""

        class HorizonForecaster:
            fit_number = 0
            last_future_timestamps = []

            def forecast_horizon(self, observations, target_time, horizon_markets):
                self.fit_number += 1
                self.last_future_timestamps = [target_time]
                return [{
                    "p01": Decimal("90"), "p10": Decimal("95"), "p25": Decimal("97"),
                    "p50": Decimal("100"), "p75": Decimal("103"), "p90": Decimal("105"),
                    "p99": Decimal("110"),
                }]

        class API:
            calls: list[str] = []

            async def get_json(self, path, params=None):
                self.calls.append(path)
                if path == "/portfolio/balance":
                    return {"balance_dollars": "80.13"}
                raise AssertionError(path)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            export = root / "closed-positions.csv"
            export.write_text(
                "Ticker,Total return ($)\n"
                "KXBTC15M-26JUL260000-00,$1.00\n"
                "KXBTC15M-26JUL260015-15,-$1.50\n",
                encoding="utf-8",
            )
            config = RegimeConfig(
                enabled=True, dry_run=True, prophet_history_source="account_series",
                prophet_reference_closed_positions_path=export, starting_balance=Decimal("100"),
                prophet_min_history=2, history_max_markets=2, prophet_training_window=3,
            )
            controller = EquityRegimeController(config, root / "state.json", root / "outputs")
            controller.forecaster = HorizonForecaster()
            await synchronize_history(controller, API(), {"markets": {}})

            self.assertEqual(controller.state["balance_source"], "colab_reference_closed_positions_csv")
            self.assertEqual(Decimal(controller.state["shadow_balance"]), Decimal("99.50"))
            self.assertEqual(controller.state["last_p10"], "95")
            self.assertEqual(controller.state["last_p50"], "100")
            self.assertEqual(controller.state["last_p90"], "105")
            self.assertEqual(API.calls, ["/portfolio/balance"])
            self.assertTrue(controller.state["history_sync"]["historical_api_sync_skipped"])

    async def test_colab_reference_handoff_preserves_post_baseline_shadow_tail(self) -> None:
        """A new Actions runner must not replay the static CSV over its checkpoint."""

        class HorizonForecaster:
            fit_number = 0
            last_future_timestamps = []

            def forecast_horizon(self, _observations, target_time, _horizon_markets):
                self.fit_number += 1
                self.last_future_timestamps = [target_time]
                return [{
                    "p01": Decimal("90"), "p10": Decimal("95"), "p25": Decimal("97"),
                    "p50": Decimal("100"), "p75": Decimal("103"), "p90": Decimal("105"),
                    "p99": Decimal("110"),
                }]

        class FirstStartupAPI:
            async def get_json(self, path, params=None):
                if path != "/portfolio/balance":
                    raise AssertionError(path)
                return {"balance_dollars": "80.13"}

        class HandoffAPI:
            async def get_json(self, path, params=None):
                raise AssertionError(f"handoff must restore committed state without calling {path}")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            export = root / "closed-positions.csv"
            export.write_text(
                "Ticker,Total return ($)\n"
                "KXBTC15M-26JUL260000-00,$1.00\n"
                "KXBTC15M-26JUL260015-15,-$1.50\n",
                encoding="utf-8",
            )
            config = RegimeConfig(
                enabled=True, dry_run=False, prophet_history_source="account_series",
                prophet_reference_closed_positions_path=export, starting_balance=Decimal("100"),
                prophet_min_history=2, history_max_markets=3, prophet_training_window=3,
            )
            state_path = root / "state.json"
            controller = EquityRegimeController(config, state_path, root / "outputs")
            controller.forecaster = HorizonForecaster()
            await synchronize_history(controller, FirstStartupAPI(), {"markets": {}})

            # Model the durable state left by a completed later live/shadow
            # market.  This is exactly the tail the next Actions checkout
            # used to erase by rebuilding the older two-row CSV.
            controller.state["actual_history"].append({
                "market_ticker": "KXBTC15M-26JUL260030-30",
                "market_close_time": "2026-07-26T00:30:30+00:00",
                "completed_at": "2026-07-26T00:30:30+00:00",
                "actual_balance_after": "82.13",
            })
            controller.state["shadow_history"].append({
                "market_ticker": "KXBTC15M-26JUL260030-30",
                "market_close_time": "2026-07-26T00:30:30+00:00",
                "completed_at": "2026-07-26T00:30:30+00:00",
                "shadow_balance_before": "99.50",
                "shadow_balance_after": "103.75",
                "shadow_realized_pnl": "4.25",
                "shadow_fill_model": "live_equivalent",
                "shadow_simulation_quality": "exact_replay",
                "live_execution_enabled": False,
            })
            controller.state.update({
                "actual_balance": "82.13",
                "shadow_balance": "103.75",
                "execution_enabled": False,
                "state_reason": "shadow_balance_at_or_above_p90",
            })
            controller.save()

            successor = EquityRegimeController(config, state_path, root / "successor-outputs")
            await synchronize_history(successor, HandoffAPI(), {"markets": {}})

            self.assertEqual(Decimal(successor.state["actual_balance"]), Decimal("82.13"))
            self.assertEqual(Decimal(successor.state["shadow_balance"]), Decimal("103.75"))
            self.assertEqual(successor.state["shadow_history"][-1]["market_ticker"], "KXBTC15M-26JUL260030-30")
            self.assertFalse(successor.execution_enabled_for_market())
            self.assertEqual(successor.state["history_sync"]["reference_curve_mode"], "restored")

    async def test_exports_ownership_evidence_and_manual_settlement_without_counting_it(self) -> None:
        manual_ticker = "KXBTC15M-26JUL262100-00"
        bot_ticker = "KXBTC15M-26JUL262115-15"

        class API:
            async def get_json(self, path, params=None):
                if path == "/historical/cutoff":
                    return {"trades_created_ts": "2026-05-27T00:00:00Z"}
                if path == "/historical/fills":
                    return {"fills": []}
                if path == "/portfolio/fills":
                    return {
                        "fills": [
                            {
                                "fill_id": "manual-fill", "ticker": manual_ticker,
                                "side": "no", "action": "buy", "count_fp": "2",
                                "no_price_dollars": "0.97", "fee_cost": "0.01",
                                "created_time": "2026-07-27T01:00:00Z",
                            },
                            {
                                "fill_id": "bot-fill", "ticker": bot_ticker,
                                "side": "yes", "action": "buy", "count_fp": "3",
                                "yes_price_dollars": "0.40", "fee_cost": "0.02",
                                "client_order_id": "settlement-contrarian-test",
                                "created_time": "2026-07-27T01:01:00Z",
                            },
                        ],
                    }
                if path == "/portfolio/settlements":
                    return {
                        "settlements": [
                            {"ticker": manual_ticker, "market_result": "no", "payout_dollars": "2.00"},
                            {"ticker": bot_ticker, "market_result": "yes", "payout_dollars": "3.00"},
                        ],
                    }
                if path == "/portfolio/balance":
                    return {"balance_dollars": "100.00"}
                raise AssertionError(path)

        result = await HistoricalSynchronizer(RegimeConfig(), {}).sync(API())

        self.assertEqual([fill.fill_id for fill in result.fills], ["bot-fill"])
        self.assertEqual(result.owned_fill_audit[0]["ownership_evidence"], "client_order_id_prefix")
        self.assertEqual(result.ambiguous_fills[0]["fill_id"], "manual-fill")
        self.assertEqual(result.ambiguous_settlement_audit[0]["ticker"], manual_ticker)
        self.assertEqual(result.ambiguous_settlement_audit[0]["payout"], "2.00")
        self.assertEqual([settlement.ticker for settlement in result.settlements], [bot_ticker])
        self.assertEqual(result.owned_settlement_audit[0]["ticker"], bot_ticker)
        self.assertIn('"market_result":"yes"', result.owned_settlement_audit[0]["raw_api_record"])

    async def test_reduce_only_yes_exit_is_not_counted_as_new_no_position_or_settlement_payout(self) -> None:
        ticker = "KXBTC15M-26JUL261315-15"
        entry_order_id = "entry"
        exit_order_id = "exit"

        class API:
            async def get_json(self, path, params=None):
                if path == "/historical/cutoff":
                    return {"trades_created_ts": "2026-05-27T00:00:00Z"}
                if path == "/historical/fills":
                    return {"fills": []}
                if path == "/portfolio/fills":
                    return {"fills": [
                        {
                            "fill_id": "entry-fill", "order_id": entry_order_id, "ticker": ticker,
                            "side": "yes", "action": "buy", "outcome_side": "yes", "book_side": "bid",
                            "count_fp": "30", "yes_price_dollars": "0.2000", "no_price_dollars": "0.8000",
                            "fee_cost": "0.0500", "created_time": "2026-07-26T17:00:00Z",
                        },
                        {
                            # Kalshi's reciprocal representation of a
                            # reduce-only exit of the held YES contracts.
                            "fill_id": "exit-fill", "order_id": exit_order_id, "ticker": ticker,
                            "side": "no", "action": "sell", "outcome_side": "no", "book_side": "ask",
                            "count_fp": "30", "yes_price_dollars": "0.0420", "no_price_dollars": "0.9580",
                            "fee_cost": "0.0300", "created_time": "2026-07-26T17:11:00Z",
                        },
                    ]}
                if path == "/portfolio/settlements":
                    return {"settlements": [{
                        "ticker": ticker, "market_result": "yes", "settled_time": "2026-07-26T17:15:00Z",
                    }]}
                if path == "/portfolio/balance":
                    return {"balance_dollars": "100.00"}
                raise AssertionError(path)

        local_state = {"markets": {ticker: {
            "locked_side": "yes",
            "orders": {"entry": {"order_id": entry_order_id, "side": "yes"}},
            "live_exit_orders": [{"order_id": exit_order_id, "held_side": "yes"}],
        }}}
        result = await HistoricalSynchronizer(RegimeConfig(), local_state).sync(API())
        market = reconstruct_realized_pnl(result.fills, result.settlements)[0]

        self.assertEqual(str(market.contracts_bought), "30")
        self.assertEqual(str(market.contracts_sold), "30")
        self.assertEqual(str(market.exit_proceeds), "1.2600")
        self.assertEqual(str(market.settlement_payout), "0")
        self.assertEqual(str(market.realized_pnl), "-4.8200")


if __name__ == "__main__":
    unittest.main()
