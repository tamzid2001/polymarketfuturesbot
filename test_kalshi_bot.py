"""Static safety checks for the BTC-only Prophet GTC ladder runner.

These checks deliberately avoid credentials, network calls, and model fitting.
They ensure the production entry point cannot select an ETH ticker or change
the four fixed rung costs/count based on a previous loss.
"""

from __future__ import annotations

import ast
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
RUNNER = ROOT / "kalshibtc15minupordown.py"


class ProphetLockedLadderStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = RUNNER.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)
        cls.functions = {
            node.name: node for node in cls.tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

    def test_fixed_btc_only_ladder_is_declared(self) -> None:
        self.assertIn("LADDER_LEVELS = (0.40, 0.30, 0.20, 0.10)", self.source)
        self.assertIn('ORDER_TIF      = "good_till_canceled"', self.source)
        self.assertIn('SERIES_TICKER  = "KXBTC15M"', self.source)

    def test_no_hedge_or_loss_sizing_is_reachable_from_main(self) -> None:
        main_source = ast.get_source_segment(self.source, self.functions["main_async"])
        loop_source = ast.get_source_segment(self.source, self.functions["strategy_loop"])
        active_source = main_source + loop_source
        for forbidden in ("ETH_SERIES_TICKER", "eth_hedge_monitor", "ARBITRAGE_SHARES",
                          "LOSS_MULTIPLIER", "next_eth_hedge_state"):
            self.assertNotIn(forbidden, active_source)

    def test_execution_entrypoint_is_locked_ladder_only(self) -> None:
        function = self.functions["execute_window_trade"]
        statements = function.body
        self.assertIsInstance(statements[0], ast.Expr)  # docstring
        self.assertIsInstance(statements[1], ast.Delete)  # `del nt`
        self.assertIsInstance(statements[2], ast.Expr)
        call = statements[2].value
        self.assertIsInstance(call, ast.Await)
        self.assertEqual(call.value.func.id, "execute_locked_ladder")
        self.assertIsInstance(statements[3], ast.Return)

    def test_no_side_translation_is_a_hedge(self) -> None:
        function = self.functions["_ladder_order_terms"]
        function_source = ast.get_source_segment(self.source, function)
        self.assertIn('BookSide.BID, f"{economic_price:.2f}"', function_source)
        self.assertIn('BookSide.ASK, f"{1.0 - economic_price:.2f}"', function_source)

    def test_dry_run_records_executable_quote_hits_not_fabricated_exchange_fills(self) -> None:
        simulation = ast.get_source_segment(
            self.source, self.functions["_simulate_dry_ladder_fills"])
        execution = ast.get_source_segment(
            self.source, self.functions["execute_locked_ladder"])
        self.assertIn("simulated_executable_quote_hit", simulation)
        self.assertIn("fresh_executable_dry_quote", simulation)
        self.assertIn("not an exchange fill", simulation)
        self.assertIn("fill_count = 0.0 if DRY_RUN", execution)

    def test_inverse_prophet_shadow_is_paper_only_and_never_calls_order_submission(self) -> None:
        shadow = ast.get_source_segment(
            self.source, self.functions["create_inverse_prophet_shadow"])
        self.assertIn("INVERSE_PROPHET_SHADOW_ENABLED", self.source)
        self.assertIn("DRY_RUN and", self.source)
        self.assertIn("paper_only_no_exchange_orders", shadow)
        self.assertIn("paper-only inverse shadow", shadow)
        self.assertNotIn("_submit(", shadow)
        self.assertNotIn("_ladder_order_terms(", shadow)

    def test_win_rate_selector_paper_ladder_never_calls_order_submission(self) -> None:
        selector = ast.get_source_segment(
            self.source, self.functions["create_prophet_selector_shadow"])
        self.assertIn("PROPHET_SELECTOR_WINDOWS = (3, 5, 7, 10, 25, 50)", self.source)
        self.assertIn("DRY_RUN and PROPHET_SELECTOR_ENABLED", selector)
        self.assertIn("paper_only_no_exchange_orders", selector)
        self.assertIn("paper-only Prophet selector", selector)
        self.assertNotIn("_submit(", selector)
        self.assertNotIn("_ladder_order_terms(", selector)

    def test_prophet_fixed_stop_study_is_paper_only(self) -> None:
        fixed = ast.get_source_segment(
            self.source, self.functions["create_prophet_weighted_fixed_stop_shadows"])
        self.assertIn("PROPHET_WEIGHTED_FIXED_STOP_SHADOW_ENABLED", self.source)
        self.assertIn("DRY_RUN and", self.source)
        self.assertIn("fixed_stop_loss_per_contract", fixed)
        self.assertNotIn("_submit(", fixed)
        self.assertNotIn("_ladder_order_terms(", fixed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
