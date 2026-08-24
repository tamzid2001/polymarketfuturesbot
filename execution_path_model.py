"""Empirically calibrated Monte Carlo model for latent Kalshi execution paths.

This model does not invent a market settlement.  Its only random variables are
entry/fill and adverse-price path events that the historical settlement API
does not expose.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, replace
from typing import Iterable


RUNG_PRICES = (0.40, 0.30, 0.20, 0.10)


def _clamp_probability(value: float) -> float:
    return min(1.0, max(0.0, value))


def isotonic_decreasing(values: Iterable[float]) -> tuple[float, ...]:
    """Pool-adjacent-violators projection for a non-increasing probability curve."""

    blocks: list[list[float]] = []  # [mean, weight]
    for value in values:
        blocks.append([_clamp_probability(float(value)), 1.0])
        while len(blocks) >= 2 and blocks[-2][0] < blocks[-1][0]:
            right = blocks.pop()
            left = blocks.pop()
            weight = left[1] + right[1]
            blocks.append([(left[0] * left[1] + right[0] * right[1]) / weight, weight])
    projected: list[float] = []
    for mean, weight in blocks:
        projected.extend([mean] * int(weight))
    return tuple(projected)


@dataclass(frozen=True)
class ExecutionPath:
    """One stochastic execution realization for a fixed actual outcome."""

    entry_filled: bool
    deepest_adverse_price_level: float | None
    stop_triggered: bool

    @property
    def reached_40(self) -> bool:
        return self.deepest_adverse_price_level is not None and self.deepest_adverse_price_level <= 0.40

    @property
    def reached_30(self) -> bool:
        return self.deepest_adverse_price_level is not None and self.deepest_adverse_price_level <= 0.30

    @property
    def reached_20(self) -> bool:
        return self.deepest_adverse_price_level is not None and self.deepest_adverse_price_level <= 0.20

    @property
    def reached_10(self) -> bool:
        return self.deepest_adverse_price_level is not None and self.deepest_adverse_price_level <= 0.10


@dataclass(frozen=True)
class ExecutionCalibration:
    """Configurable, nested execution probabilities.

    The observed ``139/(139 + 318)`` and ``209/(209 + 221)`` fractions came
    from an *old 40c ladder*: they identify the joint probability that a
    market entered that adverse 40c-region cohort, conditional on the actual
    settlement outcome.  They do **not** identify a resting 49c maker-order
    fill probability.  Treating them as 49c fills both changes the selected
    directional sample and applies the adverse-selection effect twice.

    ``*_entry_fill_probability`` is deliberately a separate, explicit
    scenario assumption until 49c order-level fill telemetry is available.
    The 40c-region probabilities remain tied to the supplied observations.
    """

    # Latent 49c maker participation.  The default base case is a neutral
    # 85% scenario, not a claim that the supplied 40c-ladder statistics
    # estimate this value.  Use ``reconstruction_compatible`` for the
    # clearly-labelled full-participation comparison to the earlier model.
    win_entry_fill_probability: float = 0.85
    loss_entry_fill_probability: float = 0.85

    # Observed joint old-ladder / 40c-region rates among the executed-or-
    # zero-fill cohort.  The legacy loss-skipped bucket is intentionally not
    # included.  These are unconditional on a 49c maker fill.
    win_reach_40_joint_probability: float = 139 / (139 + 318)
    loss_reach_40_joint_probability: float = 209 / (209 + 221)

    # Conditional continuations after the simulated 40c/entry region, for
    # eventual directional winners.  These form a strict hierarchy.
    win_continue_30_given_40: float = 39 / 59
    win_continue_20_given_30: float = 23 / 39
    win_continue_10_given_20: float = 10 / 23

    # Loss observations were operational counters rather than perfectly
    # nested paths.  Values here are the monotonic/isotonic correction of
    # raw unconditional reaches [113,111,113,113] / 113.
    loss_continue_30_given_40: float = (111 + 113 + 113) / (3 * 113)
    loss_continue_20_given_30: float = 1.0
    loss_continue_10_given_20: float = 1.0

    model_name: str = "base_case_49c_participation_85pct"

    def __post_init__(self) -> None:
        values = (
            self.win_entry_fill_probability, self.loss_entry_fill_probability,
            self.win_reach_40_joint_probability, self.loss_reach_40_joint_probability,
            self.win_continue_30_given_40, self.win_continue_20_given_30,
            self.win_continue_10_given_20, self.loss_continue_30_given_40,
            self.loss_continue_20_given_30, self.loss_continue_10_given_20,
        )
        if any(not 0.0 <= value <= 1.0 for value in values):
            raise ValueError("all execution probabilities must be within [0, 1]")
        if self.win_reach_40_joint_probability > self.win_entry_fill_probability:
            raise ValueError("win 40c-region probability cannot exceed 49c fill probability")
        if self.loss_reach_40_joint_probability > self.loss_entry_fill_probability:
            raise ValueError("loss 40c-region probability cannot exceed 49c fill probability")

    @classmethod
    def base_case(cls) -> "ExecutionCalibration":
        raw_loss_reaches = (1.0, 111 / 113, 113 / 113, 113 / 113)
        corrected = isotonic_decreasing(raw_loss_reaches)
        return cls(
            loss_continue_30_given_40=corrected[1] / corrected[0],
            loss_continue_20_given_30=corrected[2] / corrected[1] if corrected[1] else 0.0,
            loss_continue_10_given_20=corrected[3] / corrected[2] if corrected[2] else 0.0,
            model_name="base_case_49c_participation_85pct",
        )

    @classmethod
    def reconstruction_compatible(cls) -> "ExecutionCalibration":
        """Prior-reconstruction comparison: every submitted 49c order fills.

        This is an intentionally explicit sensitivity scenario, not an
        assertion that historical 49c maker orders all filled.  It preserves
        the old model's participation convention while retaining actual
        settlement outcomes and the calibrated joint 40c-region rates.
        """

        base = cls.base_case()
        return replace(
            base,
            win_entry_fill_probability=1.0,
            loss_entry_fill_probability=1.0,
            model_name="reconstruction_compatible_49c_full_participation",
        )

    @classmethod
    def conservative(cls) -> "ExecutionCalibration":
        """Lower win participation and higher loss participation/depth."""

        base = cls.base_case()
        return replace(
            base,
            win_entry_fill_probability=0.70,
            loss_entry_fill_probability=0.80,
            win_reach_40_joint_probability=_clamp_probability(base.win_reach_40_joint_probability * 1.05),
            loss_reach_40_joint_probability=_clamp_probability(base.loss_reach_40_joint_probability * 1.10),
            win_continue_30_given_40=_clamp_probability(base.win_continue_30_given_40 * 1.05),
            win_continue_20_given_30=_clamp_probability(base.win_continue_20_given_30 * 1.05),
            win_continue_10_given_20=_clamp_probability(base.win_continue_10_given_20 * 1.05),
            model_name="conservative_49c_participation",
        )

    @classmethod
    def optimistic(cls) -> "ExecutionCalibration":
        """More win-side participation and less loss-side adverse selection."""

        base = cls.base_case()
        return replace(
            base,
            win_entry_fill_probability=0.98,
            loss_entry_fill_probability=0.98,
            win_reach_40_joint_probability=_clamp_probability(base.win_reach_40_joint_probability * 0.95),
            loss_reach_40_joint_probability=_clamp_probability(base.loss_reach_40_joint_probability * 0.90),
            win_continue_30_given_40=_clamp_probability(base.win_continue_30_given_40 * 0.95),
            win_continue_20_given_30=_clamp_probability(base.win_continue_20_given_30 * 0.95),
            win_continue_10_given_20=_clamp_probability(base.win_continue_10_given_20 * 0.95),
            model_name="optimistic_49c_participation",
        )

    def reach_probabilities_given_40(self, directional_win: bool) -> tuple[float, float, float, float]:
        """Unconditional rung reaches conditional on an observed 40c touch."""

        if directional_win:
            conditional = (
                1.0,
                _clamp_probability(self.win_continue_30_given_40),
                _clamp_probability(self.win_continue_20_given_30),
                _clamp_probability(self.win_continue_10_given_20),
            )
        else:
            conditional = (
                1.0,
                _clamp_probability(self.loss_continue_30_given_40),
                _clamp_probability(self.loss_continue_20_given_30),
                _clamp_probability(self.loss_continue_10_given_20),
            )
        reaches = [conditional[0]]
        for continuation in conditional[1:]:
            reaches.append(reaches[-1] * continuation)
        # Defensive PAV correction means a custom calibration cannot generate
        # an impossible 10c-without-20c path.
        return isotonic_decreasing(reaches)

    def fill_probability(self, directional_win: bool) -> float:
        return self.win_entry_fill_probability if directional_win else self.loss_entry_fill_probability

    def reach_40_probability_given_entry(self, directional_win: bool) -> float:
        """Conditional 40c touch after a 49c fill, preserving the joint rate."""

        joint = self.win_reach_40_joint_probability if directional_win else self.loss_reach_40_joint_probability
        entry = self.fill_probability(directional_win)
        return joint / entry if entry else 0.0

    def posterior_draw(self, rng: random.Random) -> "ExecutionCalibration":
        """Beta-binomial posterior draw for optional calibration uncertainty."""

        def beta(successes: int, failures: int) -> float:
            # Uniform Beta(1,1) prior; gammavariate is deterministic under rng.
            a = rng.gammavariate(successes + 1, 1.0)
            b = rng.gammavariate(failures + 1, 1.0)
            return a / (a + b)

        # Draw nested conditional transitions directly from the reported
        # counts.  The loss 20/10 counters are capped to their parent counts
        # before sampling and then projected to a valid hierarchy.
        win_30 = beta(39, 59 - 39)
        win_20 = beta(23, 39 - 23)
        win_10 = beta(10, 23 - 10)
        loss_30 = beta(111, 113 - 111)
        loss_20 = beta(111, 111 - 111)
        loss_10 = beta(111, 111 - 111)
        loss_reach = isotonic_decreasing((1.0, loss_30, loss_30 * loss_20, loss_30 * loss_20 * loss_10))
        win_joint = beta(139, 318)
        loss_joint = beta(209, 221)
        # Keep a user-selected 49c participation scenario fixed.  The given
        # old-ladder counts quantify joint 40c-region uncertainty, not 49c
        # order-fill uncertainty.  Projection preserves P(40c) <= P(49c).
        win_joint = min(win_joint, self.win_entry_fill_probability)
        loss_joint = min(loss_joint, self.loss_entry_fill_probability)
        return replace(
            self,
            win_reach_40_joint_probability=win_joint,
            loss_reach_40_joint_probability=loss_joint,
            win_continue_30_given_40=win_30,
            win_continue_20_given_30=win_20,
            win_continue_10_given_20=win_10,
            loss_continue_30_given_40=loss_reach[1],
            loss_continue_20_given_30=loss_reach[2] / loss_reach[1] if loss_reach[1] else 0.0,
            loss_continue_10_given_20=loss_reach[3] / loss_reach[2] if loss_reach[2] else 0.0,
            model_name=f"{self.model_name}_posterior",
        )


class ExecutionPathModel:
    def __init__(self, calibration: ExecutionCalibration | None = None) -> None:
        self.calibration = calibration or ExecutionCalibration.base_case()

    def sample(
        self,
        directional_win: bool,
        rng: random.Random,
        stop_price: float | None,
    ) -> ExecutionPath:
        """Sample a nested adverse path for a fixed known settlement outcome."""

        if rng.random() >= self.calibration.fill_probability(directional_win):
            return ExecutionPath(False, None, False)
        reach_40 = self.calibration.reach_40_probability_given_entry(directional_win)
        if rng.random() >= reach_40:
            return ExecutionPath(True, 0.49, False)
        reaches = self.calibration.reach_probabilities_given_40(directional_win)
        # Sequential continuation preserves nesting by construction.
        deepest_index = 0
        for index in range(1, len(RUNG_PRICES)):
            parent = reaches[index - 1]
            continuation = reaches[index] / parent if parent > 0 else 0.0
            if rng.random() >= continuation:
                break
            deepest_index = index
        deepest = RUNG_PRICES[deepest_index]
        stop_triggered = stop_price is not None and deepest <= stop_price
        return ExecutionPath(True, deepest, stop_triggered)

    def sample_from_uniforms(
        self,
        directional_win: bool,
        entry_uniform: float,
        depth_uniforms: tuple[float, float, float, float],
        stop_price: float | None,
    ) -> ExecutionPath:
        """CRN-friendly sampler: consumes the same four uniforms per market."""

        if entry_uniform >= self.calibration.fill_probability(directional_win):
            return ExecutionPath(False, None, False)
        reach_40 = self.calibration.reach_40_probability_given_entry(directional_win)
        if depth_uniforms[0] >= reach_40:
            return ExecutionPath(True, 0.49, False)
        reaches = self.calibration.reach_probabilities_given_40(directional_win)
        deepest_index = 0
        for index, uniform in enumerate(depth_uniforms[1:], start=1):
            parent = reaches[index - 1]
            continuation = reaches[index] / parent if parent > 0 else 0.0
            if uniform >= continuation:
                break
            deepest_index = index
        deepest = RUNG_PRICES[deepest_index]
        return ExecutionPath(True, deepest, stop_price is not None and deepest <= stop_price)


def calibration_targets() -> dict[str, float]:
    return {
        # The old live labels were "executed" / "zero_fill", but their
        # calibration evidence is the 40c-region cohort.  Do not relabel
        # them as observed 49c fills without order-level telemetry.
        "observed_40_region_rate_win": 139 / (139 + 318),
        "observed_40_region_rate_loss": 209 / (209 + 221),
        "observed_40_region_directional_wr": 139 / (139 + 209),
        "observed_no_40_region_directional_wr": 318 / (318 + 221),
        "observed_rung_wr_40": 59 / (59 + 113),
        "observed_rung_wr_30": 39 / (39 + 111),
        "observed_rung_wr_20": 23 / (23 + 113),
        "observed_rung_wr_10": 10 / (10 + 113),
    }


def simulate_calibration(
    model: ExecutionPathModel,
    replications: int = 20_000,
    seed: int = 7,
) -> dict[str, float]:
    """Validate the joint 40c-region and nested-depth calibration.

    The 457 win / 430 loss observations identify the old ladder's
    executed-or-zero-fill 40c-region behavior, not an order-level 49c fill
    rate.  The supplied participation scenario is reported separately and is
    intentionally excluded from observed-versus-simulated error checks.
    """

    rng = random.Random(seed)
    directional = [True] * (139 + 318) + [False] * (209 + 221)
    totals = {key: 0 for key in (
        "entry_win", "entry_loss", "region40_win", "region40_loss",
        "no_region40_win", "no_region40_loss",
    )}
    rung_totals = {key: 0 for key in ("r40_win", "r40_loss", "r30_win", "r30_loss", "r20_win", "r20_loss", "r10_win", "r10_loss")}
    for _ in range(replications):
        for outcome in directional:
            path = model.sample(outcome, rng, stop_price=None)
            category = "win" if outcome else "loss"
            if path.entry_filled:
                totals["entry_" + category] += 1
            totals[("region40_" if path.reached_40 else "no_region40_") + category] += 1
    # Rung counters are an older, conditional data set: 59 winners and 113
    # losers were already known to have reached the 40c region.  Validate
    # depth only after conditioning on that observed reach; combining it with
    # the later executed/zero-fill cohort would conflate different samples.
    for _ in range(replications):
        for outcome, count in ((True, 59), (False, 113)):
            reaches = model.calibration.reach_probabilities_given_40(outcome)
            for _ in range(count):
                category = "win" if outcome else "loss"
                rung_totals["r40_" + category] += 1
                reached_30 = rng.random() < (reaches[1] / reaches[0] if reaches[0] else 0.0)
                if not reached_30:
                    continue
                rung_totals["r30_" + category] += 1
                reached_20 = rng.random() < (reaches[2] / reaches[1] if reaches[1] else 0.0)
                if not reached_20:
                    continue
                rung_totals["r20_" + category] += 1
                if rng.random() < (reaches[3] / reaches[2] if reaches[2] else 0.0):
                    rung_totals["r10_" + category] += 1
    scale = float(replications)
    entry_win = totals["entry_win"] / scale
    entry_loss = totals["entry_loss"] / scale
    region40_win = totals["region40_win"] / scale
    region40_loss = totals["region40_loss"] / scale
    no_region40_win = totals["no_region40_win"] / scale
    no_region40_loss = totals["no_region40_loss"] / scale

    def wr(prefix: str) -> float:
        wins, losses = rung_totals[prefix + "_win"], rung_totals[prefix + "_loss"]
        return wins / (wins + losses) if wins + losses else math.nan

    simulated = {
        "simulated_49c_entry_fill_rate_win": entry_win / (139 + 318),
        "simulated_49c_entry_fill_rate_loss": entry_loss / (209 + 221),
        "simulated_40_region_rate_win": region40_win / (139 + 318),
        "simulated_40_region_rate_loss": region40_loss / (209 + 221),
        "simulated_40_region_directional_wr": region40_win / (region40_win + region40_loss),
        "simulated_no_40_region_directional_wr": no_region40_win / (no_region40_win + no_region40_loss),
        "simulated_rung_wr_40": wr("r40"),
        "simulated_rung_wr_30": wr("r30"),
        "simulated_rung_wr_20": wr("r20"),
        "simulated_rung_wr_10": wr("r10"),
    }
    targets = calibration_targets()
    pairs = {
        "40_region_rate_win": (simulated["simulated_40_region_rate_win"], targets["observed_40_region_rate_win"]),
        "40_region_rate_loss": (simulated["simulated_40_region_rate_loss"], targets["observed_40_region_rate_loss"]),
        "40_region_directional_wr": (simulated["simulated_40_region_directional_wr"], targets["observed_40_region_directional_wr"]),
        "no_40_region_directional_wr": (simulated["simulated_no_40_region_directional_wr"], targets["observed_no_40_region_directional_wr"]),
        "rung_wr_40": (simulated["simulated_rung_wr_40"], targets["observed_rung_wr_40"]),
        "rung_wr_30": (simulated["simulated_rung_wr_30"], targets["observed_rung_wr_30"]),
        "rung_wr_20": (simulated["simulated_rung_wr_20"], targets["observed_rung_wr_20"]),
        "rung_wr_10": (simulated["simulated_rung_wr_10"], targets["observed_rung_wr_10"]),
    }
    return {
        **targets,
        **simulated,
        **{f"error_{name}": simulated_value - observed_value for name, (simulated_value, observed_value) in pairs.items()},
    }
