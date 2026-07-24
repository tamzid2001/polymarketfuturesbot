"""Leakage-safe historical replay of the Prophet normal/inverse side selector.

The input is the stored ``prophet_ml_backtest_rows.csv`` artifact produced by
``kalshi_btc15m_backtest.py``.  This script never fits Prophet, fetches prices,
or creates an order.  At each historical forecast timestamp it may use only
earlier Prophet outcomes whose *settlement timestamp* is no later than that
forecast timestamp.  That is the same outcome-availability boundary used by
the live selector.

It reports fixed normal/inverse accuracy, every trailing selector window
(3/5/7/10/25/50), and the six-window majority vote.  The window is chosen on
the first chronological 80% and evaluated on the final 20%; the vote-versus-
selected-window holdout comparison uses an exact paired sign/McNemar test.
All results are directional only: the artifact contains no executable Kalshi
quotes, fills, queue position, spread, or fee data, so dollar P&L is not
estimated.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


WINDOWS = (3, 5, 7, 10, 25, 50)
NORMAL = "normal"
INVERSE = "inverse"
VOTE = "vote"
REQUIRED_COLUMNS = {"ticker", "forecast_at", "settlement_ts", "actual_outcome", "prophet_side"}


def parse_timestamp(value: str) -> datetime:
    """Parse a required ISO-8601 timestamp into a timezone-aware UTC value."""
    if not value:
        raise ValueError("empty timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp is not timezone aware: {value}")
    return parsed.astimezone(timezone.utc)


def opposite(side: str) -> str:
    normalized = side.strip().lower()
    if normalized == "yes":
        return "no"
    if normalized == "no":
        return "yes"
    raise ValueError(f"unsupported binary side: {side!r}")


def strategy_names() -> list[str]:
    return [NORMAL, INVERSE, *[f"trailing_{window}" for window in WINDOWS], VOTE]


def read_rows(path: Path) -> list[dict[str, Any]]:
    """Read and chronologically sort valid historical Prophet signal rows."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"input CSV is missing required columns: {sorted(missing)}")
        rows: list[dict[str, Any]] = []
        for line_number, raw in enumerate(reader, start=2):
            source = str(raw.get("prophet_side") or "").lower()
            actual = str(raw.get("actual_outcome") or "").lower()
            if source not in ("yes", "no") or actual not in ("yes", "no"):
                continue
            try:
                forecast_at = parse_timestamp(str(raw.get("forecast_at") or ""))
                settlement_at = parse_timestamp(str(raw.get("settlement_ts") or ""))
            except ValueError as exc:
                raise ValueError(f"line {line_number}: {exc}") from exc
            if settlement_at < forecast_at:
                raise ValueError(
                    f"line {line_number}: settlement precedes forecast for {raw.get('ticker')}")
            rows.append({
                "ticker": str(raw.get("ticker") or ""),
                "forecast_at": forecast_at,
                "settlement_at": settlement_at,
                "source_side": source,
                "actual_outcome": actual,
            })
    rows.sort(key=lambda row: (row["forecast_at"], row["ticker"]))
    if not rows:
        raise ValueError("input has no valid binary Prophet rows")
    return rows


def leader_for_window(history: list[bool], window: int) -> tuple[str, int, int]:
    """Return normal/inverse leader from only the supplied settled history.

    ``True`` means the original Prophet side was directionally correct.  Ties
    and no usable history deliberately choose inverse, matching the deployed
    selector's deterministic conservative tie rule.
    """
    sample = history[-window:]
    normal_wins = sum(sample)
    inverse_wins = len(sample) - normal_wins
    return (NORMAL if normal_wins > inverse_wins else INVERSE, normal_wins, inverse_wins)


def replay(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, list[bool]]]:
    """Produce chronological, pre-settlement-only decisions for every strategy."""
    available_normal_correct: list[bool] = []
    pending_outcomes: list[tuple[datetime, int, bool]] = []
    decisions: list[dict[str, Any]] = []
    correctness = {name: [] for name in strategy_names()}

    index = 0
    while index < len(rows):
        forecast_at = rows[index]["forecast_at"]
        while pending_outcomes and pending_outcomes[0][0] <= forecast_at:
            _, _, normal_correct = heapq.heappop(pending_outcomes)
            available_normal_correct.append(normal_correct)

        # Decide every signal stamped at this exact instant from the same
        # pre-existing history.  A sibling's outcome must never become visible
        # merely because it is processed first.
        group_end = index + 1
        while group_end < len(rows) and rows[group_end]["forecast_at"] == forecast_at:
            group_end += 1
        group = rows[index:group_end]
        group_outcomes: list[tuple[datetime, int, bool]] = []

        for sequence, row in enumerate(group, start=index + 1):
            source_side = row["source_side"]
            actual = row["actual_outcome"]
            inverse_side = opposite(source_side)
            normal_correct = source_side == actual
            selected_modes: dict[str, str] = {
                NORMAL: NORMAL,
                INVERSE: INVERSE,
            }
            window_details: dict[int, tuple[str, int, int]] = {}
            normal_votes = 0
            for window in WINDOWS:
                mode, normal_wins, inverse_wins = leader_for_window(available_normal_correct, window)
                window_details[window] = (mode, normal_wins, inverse_wins)
                selected_modes[f"trailing_{window}"] = mode
                normal_votes += mode == NORMAL
            selected_modes[VOTE] = NORMAL if normal_votes > len(WINDOWS) - normal_votes else INVERSE

            decision = {
                "sequence": sequence,
                "ticker": row["ticker"],
                "forecast_at": row["forecast_at"].isoformat().replace("+00:00", "Z"),
                "settlement_at": row["settlement_at"].isoformat().replace("+00:00", "Z"),
                "actual_outcome": actual,
                "prophet_side": source_side,
                "inverse_side": inverse_side,
                "settled_history_available": len(available_normal_correct),
                "normal_votes": normal_votes,
                "inverse_votes": len(WINDOWS) - normal_votes,
            }
            for window, (mode, normal_wins, inverse_wins) in window_details.items():
                decision[f"trailing_{window}_mode"] = mode
                decision[f"trailing_{window}_normal_wins"] = normal_wins
                decision[f"trailing_{window}_inverse_wins"] = inverse_wins
            for name, mode in selected_modes.items():
                selected_side = source_side if mode == NORMAL else inverse_side
                is_correct = selected_side == actual
                decision[f"{name}_mode"] = mode
                decision[f"{name}_side"] = selected_side
                decision[f"{name}_correct"] = int(is_correct)
                correctness[name].append(is_correct)
            decisions.append(decision)
            # The current outcome cannot affect this or another forecast until
            # its exchange settlement timestamp becomes available.
            group_outcomes.append((row["settlement_at"], sequence, normal_correct))

        for outcome in group_outcomes:
            heapq.heappush(pending_outcomes, outcome)
        index = group_end
    return decisions, correctness


def wilson_interval(wins: int, total: int) -> list[float] | None:
    if total <= 0:
        return None
    z = 1.959963984540054
    phat = wins / total
    denominator = 1.0 + z * z / total
    centre = (phat + z * z / (2.0 * total)) / denominator
    margin = z * math.sqrt((phat * (1.0 - phat) + z * z / (4.0 * total)) / total) / denominator
    return [round(max(0.0, centre - margin), 8), round(min(1.0, centre + margin), 8)]


def metrics(values: Iterable[bool]) -> dict[str, Any]:
    observations = list(values)
    total = len(observations)
    wins = sum(observations)
    return {
        "signals": total,
        "wins": wins,
        "losses": total - wins,
        "win_rate": round(wins / total, 8) if total else None,
        "wilson_95pct": wilson_interval(wins, total),
    }


def exact_paired_sign_test(left: list[bool], right: list[bool]) -> dict[str, Any]:
    """Exact two-sided test on paired directional correctness differences."""
    if len(left) != len(right):
        raise ValueError("paired strategies must have equal-length results")
    left_only = sum(a and not b for a, b in zip(left, right))
    right_only = sum(b and not a for a, b in zip(left, right))
    discordant = left_only + right_only
    if discordant == 0:
        p_value = 1.0
    else:
        tail = sum(
            math.exp(
                math.lgamma(discordant + 1) - math.lgamma(k + 1)
                - math.lgamma(discordant - k + 1) - discordant * math.log(2.0)
            )
            for k in range(min(left_only, right_only) + 1)
        )
        p_value = min(1.0, 2.0 * tail)
    return {
        "left_only_wins": left_only,
        "right_only_wins": right_only,
        "discordant_markets": discordant,
        "two_sided_p_value": round(p_value, 8),
    }


def best_window(names: list[str], values: dict[str, list[bool]], start: int, end: int) -> str:
    """Select the strongest single window on the selection segment only.

    Exact ties prefer the larger window to reduce unnecessary switching.
    """
    return max(
        names,
        key=lambda name: (metrics(values[name][start:end])["win_rate"] or 0.0,
                          int(name.removeprefix("trailing_"))),
    )


def best_fixed_side(values: dict[str, list[bool]], start: int, end: int) -> str:
    """Select normal or inverse on a prior segment; an exact tie chooses inverse."""
    normal_rate = metrics(values[NORMAL][start:end])["win_rate"] or 0.0
    inverse_rate = metrics(values[INVERSE][start:end])["win_rate"] or 0.0
    return NORMAL if normal_rate > inverse_rate else INVERSE


def paired_strategy_comparison(
    left_name: str,
    right_name: str,
    values: dict[str, list[bool]],
    start: int,
) -> dict[str, Any]:
    """Compare two named strategies on the same final chronological segment."""
    left_values = values[left_name][start:]
    right_values = values[right_name][start:]
    return {
        "left_strategy": left_name,
        "right_strategy": right_name,
        "left_metrics": metrics(left_values),
        "right_metrics": metrics(right_values),
        "exact_paired_sign_test": exact_paired_sign_test(left_values, right_values),
    }


def recommendation(
    selected_window: str,
    selected_fixed_side: str,
    values: dict[str, list[bool]],
    holdout_start: int,
) -> dict[str, Any]:
    """Make a transparent historical paper-policy recommendation, never a live claim."""
    vote_vs_window = paired_strategy_comparison(VOTE, selected_window, values, holdout_start)
    vote_vs_fixed_side = paired_strategy_comparison(VOTE, selected_fixed_side, values, holdout_start)
    vote_rate = vote_vs_window["left_metrics"]["win_rate"] or 0.0
    window_rate = vote_vs_window["right_metrics"]["win_rate"] or 0.0
    fixed_side_rate = vote_vs_fixed_side["right_metrics"]["win_rate"] or 0.0
    window_p_value = vote_vs_window["exact_paired_sign_test"]["two_sided_p_value"]
    fixed_side_p_value = vote_vs_fixed_side["exact_paired_sign_test"]["two_sided_p_value"]
    if (vote_rate > window_rate and vote_rate > fixed_side_rate
            and window_p_value < 0.05 and fixed_side_p_value < 0.05):
        status = "voting_supported_for_paper_follow_up"
        rationale = (
            "Vote beat both pre-selected comparators on the final chronological holdout "
            "with paired-test support.")
    else:
        status = "do_not_adopt_voting_from_this_backtest"
        rationale = (
            "Vote did not deliver a statistically supported holdout improvement over both "
            "the pre-selected trailing window and the pre-selected fixed normal/inverse side.")
    return {
        "status": status,
        "rationale": rationale,
        "selected_window_from_first_80pct": selected_window,
        "selected_fixed_side_from_first_80pct": selected_fixed_side,
        "vote_vs_selected_window_final_holdout": vote_vs_window,
        "vote_vs_selected_fixed_side_final_holdout": vote_vs_fixed_side,
        "directional_only_limit": (
            "No historical quote, fill, spread, queue, slippage, or fee data is available; "
            "this must not be used as a live P&L approval."
        ),
    }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_decisions(path: Path, decisions: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({field for row in decisions for field in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(decisions)


def markdown_report(report: dict[str, Any]) -> str:
    def row(name: str, values: dict[str, Any]) -> str:
        return (f"| {name} | {values['wins']}/{values['signals']} | "
                f"{100 * (values['win_rate'] or 0.0):.2f}% |")

    full = report["full_history"]
    holdout = report["holdout_final_20pct"]
    lines = [
        "# Prophet Normal/Inverse Selector Historical Backtest",
        "",
        "## Method",
        "",
        "Each decision uses only original Prophet outcomes whose recorded settlement timestamp was no later than that row's forecast timestamp. Ties/no history select inverse. The six-window vote selects normal only with a strict majority; a 3–3 tie selects inverse.",
        "",
        "## Full chronological replay",
        "",
        "| Strategy | Wins | Win rate |",
        "|---|---:|---:|",
        *[row(name, full[name]) for name in strategy_names()],
        "",
        "## Selection and final holdout",
        "",
        f"The best single trailing window was selected using the first {report['selection_fraction']:.0%} only: **{report['best_single_window_selected_on_first_80pct']}**.",
        f"The best fixed normal/inverse side over that same selection segment was **{report['best_fixed_side_selected_on_first_80pct']}**.",
        "",
        "| Strategy | Holdout wins | Holdout win rate |",
        "|---|---:|---:|",
        *[row(name, holdout[name]) for name in strategy_names()],
        "",
        "## Vote recommendation",
        "",
        f"- Status: **{report['recommendation']['status']}**",
        f"- {report['recommendation']['rationale']}",
        "- Paired two-sided p-value, vote vs selected trailing window: "
        f"{report['recommendation']['vote_vs_selected_window_final_holdout']['exact_paired_sign_test']['two_sided_p_value']:.8f}",
        "- Paired two-sided p-value, vote vs selected fixed side: "
        f"{report['recommendation']['vote_vs_selected_fixed_side_final_holdout']['exact_paired_sign_test']['two_sided_p_value']:.8f}",
        f"- Limit: {report['recommendation']['directional_only_limit']}",
        "",
    ]
    return "\n".join(lines)


def run(input_csv: Path, output_dir: Path, source_run_id: str, selection_fraction: float) -> dict[str, Any]:
    rows = read_rows(input_csv)
    decisions, values = replay(rows)
    total = len(rows)
    selection_end = int(total * selection_fraction)
    if selection_end < 100 or total - selection_end < 100:
        raise ValueError("input needs at least 100 signals in both selection and holdout segments")
    trailing_names = [f"trailing_{window}" for window in WINDOWS]
    selected_window = best_window(trailing_names, values, 0, selection_end)
    full_best_window = best_window(trailing_names, values, 0, total)
    selected_fixed_side = best_fixed_side(values, 0, selection_end)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "source": {
            "stored_backtest_run_id": str(source_run_id),
            "input_csv": str(input_csv),
            "input_sha256": sha256(input_csv),
            "rows": total,
            "forecast_range": {
                "first": decisions[0]["forecast_at"],
                "last": decisions[-1]["forecast_at"],
            },
        },
        "method": {
            "normal": "original stored Prophet side",
            "inverse": "opposite of the original stored Prophet side",
            "windows": list(WINDOWS),
            "availability_guard": "Only outcomes with settlement_at <= current forecast_at are visible.",
            "single_window_tie_policy": "inverse",
            "vote_policy": "normal only when more than 3 of 6 windows lead normal; tie selects inverse",
            "pnl": "not calculated; source artifact has no executable quote/fill/fee data",
        },
        "selection_fraction": selection_fraction,
        "selection_first_80pct": {name: metrics(values[name][:selection_end]) for name in strategy_names()},
        "holdout_final_20pct": {name: metrics(values[name][selection_end:]) for name in strategy_names()},
        "full_history": {name: metrics(values[name]) for name in strategy_names()},
        "best_single_window_selected_on_first_80pct": selected_window,
        "best_single_window_full_history": full_best_window,
        "best_fixed_side_selected_on_first_80pct": selected_fixed_side,
        "recommendation": recommendation(selected_window, selected_fixed_side, values, selection_end),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    write_decisions(output_dir / "prophet_selector_decisions.csv", decisions)
    write_json(output_dir / "prophet_selector_backtest_report.json", report)
    (output_dir / "prophet_selector_backtest_report.md").write_text(
        markdown_report(report), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("prophet_selector_backtest_output"))
    parser.add_argument("--source-run-id", default="")
    parser.add_argument("--selection-fraction", type=float, default=0.80)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 0.05 <= args.selection_fraction <= 0.95:
        raise SystemExit("selection-fraction must be between 0.05 and 0.95")
    report = run(args.input_csv, args.output_dir, args.source_run_id, args.selection_fraction)
    vote = report["holdout_final_20pct"][VOTE]
    selected = report["holdout_final_20pct"][report["best_single_window_selected_on_first_80pct"]]
    print(
        "PROPHET SELECTOR BACKTEST | "
        f"rows={report['source']['rows']} best_window={report['best_single_window_selected_on_first_80pct']} "
        f"holdout_vote={vote['wins']}/{vote['signals']} ({100 * (vote['win_rate'] or 0.0):.2f}%) "
        f"holdout_best_single={selected['wins']}/{selected['signals']} "
        f"({100 * (selected['win_rate'] or 0.0):.2f}%) "
        f"recommendation={report['recommendation']['status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
