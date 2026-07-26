"""Offline checks for the leakage-safe Prophet selector historical replay."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import kalshi_prophet_selector_backtest as selector


UTC = timezone.utc


def row(
    sequence: int,
    forecast_minutes: int,
    settlement_minutes: int,
    normal_correct: bool,
) -> dict:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return {
        "ticker": f"KXBTC15M-TEST-{sequence}",
        "forecast_at": start + timedelta(minutes=forecast_minutes),
        "settlement_at": start + timedelta(minutes=settlement_minutes),
        "source_side": "yes",
        "actual_outcome": "yes" if normal_correct else "no",
    }


def test_replay_excludes_unsettled_and_same_timestamp_outcomes() -> None:
    # The first two outcomes settle after their forecasts.  At minute 10 only
    # the first one is known, so the third decision can use exactly one result.
    rows = [
        row(1, forecast_minutes=0, settlement_minutes=10, normal_correct=True),
        row(2, forecast_minutes=5, settlement_minutes=20, normal_correct=True),
        row(3, forecast_minutes=10, settlement_minutes=30, normal_correct=True),
        # This is a separate signal at exactly the same forecast instant.  It
        # must see the same one settled result, not row 3's later outcome.
        row(4, forecast_minutes=10, settlement_minutes=30, normal_correct=False),
    ]
    decisions, _ = selector.replay(rows)

    assert [item["settled_history_available"] for item in decisions] == [0, 0, 1, 1]
    assert decisions[0]["trailing_3_mode"] == selector.INVERSE
    assert decisions[1]["trailing_3_mode"] == selector.INVERSE
    assert decisions[2]["trailing_3_mode"] == selector.NORMAL
    assert decisions[3]["trailing_3_mode"] == selector.NORMAL


def test_window_ties_and_six_window_vote_choose_inverse() -> None:
    # 3/5/7 lead normal; 10/25/50 tie and therefore lead inverse.  The overall
    # 3-3 vote must obey the same inverse tie policy as the deployed selector.
    history = [False, False, True, False, False, False, True, True, True, True]
    expected_modes = {
        3: selector.NORMAL,
        5: selector.NORMAL,
        7: selector.NORMAL,
        10: selector.INVERSE,
        25: selector.INVERSE,
        50: selector.INVERSE,
    }
    assert {window: selector.leader_for_window(history, window)[0] for window in selector.WINDOWS} == expected_modes

    rows = [
        row(index + 1, forecast_minutes=index, settlement_minutes=index, normal_correct=value)
        for index, value in enumerate(history)
    ]
    rows.append(row(99, forecast_minutes=20, settlement_minutes=30, normal_correct=True))
    decisions, _ = selector.replay(rows)
    final = decisions[-1]
    assert final["normal_votes"] == 3
    assert final["inverse_votes"] == 3
    assert final["vote_mode"] == selector.INVERSE


def test_best_fixed_side_tie_uses_inverse() -> None:
    values = {
        selector.NORMAL: [True, False],
        selector.INVERSE: [False, True],
    }
    assert selector.best_fixed_side(values, 0, 2) == selector.INVERSE


if __name__ == "__main__":
    test_replay_excludes_unsettled_and_same_timestamp_outcomes()
    test_window_ties_and_six_window_vote_choose_inverse()
    test_best_fixed_side_tie_uses_inverse()
    print("PASS: Prophet selector historical backtest tests")
