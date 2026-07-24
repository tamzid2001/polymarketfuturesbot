"""MLB ML-side selection used only by the explicitly enabled dry/live runner mode.

The module is intentionally lazy-imported by the runner: the normal mechanical
monitor has no ML dependency and no behavioural change.  A prediction requires
a versioned artifact created by ``polymarket_mlb_ml_backtest.py``; there is no
fallback to a price-cheapest side, Prophet, or another forecast.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _predict(artifact: dict[str, Any], feature_values: dict[str, float | None]) -> float:
    raw = float(artifact["estimator"].predict_proba([[feature_values.get(column) for column in artifact["features"]]])[:, 1][0])
    method = artifact.get("calibration")
    calibrator = artifact.get("calibrator")
    if method == "isotonic" and calibrator is not None:
        return float(calibrator.predict([raw])[0])
    if method == "platt" and calibrator is not None:
        return float(calibrator.predict_proba([[raw]])[:, 1][0])
    return raw


def choose_ml_side(
    *, model_path: Path, game_start: datetime, outcomes: dict[str, dict[str, Any]], asks: dict[str, float | None],
    min_confidence: float, root: Path,
) -> dict[str, Any]:
    """Return a frozen long/short outcome, or a concrete reason not to trade.

    The caller stores the selected outcome before any order.  This guarantees
    the subsequent 10-cent averaging ladder can never flip to the other team.
    """
    try:
        import joblib
        from polymarket_mlb_ml_backtest import MlbStats, Paths, final_mlb_game, rolling_team_features
    except ImportError as exc:
        return {"error": f"ml_dependencies_unavailable:{exc}"}
    if not model_path.exists():
        return {"error": f"ml_model_artifact_missing:{model_path}"}
    try:
        artifact = joblib.load(model_path)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"ml_model_artifact_unreadable:{exc}"}
    if artifact.get("schema") != "polymarket_mlb_market_and_prior_team_features_v1":
        return {"error": "ml_model_schema_rejected"}
    if set(outcomes) != {"long", "short"}:
        return {"error": "ml_outcomes_malformed"}
    home_outcome = next((outcome for outcome, side in outcomes.items() if side.get("role") == "home"), None)
    away_outcome = next((outcome for outcome, side in outcomes.items() if side.get("role") == "away"), None)
    home_ask, away_ask = asks.get(home_outcome), asks.get(away_outcome)
    if not home_outcome or not away_outcome or home_ask is None or away_ask is None or home_ask + away_ask <= 0:
        return {"error": "ml_requires_both_executable_team_prices"}
    try:
        # Only completed games before the new game's scheduled start feed the
        # rolling state.  The current game is not final and cannot enter it.
        season_start = datetime(game_start.year, 3, 1, tzinfo=UTC)
        schedule = MlbStats(Paths(root)).schedule(season_start, game_start)
        finals = [parsed for item in schedule if (parsed := final_mlb_game(item))]
        probe_id = "__live_feature_probe__"
        probe = {
            "game_pk": probe_id, "scheduled_start": game_start.isoformat(),
            "home_team": next(side["team"] for side in outcomes.values() if side["role"] == "home"),
            "away_team": next(side["team"] for side in outcomes.values() if side["role"] == "away"),
            # The probe is used only for pre-game state. These placeholder
            # values are applied after its feature row is emitted and then
            # discarded, so they cannot enter its own prediction.
            "home_score": 0, "away_score": 0, "home_won": 0,
        }
        team_features = rolling_team_features(finals + [probe]).get(probe_id, {})
    except Exception as exc:  # noqa: BLE001
        finals, team_features = [], {}
        feature_error = f"live_team_feature_refresh_failed:{exc}"
    else:
        feature_error = "live_market_momentum_not_collected_yet"
    home_probability = home_ask / (home_ask + away_ask)
    features: dict[str, float | None] = {
        "market_implied_home": home_probability,
        "market_distance_from_half": abs(home_probability - .5),
        "favorite_strength": abs(2 * home_probability - 1),
        "price_momentum_1h": None, "price_momentum_6h": None, "price_momentum_24h": None,
        "price_volatility_24h": None, "snapshot_age_hours": 0.0, "candle_volume": None, "candle_notional": None,
    }
    features.update(team_features)
    # No fabricated team row is used.  Missing team inputs are deliberately
    # passed to the model's training-fitted imputer, and the reason is logged.
    for name in artifact.get("features", []):
        features.setdefault(name, None)
    try:
        p_home = _predict(artifact, features)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"ml_inference_failed:{exc}"}
    confidence = max(p_home, 1 - p_home)
    if not 0 <= p_home <= 1:
        return {"error": "ml_probability_out_of_range"}
    if confidence < min_confidence:
        return {"error": "ml_confidence_below_gate", "p_home": p_home, "confidence": confidence}
    selected = home_outcome if p_home >= .5 else away_outcome
    return {
        "outcome": selected, "p_home": p_home, "confidence": confidence,
        "model": artifact.get("model"), "horizon_hours": artifact.get("horizon_hours"),
        "feature_quality": feature_error,
    }
