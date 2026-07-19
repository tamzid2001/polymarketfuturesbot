"""Leakage-safe performance and edge analysis for a trading ledger.

The script accepts either a normal CSV/JSON ledger or the
``prophet_ml_backtest_rows.csv`` artifact produced by this repository.  It
never places orders.  Every model evaluation is chronological: features at a
trade only use earlier trades, and out-of-sample model predictions are made on
later time blocks.

Examples
--------
Analyze both signals in a downloaded Kalshi backtest artifact::

    python kalshi_ledger_analysis.py \
      --input ~/Desktop/kalshi-btc15m-backtest-29698207476 --signal all

Analyze a real ledger with monetary P&L::

    python kalshi_ledger_analysis.py --input my_trades.csv --signal ledger

The artifact has directional outcomes but no executable fills or realized
profit/loss.  The script intentionally refuses to manufacture dollar returns
from that data.  Monetary performance, Kelly sizing, and dollar Monte Carlo
are reported only when a real ``profit_loss`` column is present.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from prophet import Prophet
from scipy.stats import binomtest, ttest_1samp
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

XGBOOST_IMPORT_ERROR: Optional[str] = None
try:
    from xgboost import XGBClassifier
except Exception as error:  # Optional native dependency; retain the other two model tests if unavailable.
    XGBClassifier = None
    XGBOOST_IMPORT_ERROR = str(error).splitlines()[0]


LOG = logging.getLogger("kalshi_ledger_analysis")
EASTERN = ZoneInfo("America/New_York")
WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
WINDOWS = (50, 100, 500)
STREAK_LEVELS = (3, 5, 7, 10)
STARTING_BALANCE = 1000.0


@dataclass(frozen=True)
class InputInfo:
    source_path: Path
    source_kind: str
    pnl_available: bool
    signal: str


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    for name in ("cmdstanpy", "prophet", "xgboost"):
        logging.getLogger(name).setLevel(logging.WARNING)


def json_default(value: Any) -> Any:
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=json_default) + "\n",
                    encoding="utf-8")


def write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def canonical_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def column_lookup(frame: pd.DataFrame) -> dict[str, str]:
    return {canonical_name(column): column for column in frame.columns}


def find_column(frame: pd.DataFrame, *aliases: str) -> Optional[str]:
    lookup = column_lookup(frame)
    for alias in aliases:
        if canonical_name(alias) in lookup:
            return lookup[canonical_name(alias)]
    return None


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[^0-9eE+\-.]", "", regex=True),
                         errors="coerce")


def read_input(path: Path) -> tuple[pd.DataFrame, Path]:
    if path.is_dir():
        artifact = path / "prophet_ml_backtest_rows.csv"
        if artifact.exists():
            path = artifact
        else:
            candidates = sorted(path.glob("*.csv")) + sorted(path.glob("*.json"))
            if len(candidates) != 1:
                raise ValueError(f"{path} must contain prophet_ml_backtest_rows.csv or exactly one CSV/JSON file")
            path = candidates[0]
    if path.suffix.lower() == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("JSON ledger must be an array of trade records")
        return pd.DataFrame(raw), path
    if path.suffix.lower() in (".csv", ".tsv"):
        return pd.read_csv(path), path
    raise ValueError(f"Unsupported ledger format: {path.suffix}")


def market_from_ticker(ticker: Any) -> str:
    value = str(ticker or "")
    match = re.match(r"KX([A-Z]+?)(?:15M|\d|[-_])", value)
    return match.group(1) if match else (value.split("-")[0] or "UNKNOWN")


def normalize_artifact(frame: pd.DataFrame, source_path: Path, signal: str) -> tuple[pd.DataFrame, InputInfo]:
    if signal not in ("prophet", "ml"):
        raise ValueError("Backtest artifacts require --signal prophet, ml, or all")
    side_column = f"{signal}_side"
    correct_column = f"{signal}_correct"
    required = {"market_open", "actual_outcome", side_column, correct_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Backtest artifact is missing columns: {', '.join(missing)}")
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["market_open"], utc=True, errors="coerce")
    data["side"] = data[side_column].astype(str).str.upper()
    data["result"] = np.where(numeric(data[correct_column]).eq(1), "WIN", "LOSS")
    ticker = data["ticker"] if "ticker" in data else pd.Series("UNKNOWN", index=data.index)
    data["market"] = ticker.map(market_from_ticker)
    data["entry_price"] = np.nan
    data["exit_price"] = np.nan
    data["profit_loss"] = np.nan
    data["underlying_move"] = numeric(data.get("expiration_value", pd.Series(index=data.index))) - numeric(
        data.get("strike", pd.Series(index=data.index)))
    strike = numeric(data.get("strike", pd.Series(index=data.index)))
    data["underlying_move_pct"] = np.where(strike.abs() > 0, data["underlying_move"] / strike, np.nan)
    # The historical backtest has pre-entry realized-volatility features even
    # when Kalshi did not provide an expiration value for the closed market.
    data["feature_volatility"] = numeric(data.get("vol_15m_bps", pd.Series(index=data.index)))
    data["source_signal"] = signal
    data = data[data["timestamp"].notna() & data["side"].isin(["YES", "NO"])].copy()
    return finalize_ledger(data), InputInfo(source_path, "kalshi_backtest_artifact", False, signal)


def normalize_ledger(frame: pd.DataFrame, source_path: Path) -> tuple[pd.DataFrame, InputInfo]:
    timestamp_column = find_column(frame, "date/time", "datetime", "timestamp", "date", "time", "market_open")
    result_column = find_column(frame, "result", "outcome", "status")
    pnl_column = find_column(frame, "profit_loss", "pnl", "profitloss", "realized_pnl")
    side_column = find_column(frame, "side", "direction", "position")
    market_column = find_column(frame, "market", "asset", "ticker", "symbol", "trade_kind")
    entry_column = find_column(frame, "entry_price", "entry", "buy_price", "avg_entry_price")
    exit_column = find_column(frame, "exit_price", "exit", "sell_price", "settlement_price")
    underlying_column = find_column(frame, "underlying_move", "btc_move", "asset_move", "price_change")
    if timestamp_column is None:
        raise ValueError("Ledger needs a date/time or timestamp column")
    data = pd.DataFrame(index=frame.index)
    data["timestamp"] = pd.to_datetime(frame[timestamp_column], utc=True, errors="coerce")
    data["market"] = (frame[market_column].astype(str) if market_column else "UNKNOWN")
    data["side"] = (frame[side_column].astype(str).str.upper() if side_column else "")
    data["entry_price"] = numeric(frame[entry_column]) if entry_column else np.nan
    data["exit_price"] = numeric(frame[exit_column]) if exit_column else np.nan
    data["profit_loss"] = numeric(frame[pnl_column]) if pnl_column else np.nan
    if result_column:
        result = frame[result_column].astype(str).str.upper().str.strip()
    else:
        result = pd.Series("", index=frame.index)
    result = result.where(result.isin(["WIN", "LOSS"]), "")
    result = result.mask(result.eq("") & data["profit_loss"].gt(0), "WIN")
    result = result.mask(result.eq("") & data["profit_loss"].lt(0), "LOSS")
    data["result"] = result
    data["underlying_move"] = numeric(frame[underlying_column]) if underlying_column else np.nan
    data["underlying_move_pct"] = np.nan
    data["feature_volatility"] = np.nan
    data["source_signal"] = "ledger"
    data = data[data["timestamp"].notna() & data["result"].isin(["WIN", "LOSS"])].copy()
    pnl_available = bool(data["profit_loss"].notna().all())
    return finalize_ledger(data), InputInfo(source_path, "ledger", pnl_available, "ledger")


def finalize_ledger(data: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "timestamp", "market", "side", "entry_price", "exit_price", "profit_loss", "result",
        "underlying_move", "underlying_move_pct", "feature_volatility", "source_signal",
    ]
    for column in keep:
        if column not in data:
            data[column] = np.nan
    data = data[keep].sort_values("timestamp", kind="stable").reset_index(drop=True)
    data.insert(0, "trade_number", np.arange(1, len(data) + 1))
    data["win"] = data["result"].eq("WIN").astype(int)
    data["loss"] = 1 - data["win"]
    data["outcome_unit"] = np.where(data["win"].eq(1), 1.0, -1.0)
    return data


def streaks(wins: pd.Series) -> dict[str, Any]:
    if wins.empty:
        return {"current": "none", "current_length": 0, "longest_win": 0, "longest_loss": 0}
    current = bool(wins.iloc[-1])
    current_length = 0
    longest_win = longest_loss = running_win = running_loss = 0
    for value in wins.astype(bool):
        if value:
            running_win += 1
            running_loss = 0
            longest_win = max(longest_win, running_win)
        else:
            running_loss += 1
            running_win = 0
            longest_loss = max(longest_loss, running_loss)
    for value in reversed(wins.astype(bool).tolist()):
        if value != current:
            break
        current_length += 1
    return {
        "current": "WIN" if current else "LOSS",
        "current_length": current_length,
        "longest_win": longest_win,
        "longest_loss": longest_loss,
    }


def wilson_interval(successes: int, total: int) -> list[Optional[float]]:
    if total == 0:
        return [None, None]
    p = successes / total
    z = 1.96
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return [max(0.0, center - margin), min(1.0, center + margin)]


def add_time_series(data: pd.DataFrame, starting_balance: float) -> pd.DataFrame:
    frame = data.copy()
    frame["wl_counter"] = frame["outcome_unit"].cumsum()
    for window in WINDOWS:
        frame[f"rolling_{window}_balance"] = frame["outcome_unit"].rolling(window, min_periods=window).sum()
        frame[f"rolling_{window}_win_rate"] = frame["win"].rolling(window, min_periods=window).mean()
    if frame["profit_loss"].notna().all():
        frame["equity"] = starting_balance + frame["profit_loss"].cumsum()
    else:
        frame["equity"] = np.nan
    local = frame["timestamp"].dt.tz_convert(EASTERN)
    frame["hour_et"] = local.dt.hour
    frame["weekday_et"] = local.dt.day_name().str.slice(0, 3)
    frame["month_et"] = local.dt.strftime("%Y-%m")
    frame["rolling_50_regime"] = np.select(
        [frame["rolling_50_balance"] > 0, frame["rolling_50_balance"] < 0],
        ["hot", "cold"],
        default="neutral_or_unavailable",
    )
    return frame


def monetary_performance(frame: pd.DataFrame, starting_balance: float) -> dict[str, Any]:
    if not frame["profit_loss"].notna().all():
        return {
            "available": False,
            "reason": "Input has no realized profit_loss for every trade; dollar performance is intentionally not inferred from outcomes.",
        }
    pnl = frame["profit_loss"].astype(float)
    positive = pnl[pnl > 0]
    negative = pnl[pnl < 0]
    equity = starting_balance + pnl.cumsum()
    peak = pd.concat([pd.Series([starting_balance]), equity], ignore_index=True).cummax().iloc[1:].to_numpy()
    drawdown = equity.to_numpy() - peak
    drawdown_pct = np.divide(drawdown, peak, out=np.zeros_like(drawdown), where=peak > 0)
    prior_equity = np.concatenate(([starting_balance], equity.to_numpy()[:-1]))
    trade_returns = np.divide(pnl.to_numpy(), prior_equity, out=np.zeros(len(pnl)), where=prior_equity > 0)
    duration_years = max((frame["timestamp"].iloc[-1] - frame["timestamp"].iloc[0]).total_seconds() / 31_557_600, 1 / 365)
    trades_per_year = len(frame) / duration_years
    std = float(np.std(trade_returns, ddof=1)) if len(trade_returns) > 1 else 0.0
    downside = trade_returns[trade_returns < 0]
    downside_std = float(np.std(downside, ddof=1)) if len(downside) > 1 else 0.0
    sharpe = float(np.mean(trade_returns) / std * math.sqrt(trades_per_year)) if std > 0 else None
    sortino = float(np.mean(trade_returns) / downside_std * math.sqrt(trades_per_year)) if downside_std > 0 else None
    average_win = float(positive.mean()) if not positive.empty else None
    average_loss = float(negative.mean()) if not negative.empty else None
    win_rate = float((pnl > 0).mean())
    loss_rate = float((pnl < 0).mean())
    payoff_ratio = average_win / abs(average_loss) if average_win is not None and average_loss not in (None, 0) else None
    expectancy = float(pnl.mean())
    profit_factor = float(positive.sum() / abs(negative.sum())) if not negative.empty and negative.sum() != 0 else None
    ttest = ttest_1samp(pnl.to_numpy(), 0.0, alternative="two-sided") if len(pnl) > 1 else None
    return {
        "available": True,
        "average_win": average_win,
        "average_loss": average_loss,
        "reward_risk_ratio": payoff_ratio,
        "expectancy_per_trade": expectancy,
        "profit_factor": profit_factor,
        "total_profit_loss": float(pnl.sum()),
        "ending_balance": float(equity.iloc[-1]),
        "total_return": float((equity.iloc[-1] - starting_balance) / starting_balance),
        "maximum_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "maximum_drawdown_pct": float(drawdown_pct.min()) if len(drawdown_pct) else 0.0,
        "sharpe_ratio_annualized_zero_rf": sharpe,
        "sortino_ratio_annualized_zero_rf": sortino,
        "trades_per_year_estimate": trades_per_year,
        "mean_pnl_ttest_pvalue": float(ttest.pvalue) if ttest else None,
        "win_rate_from_pnl": win_rate,
        "loss_rate_from_pnl": loss_rate,
    }


def performance_summary(frame: pd.DataFrame, starting_balance: float) -> dict[str, Any]:
    total = len(frame)
    wins = int(frame["win"].sum())
    losses = total - wins
    win_rate = wins / total if total else None
    binomial = binomtest(wins, total, p=0.5, alternative="two-sided") if total else None
    summary = {
        "total_trades": total,
        "total_wins": wins,
        "total_losses": losses,
        "win_rate": win_rate,
        "win_rate_95pct_ci": wilson_interval(wins, total),
        "win_rate_vs_50pct_pvalue": float(binomial.pvalue) if binomial else None,
        "streaks": streaks(frame["win"]),
        "monetary_performance": monetary_performance(frame, starting_balance),
    }
    return summary


def grouped_performance(frame: pd.DataFrame, group_column: str, starting_balance: float) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for group, subset in frame.groupby(group_column, dropna=False):
        item = performance_summary(subset, starting_balance)
        monetary = item["monetary_performance"]
        records.append({
            "group": str(group),
            "trades": item["total_trades"],
            "wins": item["total_wins"],
            "losses": item["total_losses"],
            "win_rate": item["win_rate"],
            "win_rate_vs_50pct_pvalue": item["win_rate_vs_50pct_pvalue"],
            "total_profit_loss": monetary.get("total_profit_loss"),
            "expectancy_per_trade": monetary.get("expectancy_per_trade"),
        })
    return pd.DataFrame(records).sort_values("trades", ascending=False, kind="stable")


def streak_conditionals(frame: pd.DataFrame) -> pd.DataFrame:
    previous_loss_streak: list[int] = []
    running = 0
    for won in frame["win"].astype(bool):
        previous_loss_streak.append(running)
        running = 0 if won else running + 1
    baseline = float(frame["win"].mean())
    records: list[dict[str, Any]] = []
    for level in STREAK_LEVELS:
        mask = pd.Series(previous_loss_streak, index=frame.index) >= level
        outcomes = frame.loc[mask, "win"].astype(int)
        successes = int(outcomes.sum())
        count = len(outcomes)
        test = binomtest(successes, count, p=baseline, alternative="two-sided") if count else None
        records.append({
            "after_at_least_losses": level,
            "opportunities": count,
            "next_trade_wins": successes,
            "next_trade_win_rate": successes / count if count else None,
            "baseline_win_rate": baseline,
            "difference_from_baseline": successes / count - baseline if count else None,
            "p_value_vs_baseline": float(test.pvalue) if test else None,
            "bonferroni_p_value": min(1.0, float(test.pvalue) * len(STREAK_LEVELS)) if test else None,
            "win_rate_95pct_ci": wilson_interval(successes, count),
        })
    return pd.DataFrame(records)


def prophet_model() -> Prophet:
    return Prophet(
        daily_seasonality=False,
        weekly_seasonality=False,
        yearly_seasonality=False,
        interval_width=0.80,
        uncertainty_samples=200,
    )


def prophet_series(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    index_columns = ["trade_number", "timestamp"]
    output = {
        "cumulative_wl_counter": frame[index_columns + ["wl_counter"]].rename(columns={"timestamp": "ds", "wl_counter": "y"}),
        "rolling_50_balance": frame[index_columns + ["rolling_50_balance"]].rename(columns={"timestamp": "ds", "rolling_50_balance": "y"}),
        "rolling_100_balance": frame[index_columns + ["rolling_100_balance"]].rename(columns={"timestamp": "ds", "rolling_100_balance": "y"}),
        "rolling_50_win_rate": frame[index_columns + ["rolling_50_win_rate"]].rename(columns={"timestamp": "ds", "rolling_50_win_rate": "y"}),
    }
    if frame["equity"].notna().all():
        output["equity_curve"] = frame[index_columns + ["equity"]].rename(columns={"timestamp": "ds", "equity": "y"})
    return {name: series.dropna().reset_index(drop=True) for name, series in output.items()}


def prophet_forecasts(series: dict[str, pd.DataFrame], horizon_trades: tuple[int, ...],
                      max_history: int) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for name, raw in series.items():
        train = raw.tail(max_history).copy() if max_history else raw.copy()
        if len(train) < 30:
            continue
        train["ds"] = pd.to_datetime(train["ds"], utc=True).dt.tz_localize(None)
        model = prophet_model()
        model.fit(train[["ds", "y"]])
        gaps = pd.to_datetime(raw["ds"], utc=True).sort_values().diff().dropna()
        median_gap = gaps.median() if not gaps.empty else pd.Timedelta(minutes=15)
        median_gap = max(median_gap, pd.Timedelta(seconds=1))
        for horizon in horizon_trades:
            future_ds = pd.date_range(
                start=train["ds"].iloc[-1] + median_gap,
                periods=horizon,
                freq=median_gap,
            )
            predicted = model.predict(pd.DataFrame({"ds": future_ds}))[["ds", "yhat", "yhat_lower", "yhat_upper"]]
            predicted.insert(0, "series", name)
            predicted.insert(1, "forecast_type", "future")
            predicted.insert(2, "horizon_trades", horizon)
            records.append(predicted)
    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


def prophet_policy_evaluation(series: dict[str, pd.DataFrame], frame: pd.DataFrame,
                              min_history: int, train_window: int,
                              origins: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Forecast five actual future trades from rolling historical cutoffs.

    A policy may start only using the forecast available at the cutoff.  The
    evaluation compares the final five real outcomes with p50/p10/p90 trend
    conditions; it never uses those outcomes during fitting.
    """
    decisions: list[dict[str, Any]] = []
    for name, raw in series.items():
        raw = raw.reset_index(drop=True)
        valid_origins = np.arange(min_history, len(raw) - 5)
        if not len(valid_origins):
            continue
        chosen = np.unique(np.linspace(valid_origins[0], valid_origins[-1], min(origins, len(valid_origins)), dtype=int))
        for origin in chosen:
            train = raw.iloc[max(0, origin - train_window):origin].copy()
            if len(train) < min_history:
                continue
            train["ds"] = pd.to_datetime(train["ds"], utc=True).dt.tz_localize(None)
            actual_ds = pd.to_datetime(raw.iloc[origin:origin + 5]["ds"], utc=True).dt.tz_localize(None)
            model = prophet_model()
            model.fit(train[["ds", "y"]])
            forecast = model.predict(pd.DataFrame({"ds": actual_ds})).iloc[-1]
            actual_trade_numbers = raw.iloc[origin:origin + 5]["trade_number"].to_numpy()
            actual_rows = frame.set_index("trade_number").loc[actual_trade_numbers]
            last_value = float(train["y"].iloc[-1])
            future_win_rate = float(actual_rows["win"].mean())
            item = {
                "series": name,
                "origin_trade": int(raw.iloc[origin - 1]["trade_number"]),
                "origin_timestamp": raw.iloc[origin - 1]["ds"],
                "last_observed_value": last_value,
                "forecast_p10": float(forecast["yhat_lower"]),
                "forecast_p50": float(forecast["yhat"]),
                "forecast_p90": float(forecast["yhat_upper"]),
                "actual_future_value": float(raw.iloc[origin + 4]["y"]),
                "next_5_win_rate": future_win_rate,
                "next_5_profit_loss": float(actual_rows["profit_loss"].sum()) if actual_rows["profit_loss"].notna().all() else np.nan,
            }
            item["start_on_p50_up"] = item["forecast_p50"] > last_value
            item["start_on_p10_up"] = item["forecast_p10"] > last_value
            item["pause_on_p50_down"] = item["forecast_p50"] < last_value
            item["pause_on_p90_down"] = item["forecast_p90"] < last_value
            decisions.append(item)
    decision_frame = pd.DataFrame(decisions)
    records: list[dict[str, Any]] = []
    for series_name, subset in decision_frame.groupby("series") if not decision_frame.empty else []:
        for policy in ("start_on_p50_up", "start_on_p10_up", "pause_on_p50_down", "pause_on_p90_down"):
            selected = subset[subset[policy]]
            records.append({
                "series": series_name,
                "policy": policy,
                "cutoffs": len(selected),
                "mean_next_5_win_rate": float(selected["next_5_win_rate"].mean()) if len(selected) else None,
                "mean_next_5_profit_loss": float(selected["next_5_profit_loss"].mean()) if selected["next_5_profit_loss"].notna().any() else None,
            })
    return decision_frame, pd.DataFrame(records)


def feature_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    data = frame.copy()
    wins = data["win"].astype(float)
    prior_wins = wins.shift(1)
    loss_streak: list[int] = []
    win_streak: list[int] = []
    losses = running_losses = running_wins = 0
    for value in wins.astype(int):
        loss_streak.append(running_losses)
        win_streak.append(running_wins)
        if value:
            running_wins += 1
            running_losses = 0
        else:
            running_losses += 1
            running_wins = 0
    data["current_losing_streak"] = loss_streak
    data["current_winning_streak"] = win_streak
    for window in (50, 100):
        data[f"rolling_{window}_win_rate_feature"] = prior_wins.rolling(window, min_periods=10).mean()
    for lag in range(1, 11):
        data[f"previous_result_{lag}"] = wins.shift(lag)
    if data["profit_loss"].notna().all():
        volatility_source = data["profit_loss"]
    elif data["feature_volatility"].notna().any():
        volatility_source = data["feature_volatility"]
    else:
        volatility_source = data["underlying_move"]
    data["volatility"] = volatility_source.shift(1).rolling(50, min_periods=10).std()
    local = data["timestamp"].dt.tz_convert(EASTERN)
    radians = 2.0 * math.pi * (local.dt.hour * 60 + local.dt.minute) / (24.0 * 60.0)
    data["time_sin"] = np.sin(radians)
    data["time_cos"] = np.cos(radians)
    data["weekday"] = local.dt.dayofweek.astype(str)
    numeric_columns = [
        "current_losing_streak", "current_winning_streak", "rolling_50_win_rate_feature",
        "rolling_100_win_rate_feature", "volatility", "time_sin", "time_cos",
        *[f"previous_result_{lag}" for lag in range(1, 11)],
    ]
    numeric_columns = [column for column in numeric_columns if data[column].notna().any()]
    categorical_columns = ["market", "weekday"]
    return data, numeric_columns, categorical_columns


def model_metrics(actual: np.ndarray, probabilities: np.ndarray, baseline_probabilities: np.ndarray,
                  model_name: str) -> dict[str, Any]:
    predicted = (probabilities >= 0.5).astype(int)
    baseline_predicted = (baseline_probabilities >= 0.5).astype(int)
    discordant_model = int(np.sum((predicted == actual) & (baseline_predicted != actual)))
    discordant_baseline = int(np.sum((predicted != actual) & (baseline_predicted == actual)))
    discordant = discordant_model + discordant_baseline
    mcnemar = binomtest(discordant_model, discordant, p=0.5) if discordant else None
    return {
        "model": model_name,
        "predictions": int(len(actual)),
        "accuracy": float(accuracy_score(actual, predicted)),
        "baseline_accuracy": float(accuracy_score(actual, baseline_predicted)),
        "accuracy_improvement": float(accuracy_score(actual, predicted) - accuracy_score(actual, baseline_predicted)),
        "brier_score": float(brier_score_loss(actual, probabilities)),
        "baseline_brier_score": float(brier_score_loss(actual, baseline_probabilities)),
        "brier_improvement": float(brier_score_loss(actual, baseline_probabilities) - brier_score_loss(actual, probabilities)),
        "log_loss": float(log_loss(actual, np.clip(probabilities, 1e-9, 1 - 1e-9))),
        "baseline_log_loss": float(log_loss(actual, np.clip(baseline_probabilities, 1e-9, 1 - 1e-9))),
        "roc_auc": float(roc_auc_score(actual, probabilities)) if len(np.unique(actual)) == 2 else None,
        "model_only_correct": discordant_model,
        "baseline_only_correct": discordant_baseline,
        "mcnemar_exact_pvalue": float(mcnemar.pvalue) if mcnemar else None,
    }


def evaluate_models(frame: pd.DataFrame, output_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    data, numeric_columns, categorical_columns = feature_frame(frame)
    features = data[numeric_columns + categorical_columns]
    target = data["win"].astype(int).to_numpy()
    if len(data) < 200 or len(np.unique(target)) < 2:
        return {"available": False, "reason": "Need at least 200 settled trades with both outcomes."}, pd.DataFrame()
    splits = 5
    test_size = max(25, len(data) // (splits + 1))
    first_train_end = len(data) - splits * test_size
    if first_train_end < 100:
        splits = 3
        test_size = max(25, len(data) // (splits + 1))
        first_train_end = len(data) - splits * test_size
    numeric_transformer = Pipeline([("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler())])
    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])
    preprocessor = ColumnTransformer([
        ("numeric", numeric_transformer, numeric_columns),
        ("categorical", categorical_transformer, categorical_columns),
    ])
    models: dict[str, Any] = {
        "logistic_regression": LogisticRegression(max_iter=2000, class_weight="balanced", random_state=0),
        "random_forest": RandomForestClassifier(
            n_estimators=400, min_samples_leaf=20, class_weight="balanced_subsample", n_jobs=-1, random_state=0),
    }
    if XGBClassifier is not None:
        models["xgboost"] = XGBClassifier(
            n_estimators=300, max_depth=3, learning_rate=0.03, subsample=0.8,
            colsample_bytree=0.8, eval_metric="logloss", n_jobs=-1, random_state=0,
        )
    reports: dict[str, Any] = {
        "available": True,
        "evaluation": "Five expanding chronological test blocks. Each prediction uses only earlier trades.",
        "feature_columns": numeric_columns + categorical_columns,
        "models": {},
    }
    all_predictions: list[pd.DataFrame] = []
    for name, estimator in models.items():
        probability = np.full(len(data), np.nan)
        baseline_probability = np.full(len(data), np.nan)
        for fold in range(splits):
            train_end = first_train_end + fold * test_size
            test_end = min(len(data), train_end + test_size)
            x_train, x_test = features.iloc[:train_end], features.iloc[train_end:test_end]
            y_train = target[:train_end]
            if len(np.unique(y_train)) < 2:
                continue
            pipeline = Pipeline([("preprocess", preprocessor), ("model", estimator)])
            pipeline.fit(x_train, y_train)
            probability[train_end:test_end] = pipeline.predict_proba(x_test)[:, 1]
            baseline_probability[train_end:test_end] = float(np.mean(y_train))
        mask = np.isfinite(probability)
        if not mask.any():
            reports["models"][name] = {"available": False, "reason": "No valid chronological test folds."}
            continue
        reports["models"][name] = model_metrics(target[mask], probability[mask], baseline_probability[mask], name)
        predictions = data.loc[mask, ["trade_number", "timestamp", "market", "side", "result"]].copy()
        predictions["model"] = name
        predictions["actual_win"] = target[mask]
        predictions["probability_win"] = probability[mask]
        predictions["predicted_win"] = (probability[mask] >= 0.5).astype(int)
        predictions["baseline_probability_win"] = baseline_probability[mask]
        all_predictions.append(predictions)
    if XGBClassifier is None:
        reports["models"]["xgboost"] = {
            "available": False,
            "reason": f"xgboost could not be loaded: {XGBOOST_IMPORT_ERROR or 'not installed'}",
        }
    prediction_frame = pd.concat(all_predictions, ignore_index=True) if all_predictions else pd.DataFrame()
    if not prediction_frame.empty:
        write_csv(output_dir / "ml_predictions.csv", prediction_frame)
    return reports, prediction_frame


def monte_carlo(frame: pd.DataFrame, simulations: int, future_trades: int,
                starting_balance: float, rng: np.random.Generator) -> dict[str, Any]:
    if not frame["profit_loss"].notna().all():
        return {"available": False, "reason": "Requires actual realized profit_loss; outcome-only backtests cannot support a dollar Monte Carlo simulation."}
    pnl = frame["profit_loss"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    if wins.empty or losses.empty:
        return {"available": False, "reason": "Requires at least one realized win and loss."}
    win_rate = float((pnl > 0).mean())
    avg_win = float(wins.mean())
    avg_loss = float(losses.mean())
    all_returns = np.empty(simulations)
    max_drawdowns = np.empty(simulations)
    max_loss_streaks = np.empty(simulations)
    ruins = np.zeros(simulations, dtype=bool)
    for index in range(simulations):
        outcomes = rng.random(future_trades) < win_rate
        sampled = np.where(outcomes, avg_win, avg_loss)
        equity = starting_balance + np.cumsum(sampled)
        peak = np.maximum.accumulate(np.concatenate(([starting_balance], equity)))[1:]
        max_drawdowns[index] = np.min(equity - peak)
        ruins[index] = np.any(equity <= 0)
        running = longest = 0
        for won in outcomes:
            if won:
                running = 0
            else:
                running += 1
                longest = max(longest, running)
        max_loss_streaks[index] = longest
        all_returns[index] = equity[-1] - starting_balance
    return {
        "available": True,
        "simulations": simulations,
        "future_trades_per_simulation": future_trades,
        "assumption": "Independent Bernoulli wins with observed win rate and constant observed average win/loss. This does not model regime shifts, fees, slippage, or serial dependence.",
        "expected_return": float(np.mean(all_returns)),
        "return_95pct_interval": [float(np.quantile(all_returns, 0.025)), float(np.quantile(all_returns, 0.975))],
        "worst_simulated_drawdown": float(np.min(max_drawdowns)),
        "drawdown_95pct_interval": [float(np.quantile(max_drawdowns, 0.025)), float(np.quantile(max_drawdowns, 0.975))],
        "probability_of_ruin": float(np.mean(ruins)),
        "expected_maximum_losing_streak": float(np.mean(max_loss_streaks)),
        "maximum_losing_streak_95pct_interval": [float(np.quantile(max_loss_streaks, 0.025)), float(np.quantile(max_loss_streaks, 0.975))],
    }


def conclusion(performance: dict[str, Any], streak_frame: pd.DataFrame,
               ml_report: dict[str, Any]) -> dict[str, str]:
    monetary = performance["monetary_performance"]
    directional_p = performance["win_rate_vs_50pct_pvalue"]
    significant_direction = directional_p is not None and directional_p < 0.05
    hot_cold = bool(not streak_frame.empty and (streak_frame["bonferroni_p_value"] < 0.05).any())
    model_significant = False
    if ml_report.get("available"):
        model_significant = any(
            report.get("accuracy_improvement", 0) > 0 and (report.get("mcnemar_exact_pvalue") or 1) < 0.05
            for report in ml_report.get("models", {}).values() if isinstance(report, dict)
        )
    if not monetary.get("available"):
        profitable = "Unknown: the input contains no realized profit_loss, entry prices, or exit prices. Directional wins alone cannot prove profitability."
        sizing = "No monetary sizing recommendation is valid without realized payout, cost, fees, and slippage. Keep sizing fixed or remain in research mode."
    else:
        profitable = "Profitable in sample" if monetary["total_profit_loss"] > 0 else "Not profitable in sample"
        payoff = monetary.get("reward_risk_ratio")
        win_rate = performance.get("win_rate") or 0.0
        kelly = max(0.0, win_rate - (1.0 - win_rate) / payoff) if payoff and payoff > 0 else 0.0
        sizing = (f"Estimated full Kelly fraction is {kelly:.2%} using in-sample average payoff. "
                  "Use no more than quarter-Kelly with a hard drawdown cap only after out-of-sample validation.")
    return {
        "profitability": profitable,
        "statistical_edge": ("Directional win rate differs from 50% at the 5% level." if significant_direction
                               else "No statistically significant directional edge versus 50% at the 5% level."),
        "hot_cold_periods": ("At least one streak condition differs after Bonferroni correction; inspect its sample size before acting."
                              if hot_cold else "No streak condition survives the multiple-test correction."),
        "streak_predictive_value": ("At least one ML model improves on the chronological baseline significantly."
                                     if model_significant else "No ML model shows statistically significant improvement over the chronological baseline."),
        "position_sizing": sizing,
    }


def markdown_report(report: dict[str, Any]) -> str:
    performance = report["performance"]
    monetary = performance["monetary_performance"]
    lines = [
        "# Trading Ledger Analysis",
        "",
        f"- Signal: {report['input']['signal']}",
        f"- Source: `{report['input']['source_path']}`",
        f"- Trades: {performance['total_trades']} | Wins: {performance['total_wins']} | Losses: {performance['total_losses']} | Win rate: {performance['win_rate']:.2%}",
        f"- Win-rate 95% CI: {performance['win_rate_95pct_ci'][0]:.2%} to {performance['win_rate_95pct_ci'][1]:.2%} | p vs 50%: {performance['win_rate_vs_50pct_pvalue']:.4g}",
        f"- Streaks: current {performance['streaks']['current']} {performance['streaks']['current_length']}; longest W {performance['streaks']['longest_win']} / L {performance['streaks']['longest_loss']}",
        "",
        "## Monetary Performance",
        "",
    ]
    if monetary["available"]:
        lines.extend([
            f"- Total P&L: ${monetary['total_profit_loss']:.2f} | Total return: {monetary['total_return']:.2%}",
            f"- Expectancy: ${monetary['expectancy_per_trade']:.4f} per trade | Profit factor: {monetary['profit_factor']:.3f}",
            f"- Max drawdown: ${monetary['maximum_drawdown']:.2f} ({monetary['maximum_drawdown_pct']:.2%})",
            f"- Sharpe: {monetary['sharpe_ratio_annualized_zero_rf']} | Sortino: {monetary['sortino_ratio_annualized_zero_rf']}",
        ])
    else:
        lines.append(f"- Not available: {monetary['reason']}")
    lines.extend(["", "## Conclusion", ""])
    for key, value in report["conclusion"].items():
        lines.append(f"- {key.replace('_', ' ').title()}: {value}")
    lines.extend([
        "",
        "## Limits",
        "",
        "- This analysis can reject weak evidence; it cannot establish a guaranteed future return.",
        "- Backtest outcomes must be compared with executable entry prices, fees, spread, slippage, and risk limits before capital is deployed.",
        "",
    ])
    return "\n".join(lines)


def run_one(frame: pd.DataFrame, info: InputInfo, args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    series = add_time_series(frame, args.starting_balance)
    write_csv(output_dir / "normalized_ledger.csv", series)
    write_csv(output_dir / "time_series.csv", series)
    performance = performance_summary(series, args.starting_balance)
    by_market = grouped_performance(series, "market", args.starting_balance)
    by_side = grouped_performance(series, "side", args.starting_balance)
    by_hour = grouped_performance(series, "hour_et", args.starting_balance)
    by_weekday = grouped_performance(series, "weekday_et", args.starting_balance)
    by_month = grouped_performance(series, "month_et", args.starting_balance)
    by_regime = grouped_performance(series, "rolling_50_regime", args.starting_balance)
    write_csv(output_dir / "performance_by_market.csv", by_market)
    write_csv(output_dir / "performance_by_side.csv", by_side)
    write_csv(output_dir / "performance_by_hour_et.csv", by_hour)
    write_csv(output_dir / "performance_by_weekday_et.csv", by_weekday)
    write_csv(output_dir / "performance_by_month_et.csv", by_month)
    write_csv(output_dir / "performance_by_rolling_50_regime.csv", by_regime)
    streak_frame = streak_conditionals(series)
    write_csv(output_dir / "streak_conditionals.csv", streak_frame)
    forecast_inputs = prophet_series(series)
    LOG.info("%s: Prophet final forecasts and historical cutoff tests", info.signal)
    forecasts = prophet_forecasts(forecast_inputs, (100, 500), args.prophet_max_history)
    if not forecasts.empty:
        write_csv(output_dir / "prophet_future_forecasts.csv", forecasts)
    decisions, policies = prophet_policy_evaluation(
        forecast_inputs, series, args.prophet_min_history, args.prophet_train_window, args.prophet_origins)
    if not decisions.empty:
        write_csv(output_dir / "prophet_cutoff_decisions.csv", decisions)
    if not policies.empty:
        write_csv(output_dir / "prophet_policy_summary.csv", policies)
    LOG.info("%s: chronological ML evaluation", info.signal)
    ml_report, _ = evaluate_models(series, output_dir)
    write_json(output_dir / "ml_summary.json", ml_report)
    monte_carlo_report = monte_carlo(
        series, args.monte_carlo_simulations, args.monte_carlo_trades,
        args.starting_balance, np.random.default_rng(args.random_seed))
    write_json(output_dir / "monte_carlo.json", monte_carlo_report)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input": {
            "source_path": str(info.source_path),
            "source_kind": info.source_kind,
            "pnl_available": info.pnl_available,
            "signal": info.signal,
        },
        "performance": performance,
        "prophet_policy_summary": policies.to_dict(orient="records"),
        "machine_learning": ml_report,
        "monte_carlo": monte_carlo_report,
    }
    report["conclusion"] = conclusion(performance, streak_frame, ml_report)
    write_json(output_dir / "performance_summary.json", report)
    (output_dir / "performance_summary.md").write_text(markdown_report(report), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="CSV/JSON ledger or extracted backtest artifact directory")
    parser.add_argument("--output-dir", type=Path, default=Path("ledger_analysis_output"))
    parser.add_argument("--signal", choices=("all", "prophet", "ml", "ledger"), default="all")
    parser.add_argument("--starting-balance", type=float, default=STARTING_BALANCE)
    parser.add_argument("--prophet-min-history", type=int, default=500)
    parser.add_argument("--prophet-train-window", type=int, default=1000)
    parser.add_argument("--prophet-max-history", type=int, default=5000)
    parser.add_argument("--prophet-origins", type=int, default=25)
    parser.add_argument("--monte-carlo-simulations", type=int, default=10_000)
    parser.add_argument("--monte-carlo-trades", type=int, default=500)
    parser.add_argument("--random-seed", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging()
    if args.starting_balance <= 0 or args.prophet_min_history < 30 or args.prophet_origins < 1:
        raise SystemExit("starting balance must be positive; Prophet minimum history must be >= 30; origins must be positive")
    raw, source_path = read_input(args.input.expanduser())
    is_artifact = {"market_open", "actual_outcome", "prophet_side", "ml_side"}.issubset(raw.columns)
    if args.signal == "all":
        signals = ["prophet", "ml"] if is_artifact else ["ledger"]
    else:
        signals = [args.signal]
    reports: dict[str, Any] = {}
    for signal in signals:
        if is_artifact:
            ledger, info = normalize_artifact(raw, source_path, signal)
        else:
            if signal != "ledger":
                raise SystemExit("Non-artifact ledgers require --signal ledger or --signal all")
            ledger, info = normalize_ledger(raw, source_path)
        if ledger.empty:
            raise RuntimeError(f"No settled {signal} trades remain after normalization")
        LOG.info("%s: analyzing %d trades", signal, len(ledger))
        reports[signal] = run_one(ledger, info, args, args.output_dir / signal)
    write_json(args.output_dir / "analysis_index.json", {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "signals": {signal: {"trades": report["performance"]["total_trades"], "path": signal}
                    for signal, report in reports.items()},
    })
    LOG.info("Ledger analysis complete: %s", args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
