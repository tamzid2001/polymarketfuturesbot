"""Public-settlement directional replay across active 15-minute crypto/commodity series.

No authentication, orders, execution simulation, live configuration or runtime
ledger mutations. The original BTC signal function is reused without changes.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import logging
import math
import time
import zipfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from kalshi_settlement_loader import (
    KalshiSettlementLoader, PUBLIC_ENDPOINTS, SettlementMarket,
    parse_timestamp, reconstruct_signals, signal_summary, timestamp_text,
)
from strategy_core import sticky_directional_prediction

LOG = logging.getLogger(__name__)
BASE = "https://external-api.kalshi.com/trade-api/v2"


class PublicReader:
    def __init__(self):
        self.last_request = 0.0
        self.requests = 0
        self.retries = 0

    def get(self, url, params):
        for attempt in range(7):
            time.sleep(max(0, 0.8 - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            self.requests += 1
            request = Request(url + "?" + urlencode(params), headers={"Accept": "application/json", "User-Agent": "kalshi-directional-research/1.0"})
            try:
                with urlopen(request, timeout=40) as response:
                    return json.load(response)
            except HTTPError as exc:
                if exc.code not in (408, 429, 500, 502, 503, 504) or attempt == 6:
                    raise
                delay = min(45, max(2 ** (attempt + 1), float(exc.headers.get("Retry-After", 0))))
                LOG.warning("public API HTTP %s; retry in %.1fs", exc.code, delay)
            except (URLError, TimeoutError):
                if attempt == 6:
                    raise
                delay = min(45, 2 ** (attempt + 1))
                LOG.warning("public API read timeout; retry in %.1fs", delay)
            self.retries += 1
            time.sleep(delay)
        raise RuntimeError("unreachable retry exhaustion")


class AuditedLoader(KalshiSettlementLoader):
    def __init__(self, path, series, reader, cutoff):
        super().__init__(path, series_ticker=series)
        self.reader, self.cutoff = reader, cutoff
        self.pages = []

    def _get_json(self, url, params):
        params = dict(params, max_close_ts=str(int(self.cutoff.timestamp())))
        result = self.reader.get(url, params)
        rows = result.get("markets")
        if not isinstance(rows, list):
            raise ValueError("public endpoint did not return a market array")
        invalid = [r.get("ticker") if isinstance(r, dict) else None for r in rows if not isinstance(r, dict) or SettlementMarket.from_api(r, "audit", self.series_ticker) is None]
        if invalid:
            raise ValueError(f"{self.series_ticker}: invalid settlement rows: {invalid[:5]}")
        self.pages.append({"endpoint": url, "rows": len(rows), "has_next": bool(result.get("cursor"))})
        LOG.info("%s page=%d rows=%d endpoint=%s", self.series_ticker, len(self.pages), len(rows), url.rsplit("/v2/", 1)[-1])
        return result

    def refresh(self):
        # Unlike the legacy convenience loader, any unavailable endpoint aborts
        # this report. A partially downloaded history must not look complete.
        records = {}
        duplicates = excluded_future = 0
        for url, params in PUBLIC_ENDPOINTS:
            for record in self._fetch_endpoint(url, params):
                if record.settlement_time > self.cutoff:
                    excluded_future += 1
                    continue
                prior = records.get(record.ticker)
                if prior:
                    duplicates += 1
                    if (prior.result, prior.open_time, prior.close_time) != (record.result, record.open_time, record.close_time):
                        raise ValueError(f"conflicting duplicate settlement {record.ticker}")
                if prior is None or record.settlement_time > prior.settlement_time:
                    records[record.ticker] = record
        if not records:
            raise ValueError(f"{self.series_ticker}: no valid historical settlements")
        markets = sorted(records.values(), key=lambda r: (r.open_time, r.ticker))
        if len({r.open_time for r in markets}) != len(markets):
            raise ValueError(f"{self.series_ticker}: overlapping market opens require investigation")
        if any((r.close_time-r.open_time).total_seconds() != 900 for r in markets):
            raise ValueError(f"{self.series_ticker}: not an exclusively 15-minute series")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "series_ticker": self.series_ticker, "downloaded_at": timestamp_text(datetime.now(UTC)),
            "as_of": timestamp_text(self.cutoff), "pages": self.pages,
            "duplicate_rows": duplicates, "excluded_post_cutoff_settlements": excluded_future,
            "markets": [r.to_cache() for r in markets],
        }
        temporary = self.cache_path.with_suffix(".partial.json")
        temporary.write_text(json.dumps(payload, separators=(",", ":")) + "\n")
        temporary.replace(self.cache_path)
        return markets


def summary(signals, metadata):
    result = signal_summary(signals, metadata)
    outcomes = [s.directional_win for s in signals]
    n, wins = len(outcomes), sum(outcomes)
    if not n:
        return result
    rate = wins / n
    z = 1.959963984540054
    centre = (rate + z*z/(2*n)) / (1+z*z/n)
    radius = z * math.sqrt(rate*(1-rate)/n + z*z/(4*n*n)) / (1+z*z/n)
    result.update(ci95_low=centre-radius, ci95_high=centre+radius)
    # Exact two-sided p=0.5 binomial test; nominal only, not independence proof.
    tail = min(wins, n-wins)
    logs = [math.lgamma(n+1)-math.lgamma(k+1)-math.lgamma(n-k+1)-n*math.log(2) for k in range(tail+1)]
    largest = max(logs)
    result["nominal_binomial_p_two_sided"] = min(1, 2*math.exp(largest)*sum(math.exp(v-largest) for v in logs))
    max_w = max_l = run = 0
    last = None
    for outcome in outcomes:
        run = run+1 if outcome == last else 1
        if outcome:
            max_w = max(max_w, run)
        else:
            max_l = max(max_l, run)
        last = outcome
    result.update(longest_win_streak=max_w, longest_loss_streak=max_l, current_streak_side="W" if last else "L", current_streak_length=run)
    half = n//2
    result.update(first_half_wr=sum(outcomes[:half])/half if half else None,
                  second_half_wr=sum(outcomes[half:])/(n-half),
                  latest_1000_n=min(1000,n), latest_1000_wr=sum(outcomes[-1000:])/min(1000,n))
    return result


def write_csv(path, rows, *, compressed=False):
    if not rows:
        return
    fields = list(dict.fromkeys(k for row in rows for k in row))
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    raw = buf.getvalue().encode()
    path.write_bytes(gzip.compress(raw, mtime=0) if compressed else raw)


def run(output: Path, cache: Path, offline: bool = False):
    output.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    reader = PublicReader()
    discovery_path = output / "series_discovery.json"
    if offline:
        discovery = json.loads(discovery_path.read_text())
        cutoff = parse_timestamp(discovery["as_of"])
    else:
        cutoff = datetime.now(UTC)
        catalog = reader.get(BASE + "/series", {"include_product_metadata": "true"})["series"]
        candidates = [s for s in catalog if s.get("frequency") == "fifteen_min"
                      and s.get("category") in ("Crypto", "Commodities")
                      and s.get("ticker") not in ("KXCRYPTOCOMP15M", "KXCRYPTOLEAD15M")]
        discovery = {"as_of": timestamp_text(cutoff), "catalog_endpoint": BASE + "/series", "series": []}
        for item in sorted(candidates, key=lambda s: s["ticker"]):
            settled = reader.get(BASE + "/markets", {"series_ticker": item["ticker"], "status": "settled", "limit": 1, "max_close_ts": int(cutoff.timestamp())})["markets"]
            row = {k: item.get(k) for k in ("ticker", "title", "category", "frequency")}
            row["included"] = bool(settled)
            row["reason"] = "recent settled up/down markets available" if settled else "no settled rows in current endpoint; archived-only availability not claimed absent"
            discovery["series"].append(row)
        discovery_path.write_text(json.dumps(discovery, indent=2) + "\n")
    selected = [s for s in discovery["series"] if s["included"]]
    if not selected or not any(s["ticker"] == "KXBTC15M" for s in selected):
        raise ValueError("series discovery has no BTC control; refusing an incomplete/empty report")
    LOG.info("ACTIVE UNIVERSE: %d non-BTC + BTC control=%s", sum(s["ticker"] != "KXBTC15M" for s in selected), any(s["ticker"] == "KXBTC15M" for s in selected))
    primary, proxy, regimes = [], [], []
    for info in selected:
        series = info["ticker"]
        loader = AuditedLoader(cache / (series + ".json"), series, reader, cutoff)
        if offline:
            metadata = json.loads(loader.cache_path.read_text())
            if metadata["as_of"] != discovery["as_of"]:
                raise ValueError("cache cutoff mismatch")
            markets = loader.load()
        else:
            markets = loader.refresh()
        if len({r.open_time for r in markets}) != len(markets) or any((r.close_time-r.open_time).total_seconds() != 900 for r in markets):
            raise ValueError(f"{series}: cached history must have unique 15-minute windows")
        for mode, rows in (("inverse_latest_settlement", primary), ("sticky_until_directional_win", proxy)):
            signals, metadata = reconstruct_signals(markets, decision_delay_seconds=45, signal_mode=mode)
            prior_side = None
            for signal in signals:
                if mode == "inverse_latest_settlement" and signal.source_settlement_time > signal.decision_time:
                    raise AssertionError("look-ahead in causal replay")
                side, _ = sticky_directional_prediction(prior_side, signal.source_result)
                if side != signal.predicted_side:
                    raise AssertionError("sticky/inverse directional transition disagrees")
                prior_side = side
            stats = dict(series=series, title=info["title"], category=info["category"], **summary(signals, metadata))
            rows.append(stats)
            write_csv(output / f"{series}_{mode}_signals.csv.gz", [s.to_row() for s in signals], compressed=True)
            if mode == "inverse_latest_settlement":
                month = defaultdict(list)
                for signal in signals:
                    month[signal.open_time.strftime("%Y-%m")].append(signal.directional_win)
                for period, results in month.items():
                    regimes.append(dict(series=series, month=period, predictions=len(results), wins=sum(results), losses=len(results)-sum(results), win_rate=sum(results)/len(results)))
                LOG.info("RESULT %s: settled=%s signals=%s W/L=%s/%s WR=%.4f%%", series, len(markets), len(signals), stats["directional_wins"], stats["directional_losses"], 100*stats["directional_win_rate"])
                if series == "KXBTC15M":
                    first = signals[:20778]
                    baseline = dict(predictions=len(first), wins=sum(s.directional_win for s in first), losses=sum(not s.directional_win for s in first), expected=[20778,10751,10027])
                    baseline["matches_original"] = [baseline["predictions"], baseline["wins"], baseline["losses"]] == baseline["expected"]
                    (output / "btc_baseline_comparison.json").write_text(json.dumps(baseline, indent=2) + "\n")
    if not offline:
        (output / "collection_stats.json").write_text(json.dumps({
            "requests": reader.requests, "retries": reader.retries,
            "as_of": discovery["as_of"], "settled_markets": sum(s["total_settled_markets"] for s in primary),
            "eligible_signals": sum(s["eligible_predictions"] for s in primary),
        }, indent=2) + "\n")
    write_csv(output / "directional_summary.csv", primary)
    write_csv(output / "boundary_proxy_summary.csv", proxy)
    write_csv(output / "monthly_directional_results.csv", regimes)
    other_series = [s for s in primary if s["series"] != "KXBTC15M"]
    family_size = len(other_series)
    lines = ["# Multi-series historical settlement directional replay", "", f"Snapshot cutoff: {discovery['as_of']}", "",
             "Primary: unchanged original BTC algorithm, market open +45s, inverse of the most recent available official prior settlement. Every eligible market is scored against its actual settlement. No loss-based skips. Sticky-after-loss/flip-after-win was checked against the same source sequence.", "",
             "Secondary boundary proxy: immediately previous eventual settlement used as a provisional-outcome proxy. This is NOT proof that the result was known at the opening boundary; separate CSV prevents mixing it into causal historical evidence.", "",
             "No fills, stops, fees, recovery-sizing P&L, or live orders are inferred or simulated in this directional-only test.", "",
             "| Series | Settled | Eligible | W / L | WR | 95% Wilson CI | Binomial p | Bonferroni p | Max W/L streak |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for s in primary:
        p_value = s["nominal_binomial_p_two_sided"]
        adjusted = f"{min(1, family_size*p_value):.6g}" if s["series"] != "KXBTC15M" else "control; excluded"
        lines.append(f"| {s['series']} | {s['total_settled_markets']:,} | {s['eligible_predictions']:,} | {s['directional_wins']:,} / {s['directional_losses']:,} | {s['directional_win_rate']:.4%} | {s['ci95_low']:.2%}–{s['ci95_high']:.2%} | {p_value:.6g} | {adjusted} | {s['longest_win_streak']} / {s['longest_loss_streak']} |")
    if other_series:
        total_n = sum(s["eligible_predictions"] for s in other_series)
        total_w = sum(s["directional_wins"] for s in other_series)
        total_markets = sum(s["total_settled_markets"] for s in other_series)
        lines += ["", f"Non-BTC descriptive total: **{family_size} series; {total_markets:,} settled markets; {total_n:,} eligible signals; {total_w:,} wins / {total_n-total_w:,} losses; {total_w/total_n:.4%} WR**. No pooled binomial p-value or cross-series streak is claimed: simultaneous asset signals are correlated and do not form one independent trading sequence."]
    lines += ["", "## Statistical interpretation", "",
              "Two-sided exact binomial test: H0 is P(directional win)=0.50 versus H1 !=0.50. For n signals and w wins, p=min(1, 2*sum(comb(n,k), k=0..min(w,n-w))/2**n). The implementation evaluates this probability in log space. This is not the probability that H0 is true, and 50% is not the live strategy's fee-adjusted break-even rate.", "",
              f"Bonferroni p=min(1, {family_size}*raw p) across the {family_size} non-BTC primary tests; BTC is a separate pre-existing control. A corrected p<0.05 survives this specified comparison family only. Wilson intervals are individual, not simultaneous. Both intervals and tests are nominal IID-Bernoulli benchmarks: correction for multiple series does not fix serial dependence, earlier strategy selection, or execution uncertainty.", "",
              "## Coverage and chronological stability", "",
              "Halves split eligible signals chronologically, not calendar days; the second half has one extra observation when n is odd. The recent window is the latest min(1000,n) eligible signals. Current streak means at the frozen snapshot, not the current live worker.", "",
              "| Series | First settled market open (UTC) | Last settled market open (UTC) | No eligible signal | First-half WR | Second-half WR | Recent n | Recent W / L | Recent WR | Current streak |",
              "|---|---|---|---:|---:|---:|---:|---:|---:|---|"]
    for s in primary:
        recent_n = s["latest_1000_n"]
        recent_w = round(recent_n*s["latest_1000_wr"])
        lines.append(f"| {s['series']} | {s['first_settled_market_timestamp']} | {s['last_settled_market_timestamp']} | {s['total_settled_markets']-s['eligible_predictions']} | {s['first_half_wr']:.2%} | {s['second_half_wr']:.2%} | {recent_n:,} | {recent_w} / {recent_n-recent_w} | {s['latest_1000_wr']:.2%} | {s['current_streak_length']}{s['current_streak_side']} |")
    lines += ["", "The universe is the catalog's active fifteen-minute Crypto/Commodities up-or-down series with settled markets in the current API, plus BTC as a control. Templates with no current settled rows are listed separately in series_discovery.json; this does not assert their archive is empty.", "",
              "Both current and historical settlement endpoints must finish pagination. Duplicate tickers are deduplicated, conflicts abort, and post-cutoff settlements are excluded. Young series have shorter histories; their estimates are not as precise as BTC's. CI/p-values are nominal independent-trial benchmarks; serial dependence, cross-asset correlation, and testing multiple series can invalidate a simple significance interpretation. Streaks span eligible signals, including market/session gaps.", "",
              "Sources: [Kalshi markets](https://docs.kalshi.com/api-reference/market/get-markets), [series discovery](https://docs.kalshi.com/api-reference/market/get-series-list), and [binomial-test definition](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.binomtest.html).", "",
              f"Reproduce offline in this checkout: `python kalshi_multiseries_backtest.py --output {output} --cache {cache} --offline`", "",
              "Or extract reproducible_settlement_replay.zip into an empty folder and run: `python kalshi_multiseries_backtest.py --output reports --cache cache --offline` (Python 3.11+; standard library only).", ""]
    (output / "backtest_summary.md").write_text("\n".join(lines))
    bundle = output / "reproducible_settlement_replay.zip"
    manifest = {"as_of": discovery["as_of"], "run_mode": "offline_replay" if offline else "public_download",
                "requests_this_execution": reader.requests, "retries_this_execution": reader.retries, "files": {}}
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        paths = [(p, "reports/" + p.name) for p in sorted(output.iterdir()) if p.is_file() and p.name not in (bundle.name, "manifest.json")]
        paths += [(cache / (s["ticker"] + ".json"), "cache/" + s["ticker"] + ".json") for s in selected]
        paths += [(Path(__file__).parent / name, name) for name in ("kalshi_multiseries_backtest.py", "kalshi_settlement_loader.py", "strategy_core.py", "recovery_sizing.py")]
        for path, name in paths:
            raw = path.read_bytes()
            manifest["files"][name] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
            archive.writestr(name, raw)
        archive.writestr("manifest.json", json.dumps(manifest, indent=2))
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    snapshot_name = "multiseries_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    p.add_argument("--output", type=Path, default=Path("reports") / snapshot_name)
    p.add_argument("--cache", type=Path, default=Path("data/raw") / snapshot_name)
    p.add_argument("--offline", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args.output, args.cache, args.offline)
