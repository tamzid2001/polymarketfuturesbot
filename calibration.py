"""Calibration reporting for the execution-only Monte Carlo layer."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from pathlib import Path

from execution_path_model import ExecutionCalibration, ExecutionPathModel, calibration_targets, simulate_calibration


def calibration_rows(report: dict[str, float]) -> list[dict[str, float | str]]:
    names = (
        "40_region_rate_win", "40_region_rate_loss", "40_region_directional_wr", "no_40_region_directional_wr",
        "rung_wr_40", "rung_wr_30", "rung_wr_20", "rung_wr_10",
    )
    return [
        {
            "metric": name,
            "observed": report[f"observed_{name}"],
            "simulated": report[f"simulated_{name}"],
            "error": report[f"error_{name}"],
        }
        for name in names
    ]


def write_calibration_report(
    path: Path,
    calibration: ExecutionCalibration | None = None,
    replications: int = 20_000,
    seed: int = 7,
) -> dict[str, float]:
    model = ExecutionPathModel(calibration or ExecutionCalibration.base_case())
    report = simulate_calibration(model, replications=replications, seed=seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=("metric", "observed", "simulated", "error"))
        writer.writeheader()
        writer.writerows(calibration_rows(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("calibration_report.csv"))
    parser.add_argument("--replications", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    report = write_calibration_report(args.output, replications=args.replications, seed=args.seed)
    for row in calibration_rows(report):
        print("{metric}: observed={observed:.4%} simulated={simulated:.4%} error={error:+.4%}".format(**row))
    print(
        "49c maker participation is a scenario assumption; the supplied old-ladder "
        "statistics validate the joint 40c-region rates, not 49c fills."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
