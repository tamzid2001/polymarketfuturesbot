"""Google Colab upload entry point for the strict equity walk-forward study.

Usage in a Colab notebook after cloning/uploading this repository:

    !pip -q install -r requirements_equity_backtest.txt
    from backtest.colab_entry import run_uploaded_csv
    run_uploaded_csv()
"""

from __future__ import annotations

from pathlib import Path

from .run_backtest import main


def run_uploaded_csv(
    input_format: str = "kalshi",
    output_dir: str = "outputs",
    starting_balance: float = 100.0,
    min_training_trades: int = 100,
) -> int:
    """Open Colab's upload picker and run the same CLI implementation."""

    try:
        from google.colab import files
    except ImportError as exc:  # keeps normal Python imports honest.
        raise RuntimeError("run_uploaded_csv is intended for Google Colab") from exc
    uploaded = files.upload()
    if not uploaded:
        raise RuntimeError("No CSV was uploaded")
    filename = next(iter(uploaded))
    return main([
        "--input", str(Path(filename)), "--input-format", input_format,
        "--output-dir", output_dir, "--starting-balance", str(starting_balance),
        "--min-training-trades", str(min_training_trades), "--refit-every", "1",
        "--signal-mode", "level", "--run-sensitivity",
    ])
