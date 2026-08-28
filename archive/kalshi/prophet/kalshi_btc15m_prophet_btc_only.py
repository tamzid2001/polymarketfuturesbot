"""BTC-only entry point for the Prophet locked-side 15-minute ladder.

This is a deliberately separate executable name for the refactored legacy
Prophet runner.  It imports the same BTC-only implementation, defaults to its
own state files, and has no ETH contract, hedge, multiplier, or loss-sizing
behaviour.  It does not start automatically; execute it explicitly.
"""

from __future__ import annotations

import os


os.environ.setdefault("TRADE_HISTORY_FILE", "prophet_btc_only_trade_history.json")
os.environ.setdefault(
    "TRADED_TICKERS_FILE", "prophet_btc_only_traded_market_tickers.json")

from kalshibtc15minupordown import main  # noqa: E402


if __name__ == "__main__":
    main()
