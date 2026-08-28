#!/usr/bin/env python3
"""Read-only per-ticker Kalshi position audit for the BTC 15-minute runner."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from kalshi_btc15m_average_down import KalshiREST


def position_text(position: float | None) -> str:
    if position is None:
        return "unavailable"
    if position > 0.004:
        return f"{position:+.2f} contracts (long YES)"
    if position < -0.004:
        return f"{position:+.2f} contracts (long NO)"
    return "0.00 contracts (flat)"


async def run(ticker: str) -> int:
    api_key = os.getenv("KALSHI_API_KEY_ID", "")
    pem_path = Path(os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem"))
    if not api_key or not pem_path.exists():
        raise SystemExit("KALSHI_API_KEY_ID and KALSHI_PEM_PATH are required")
    rest = KalshiREST(
        api_key,
        pem_path,
        os.getenv("KALSHI_DEMO", "false").lower() in {"1", "true", "yes"},
    )
    try:
        position = await rest.position_for_ticker(ticker)
        print(f"POSITION AUDIT | ticker={ticker} exchange_position={position_text(position)}")
        if position is None:
            return 2
        return 0
    finally:
        await rest.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", required=True, help="Exact Kalshi ticker to inspect")
    return asyncio.run(run(parser.parse_args().ticker.strip()))


if __name__ == "__main__":
    raise SystemExit(main())
