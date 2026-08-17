from __future__ import annotations

import csv
import struct
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from polymarket_us_mlb_pregame_prices import (
    MAX_PRICE,
    MIN_PRICE,
    MoneylineMarket,
    PricePoint,
    TeamSide,
    _atomic_write_csv,
    _encode_varint,
    _integer_field,
    _message_field,
    build_minute_rows,
    decode_price_history_response,
    discover_moneylines,
    discover_upcoming_moneylines,
    encode_price_history_request,
)


def fixed32_field(number: int, value: float) -> bytes:
    return _encode_varint((number << 3) | 5) + struct.pack("<f", value)


def response_point(timestamp: int, long_price: float, short_price: float) -> bytes:
    point = (
        _integer_field(1, timestamp)
        + fixed32_field(2, long_price)
        + fixed32_field(3, short_price)
    )
    return _message_field(1, point)


class PolymarketMlbPregamePriceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.created = datetime(2026, 8, 14, 4, 5, tzinfo=UTC)
        self.start = datetime(2026, 8, 17, 22, 5, tzinfo=UTC)
        self.long = TeamSide("853588", "Baltimore Orioles", "long")
        self.short = TeamSide("853589", "Tampa Bay Rays", "short")
        self.market = MoneylineMarket(
            "mlb-bal-tb-2026-08-17",
            "Baltimore Orioles vs. Tampa Bay Rays",
            "427038",
            "aec-mlb-bal-tb-2026-08-17",
            self.created,
            self.start,
            (self.long, self.short),
        )

    def test_decodes_timestamp_and_both_team_prices(self) -> None:
        payload = response_point(100, 0.395, 0.61) + response_point(160, 0.4, 0.605)
        points = decode_price_history_response(payload)
        self.assertEqual([point.timestamp for point in points], [100, 160])
        self.assertEqual(str(points[0].long_price), "0.3950")
        self.assertEqual(str(points[0].short_price), "0.6100")

    def test_request_has_nested_range_and_minute_fidelity(self) -> None:
        payload = encode_price_history_request(
            self.market.market_slug,
            self.created,
            self.created + timedelta(hours=1),
            fidelity_minutes=1,
        )
        self.assertIn(self.market.market_slug.encode(), payload)
        self.assertTrue(payload.endswith(_integer_field(4, 1)))

    def test_minute_rows_use_last_observation_and_fill_gaps(self) -> None:
        first = int(self.created.timestamp())
        end = self.created + timedelta(minutes=5, seconds=30)
        points = [
            PricePoint(first + 2, MIN_PRICE, MAX_PRICE),
            PricePoint(first + 40, MAX_PRICE, MIN_PRICE),
            PricePoint(first + 180, MIN_PRICE, MAX_PRICE),
        ]
        rows, observed = build_minute_rows(points, self.long, self.market, end)
        self.assertEqual(observed, 2)
        self.assertEqual(len(rows), 5)
        self.assertEqual([row.price for row in rows], ["0.9900", "0.9900", "0.9900", "0.0100", "0.0100"])
        self.assertTrue(all(row.item_id == "853588" for row in rows))

    def test_short_side_uses_short_price_and_never_includes_game_time(self) -> None:
        before = int((self.start - timedelta(minutes=2)).timestamp())
        points = [
            PricePoint(before, MIN_PRICE, MAX_PRICE),
            PricePoint(int(self.start.timestamp()), MAX_PRICE, MIN_PRICE),
        ]
        rows, observed = build_minute_rows(points, self.short, self.market, self.start + timedelta(hours=1))
        self.assertEqual(observed, 1)
        self.assertEqual(rows[-1].price, "0.9900")
        self.assertLess(datetime.fromisoformat(rows[-1].datetime.replace("Z", "+00:00")), self.start)

    def test_full_history_includes_post_start_and_terminal_prices_without_extending_to_now(self) -> None:
        before = int((self.start - timedelta(minutes=1)).timestamp())
        after = int((self.start + timedelta(minutes=2)).timestamp())
        points = [
            PricePoint(before, MIN_PRICE, MAX_PRICE),
            PricePoint(after, MIN_PRICE - MIN_PRICE, MAX_PRICE + MIN_PRICE),
        ]
        rows, observed = build_minute_rows(
            points,
            self.long,
            self.market,
            self.start + timedelta(days=30),
            pregame_only=False,
        )
        self.assertEqual(observed, 2)
        self.assertEqual(rows[-1].price, "0.0000")
        self.assertEqual(rows[-1].datetime, datetime.fromtimestamp(after, UTC).isoformat().replace("+00:00", "Z"))
        self.assertEqual(len(rows), 4)

    def test_zero_and_one_prices_are_not_exported(self) -> None:
        first = int(self.created.timestamp())
        points = [PricePoint(first, MIN_PRICE - MIN_PRICE, MAX_PRICE + MIN_PRICE)]
        rows, observed = build_minute_rows(points, self.long, self.market, self.created + timedelta(minutes=2))
        self.assertEqual(rows, [])
        self.assertEqual(observed, 0)

    def test_discovers_only_upcoming_full_game_moneyline(self) -> None:
        payload = {
            "events": [{
                "ticker": self.market.event_ticker,
                "title": self.market.event_title,
                "startTime": self.start.isoformat(),
                "createdAt": self.created.isoformat(),
                "markets": [{
                    "id": self.market.market_id,
                    "slug": self.market.market_slug,
                    "createdAt": self.created.isoformat(),
                    "gameStartTime": self.start.isoformat(),
                    "active": True,
                    "closed": False,
                    "sportsMarketType": "baseball_team_full_game_winner",
                    "marketSides": [
                        {"id": self.long.item_id, "long": True, "team": {"name": self.long.team}},
                        {"id": self.short.item_id, "long": False, "team": {"name": self.short.team}},
                    ],
                }, {
                    "id": "other",
                    "slug": "spread",
                    "createdAt": self.created.isoformat(),
                    "gameStartTime": self.start.isoformat(),
                    "sportsMarketType": "baseball_team_full_game_spread",
                }],
            }],
        }
        found = discover_upcoming_moneylines(payload, self.start - timedelta(hours=1))
        self.assertEqual(found, [self.market])
        self.assertEqual(discover_upcoming_moneylines(payload, self.start), [])

    def test_discovers_previous_legacy_moneyline_and_rejects_it_as_upcoming(self) -> None:
        payload = {
            "events": [{
                "ticker": self.market.event_ticker,
                "title": self.market.event_title,
                "startTime": self.start.isoformat(),
                "createdAt": self.created.isoformat(),
                "markets": [{
                    "id": self.market.market_id,
                    "slug": self.market.market_slug,
                    "createdAt": self.created.isoformat(),
                    "gameStartTime": self.start.isoformat(),
                    "active": True,
                    "closed": True,
                    "status": "MARKET_STATUS_RESOLVED",
                    "sportsMarketType": "moneyline",
                    "marketSides": [
                        {"id": self.long.item_id, "long": True, "team": {"name": self.long.team}},
                        {"id": self.short.item_id, "long": False, "team": {"name": self.short.team}},
                    ],
                }],
            }],
        }
        as_of = self.start + timedelta(days=1)
        self.assertEqual(discover_moneylines(payload, as_of, game_scope="previous"), [self.market])
        self.assertEqual(discover_moneylines(payload, as_of, game_scope="upcoming"), [])

    def test_csv_contract_is_exact_and_clean(self) -> None:
        first = int(self.created.timestamp())
        rows, _ = build_minute_rows(
            [PricePoint(first, MIN_PRICE, MAX_PRICE)],
            self.long,
            self.market,
            self.created + timedelta(minutes=2),
        )
        with TemporaryDirectory() as directory:
            output = Path(directory) / "prices.csv"
            _atomic_write_csv(output, rows)
            with output.open(newline="", encoding="utf-8") as handle:
                parsed = list(csv.DictReader(handle))
                self.assertEqual(handle.seek(0), 0)
                header = handle.readline().strip()
        self.assertEqual(header, "item_id,datetime,price")
        self.assertEqual(set(parsed[0]), {"item_id", "datetime", "price"})


if __name__ == "__main__":
    unittest.main()
