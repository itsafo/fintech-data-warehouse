"""Binance public ticker API -> bronze.raw_crypto. No auth required; used
instead of CoinGecko because its free-tier rate limits (1200 weight/min)
comfortably support the every-5-minutes polling crypto_realtime_pipeline
does, and prices move continuously so there's a real delta each poll.
"""
from __future__ import annotations

import json

import requests

from include.extractors.base_extractor import BaseExtractor


class BinanceExtractor(BaseExtractor):
    def fetch(self) -> list[dict]:
        symbols = self.query_params.get("symbols", [])
        url = f"{self.source_config['base_url']}{self.source_config['endpoint_path']}"
        resp = requests.get(
            url,
            # Binance expects a JSON-array-formatted string for multi-symbol
            # requests, e.g. '["BTCUSDT","ETHUSDT"]' -- requests URL-encodes it.
            params={"symbols": json.dumps(symbols)},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def transform(self, raw: list[dict]) -> list[dict]:
        rows = []
        for ticker in raw:
            symbol = ticker["symbol"]
            coin_id = symbol[:-4] if symbol.endswith("USDT") else symbol
            rows.append(
                {
                    "coin_id": coin_id,
                    "vs_currency": "usdt",
                    "price": float(ticker["lastPrice"]),
                    "market_cap": None,
                    "price_change_24h_pct": float(ticker["priceChangePercent"]),
                }
            )
        return rows

    def load(self, conn, rows: list[dict]) -> int:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    insert into bronze.raw_crypto
                        (coin_id, vs_currency, price, market_cap, price_change_24h_pct, captured_at, minio_object_key)
                    values (%(coin_id)s, %(vs_currency)s, %(price)s, %(market_cap)s, %(price_change_24h_pct)s,
                            %(captured_at)s, %(minio_object_key)s)
                    """,
                    row,
                )
        return len(rows)
