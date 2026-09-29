"""Frankfurter FX rates API -> bronze.raw_fx. No auth required."""
from __future__ import annotations

import requests

from include.extractors.base_extractor import BaseExtractor


class FrankfurterExtractor(BaseExtractor):
    def fetch(self) -> dict:
        base_currency = self.query_params.get("base", "USD")
        symbols = self.query_params.get("symbols", [])
        url = f"{self.source_config['base_url']}{self.source_config['endpoint_path']}"
        resp = requests.get(
            url,
            params={"base": base_currency, "symbols": ",".join(symbols)},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def transform(self, raw: dict) -> list[dict]:
        base_currency = raw.get("base", self.query_params.get("base", "USD"))
        rows = []
        for quote_currency, rate in raw.get("rates", {}).items():
            rows.append(
                {
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "rate": rate,
                }
            )
        return rows

    def load(self, conn, rows: list[dict]) -> int:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    insert into bronze.raw_fx
                        (base_currency, quote_currency, rate, captured_at, minio_object_key)
                    values (%(base_currency)s, %(quote_currency)s, %(rate)s, %(captured_at)s, %(minio_object_key)s)
                    """,
                    row,
                )
        return len(rows)
