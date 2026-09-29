"""Open-Meteo current-weather API -> bronze.raw_weather. No auth required."""
from __future__ import annotations

import requests

from include.extractors.base_extractor import BaseExtractor


class OpenMeteoExtractor(BaseExtractor):
    def fetch(self) -> list[dict]:
        cities = self.query_params.get("cities", [])
        url = f"{self.source_config['base_url']}{self.source_config['endpoint_path']}"
        responses = []
        for city in cities:
            resp = requests.get(
                url,
                params={
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "current": "temperature_2m,wind_speed_10m,weather_code",
                },
                timeout=15,
            )
            resp.raise_for_status()
            responses.append({"city": city["name"], "lat": city["lat"], "lon": city["lon"], "payload": resp.json()})
        return responses

    def transform(self, raw: list[dict]) -> list[dict]:
        rows = []
        for entry in raw:
            current = entry["payload"]["current"]
            rows.append(
                {
                    "city": entry["city"],
                    "latitude": entry["lat"],
                    "longitude": entry["lon"],
                    "temperature_c": current["temperature_2m"],
                    "windspeed_kmh": current["wind_speed_10m"],
                    "weather_code": current.get("weather_code"),
                    "observed_at": current["time"],
                }
            )
        return rows

    def load(self, conn, rows: list[dict]) -> int:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    insert into bronze.raw_weather
                        (city, latitude, longitude, temperature_c, windspeed_kmh, weather_code,
                         observed_at, captured_at, minio_object_key)
                    values (%(city)s, %(latitude)s, %(longitude)s, %(temperature_c)s, %(windspeed_kmh)s,
                            %(weather_code)s, %(observed_at)s, %(captured_at)s, %(minio_object_key)s)
                    """,
                    row,
                )
        return len(rows)
