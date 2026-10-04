"""Unit tests for the API extractors and BaseExtractor.run(). HTTP, MinIO and
Postgres are all faked -- nothing here touches the network or a database."""
from __future__ import annotations

import json

import pytest
from tenacity import wait_none

from include.extractors import base_extractor
from include.extractors.base_extractor import BaseExtractor
from include.extractors.binance import BinanceExtractor
from include.extractors.frankfurter import FrankfurterExtractor
from include.extractors.open_meteo import OpenMeteoExtractor
from include.extractors.registry import build_extractor


def _config(source_name: str, base_url: str, endpoint_path: str, query_params: dict) -> dict:
    return {
        "source_id": 1,
        "source_name": source_name,
        "base_url": base_url,
        "endpoint_path": endpoint_path,
        "query_params": query_params,
    }


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


# --------------------------------------------------------------------------
# Binance
# --------------------------------------------------------------------------

BINANCE_CONFIG = _config(
    "binance", "https://api.binance.com", "/api/v3/ticker/24hr", {"symbols": ["BTCUSDT", "ETHUSDT"]}
)


def test_binance_transform_strips_usdt_suffix():
    extractor = BinanceExtractor(BINANCE_CONFIG, "run", "task")
    raw = [
        {"symbol": "BTCUSDT", "lastPrice": "65000.50", "priceChangePercent": "-1.25"},
        {"symbol": "ETHUSDT", "lastPrice": "3200.10", "priceChangePercent": "0.40"},
    ]
    rows = extractor.transform(raw)
    assert [r["coin_id"] for r in rows] == ["BTC", "ETH"]
    assert rows[0] == {
        "coin_id": "BTC",
        "vs_currency": "usdt",
        "price": 65000.50,
        "market_cap": None,
        "price_change_24h_pct": -1.25,
    }


def test_binance_transform_keeps_symbols_without_usdt_suffix():
    extractor = BinanceExtractor(BINANCE_CONFIG, "run", "task")
    rows = extractor.transform([{"symbol": "BTCEUR", "lastPrice": "1", "priceChangePercent": "0"}])
    assert rows[0]["coin_id"] == "BTCEUR"


def test_binance_fetch_sends_json_array_of_symbols(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        return FakeResponse([])

    monkeypatch.setattr("include.extractors.binance.requests.get", fake_get)
    BinanceExtractor(BINANCE_CONFIG, "run", "task").fetch()

    url, params, _ = calls[0]
    assert url == "https://api.binance.com/api/v3/ticker/24hr"
    # Exact string, no spaces: Binance returns HTTP 400 for '["A", "B"]'.
    assert params["symbols"] == '["BTCUSDT","ETHUSDT"]'


# --------------------------------------------------------------------------
# Frankfurter
# --------------------------------------------------------------------------

FX_CONFIG = _config(
    "frankfurter", "https://api.frankfurter.dev", "/v1/latest", {"base": "USD", "symbols": ["GBP", "EUR"]}
)


def test_frankfurter_transform_one_row_per_quote_currency():
    extractor = FrankfurterExtractor(FX_CONFIG, "run", "task")
    raw = {"base": "USD", "date": "2026-09-28", "rates": {"GBP": 0.75, "EUR": 0.88}}
    rows = extractor.transform(raw)
    assert rows == [
        {"base_currency": "USD", "quote_currency": "GBP", "rate": 0.75},
        {"base_currency": "USD", "quote_currency": "EUR", "rate": 0.88},
    ]


def test_frankfurter_transform_falls_back_to_configured_base():
    extractor = FrankfurterExtractor(FX_CONFIG, "run", "task")
    rows = extractor.transform({"rates": {"GBP": 0.75}})
    assert rows[0]["base_currency"] == "USD"


def test_frankfurter_transform_handles_empty_rates():
    assert FrankfurterExtractor(FX_CONFIG, "run", "task").transform({"rates": {}}) == []


def test_frankfurter_fetch_params(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "include.extractors.frankfurter.requests.get",
        lambda url, params, timeout: calls.append((url, params)) or FakeResponse({}),
    )
    FrankfurterExtractor(FX_CONFIG, "run", "task").fetch()
    url, params = calls[0]
    assert url == "https://api.frankfurter.dev/v1/latest"
    assert params == {"base": "USD", "symbols": "GBP,EUR"}


# --------------------------------------------------------------------------
# Open-Meteo
# --------------------------------------------------------------------------

WEATHER_CONFIG = _config(
    "open_meteo",
    "https://api.open-meteo.com",
    "/v1/forecast",
    {"cities": [{"name": "London", "lat": 51.5, "lon": -0.12}, {"name": "Tokyo", "lat": 35.7, "lon": 139.7}]},
)


def test_open_meteo_fetch_makes_one_request_per_city(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        return FakeResponse({"current": {}})

    monkeypatch.setattr("include.extractors.open_meteo.requests.get", fake_get)
    raw = OpenMeteoExtractor(WEATHER_CONFIG, "run", "task").fetch()

    assert [c["latitude"] for c in calls] == [51.5, 35.7]
    assert [entry["city"] for entry in raw] == ["London", "Tokyo"]


def test_open_meteo_transform_maps_current_block():
    extractor = OpenMeteoExtractor(WEATHER_CONFIG, "run", "task")
    raw = [
        {
            "city": "London",
            "lat": 51.5,
            "lon": -0.12,
            "payload": {
                "current": {
                    "time": "2026-09-29T10:45",
                    "temperature_2m": 14.2,
                    "wind_speed_10m": 11.0,
                    "weather_code": 3,
                }
            },
        }
    ]
    assert extractor.transform(raw) == [
        {
            "city": "London",
            "latitude": 51.5,
            "longitude": -0.12,
            "temperature_c": 14.2,
            "windspeed_kmh": 11.0,
            "weather_code": 3,
            "observed_at": "2026-09-29T10:45",
        }
    ]


def test_open_meteo_transform_tolerates_missing_weather_code():
    extractor = OpenMeteoExtractor(WEATHER_CONFIG, "run", "task")
    raw = [
        {
            "city": "Tokyo",
            "lat": 35.7,
            "lon": 139.7,
            "payload": {"current": {"time": "t", "temperature_2m": 20, "wind_speed_10m": 5}},
        }
    ]
    assert extractor.transform(raw)[0]["weather_code"] is None


# --------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_name, expected_cls",
    [("open_meteo", OpenMeteoExtractor), ("binance", BinanceExtractor), ("frankfurter", FrankfurterExtractor)],
)
def test_registry_builds_the_right_extractor(source_name, expected_cls):
    extractor = build_extractor(_config(source_name, "http://x", "/", {}), dag_run_id="r", task_id="t")
    assert type(extractor) is expected_cls
    assert extractor.dag_run_id == "r"


def test_registry_rejects_unknown_source():
    with pytest.raises(ValueError, match="No extractor registered"):
        build_extractor(_config("nope", "http://x", "/", {}), dag_run_id="r", task_id="t")


# --------------------------------------------------------------------------
# BaseExtractor.run()
# --------------------------------------------------------------------------


class FakeConn:
    def __init__(self):
        self.committed = False
        self.closed = False

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class StubExtractor(BaseExtractor):
    def __init__(self, fetch_error: Exception | None = None):
        super().__init__({"source_id": 7, "source_name": "stub"}, dag_run_id="run-1", task_id="task-1")
        self.fetch_error = fetch_error
        self.loaded_rows: list[dict] = []
        self.conn = FakeConn()
        self.run_logs: list[dict] = []
        self.error_logs: list[str] = []
        self.log_run_error: Exception | None = None

    def fetch(self):
        if self.fetch_error:
            raise self.fetch_error
        return {"ok": True}

    def transform(self, raw):
        return [{"a": 1}, {"a": 2}]

    def load(self, conn, rows):
        self.loaded_rows = rows
        return len(rows)

    # run() would otherwise open real Postgres connections / retry with sleeps.
    def _connect(self):
        return self.conn

    def _fetch_with_retry(self):
        return self.fetch()

    def _log_run(self, started_at, ended_at, status, rows_loaded):
        if self.log_run_error:
            raise self.log_run_error
        self.run_logs.append({"status": status, "rows_loaded": rows_loaded})

    def _log_error(self, message, tb):
        self.error_logs.append(message)


@pytest.fixture
def archived(monkeypatch):
    keys = []

    def fake_archive(source_name, captured_at, payload):
        keys.append((source_name, payload))
        return f"{source_name}/key.json"

    monkeypatch.setattr(base_extractor, "archive_raw_payload", fake_archive)
    return keys


def test_run_success_archives_stamps_rows_loads_and_logs(archived):
    extractor = StubExtractor()
    result = extractor.run()

    assert result == {
        "source_name": "stub",
        "status": "success",
        "rows_loaded": 2,
        "minio_object_key": "stub/key.json",
    }
    assert archived == [("stub", {"ok": True})]
    # every row from one poll shares captured_at and points back at the raw object
    assert {r["minio_object_key"] for r in extractor.loaded_rows} == {"stub/key.json"}
    assert len({r["captured_at"] for r in extractor.loaded_rows}) == 1
    assert extractor.conn.committed and extractor.conn.closed
    assert extractor.run_logs == [{"status": "success", "rows_loaded": 2}]
    assert extractor.error_logs == []


def test_run_failure_logs_and_reraises_original_error(archived):
    extractor = StubExtractor(fetch_error=RuntimeError("api down"))

    with pytest.raises(RuntimeError, match="api down"):
        extractor.run()

    assert extractor.run_logs == [{"status": "failed", "rows_loaded": None}]
    assert extractor.error_logs == ["api down"]
    assert extractor.loaded_rows == []


def test_run_failure_bookkeeping_error_does_not_mask_original(archived):
    extractor = StubExtractor(fetch_error=RuntimeError("api down"))
    extractor.log_run_error = ConnectionError("warehouse unreachable")

    with pytest.raises(RuntimeError, match="api down"):
        extractor.run()


def test_run_success_survives_run_log_failure(archived):
    """Rows are already committed; raising here would make Airflow retry and
    append the same poll twice."""
    extractor = StubExtractor()
    extractor.log_run_error = ConnectionError("warehouse unreachable")

    result = extractor.run()

    assert result["rows_loaded"] == 2
    assert extractor.conn.committed


def test_fetch_retries_three_times_then_reraises_the_original_error():
    class Flaky(StubExtractor):
        calls = 0

        def fetch(self):
            Flaky.calls += 1
            raise ValueError("HTTP 400 from api")

    extractor = Flaky()
    no_wait = BaseExtractor._fetch_with_retry.retry_with(wait=wait_none())

    with pytest.raises(ValueError, match="HTTP 400 from api"):
        no_wait(extractor)

    assert Flaky.calls == 3
