import asyncio
import json
import sqlite3
from contextlib import closing
from datetime import date

import httpx
import pytest

from src.nse_downloader.acquisition import AcquisitionError, NSEClient, fetch_all
from src.nse_downloader.config import DatasetConfig, Settings
from src.nse_downloader.storage import sha256_file, write_csv, write_manifest, write_parquet, write_sqlite
from src.nse_downloader.validation import DataValidationError, validate_records, validate_records_with_stats


def test_bootstrap_and_fetch_success() -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            if request.url.path == "/landing":
                return httpx.Response(200, text="ok")
            return httpx.Response(200, json={"data": [{"symbol": "ABC", "ltp": 10}]})

        client = NSEClient(Settings(rate_limit_seconds=0), transport=httpx.MockTransport(handler))
        try:
            config = DatasetConfig(landing_url="https://test/landing", api_url="https://test/api", expected_columns=["symbol"], natural_key="symbol")
            payload = await client.fetch(config)
            assert payload["data"][0]["symbol"] == "ABC"
            assert calls == ["https://test/landing", "https://test/api"]
        finally:
            await client.close()

    asyncio.run(scenario())


def test_validation_deduplicates_and_rejects_empty() -> None:
    records = validate_records({"data": [{"symbol": "ABC", "price": 1}, {"symbol": "ABC", "price": 2}]}, ["symbol"], "symbol")
    assert len(records) == 1
    with pytest.raises(DataValidationError, match="non-empty"):
        validate_records({"data": []}, ["symbol"], "symbol")
    with pytest.raises(DataValidationError, match="missing"):
        validate_records({"data": [{"price": 1}]}, ["symbol"], "symbol")
    with pytest.raises(DataValidationError, match="natural key"):
        validate_records({"data": [{"price": 1}, {"price": 2}]}, ["price"], "symbol")
    with pytest.raises(DataValidationError, match="scalar"):
        validate_records({"data": [{"symbol": {"nested": True}, "price": 1}]}, ["price"], "symbol")


def test_dataset_contracts_accept_valid_rows_and_coerce_numeric_strings() -> None:
    examples = {
        "top-gainers-losers": {"symbol": "ABC", "ltp": "87.66", "trade_quantity": "133798", "perChange": "20"},
        "upper-band-hitters": {"symbol": "ABC", "ltp": "2083", "pChange": " 9.07", "priceBand": "10"},
        "volume-gainers-spurts": {"symbol": "ABC", "ltp": "261.99", "volume": "2607269", "pChange": "1.19"},
        "52-week-high": {"symbol": "ABC", "ltp": "101", "new52WHL": "103.5", "pChange": "-0.7"},
    }
    for dataset, row in examples.items():
        records = validate_records({"data": [row]}, ["symbol"], "symbol", dataset_name=dataset)
        assert records[0]["symbol"] == "ABC"
        assert isinstance(records[0]["ltp"], float)


def test_dataset_contracts_reject_malformed_numeric_and_range_values() -> None:
    with pytest.raises(DataValidationError, match="TopGainersLosersRow"):
        validate_records({"data": [{"symbol": "ABC", "ltp": "not-a-price"}]}, ["symbol"], "symbol", dataset_name="top-gainers-losers")
    with pytest.raises(DataValidationError, match="greater than 0"):
        validate_records({"data": [{"symbol": "ABC", "ltp": "0"}]}, ["symbol"], "symbol", dataset_name="volume-gainers-spurts")
    with pytest.raises(DataValidationError, match="less than or equal to 1000"):
        validate_records({"data": [{"symbol": "ABC", "pChange": "1001"}]}, ["symbol"], "symbol", dataset_name="52-week-high")


def test_csv_overwrites_same_trading_day(tmp_path) -> None:
    first = write_csv(tmp_path, "top-gainers-losers", date(2026, 9, 18), [{"symbol": "ABC", "price": 1}])
    write_csv(tmp_path, "top-gainers-losers", date(2026, 9, 18), [{"symbol": "XYZ", "price": 2}])
    assert first.read_text(encoding="utf-8").count("ABC") == 0
    assert "XYZ" in first.read_text(encoding="utf-8")


def test_parquet_and_manifest_are_written_with_hashes(tmp_path) -> None:
    trading_date = date(2026, 9, 18)
    records = [{"symbol": "ABC", "price": 1}]
    parquet = write_parquet(tmp_path, "demo", trading_date, records)
    manifest_path = write_manifest(
        tmp_path,
        trading_date,
        {
            "execution_timestamp": "2026-09-18T00:00:00+00:00",
            "runtime_seconds": 0.25,
            "successful_datasets": 1,
            "failed_datasets": 0,
            "datasets": {"demo": {"success": True, "record_count": 1, "files": {str(parquet): sha256_file(parquet)}}},
        },
    )
    assert parquet.exists()
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["datasets"]["demo"]["record_count"] == 1
    assert sha256_file(parquet) == json.loads(manifest_path.read_text(encoding="utf-8"))["datasets"]["demo"]["files"][str(parquet)]


def test_sqlite_supports_new_columns_on_later_runs(tmp_path) -> None:
    database = write_sqlite(tmp_path, "demo", date(2026, 9, 18), [{"symbol": "ABC", "price": 1}])
    write_sqlite(tmp_path, "demo", date(2026, 9, 19), [{"symbol": "XYZ", "volume": 2}])
    with closing(sqlite3.connect(database)) as connection:
        columns = {row[1] for row in connection.execute('PRAGMA table_info("data_demo")')}
        count = connection.execute('SELECT COUNT(*) FROM data_demo').fetchone()[0]
    assert {"symbol", "price", "volume"}.issubset(columns)
    assert count == 2


def test_sqlite_same_day_rerun_replaces_natural_key(tmp_path) -> None:
    database = write_sqlite(tmp_path, "demo", date(2026, 9, 18), [{"symbol": "ABC", "price": 1}])
    write_sqlite(tmp_path, "demo", date(2026, 9, 18), [{"symbol": "ABC", "price": 2}])
    with closing(sqlite3.connect(database)) as connection:
        rows = connection.execute('SELECT trading_date, symbol, price FROM data_demo').fetchall()
    assert rows == [("2026-09-18", "ABC", "2")]


def test_validation_counts_invalid_and_duplicates_separately() -> None:
    result = validate_records_with_stats(
        {"data": [{"symbol": "AAA", "ltp": 10, "pChange": 1.5}, {"symbol": "AAA", "ltp": 12, "pChange": 2.0}, {"symbol": "BBB", "ltp": "bad", "pChange": 1.0}]},
        ["symbol", "ltp", "pChange"],
        "symbol",
        dataset_name="52-week-high",
    )
    assert result.records_received == 3
    assert result.invalid_records == 1
    assert result.duplicates_dropped == 1
    assert result.records_accepted == 1
    assert result.records_valid == 2
    assert [row["symbol"] for row in result.records] == ["AAA"]


def test_validation_duplicate_only_rows_are_counted_correctly() -> None:
    result = validate_records_with_stats(
        {"data": [{"symbol": "AAA", "ltp": 10, "pChange": 1.5}, {"symbol": "AAA", "ltp": 11, "pChange": 2.0}, {"symbol": "BBB", "ltp": 8, "pChange": -1.0}]},
        ["symbol", "ltp", "pChange"],
        "symbol",
        dataset_name="52-week-high",
    )
    assert result.records_received == 3
    assert result.invalid_records == 0
    assert result.duplicates_dropped == 1
    assert result.records_accepted == 2
    assert result.records_valid == 3


def test_validation_counts_multiple_invalids_and_duplicates() -> None:
    result = validate_records_with_stats(
        {"data": [
            {"symbol": "AAA", "ltp": 10, "pChange": 1.5},
            {"symbol": "AAA", "ltp": 11, "pChange": 2.0},
            {"symbol": "BBB", "ltp": "bad", "pChange": 1.0},
            {"symbol": "CCC", "ltp": 9, "pChange": 0.3},
            {"symbol": "CCC", "ltp": 9.5, "pChange": 0.4},
            {"symbol": "DDD", "ltp": "oops", "pChange": 0.5},
        ]},
        ["symbol", "ltp", "pChange"],
        "symbol",
        dataset_name="52-week-high",
    )
    assert result.records_received == 6
    assert result.invalid_records == 2
    assert result.duplicates_dropped == 2
    assert result.records_accepted == 2
    assert result.records_valid == 4


def test_validation_raises_when_all_rows_are_invalid() -> None:
    with pytest.raises(DataValidationError, match="failed|natural key|no records remained"):
        validate_records_with_stats(
            {"data": [{"symbol": "BBB", "ltp": "bad", "pChange": 1.0}, {"symbol": "CCC", "ltp": "still-bad", "pChange": 2.0}]},
            ["symbol", "ltp", "pChange"],
            "symbol",
            dataset_name="52-week-high",
        )


def test_failed_request_is_reported_without_raising_to_caller() -> None:
    async def scenario() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="server error")

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=0), transport=httpx.MockTransport(handler))
        config = DatasetConfig(landing_url="https://test/landing", api_url="https://test/api", expected_columns=["symbol"], natural_key="symbol")
        try:
            with pytest.raises(AcquisitionError, match="500"):
                await client.fetch(config)
        finally:
            await client.close()

    asyncio.run(scenario())


def test_connection_failure_is_reported() -> None:
    async def scenario() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=0), transport=httpx.MockTransport(handler))
        config = DatasetConfig(landing_url="https://test/landing", api_url="https://test/api", expected_columns=["symbol"], natural_key="symbol")
        try:
            with pytest.raises(AcquisitionError, match="connection refused"):
                await client.fetch(config)
        finally:
            await client.close()

    asyncio.run(scenario())


def test_forbidden_response_refreshes_session_and_retries() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        api_attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal api_attempts
            calls.append(str(request.url))
            if request.url.path == "/landing":
                return httpx.Response(200)
            api_attempts += 1
            return httpx.Response(403) if api_attempts == 1 else httpx.Response(200, json={"data": [{"symbol": "ABC"}]})

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=1), transport=httpx.MockTransport(handler))
        config = DatasetConfig(landing_url="https://test/landing", api_url="https://test/api", expected_columns=["symbol"], natural_key="symbol")
        try:
            payload = await client.fetch(config)
            assert payload["data"][0]["symbol"] == "ABC"
            assert calls == ["https://test/landing", "https://test/api", "https://test/landing", "https://test/api"]
        finally:
            await client.close()

    asyncio.run(scenario())


def test_permanent_http_and_malformed_json_are_not_retried() -> None:
    async def scenario() -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if request.url.path == "/landing":
                return httpx.Response(200)
            return httpx.Response(404) if calls == 2 else httpx.Response(200, text="{")

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=3), transport=httpx.MockTransport(handler))
        config = DatasetConfig(landing_url="https://test/landing", api_url="https://test/api", expected_columns=["symbol"], natural_key="symbol")
        try:
            with pytest.raises(AcquisitionError, match="404"):
                await client.fetch(config)
            with pytest.raises(AcquisitionError, match="malformed JSON"):
                await client.fetch(config)
            assert calls == 3
        finally:
            await client.close()

    asyncio.run(scenario())


def test_timeout_is_retried_then_reported() -> None:
    async def scenario() -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if request.url.path == "/landing":
                return httpx.Response(200)
            raise httpx.ReadTimeout("timed out")

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=1), transport=httpx.MockTransport(handler))
        config = DatasetConfig(landing_url="https://test/landing", api_url="https://test/api", expected_columns=["symbol"], natural_key="symbol")
        try:
            with pytest.raises(AcquisitionError, match="timed out"):
                await client.fetch(config)
            assert calls == 3
        finally:
            await client.close()

    asyncio.run(scenario())


def test_429_and_5xx_responses_are_retried() -> None:
    async def scenario() -> None:
        attempts: dict[str, int] = {"429": 0, "500": 0}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/landing":
                return httpx.Response(200)
            key = request.url.host
            attempts[key] += 1
            if attempts[key] == 1:
                return httpx.Response(429 if key == "429" else 500)
            return httpx.Response(200, json={"data": [{"symbol": "ABC"}]})

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=1), transport=httpx.MockTransport(handler))
        try:
            for host in ("429", "500"):
                config = DatasetConfig(landing_url=f"https://{host}/landing", api_url=f"https://{host}/api", expected_columns=["symbol"], natural_key="symbol")
                payload = await client.fetch(config)
                assert payload["data"][0]["symbol"] == "ABC"
            assert attempts == {"429": 2, "500": 2}
        finally:
            await client.close()

    asyncio.run(scenario())


def test_fetch_all_isolates_dataset_failure() -> None:
    async def scenario() -> None:
        results: list[tuple[str, Exception | None]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/bad":
                return httpx.Response(500)
            return httpx.Response(200, json={"data": [{"symbol": "ABC"}]})

        client = NSEClient(Settings(rate_limit_seconds=0, max_retries=0), transport=httpx.MockTransport(handler))
        configs = {
            "good": DatasetConfig(landing_url="https://test/landing", api_url="https://test/good", expected_columns=["symbol"], natural_key="symbol"),
            "bad": DatasetConfig(landing_url="https://test/landing", api_url="https://test/bad", expected_columns=["symbol"], natural_key="symbol"),
        }

        async def collect(name: str, payload: object, error: Exception | None) -> None:
            results.append((name, error))

        try:
            await fetch_all(client, configs, collect)
        finally:
            await client.close()
        assert {name for name, error in results if error is None} == {"good"}
        assert {name for name, error in results if error is not None} == {"bad"}

    asyncio.run(scenario())


@pytest.mark.network
def test_live_network_placeholder() -> None:
    """Reserved for an opt-in smoke test against NSE."""
