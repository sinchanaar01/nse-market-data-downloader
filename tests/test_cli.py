import asyncio
import csv
import json
from argparse import Namespace
from pathlib import Path

import pytest

import main
from src.nse_downloader.config import load_config


class FakeClient:
    def __init__(self, settings: object, logger: object = None) -> None:
        self.settings = settings
        self.logger = logger

    async def close(self) -> None:
        return None


def test_load_config_rejects_invalid_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "invalid.yaml"
    config_path.write_text("settings:\n  max_retries: -1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid configuration"):
        load_config(config_path)


def test_parse_args_rejects_invalid_date(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["main.py", "--date", "18-09-2026"])
    with pytest.raises(SystemExit) as error:
        main.parse_args()
    assert error.value.code == 2


def test_run_isolates_failure_and_writes_manifest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fake_fetch_all(client: FakeClient, datasets: dict[str, object], on_result: object) -> None:
        callback = on_result
        await callback("top-gainers-losers", {"allSec": {"data": [{"symbol": "ABC", "ltp": "10", "perChange": "1.5"}]}}, None)
        await callback("upper-band-hitters", None, RuntimeError("simulated failure"))

    monkeypatch.setattr(main, "NSEClient", FakeClient)
    monkeypatch.setattr(main, "fetch_all", fake_fetch_all)
    args = Namespace(
        config="config/datasets.yaml",
        date="2026-09-18",
        output_dir=str(tmp_path / "data"),
        log_dir=str(tmp_path / "logs"),
        dataset=None,
        sqlite=False,
    )

    result = asyncio.run(main.run(args))

    assert result == 1
    assert (tmp_path / "data" / "top-gainers-losers" / "2026-09-18.csv").exists()
    manifest = json.loads((tmp_path / "data" / "2026-09-18" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["overall_run_status"] == "failed"
    assert manifest["successful_datasets"] == ["top-gainers-losers"]
    assert manifest["failed_datasets"] == ["upper-band-hitters"]
    assert manifest["datasets"]["top-gainers-losers"]["record_count"] == 1
    assert manifest["datasets"]["upper-band-hitters"]["error"] == "simulated failure"
    assert manifest["datasets"]["top-gainers-losers"]["csv_write_status"] == "success"
    assert manifest["datasets"]["top-gainers-losers"]["parquet_write_status"] == "success"
    assert manifest["datasets"]["top-gainers-losers"]["records_received"] == 1
    assert manifest["datasets"]["top-gainers-losers"]["records_accepted"] == 1
    assert manifest["datasets"]["top-gainers-losers"]["duplicates_dropped"] == 0


def test_parquet_failure_does_not_invalidate_csv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fake_fetch_all(client: FakeClient, datasets: dict[str, object], on_result: object) -> None:
        await on_result("volume-gainers-spurts", {"data": [{"symbol": "ABC", "ltp": "10", "volume": "100", "pChange": "1"}]}, None)

    def fail_parquet(*args: object, **kwargs: object) -> Path:
        raise OSError("Parquet engine unavailable")

    monkeypatch.setattr(main, "NSEClient", FakeClient)
    monkeypatch.setattr(main, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(main, "write_parquet", fail_parquet)
    args = Namespace(config="config/datasets.yaml", date="2026-09-18", output_dir=str(tmp_path / "data"), log_dir=str(tmp_path / "logs"), dataset=None, sqlite=False)

    assert asyncio.run(main.run(args)) == 0
    assert (tmp_path / "data" / "volume-gainers-spurts" / "2026-09-18.csv").exists()
    manifest = json.loads((tmp_path / "data" / "2026-09-18" / "manifest.json").read_text(encoding="utf-8"))
    result = manifest["datasets"]["volume-gainers-spurts"]
    assert result["success"] is True
    assert result["csv_write_status"] == "success"
    assert result["parquet_write_status"].startswith("failed:")


def test_top_gainers_losers_csv_contains_both_categories(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fake_fetch_all(client: FakeClient, datasets: dict[str, object], on_result: object) -> None:
        await on_result(
            "top-gainers-losers",
            [
                ({"allSec": {"data": [{"symbol": "GAIN", "ltp": "10", "perChange": "2"}]}}, "gainer"),
                ({"allSec": {"data": [{"symbol": "LOSS", "ltp": "8", "perChange": "-2"}]}}, "loser"),
            ],
            None,
        )

    monkeypatch.setattr(main, "NSEClient", FakeClient)
    monkeypatch.setattr(main, "fetch_all", fake_fetch_all)
    args = Namespace(config="config/datasets.yaml", date="2026-09-18", output_dir=str(tmp_path / "data"), log_dir=str(tmp_path / "logs"), dataset="top-gainers-losers", sqlite=False)

    assert asyncio.run(main.run(args)) == 0
    with (tmp_path / "data" / "top-gainers-losers" / "2026-09-18.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["category"] for row in rows} == {"gainer", "loser"}
