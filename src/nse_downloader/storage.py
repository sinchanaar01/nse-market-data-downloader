import csv
import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


class StorageError(RuntimeError):
    """Raised when a generated output cannot be written safely."""


def write_csv(output_dir: Path, dataset: str, trading_date: date, records: list[dict[str, Any]]) -> Path:
    folder = output_dir / dataset
    destination = folder / f"{trading_date.isoformat()}.csv"
    columns = sorted({key for record in records for key in record})
    temporary = destination.with_suffix(".tmp")
    try:
        folder.mkdir(parents=True, exist_ok=True)
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        temporary.replace(destination)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise StorageError(f"could not write CSV output {destination}: {exc}") from exc
    return destination


def write_parquet(output_dir: Path, dataset: str, trading_date: date, records: list[dict[str, Any]]) -> Path:
    """Write dataset records to a date-stamped Parquet file next to the CSV."""
    folder = output_dir / dataset
    destination = folder / f"{trading_date.isoformat()}.parquet"
    temporary = destination.with_suffix(".parquet.tmp")
    try:
        folder.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(records).to_parquet(temporary, index=False, engine="pyarrow")
        temporary.replace(destination)
    except (ImportError, OSError, ValueError) as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise StorageError(f"could not write Parquet output {destination}: {exc}") from exc
    return destination


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest for a generated file."""
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise StorageError(f"could not hash generated file {path}: {exc}") from exc
    return digest.hexdigest()


def write_manifest(output_dir: Path, trading_date: date, manifest: dict[str, Any]) -> Path:
    """Atomically write the execution manifest in the trading-date directory."""
    folder = output_dir / trading_date.isoformat()
    destination = folder / "manifest.json"
    temporary = destination.with_suffix(".json.tmp")
    try:
        folder.mkdir(parents=True, exist_ok=True)
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(destination)
    except (OSError, TypeError, ValueError) as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise StorageError(f"could not write execution manifest {destination}: {exc}") from exc
    return destination


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def write_sqlite(output_dir: Path, dataset: str, trading_date: date, records: list[dict[str, Any]], natural_key: str | list[str] = "symbol") -> Path:
    database = output_dir / "market_data.db"
    database.parent.mkdir(parents=True, exist_ok=True)
    table = "data_" + dataset.replace("-", "_")
    columns = sorted({key for record in records for key in record})
    natural_keys = [natural_key] if isinstance(natural_key, str) else natural_key
    if any(key not in columns for key in natural_keys):
        raise ValueError(f"natural key is absent from records: {natural_keys}")
    connection = sqlite3.connect(database)
    try:
        with connection:
            definitions = ", ".join(f"{_quote_identifier(column)} TEXT" for column in columns)
            primary_key = ", ".join(_quote_identifier(key) for key in natural_keys)
            connection.execute(f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table)} (trading_date TEXT, {definitions}, PRIMARY KEY (trading_date, {primary_key}))")
            existing_columns = {row[1] for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})")}
            for column in columns:
                if column not in existing_columns:
                    connection.execute(f"ALTER TABLE {_quote_identifier(table)} ADD COLUMN {_quote_identifier(column)} TEXT")
            placeholders = ", ".join("?" for _ in columns)
            names = ", ".join(_quote_identifier(column) for column in columns)
            for record in records:
                values = [trading_date.isoformat(), *[json.dumps(record.get(column)) if isinstance(record.get(column), (dict, list)) else str(record.get(column, "")) for column in columns]]
                connection.execute(f"INSERT OR REPLACE INTO {_quote_identifier(table)} (trading_date, {names}) VALUES (?, {placeholders})", values)
    finally:
        connection.close()
    return database
