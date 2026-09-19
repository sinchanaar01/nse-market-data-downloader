import argparse
import asyncio
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.nse_downloader.acquisition import NSEClient, fetch_all
from src.nse_downloader.config import load_config
from src.nse_downloader.logging_utils import configure_logging
from src.nse_downloader.storage import sha256_file, write_csv, write_manifest, write_parquet, write_sqlite
from src.nse_downloader.validation import DataValidationError, extract_records, validate_records_with_stats


async def run(args: argparse.Namespace) -> int:
    started = time.perf_counter()
    execution_timestamp = datetime.now(timezone.utc).isoformat()
    config = load_config(Path(args.config))
    trading_date = date.fromisoformat(args.date) if args.date else date.today()
    output_dir = Path(args.output_dir or os.getenv("NSE_OUTPUT_DIR", "data"))
    logger = configure_logging(Path(args.log_dir), trading_date)
    if args.dataset and args.dataset not in config.datasets:
        raise ValueError(f"dataset '{args.dataset}' is not defined in {args.config}")
    selected = {args.dataset: config.datasets[args.dataset]} if args.dataset else config.datasets
    failures = 0
    results: dict[str, dict[str, Any]] = {}

    async def handle(name: str, payload: Any, error: Exception | None) -> None:
        nonlocal failures
        dataset_config = selected[name]
        retry_count = sum(getattr(client, "retry_counts", {}).get(url, 0) for url in dataset_config.api_urls)
        duration = getattr(client, "durations", {}).get(dataset_config.api_urls[0])
        optional_sqlite_status = "not_enabled" if not args.sqlite else "not_attempted"
        if error:
            failures += 1
            results[name] = {"success": False, "records_received": 0, "records_accepted": 0, "duplicates_dropped": 0, "record_count": 0, "csv_write_status": "not_attempted", "parquet_write_status": "not_attempted", "sqlite_write_status": optional_sqlite_status, "retry_count": retry_count, "duration_seconds": duration, "error": str(error), "files": {}}
            logger.error("dataset failed", extra={"context": {"dataset": name, "success": False, "record_count": 0, "error": str(error)}})
            return
        try:
            try:
                validation = validate_records_with_stats(payload, dataset_config.expected_columns, dataset_config.natural_key, dataset_config.records_path, name)
            except DataValidationError as exc:
                try:
                    received = sum(len(extract_records(item[0], dataset_config.records_path)) for item in payload) if isinstance(payload, list) else len(extract_records(payload, dataset_config.records_path))
                except DataValidationError:
                    received = 0
                raise DataValidationError(f"{exc}; records_received={received}") from exc
            records = validation.records
            csv_path = write_csv(output_dir, name, trading_date, records)
            parquet_path = None
            parquet_error = None
            try:
                parquet_path = write_parquet(output_dir, name, trading_date, records)
            except Exception as exc:
                parquet_error = str(exc)
                logger.warning("optional Parquet output failed", extra={"context": {"dataset": name, "error": parquet_error}})
            sqlite_status = "not_enabled"
            if args.sqlite:
                try:
                    write_sqlite(output_dir, name, trading_date, records, dataset_config.natural_key)
                    sqlite_status = "success"
                except Exception as exc:
                    sqlite_status = f"failed: {exc}"
                    logger.warning("optional SQLite output failed", extra={"context": {"dataset": name, "error": str(exc)}})
            files = {str(csv_path): sha256_file(csv_path)}
            parquet_hash_error = None
            if parquet_path is not None:
                try:
                    files[str(parquet_path)] = sha256_file(parquet_path)
                except Exception as exc:
                    parquet_hash_error = str(exc)
                    logger.warning("optional Parquet checksum failed", extra={"context": {"dataset": name, "error": parquet_hash_error}})
            results[name] = {
                "success": True,
                "records_received": validation.records_received,
                "records_accepted": validation.records_received - validation.duplicates_dropped,
                "duplicates_dropped": validation.duplicates_dropped,
                "record_count": len(records),
                "csv_write_status": "success",
                "parquet_write_status": "success" if parquet_path is not None and parquet_hash_error is None else f"failed: {parquet_error or parquet_hash_error}",
                "sqlite_write_status": sqlite_status,
                "retry_count": retry_count,
                "duration_seconds": duration,
                "error": None,
                "files": files,
            }
            logger.info("dataset downloaded", extra={"context": {"dataset": name, "success": True, "record_count": len(records), "path": str(csv_path)}})
        except Exception as exc:
            failures += 1
            results[name] = {"success": False, "records_received": 0, "records_accepted": 0, "duplicates_dropped": 0, "record_count": 0, "csv_write_status": "failed", "parquet_write_status": "not_attempted", "sqlite_write_status": optional_sqlite_status, "retry_count": retry_count, "duration_seconds": duration, "error": str(exc), "files": {}}
            logger.error("dataset rejected", extra={"context": {"dataset": name, "success": False, "record_count": 0, "error": str(exc)}})

    client = NSEClient(config.settings, logger=logger)
    try:
        await fetch_all(client, selected, handle)
    finally:
        await client.close()
    successful_names = [name for name, result in results.items() if result["success"]]
    failed_names = [name for name, result in results.items() if not result["success"]]
    manifest = {
        "overall_run_status": "failed" if failures else "success",
        "execution_timestamp": execution_timestamp,
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "trading_date": trading_date.isoformat(),
        "successful_datasets": successful_names,
        "failed_datasets": failed_names,
        "successful_dataset_count": len(successful_names),
        "failed_dataset_count": len(failed_names),
        "datasets": results,
    }
    try:
        write_manifest(output_dir, trading_date, manifest)
    except Exception as exc:
        logger.error("manifest failed", extra={"context": {"success": False, "error": str(exc)}})
        return 1
    return 1 if failures else 0


def parse_args() -> argparse.Namespace:
    def parse_date(value: str) -> str:
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("date must use YYYY-MM-DD format") from exc
        return value

    parser = argparse.ArgumentParser(description="Download NSE market datasets")
    parser.add_argument("--dataset", choices=["top-gainers-losers", "upper-band-hitters", "volume-gainers-spurts", "52-week-high"])
    parser.add_argument("--config", default="config/datasets.yaml")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument("--date", type=parse_date, help="Trading date in YYYY-MM-DD format; defaults to today")
    parser.add_argument("--sqlite", action="store_true", help="Also write records to SQLite")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
