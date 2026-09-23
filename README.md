# NSE Market Data Downloader

This project downloads four NSE market-data datasets, validates the returned rows, removes duplicates using each dataset's natural key, and writes CSV output to a date-stamped folder. It also writes an execution manifest and can optionally create Parquet and SQLite outputs.

## Datasets

- Top Gainers / Losers
- Upper Band Hitters
- Volume Gainers / Spurts
- 52 Week High

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate    # Windows
pip install -r requirements-dev.txt
```

`requirements-dev.txt` includes the runtime dependencies plus the development tools needed to run the test suite and quality checks.

## Running

```bash
python main.py
python main.py --dataset top-gainers-losers
python main.py --sqlite
python main.py --output-dir ./exports --date 2026-09-18
```

## Output location

CSV files are written under the project `data/` directory using the pattern:

```text
data/<dataset-name>/<YYYY-MM-DD>.csv
```

The run manifest is written to:

```text
data/<YYYY-MM-DD>/manifest.json
```

Parquet and SQLite output are optional. If they fail, the CSV result remains the source of truth and the manifest records the failure.

## Acquisition process

The downloader loads the dataset configuration from `config/datasets.yaml`, boots the NSE session with the relevant landing page, then requests each API endpoint with a shared rate limiter. Retries use exponential backoff and jitter, and a 403 response triggers a session refresh before retrying.

The four dataset requests are executed asynchronously with a shared rate limiter.

## Error handling

Per-dataset failures are isolated so one bad source does not stop the others. The code handles timeouts, connection errors, HTTP errors, malformed JSON, empty response payloads, and validation failures. A failed dataset is reported in the manifest and logs without aborting the overall run.

## Validation and duplicates

Each dataset is validated against its expected columns and natural key. Rows that are missing required fields, have invalid data, or fail the Pydantic contract are counted as invalid records. Rows that are structurally valid but repeat an existing natural key are counted as duplicates and dropped. Only valid, non-duplicate rows are accepted and written.

Repeated runs for the same trading date overwrite the existing CSV file instead of creating confusing `_1`, `_2` duplicates.

## Tests

The normal test suite uses mocked HTTP responses and does not require live NSE access. The `network` marker is reserved for an optional live NSE smoke test.

```bash
python -m pytest
python -m pytest --cov=src
ruff check .
mypy src main.py
```

## Architecture

- `main.py`: orchestration and manifest creation
- `src/nse_downloader/acquisition.py`: NSE session and HTTP acquisition logic
- `src/nse_downloader/validation.py`: data validation and duplicate detection
- `src/nse_downloader/storage.py`: CSV, Parquet, SQLite, and manifest writing
- `src/nse_downloader/config.py`: dataset configuration loading
- `src/nse_downloader/logging_utils.py`: structured JSON logging
- `config/datasets.yaml`: dataset URLs, expected columns, and natural keys

## Limitations

- The project relies on the current NSE public API layout and session behavior.
- If NSE changes its JSON schema or session requirements, the configuration and acquisition logic may need updates.
- Parquet and SQLite output are optional convenience outputs; CSV is the required source of truth.
