# NSE Market Data Downloader

A production-minded Python pipeline that automatically fetches, validates, and stores CSV data from four NSE market-data pages:

- Top Gainers / Losers
- Upper Band Hitters
- Volume Gainers / Spurts
- 52 Week High — Equity Market

## What This Application Does

Each trading day, this tool pulls structured market data directly from NSE's public endpoints (no manual copy-paste), validates it, and stores it as clean, de-duplicated CSV files — plus an optional local database for historical querying. It's built as a small but real data pipeline: acquisition, validation, storage, and orchestration are separated so each dataset can fail independently without breaking the rest of the run.

## Installation

```bash
git clone <repo-url>
cd nse-market-data-downloader
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
# Optional development tools
pip install -r requirements-dev.txt
```

## How to Run

```bash
# Download all 4 datasets
python main.py

# Download a single dataset
python main.py --dataset top-gainers-losers

# Write CSVs and SQLite together
python main.py --sqlite

# Use a different output location and explicit trading date
python main.py --output-dir ./exports --date 2026-09-18

```

## Automatic Execution

On Linux/macOS, run the downloader every trading day at 09:00 with cron:

```cron
0 9 * * 1-5 cd /path/to/nse-market-data-downloader && /path/to/venv/bin/python main.py >> logs/cron.log 2>&1
```

On Windows Task Scheduler, create a daily task that runs `C:\path\to\venv\Scripts\python.exe` with argument `main.py`, using the project directory as the working directory. This keeps scheduling outside the downloader and lets the operating system handle missed or repeated runs.

## How Data Acquisition Works

NSE's market-data pages are backed by internal JSON APIs, not static HTML tables. NSE also blocks requests that don't look like they came from a real browser session, so acquisition:

1. Opens a session and first hits the relevant NSE landing page to collect the required cookies/headers (session bootstrap).
2. Uses those cookies to call the underlying data API for each dataset.
3. Retries transient failures with exponential backoff + jitter, and refreshes the session if it's been rejected (cookie expiry).
4. Runs all 4 dataset fetches concurrently (async) with a shared `asyncio.Lock`-based rate limiter, so a slow/failing source doesn't hold up the others.

Dataset URLs, expected columns, and output filenames live in `config/datasets.yaml` — never hardcoded in the acquisition logic — so adding a 5th dataset later means adding a config entry, not touching the pipeline.

## Where Files Are Stored

```
data/
  top-gainers-losers/2026-09-18.csv
  top-gainers-losers/2026-09-18.parquet
  upper-band-hitters/2026-09-18.csv
  volume-gainers-spurts/2026-09-18.csv
  52-week-high/2026-09-18.csv
  2026-09-18/manifest.json
data/market_data.db        # optional SQLite store, same records, queryable across days
logs/app_2026-09-18.log     # structured JSON logs, rotated daily
```

Filenames always encode `<dataset-name>/<trading-date>` for both CSV and Parquet outputs. The date-scoped
manifest records execution timing, per-dataset received/accepted counts, duplicates dropped, retry count,
duration, CSV/Parquet/SQLite write statuses, dataset errors, and SHA-256 hashes for generated files.
It also records `overall_run_status` as `success` or `failed`.

## How Errors Are Handled

- Network failure, timeout, non-2xx HTTP, empty body, and malformed JSON are all caught per-dataset, logged with context, and reported in the run summary — they never crash the whole run.
- Each dataset's outcome (success, record count, or failure reason) is logged individually, so one bad source is visible without hiding the other three succeeding.
- A run's overall exit code reflects whether *any* dataset failed, so it's automation/CI-friendly.
- CSV is the required source of truth. Parquet and SQLite are optional outputs; their failures are recorded in the manifest without invalidating a successfully written CSV.

## How Duplicate Data Is Handled

- The output filename already keys on trading date, so re-running the app the same day overwrites/updates that day's file instead of creating `_1`, `_2` copies.
- Before writing, rows are de-duplicated on the dataset's natural key (e.g. symbol), and the dated file is atomically overwritten. This makes a mid-day re-run deterministic and prevents `_1`/duplicate files.

## How Tests Are Run

```bash
pytest                      # full suite
pytest -m "not network"     # skip live-network tests, use recorded fixtures
pytest --cov=src            # with coverage report
```

Covers: successful download, failed request, timeout, 403 session refresh, malformed/empty response, Pydantic contract rejection, natural-key deduplication, file creation, and duplicate-run handling — each against mocked/recorded NSE responses so tests are deterministic and don't depend on NSE being up.

The CI-equivalent quality checks are:

```bash
pytest -m "not network" --cov=src
ruff check .
mypy src main.py
```

The live-network smoke test is marked `network` and is excluded from normal CI.

## Project Structure

```text
main.py                         # CLI orchestration and execution manifest
config/datasets.yaml            # endpoints, response paths, columns, natural keys
src/nse_downloader/acquisition.py  # NSE session, async requests, retries, rate limiting
src/nse_downloader/validation.py   # Pydantic row contracts and deduplication
src/nse_downloader/storage.py      # CSV, Parquet, SQLite, hashes, manifest files
src/nse_downloader/logging_utils.py # daily JSON logging
tests/                           # offline mocked behavior tests
```

## Illustrative Samples

The `sample/` directory contains clearly marked illustrative CSV outputs for all four datasets. They are examples only, not live market data.

## Limitations / Assumptions

- Relies on NSE's current unofficial JSON endpoints and anti-bot cookie behavior; if NSE changes either, acquisition will need updating.
- The dashboard currently compares symbol membership; it does not infer a universal price-movement metric because the four NSE payloads expose different columns.
- SQLite storage runs alongside CSV output for convenience/history; CSVs remain the source of truth if the two ever diverge.

---

## Beyond the Brief: 6 Extra Features

These go past the assignment's own core/bonus list, aimed at demonstrating production judgment rather than just meeting the spec:

1. **NSE Session & Cookie Bootstrapping** — automatically warms up headers and dynamic cookies from the NSE homepage, with auto-refresh logic on `403 Forbidden` responses. This is the single most common reason a "simple" NSE scraper breaks in practice.
2. **Async Concurrent Ingestion with Rate Limiting** — uses `httpx.AsyncClient` and `asyncio.Lock` for high-performance concurrent fetching, bounded by strict rate limits so the app doesn't hammer NSE's servers.
3. **Data Quality Contracts (Pydantic Validation)** — validates configuration and all four dataset row shapes with Pydantic, including numeric coercion, price/percentage sanity ranges, required columns, non-empty values, and natural-key deduplication before saving.
4. **Dual Storage Persistence Engine (CSV + SQLite)** — automatically writes structured date-stamped CSV files while optionally persisting records into an embedded SQLite database, enabling historical querying without giving up plain CSV output.
5. **Production Observability (Structured JSON Logging)** — uses Python's standard logging handlers to produce daily-rotating, queryable logs containing execution metadata and record counts.
6. **Containerized CI/CD Pipeline (Docker + GitHub Actions)** — packages the application with a `Dockerfile` and runs automated `pytest` workflows on every push via GitHub Actions.
