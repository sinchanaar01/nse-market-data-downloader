import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, ClassVar, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class DataValidationError(ValueError):
    """Raised when an API payload violates a dataset contract."""


@dataclass(frozen=True)
class ValidationResult:
    records: list[dict[str, Any]]
    records_received: int
    duplicates_dropped: int


class DatasetRow(BaseModel):
    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)
    contract_name: ClassVar[str] = "generic"
    symbol: str = Field(min_length=1)


class TopGainersLosersRow(DatasetRow):
    contract_name: ClassVar[str] = "top-gainers-losers"
    category: str | None = None
    high_price: float | None = Field(default=None, gt=0)
    low_price: float | None = Field(default=None, gt=0)
    ltp: float | None = Field(default=None, gt=0)
    net_price: float | None = Field(default=None, ge=-100, le=1000)
    open_price: float | None = Field(default=None, gt=0)
    perChange: float | None = Field(default=None, ge=-100, le=1000)
    prev_price: float | None = Field(default=None, gt=0)
    trade_quantity: int | None = Field(default=None, ge=0)
    turnover: float | None = Field(default=None, ge=0)


class UpperBandHittersRow(DatasetRow):
    contract_name: ClassVar[str] = "upper-band-hitters"
    change: float | None = None
    highPrice: float | None = Field(default=None, gt=0)
    lowPrice: float | None = Field(default=None, gt=0)
    ltp: float | None = Field(default=None, gt=0)
    pChange: float | None = Field(default=None, ge=-100, le=1000)
    priceBand: float | None = Field(default=None, ge=0, le=100)
    totalTradedVol: float | None = Field(default=None, ge=0)
    turnover: float | None = Field(default=None, ge=0)
    yearHigh: float | None = Field(default=None, gt=0)
    yearLow: float | None = Field(default=None, gt=0)


class VolumeGainersSpurtsRow(DatasetRow):
    contract_name: ClassVar[str] = "volume-gainers-spurts"
    ltp: float | None = Field(default=None, gt=0)
    pChange: float | None = Field(default=None, ge=-100, le=1000)
    turnover: float | None = Field(default=None, ge=0)
    volume: int | None = Field(default=None, ge=0)
    week1AvgVolume: int | None = Field(default=None, ge=0)
    week1volChange: float | None = Field(default=None, ge=-100, le=10000)
    week2AvgVolume: int | None = Field(default=None, ge=0)
    week2volChange: float | None = Field(default=None, ge=-100, le=10000)


class FiftyTwoWeekHighRow(DatasetRow):
    contract_name: ClassVar[str] = "52-week-high"
    change: float | None = None
    ltp: float | None = Field(default=None, gt=0)
    new52WHL: float | None = Field(default=None, gt=0)
    pChange: float | None = Field(default=None, ge=-100, le=1000)
    prev52WHL: float | None = Field(default=None, ge=0)
    prevClose: float | None = Field(default=None, ge=0)


CONTRACTS: dict[str, type[DatasetRow]] = {
    TopGainersLosersRow.contract_name: TopGainersLosersRow,
    UpperBandHittersRow.contract_name: UpperBandHittersRow,
    VolumeGainersSpurtsRow.contract_name: VolumeGainersSpurtsRow,
    FiftyTwoWeekHighRow.contract_name: FiftyTwoWeekHighRow,
}


def extract_records(payload: Any, records_path: str) -> list[dict[str, Any]]:
    records: Any = payload
    for part in records_path.split("."):
        if not isinstance(records, dict) or part not in records:
            records = None
            break
        records = records[part]
    if not isinstance(records, list) or not records or not all(isinstance(row, dict) for row in records):
        raise DataValidationError("response does not contain a non-empty list of records")
    return records


def validate_records(
    payload: Any,
    expected_columns: Iterable[str],
    natural_key: str | list[str],
    records_path: str = "data",
    dataset_name: str | None = None,
) -> list[dict[str, Any]]:
    return validate_records_with_stats(payload, expected_columns, natural_key, records_path, dataset_name).records


def validate_records_with_stats(
    payload: Any,
    expected_columns: Iterable[str],
    natural_key: str | list[str],
    records_path: str = "data",
    dataset_name: str | None = None,
) -> ValidationResult:
    if isinstance(payload, list) and all(isinstance(item, tuple) and len(item) == 2 for item in payload):
        payloads = payload
    else:
        payloads = [(payload, None)]
    records_with_categories: list[tuple[dict[str, Any], str | None]] = []
    for request_payload, category in payloads:
        records_with_categories.extend((row, category) for row in extract_records(request_payload, records_path))
    records = [row for row, _ in records_with_categories]
    expected = list(expected_columns)
    natural_keys = [natural_key] if isinstance(natural_key, str) else natural_key
    logger = logging.getLogger(__name__)

    contract: type[DatasetRow]
    if dataset_name is None:
        contract = DatasetRow
    else:
        contract = cast(type[DatasetRow], CONTRACTS.get(dataset_name, DatasetRow))
    seen: set[tuple[str, ...]] = set()
    valid: list[dict[str, Any]] = []
    dropped_reasons: list[str] = []
    for index, (source_row, category) in enumerate(records_with_categories):
        row = dict(source_row)
        if category is not None:
            row["category"] = category
        elif dataset_name == "top-gainers-losers" and "category" in natural_keys:
            row["category"] = "gainer"
        missing_columns = [column for column in expected if column not in row]
        missing_keys = [key for key in natural_keys if key not in row]
        if missing_columns or missing_keys:
            dropped_reasons.append(f"missing expected columns or natural key at record {index}")
            logger.warning("dropping malformed row", extra={"context": {"record_index": index, "missing_columns": missing_columns, "missing_natural_keys": missing_keys}})
            continue
        key_values = [row[key] for key in natural_keys]
        if any(isinstance(value, (dict, list, tuple, set)) for value in key_values):
            dropped_reasons.append(f"natural key '{', '.join(natural_keys)}' must be scalar")
            logger.warning("dropping malformed row", extra={"context": {"record_index": index, "reason": "natural key must be scalar"}})
            continue
        try:
            model = contract.model_validate(row)
        except ValidationError as exc:
            dropped_reasons.append(f"record {index} failed {contract.__name__}: {exc}")
            logger.warning("dropping malformed row", extra={"context": {"record_index": index, "error": str(exc)}})
            continue
        key = tuple(str(value).strip() for value in key_values)
        if any(not value for value in key):
            dropped_reasons.append(f"natural key '{', '.join(natural_keys)}' must not be empty")
            logger.warning("dropping malformed row", extra={"context": {"record_index": index, "reason": "natural key must not be empty"}})
            continue
        if key not in seen:
            seen.add(key)
            valid.append(model.model_dump(mode="python", exclude_none=True))
    if not valid:
        if records and all(any(key not in row for key in natural_keys) for row in records):
            raise DataValidationError(f"all records are missing natural key '{', '.join(natural_keys)}'")
        raise DataValidationError(dropped_reasons[0] if dropped_reasons else "no records remained after validation")
    return ValidationResult(valid, len(records), len(records) - len(valid))
