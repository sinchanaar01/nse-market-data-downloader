from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class DatasetRequest(BaseModel):
    api_url: str
    category: str | None = None


class DatasetConfig(BaseModel):
    landing_url: str
    requests: list[DatasetRequest] = Field(min_length=1)
    api_url: str | None = None
    expected_columns: list[str] = Field(min_length=1)
    natural_key: str | list[str]
    records_path: str = "data"

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_request(cls, value: object) -> object:
        if isinstance(value, dict) and "requests" not in value and "api_url" in value:
            value = dict(value)
            value["requests"] = [{"api_url": value["api_url"]}]
        return value

    @property
    def api_urls(self) -> list[str]:
        return [request.api_url for request in self.requests]

    @field_validator("expected_columns", "natural_key", "records_path")
    @classmethod
    def values_are_not_blank(cls, value: list[str] | str) -> list[str] | str:
        values = value if isinstance(value, list) else [value]
        if any(not item.strip() for item in values):
            raise ValueError("column names must not be blank")
        return value


class Settings(BaseModel):
    request_timeout_seconds: float = Field(default=30, gt=0)
    max_retries: int = Field(default=3, ge=0, le=10)
    rate_limit_seconds: float = Field(default=0.25, ge=0)
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"


class AppConfig(BaseModel):
    datasets: dict[str, DatasetConfig] = Field(min_length=1)
    settings: Settings = Field(default_factory=Settings)


def load_config(path: Path) -> AppConfig:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return AppConfig.model_validate(raw)
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise ValueError(f"Invalid configuration {path}: {exc}") from exc
