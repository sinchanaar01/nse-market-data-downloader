import json
import logging
from datetime import date
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"), "level": record.levelname, "message": record.getMessage()}
        payload.update(getattr(record, "context", {}))
        return json.dumps(payload, default=str)


def configure_logging(log_dir: Path, trading_date: date) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("nse_downloader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = TimedRotatingFileHandler(log_dir / f"app_{trading_date.isoformat()}.log", when="midnight", backupCount=14, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    console = logging.StreamHandler()
    console.setFormatter(JsonFormatter())
    logger.addHandler(console)
    return logger
