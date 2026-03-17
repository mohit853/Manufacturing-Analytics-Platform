"""
etl/utils/logger.py
Centralized logging configuration for Glue PySpark jobs.
Supports CloudWatch-compatible structured log output.
"""

import logging
import sys
import json
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """
    Emits log records as single-line JSON — compatible with
    CloudWatch Logs Insights queries.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
            "module":    record.module,
            "line":      record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str = "manufacturing_etl", level: int = logging.INFO) -> logging.Logger:
    """
    Return a logger that writes structured JSON to stdout.
    Safe to call multiple times — won't add duplicate handlers.

    Usage:
        from etl.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Job started")
        logger.warning("Null values detected", extra={"count": 42})
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate Glue logs)
    logger.propagate = False

    return logger


def get_job_logger(job_name: str) -> logging.Logger:
    """
    Convenience wrapper that prefixes the logger name with the Glue job name.
    Call once at job entry point.

        logger = get_job_logger(args["JOB_NAME"])
    """
    logger = get_logger(f"glue.{job_name}")
    logger.info(f"Logger initialised for job: {job_name}")
    return logger