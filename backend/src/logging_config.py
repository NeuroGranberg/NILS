"""Central logging configuration for backend services."""

from __future__ import annotations

import logging
import logging.config
import os
from typing import Optional


_CONFIGURED = False


def configure_logging(default_level: Optional[str] = None) -> None:
    """Ensure the application logs to stdout with a consistent formatter."""

    global _CONFIGURED
    if _CONFIGURED:
        return

    level_name = (default_level or os.getenv("LOG_LEVEL", "INFO")).upper()
    formatter = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": formatter,
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                }
            },
            "handlers": {
                "stdout": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "stream": "ext://sys.stdout",
                }
            },
            "root": {
                "level": level_name,
                "handlers": ["stdout"],
            },
            "loggers": {
                "uvicorn": {
                    "level": level_name,
                    "handlers": ["stdout"],
                    "propagate": False,
                },
                "uvicorn.error": {
                    "level": level_name,
                    "handlers": ["stdout"],
                    "propagate": False,
                },
                "uvicorn.access": {
                    "level": level_name,
                    "handlers": ["stdout"],
                    "propagate": False,
                },
            },
        }
    )

    _CONFIGURED = True
