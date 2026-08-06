"""Structured logging setup shared by the ETL, the API and the Flyte tasks."""

from __future__ import annotations

import logging
import sys

from .config import get_settings

_CONFIGURED = False

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s | %(message)s"


def configure_logging(level: str | None = None, *, force: bool = False) -> None:
    """Install a single stdout handler on the root logger.

    Idempotent: repeated calls are ignored unless ``force`` is set, so importing
    a module that configures logging never duplicates handlers.
    """
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    resolved = (level or get_settings().log_level).upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S"))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolved)

    # psycopg logs every connection attempt at INFO, which drowns out the ETL
    # progress lines during a bulk load.
    logging.getLogger("psycopg").setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a logger, ensuring logging has been configured first."""
    configure_logging()
    return logging.getLogger(name)
