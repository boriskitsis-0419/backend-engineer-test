"""Chunked CSV extraction.

Files are read in bounded chunks so a 20M-row input never has to fit in
memory. Everything is read as string dtype and coerced explicitly during
validation, rather than letting pandas infer types: inference varies between
chunks (a column of integers with one blank becomes float in one chunk and int
in the next), which would silently change what gets written to the database.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from ..logging_config import get_logger
from .spec import EntitySpec

logger = get_logger(__name__)


class MissingSourceFile(FileNotFoundError):
    """Raised when an entity's CSV is not present in the source directory."""


class MissingColumns(ValueError):
    """Raised when a CSV lacks columns the spec requires."""


def source_path(spec: EntitySpec, source_dir: Path) -> Path:
    path = source_dir / spec.filename
    if not path.is_file():
        raise MissingSourceFile(f"{spec.name}: no such file: {path}")
    return path


def peek_header(path: Path) -> list[str]:
    return list(pd.read_csv(path, nrows=0).columns)


def validate_header(spec: EntitySpec, path: Path) -> None:
    """Fail before reading 20M rows if the file is the wrong shape."""
    header = set(peek_header(path))
    required = set(spec.source_columns)
    missing = required - header
    if missing:
        raise MissingColumns(
            f"{spec.name}: {path.name} is missing column(s): {sorted(missing)}"
        )


def read_chunks(
    spec: EntitySpec,
    source_dir: Path,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    """Yield raw chunks with source columns selected and renamed."""
    path = source_path(spec, source_dir)
    validate_header(spec, path)

    reader = pd.read_csv(
        path,
        usecols=list(spec.source_columns),
        dtype=str,
        chunksize=chunk_size,
        keep_default_na=True,
        na_values=[""],
    )

    for chunk in reader:
        if spec.rename:
            chunk = chunk.rename(columns=spec.rename)
        yield chunk


def count_rows(spec: EntitySpec, source_dir: Path) -> int:
    """Row count excluding the header, without loading the file."""
    path = source_path(spec, source_dir)
    with path.open("rb") as fh:
        return max(sum(1 for _ in fh) - 1, 0)
