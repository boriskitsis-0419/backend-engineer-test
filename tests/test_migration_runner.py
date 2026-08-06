"""Unit tests for migration discovery. No database required."""

from __future__ import annotations

import pytest

from ecommerce_pipeline.migrations.runner import (
    Migration,
    _verify_checksums,
    discover,
)


def _write(directory, name: str, body: str = "SELECT 1;") -> None:
    (directory / name).write_text(body, encoding="utf-8")


def test_discover_orders_numerically_not_lexicographically(tmp_path):
    # "010" sorts before "9" as a string; discovery must order by parsed int.
    _write(tmp_path, "001_first.sql")
    _write(tmp_path, "009_ninth.sql")
    _write(tmp_path, "010_tenth.sql")

    versions = [m.version for m in discover(tmp_path)]

    assert versions == [1, 9, 10]


def test_discover_ignores_non_migration_files(tmp_path):
    _write(tmp_path, "001_first.sql")
    _write(tmp_path, "notes.sql")
    (tmp_path / "README.md").write_text("ignored", encoding="utf-8")

    assert [m.name for m in discover(tmp_path)] == ["first"]


def test_discover_rejects_duplicate_versions(tmp_path):
    _write(tmp_path, "001_first.sql")
    _write(tmp_path, "001_also_first.sql")

    with pytest.raises(ValueError, match="duplicate migration version 1"):
        discover(tmp_path)


def test_discover_missing_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        discover(tmp_path / "does-not-exist")


def test_checksum_is_content_addressed(tmp_path):
    _write(tmp_path, "001_first.sql", "SELECT 1;")
    original = discover(tmp_path)[0].checksum

    _write(tmp_path, "001_first.sql", "SELECT 2;")
    assert discover(tmp_path)[0].checksum != original


def test_verify_checksums_rejects_edited_migration(tmp_path):
    _write(tmp_path, "001_first.sql", "SELECT 1;")
    migrations = discover(tmp_path)

    # Simulate the file having been applied with different content.
    with pytest.raises(RuntimeError, match="was modified after being applied"):
        _verify_checksums(migrations, {1: "0" * 64})


def test_verify_checksums_passes_for_unchanged_and_new(tmp_path):
    _write(tmp_path, "001_first.sql", "SELECT 1;")
    _write(tmp_path, "002_second.sql", "SELECT 2;")
    migrations = discover(tmp_path)

    # 001 applied and unchanged, 002 not yet applied -> no error.
    _verify_checksums(migrations, {1: migrations[0].checksum})


def test_migration_str_is_zero_padded(tmp_path):
    migration = Migration(7, "add_index", tmp_path / "007_add_index.sql")
    assert str(migration) == "007_add_index"
