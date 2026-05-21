from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from data.services.kline_archive_service import (
    migrate_kline_archive,
    plan_kline_archive_migration,
)


def test_plan_kline_archive_migration_previews_supported_files(tmp_path: Path) -> None:
    source = tmp_path / "market"
    destination = tmp_path / "archive"
    (source / "daily" / "qfq").mkdir(parents=True)
    (source / "daily" / "qfq" / "000001.SZ.parquet").write_text("p", encoding="utf-8")
    (source / "daily" / "qfq" / "note.txt").write_text("x", encoding="utf-8")
    (source / "30m").mkdir()
    (source / "30m" / "000001.SZ.csv").write_text("c", encoding="utf-8")

    plan = plan_kline_archive_migration(
        source_path=source,
        destination_path=destination,
        operation="copy",
        overwrite=False,
    )

    assert list(plan["relative_path"]) == [
        "30m/000001.SZ.csv",
        "daily/qfq/000001.SZ.parquet",
    ]
    assert set(plan["action"]) == {"copy"}
    assert int(plan["file_size_bytes"].sum()) == 2


def test_migrate_kline_archive_moves_files_and_preserves_tree(tmp_path: Path) -> None:
    source = tmp_path / "market"
    destination = tmp_path / "archive"
    source_file = source / "daily" / "qfq" / "000001.SZ.parquet"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("payload", encoding="utf-8")

    result = migrate_kline_archive(
        source_path=source,
        destination_path=destination,
        operation="move",
        overwrite=False,
    )

    target_file = destination / "daily" / "qfq" / "000001.SZ.parquet"
    assert not source_file.exists()
    assert target_file.read_text(encoding="utf-8") == "payload"
    assert result.iloc[0]["status"] == "moved"


def test_kline_archive_rejects_destination_inside_source(tmp_path: Path) -> None:
    source = tmp_path / "market"
    source.mkdir()

    with pytest.raises(ValueError, match="目标目录不能位于源路径内部"):
        plan_kline_archive_migration(
            source_path=source,
            destination_path=source / "archive",
            operation="copy",
        )


def test_plan_kline_archive_marks_existing_target_as_skip_without_overwrite(
    tmp_path: Path,
) -> None:
    source = tmp_path / "market"
    destination = tmp_path / "archive"
    source_file = source / "000001.SZ.parquet"
    target_file = destination / "000001.SZ.parquet"
    source.mkdir()
    destination.mkdir()
    source_file.write_text("new", encoding="utf-8")
    target_file.write_text("old", encoding="utf-8")

    plan = plan_kline_archive_migration(
        source_path=source,
        destination_path=destination,
        operation="copy",
        overwrite=False,
    )

    assert plan.iloc[0]["action"] == "skip_exists"
    assert pd.notna(plan.iloc[0]["target_path"])
