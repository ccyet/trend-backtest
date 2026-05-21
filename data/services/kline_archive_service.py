from __future__ import annotations

from pathlib import Path
import shutil
from typing import Literal

import pandas as pd


ARCHIVE_COLUMNS = [
    "relative_path",
    "source_path",
    "target_path",
    "file_size_bytes",
    "operation",
    "action",
    "status",
]
SUPPORTED_KLINE_SUFFIXES = frozenset({".parquet", ".csv", ".xlsx", ".xls"})
ArchiveOperation = Literal["copy", "move"]


def _empty_archive_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(ARCHIVE_COLUMNS))


def _normalize_operation(operation: str) -> ArchiveOperation:
    normalized = str(operation).strip().lower()
    if normalized not in {"copy", "move"}:
        raise ValueError("operation 只能是 copy 或 move。")
    return normalized  # type: ignore[return-value]


def _resolve_archive_paths(
    source_path: str | Path, destination_path: str | Path
) -> tuple[Path, Path]:
    source = Path(source_path).expanduser().resolve()
    destination = Path(destination_path).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(f"源路径不存在：{source}")
    if destination == source or destination.is_relative_to(source):
        raise ValueError("目标目录不能位于源路径内部。")
    return source, destination


def _iter_kline_files(source: Path) -> list[Path]:
    if source.is_file():
        return [source] if source.suffix.lower() in SUPPORTED_KLINE_SUFFIXES else []
    return sorted(
        path
        for path in source.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_KLINE_SUFFIXES
    )


def _relative_file_path(source: Path, file_path: Path) -> Path:
    if source.is_file():
        return Path(file_path.name)
    return file_path.relative_to(source)


def plan_kline_archive_migration(
    *,
    source_path: str | Path,
    destination_path: str | Path,
    operation: str,
    overwrite: bool = False,
) -> pd.DataFrame:
    source, destination = _resolve_archive_paths(source_path, destination_path)
    normalized_operation = _normalize_operation(operation)

    records: list[dict[str, object]] = []
    for file_path in _iter_kline_files(source):
        relative_path = _relative_file_path(source, file_path)
        target_path = destination / relative_path
        action = normalized_operation
        if target_path.exists() and not overwrite:
            action = "skip_exists"
        records.append(
            {
                "relative_path": relative_path.as_posix(),
                "source_path": str(file_path),
                "target_path": str(target_path),
                "file_size_bytes": int(file_path.stat().st_size),
                "operation": normalized_operation,
                "action": action,
                "status": "planned",
            }
        )

    if not records:
        return _empty_archive_frame()
    return pd.DataFrame(records, columns=pd.Index(ARCHIVE_COLUMNS))


def migrate_kline_archive(
    *,
    source_path: str | Path,
    destination_path: str | Path,
    operation: str,
    overwrite: bool = False,
) -> pd.DataFrame:
    plan = plan_kline_archive_migration(
        source_path=source_path,
        destination_path=destination_path,
        operation=operation,
        overwrite=overwrite,
    )
    if plan.empty:
        return plan

    result = plan.copy()
    for index, row in result.iterrows():
        action = str(row["action"])
        if action == "skip_exists":
            result.at[index, "status"] = "skipped_exists"
            continue

        source = Path(str(row["source_path"]))
        target = Path(str(row["target_path"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and overwrite:
            target.unlink()

        if action == "move":
            shutil.move(str(source), str(target))
            result.at[index, "status"] = "moved"
        else:
            shutil.copy2(source, target)
            result.at[index, "status"] = "copied"

    return result.loc[:, ARCHIVE_COLUMNS].reset_index(drop=True)
