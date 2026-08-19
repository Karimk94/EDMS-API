import logging
import os
from pathlib import Path
from typing import Any, Iterator


EDOCS_CACHE_RELATIVE_PATH = Path("OpenText") / "DM" / "Cache"

TARGET_ZONE_EXACT_NAMES = {
    "default",
    "metadata",
    "metadataindex",
    "metadataindexes",
    "metadata_index",
    "metadata_indices",
    "index",
    "indexes",
    "indices",
    "temp",
    "temporary",
    "echo",
    "echoes",
    "documentecho",
    "documentechoes",
    "document_echo",
    "document_echoes",
}
TARGET_ZONE_NAME_TOKENS = ("metadata", "index", "echo", "temp", "document")


def resolve_appdata_path(appdata_override: str | Path | None = None) -> Path:
    if appdata_override is not None:
        return Path(appdata_override).expanduser()

    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata).expanduser()

    return Path.home() / "AppData" / "Roaming"


def get_server_edocs_cache_root(appdata_override: str | Path | None = None) -> Path:
    return resolve_appdata_path(appdata_override) / EDOCS_CACHE_RELATIVE_PATH


def run_server_edocs_cache_clear(requested_by: str, user_id: str | None = None) -> dict[str, Any]:
    result = passive_clear_edocs_dm_cache()
    logging.info(
        "Server eDOCS cache clear completed: requested_by=%s user_id=%s result=%s",
        requested_by,
        user_id or "",
        result,
    )
    return result


def passive_clear_edocs_dm_cache(appdata_override: str | Path | None = None) -> dict[str, Any]:
    """
    Passively clear local OpenText eDOCS DM cache files.

    This routine intentionally does not kill processes, close applications, take
    ownership, or force-delete files. Locked files are skipped so active editor
    sessions and unsaved user changes are preserved.
    """
    cache_root = get_server_edocs_cache_root(appdata_override)
    result: dict[str, Any] = {
        "status": "success",
        "cache_root": str(cache_root),
        "cache_root_exists": False,
        "target_roots": [],
        "scanned_directories": 0,
        "deleted_files": 0,
        "skipped_locked_or_in_use": 0,
        "removed_empty_directories": 0,
    }

    try:
        cache_root_exists = cache_root.exists()
    except (PermissionError, OSError):
        result["skipped_locked_or_in_use"] = 1
        return result

    if not cache_root_exists:
        return result

    result["cache_root_exists"] = True

    for target_root in _iter_target_cache_roots(cache_root):
        result["target_roots"].append(str(target_root))
        files_to_delete, directories_to_clean = _collect_cache_tree(target_root)
        result["scanned_directories"] += len(directories_to_clean)

        for file_path in files_to_delete:
            try:
                file_path.unlink()
                result["deleted_files"] += 1
            except (PermissionError, OSError):
                result["skipped_locked_or_in_use"] += 1

        for directory_path in sorted(
            directories_to_clean,
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            if directory_path == target_root:
                continue
            try:
                directory_path.rmdir()
                result["removed_empty_directories"] += 1
            except (PermissionError, OSError):
                pass

    return result


def _iter_target_cache_roots(cache_root: Path) -> Iterator[Path]:
    seen: set[Path] = set()
    default_root = cache_root / "Default"

    for candidate in (default_root,):
        try:
            if candidate.is_dir():
                resolved = candidate.resolve()
                seen.add(resolved)
                yield candidate
        except (PermissionError, OSError):
            continue

    try:
        children = list(cache_root.iterdir())
    except (PermissionError, OSError):
        return

    for child in children:
        try:
            if not child.is_dir() or not _is_target_cache_zone(child):
                continue
            resolved = child.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            yield child
        except (PermissionError, OSError):
            continue


def _is_target_cache_zone(path: Path) -> bool:
    normalized_name = path.name.casefold().replace(" ", "").replace("-", "_")
    return (
        normalized_name in TARGET_ZONE_EXACT_NAMES
        or any(token in normalized_name for token in TARGET_ZONE_NAME_TOKENS)
    )


def _collect_cache_tree(root: Path) -> tuple[list[Path], list[Path]]:
    files: list[Path] = []
    directories: list[Path] = [root]
    stack: list[Path] = [root]

    while stack:
        directory = stack.pop()
        try:
            children = list(directory.iterdir())
        except (PermissionError, OSError):
            continue

        for child in children:
            try:
                if child.is_symlink() or child.is_file():
                    files.append(child)
                    continue
                if child.is_dir():
                    directories.append(child)
                    stack.append(child)
            except (PermissionError, OSError):
                continue

    return files, directories
