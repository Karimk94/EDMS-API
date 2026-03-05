"""
Video cache eviction utility.
Cleans up old cached video files to prevent unbounded disk usage.
Can be run as a background task on app startup or on a schedule.
"""
import os
import time
import logging

# Default: delete files older than 7 days, max cache size 10GB
DEFAULT_MAX_AGE_DAYS = 7
DEFAULT_MAX_CACHE_SIZE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB


def cleanup_video_cache(cache_dir: str, max_age_days: int = DEFAULT_MAX_AGE_DAYS,
                        max_cache_size_bytes: int = DEFAULT_MAX_CACHE_SIZE_BYTES) -> dict:
    """
    Evicts old files from the video cache directory.

    Strategy:
    1. Delete all files older than max_age_days
    2. If total size still exceeds max_cache_size_bytes, delete oldest files until under limit

    Returns a dict with cleanup stats.
    """
    if not os.path.isdir(cache_dir):
        return {"status": "skip", "reason": "Cache directory does not exist"}

    now = time.time()
    max_age_seconds = max_age_days * 86400
    deleted_count = 0
    deleted_bytes = 0
    remaining_files = []

    # Phase 1: Delete files older than max_age_days
    for filename in os.listdir(cache_dir):
        filepath = os.path.join(cache_dir, filename)
        if not os.path.isfile(filepath):
            continue

        file_stat = os.stat(filepath)
        file_age = now - file_stat.st_mtime

        if file_age > max_age_seconds:
            try:
                os.remove(filepath)
                deleted_count += 1
                deleted_bytes += file_stat.st_size
                logging.info(f"Cache eviction: deleted {filename} (age: {file_age / 86400:.1f} days)")
            except OSError as e:
                logging.warning(f"Cache eviction: failed to delete {filename}: {e}")
        else:
            remaining_files.append((filepath, file_stat.st_mtime, file_stat.st_size))

    # Phase 2: If still over size limit, delete oldest files first
    total_remaining = sum(f[2] for f in remaining_files)
    if total_remaining > max_cache_size_bytes:
        # Sort by modification time (oldest first)
        remaining_files.sort(key=lambda x: x[1])

        for filepath, mtime, size in remaining_files:
            if total_remaining <= max_cache_size_bytes:
                break
            try:
                os.remove(filepath)
                deleted_count += 1
                deleted_bytes += size
                total_remaining -= size
                logging.info(f"Cache eviction (size limit): deleted {os.path.basename(filepath)}")
            except OSError as e:
                logging.warning(f"Cache eviction: failed to delete {os.path.basename(filepath)}: {e}")

    return {
        "status": "done",
        "deleted_count": deleted_count,
        "deleted_bytes": deleted_bytes,
        "remaining_bytes": total_remaining if remaining_files else 0
    }
