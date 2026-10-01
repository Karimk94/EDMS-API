import asyncio
import logging
import os
import sqlite3
import time
from threading import Lock
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import db_connector
from services.processor import process_document

# Task timeout configuration (seconds)
DEFAULT_TASK_TIMEOUT_SECONDS = int(os.getenv('PROCESSING_TASK_TIMEOUT_SECONDS', '300'))  # 5 minutes


class LocalProcessingQueue:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _initialize(self) -> None:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processing_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    docnumber INTEGER NOT NULL UNIQUE,
                    source TEXT,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    available_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_processing_jobs_pick ON processing_jobs(status, available_at, updated_at)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_mode_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    previous_mode TEXT,
                    new_mode TEXT NOT NULL,
                    actor TEXT,
                    reason TEXT,
                    changed_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def enqueue_docnumbers(self, docnumbers: List[int], source: str = "api") -> Dict[str, int]:
        now_iso = datetime.utcnow().isoformat()
        inserted = 0
        requeued = 0
        skipped = 0

        with self._connect() as conn:
            for raw_doc in docnumbers:
                try:
                    docnumber = int(raw_doc)
                except (TypeError, ValueError):
                    skipped += 1
                    continue

                existing = conn.execute(
                    "SELECT status FROM processing_jobs WHERE docnumber = ?",
                    (docnumber,),
                ).fetchone()

                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO processing_jobs (
                            docnumber, source, status, attempts, last_error,
                            available_at, created_at, updated_at
                        ) VALUES (?, ?, 'queued', 0, NULL, ?, ?, ?)
                        """,
                        (docnumber, source, now_iso, now_iso, now_iso),
                    )
                    inserted += 1
                    continue

                current_status = str(existing["status"]).lower()
                if current_status in ["completed", "failed"]:
                    conn.execute(
                        """
                        UPDATE processing_jobs
                        SET status = 'queued',
                            available_at = ?,
                            updated_at = ?,
                            last_error = NULL
                        WHERE docnumber = ?
                        """,
                        (now_iso, now_iso, docnumber),
                    )
                    requeued += 1
                else:
                    skipped += 1

            conn.commit()

        return {"inserted": inserted, "requeued": requeued, "skipped": skipped}

    def claim_jobs(self, limit: int = 3) -> List[int]:
        now_iso = datetime.utcnow().isoformat()
        claimed: List[int] = []

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id, docnumber
                FROM processing_jobs
                WHERE status = 'queued' AND available_at <= ?
                ORDER BY updated_at ASC, id ASC
                LIMIT ?
                """,
                (now_iso, max(1, limit)),
            ).fetchall()

            for row in rows:
                conn.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'in_progress',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now_iso, int(row["id"])),
                )
                claimed.append(int(row["docnumber"]))

            conn.commit()

        return claimed

    def mark_completed(self, docnumber: int) -> None:
        now_iso = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'completed',
                    updated_at = ?,
                    last_error = NULL
                WHERE docnumber = ?
                """,
                (now_iso, int(docnumber)),
            )
            conn.commit()

    def mark_failed(self, docnumber: int, error: str) -> None:
        now_iso = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'failed',
                    updated_at = ?,
                    last_error = ?
                WHERE docnumber = ?
                """,
                (now_iso, (error or "")[:2000], int(docnumber)),
            )
            conn.commit()

    def requeue_with_delay(self, docnumber: int, attempts: int, error: str = "", delay_seconds: int = 20) -> None:
        now = datetime.utcnow()
        now_iso = now.isoformat()
        available_at = (now + timedelta(seconds=max(1, delay_seconds))).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'queued',
                    attempts = ?,
                    available_at = ?,
                    updated_at = ?,
                    last_error = ?
                WHERE docnumber = ?
                """,
                (max(0, int(attempts)), available_at, now_iso, (error or "")[:2000], int(docnumber)),
            )
            conn.commit()

    def get_status_summary(self) -> Dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM processing_jobs GROUP BY status"
            ).fetchall()

        summary = {
            "queued": 0,
            "in_progress": 0,
            "completed": 0,
            "failed": 0,
            "total": 0,
        }
        for row in rows:
            status = str(row["status"]).lower()
            count = int(row["count"])
            if status in summary:
                summary[status] = count
            summary["total"] += count

        return summary

    def get_recent_failures(self, limit: int = 10) -> List[Dict[str, str]]:
        return self.get_recent_jobs_by_status(status="failed", limit=limit)

    def get_recent_jobs_by_status(self, status: str, limit: int = 10) -> List[Dict[str, str]]:
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in ["queued", "in_progress", "completed", "failed"]:
            return []

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT docnumber, attempts, last_error, updated_at, status
                FROM processing_jobs
                WHERE status = ?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (normalized_status, max(1, min(50, int(limit)))),
            ).fetchall()

        failures: List[Dict[str, str]] = []
        for row in rows:
            failures.append(
                {
                    "docnumber": int(row["docnumber"]),
                    "attempts": int(row["attempts"]),
                    "error": str(row["last_error"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                    "status": str(row["status"] or normalized_status),
                }
            )
        return failures

    def get_failed_docnumbers(self, limit: int = 500) -> List[int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT docnumber
                FROM processing_jobs
                WHERE status = 'failed'
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, min(2000, int(limit))),),
            ).fetchall()
        return [int(row["docnumber"]) for row in rows]

    def retry_failed_jobs(self, docnumbers: List[int]) -> Dict[str, int]:
        if not docnumbers:
            return {"requeued": 0}

        now_iso = datetime.utcnow().isoformat()
        requeued = 0
        with self._connect() as conn:
            for raw_doc in docnumbers:
                try:
                    docnumber = int(raw_doc)
                except (TypeError, ValueError):
                    continue

                updated = conn.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'queued',
                        attempts = 0,
                        available_at = ?,
                        updated_at = ?,
                        last_error = NULL
                    WHERE docnumber = ?
                    """,
                    (now_iso, now_iso, docnumber),
                ).rowcount
                if updated:
                    requeued += 1

            conn.commit()

        return {"requeued": requeued}

    def purge_completed_jobs(self, older_than_hours: int = 24) -> Dict[str, int]:
        safe_hours = max(0, int(older_than_hours))
        cutoff_iso = (datetime.utcnow() - timedelta(hours=safe_hours)).isoformat()

        with self._connect() as conn:
            deleted = conn.execute(
                """
                DELETE FROM processing_jobs
                WHERE status = 'completed'
                  AND updated_at <= ?
                """,
                (cutoff_iso,),
            ).rowcount
            conn.commit()

        return {"deleted": int(deleted), "older_than_hours": safe_hours}

    def insert_worker_mode_audit(self, previous_mode: str, new_mode: str, actor: str = "system", reason: str = "") -> None:
        now_iso = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO worker_mode_audit (previous_mode, new_mode, actor, reason, changed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    (previous_mode or "").strip().lower() or None,
                    (new_mode or "").strip().lower(),
                    (actor or "system")[:120],
                    (reason or "")[:500],
                    now_iso,
                ),
            )
            conn.commit()

    def get_last_worker_mode_change(self) -> Dict[str, str]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT previous_mode, new_mode, actor, reason, changed_at
                FROM worker_mode_audit
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        if not row:
            return {}

        return {
            "previous_mode": str(row["previous_mode"] or ""),
            "new_mode": str(row["new_mode"] or ""),
            "actor": str(row["actor"] or ""),
            "reason": str(row["reason"] or ""),
            "changed_at": str(row["changed_at"] or ""),
        }

    def persist_worker_mode(self, mode: str) -> None:
        """Persist the worker mode so manual pause/resume survives restarts."""
        now_iso = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO worker_state (key, value, updated_at) VALUES ('worker_mode', ?, ?)",
                ((mode or "").strip().lower(), now_iso),
            )
            conn.commit()

    def load_persisted_worker_mode(self) -> Optional[str]:
        """Load the persisted worker mode, or None if not set."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT value FROM worker_state WHERE key = 'worker_mode'"
                ).fetchone()
            return str(row["value"]).strip().lower() if row else None
        except Exception:
            return None

    def recover_stale_in_progress_jobs(self, stale_after_seconds: Optional[int] = None) -> int:
        """Requeue jobs stuck in 'in_progress' (e.g., after a crash/restart).

        Only recovers jobs whose updated_at is older than the threshold so that
        legitimately in-flight jobs are not disturbed.
        """
        if stale_after_seconds is None:
            stale_after_seconds = DEFAULT_TASK_TIMEOUT_SECONDS + 60
        cutoff_iso = (datetime.utcnow() - timedelta(seconds=max(1, int(stale_after_seconds)))).isoformat()
        now_iso = datetime.utcnow().isoformat()
        with self._connect() as conn:
            updated = conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'queued',
                    available_at = ?,
                    updated_at = ?,
                    last_error = 'Recovered: was stuck in_progress (restart/timeout)'
                WHERE status = 'in_progress' AND updated_at <= ?
                """,
                (now_iso, now_iso, cutoff_iso),
            ).rowcount
            conn.commit()
        return int(updated or 0)


# The processing queue is a SQLite database. In a multi-node (load-balanced)
# deployment this path MUST point at a shared network location (NAS / SMB share)
# so that a job enqueued on one node is visible to the worker on the other node.
# Set PROCESSING_QUEUE_DB_PATH in the environment to override the default local path.
_queue_path = os.getenv(
    'PROCESSING_QUEUE_DB_PATH',
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "processing_queue.sqlite3"),
)
processing_queue = LocalProcessingQueue(_queue_path)

_worker_state_lock = Lock()


def _resolve_initial_worker_mode() -> str:
    """Determine the worker mode at startup.

    Priority:
      1. Persisted mode (so an admin's manual pause/resume survives restarts).
      2. PROCESSING_WORKER_MODE env var.
      3. Default "running" so queued documents are actually processed.

    The previous hard-coded default of "paused" meant that every application
    restart left the worker paused, silently stranding queued documents.
    """
    try:
        persisted = processing_queue.load_persisted_worker_mode()
        if persisted in ("running", "paused"):
            return persisted
        # 'draining' is transient; if we restarted mid-drain, resume normal work.
    except Exception:
        logging.exception("Failed to load persisted worker mode; falling back to env/default.")

    env_mode = os.getenv("PROCESSING_WORKER_MODE", "running").strip().lower()
    return "paused" if env_mode == "paused" else "running"


def _persist_mode_safe(mode: str) -> None:
    try:
        processing_queue.persist_worker_mode(mode)
    except Exception:
        logging.exception(f"Failed to persist worker mode '{mode}'.")


# running | draining | paused  (persisted across restarts; defaults to running)
_worker_mode = _resolve_initial_worker_mode()


def set_worker_paused(paused: bool) -> bool:
    global _worker_mode
    with _worker_state_lock:
        previous_mode = _worker_mode
        _worker_mode = "paused" if bool(paused) else "running"
        new_mode = _worker_mode
    _persist_mode_safe(new_mode)
    if previous_mode != new_mode:
        processing_queue.insert_worker_mode_audit(previous_mode, new_mode, actor="system", reason="worker pause toggle")
    return new_mode == "paused"


def request_worker_drain() -> str:
    global _worker_mode
    with _worker_state_lock:
        previous_mode = _worker_mode
        if _worker_mode != "paused":
            _worker_mode = "draining"
    if previous_mode != _worker_mode:
        processing_queue.insert_worker_mode_audit(previous_mode, _worker_mode, actor="system", reason="worker drain requested")
    return _worker_mode


def set_worker_mode(new_mode: str, actor: str = "system", reason: str = "") -> str:
    global _worker_mode
    normalized_mode = str(new_mode or "running").strip().lower()
    if normalized_mode not in ["running", "draining", "paused"]:
        normalized_mode = "running"

    with _worker_state_lock:
        previous_mode = _worker_mode
        _worker_mode = normalized_mode

    # Persist non-transient modes so the decision survives restarts.
    if normalized_mode in ("running", "paused"):
        _persist_mode_safe(normalized_mode)

    if previous_mode != _worker_mode:
        processing_queue.insert_worker_mode_audit(previous_mode, _worker_mode, actor=actor, reason=reason)
    return _worker_mode


def get_worker_mode() -> str:
    with _worker_state_lock:
        return _worker_mode


def is_worker_paused() -> bool:
    return get_worker_mode() == "paused"


async def processing_worker_loop(stop_event: asyncio.Event):
    """Continuously claims queued jobs and processes them with existing processor logic."""
    # One-time startup recovery: requeue jobs stranded in 'in_progress' by a prior
    # crash/restart so they are not stuck forever.
    try:
        recovered = processing_queue.recover_stale_in_progress_jobs()
        if recovered:
            logging.info(f"Startup recovery: requeued {recovered} stale in_progress job(s).")
    except Exception:
        logging.exception("Startup recovery of stale in_progress jobs failed.")

    last_stale_sweep = time.monotonic()

    while not stop_event.is_set():
        try:
            mode = get_worker_mode()

            # --- Multi-node guard: only the leader node claims/ processes jobs ---
            # In a load-balanced deployment, follower nodes must NOT pull jobs
            # from the shared queue, otherwise the same document would be
            # processed by multiple nodes simultaneously.
            try:
                from utils.leader_election import leader_election
                if not leader_election.is_leader():
                    await asyncio.sleep(2)
                    continue
            except Exception:
                # If leader election cannot be imported/evaluated, do NOT block
                # processing (single-node deployments must keep working).
                pass

            # Periodic safety sweep: requeue any job stuck in_progress longer than
            # the task timeout (guards against hard crashes while running).
            if time.monotonic() - last_stale_sweep >= 60:
                last_stale_sweep = time.monotonic()
                try:
                    swept = processing_queue.recover_stale_in_progress_jobs()
                    if swept:
                        logging.info(f"Recovered {swept} stale in_progress job(s) during sweep.")
                except Exception:
                    logging.exception("Periodic stale-job sweep failed.")

            if mode == "paused":
                await asyncio.sleep(1)
                continue

            if mode == "draining":
                # Graceful drain: do not claim new jobs. If nothing is in progress, switch to paused.
                summary = processing_queue.get_status_summary()
                if int(summary.get("in_progress", 0)) <= 0:
                    set_worker_mode("paused", actor="system", reason="drain completed")
                await asyncio.sleep(1)
                continue

            claimed_docnumbers = processing_queue.claim_jobs(limit=3)
            if not claimed_docnumbers:
                await asyncio.sleep(2)
                continue

            docs = await db_connector.get_specific_documents_for_processing(claimed_docnumbers)
            docs_by_id = {int(doc.get("docnumber")): doc for doc in docs}

            dms_session_token = db_connector.dms_system_login()
            if not dms_session_token:
                for docnumber in claimed_docnumbers:
                    processing_queue.requeue_with_delay(docnumber, attempts=0, error="DMS auth failed", delay_seconds=30)
                await asyncio.sleep(3)
                continue

            for docnumber in claimed_docnumbers:
                doc = docs_by_id.get(int(docnumber))
                if not doc:
                    processing_queue.mark_failed(docnumber, "Document not found for processing")
                    continue

                try:
                    # Enforce task timeout to prevent hung tasks from blocking the queue
                    result_data = await asyncio.wait_for(
                        process_document(doc, dms_session_token),
                        timeout=DEFAULT_TASK_TIMEOUT_SECONDS
                    )
                except asyncio.TimeoutError:
                    logging.warning(f"Processing task TIMEOUT for docnumber {docnumber} after {DEFAULT_TASK_TIMEOUT_SECONDS}s")
                    processing_queue.mark_failed(
                        docnumber,
                        f"Task timeout after {DEFAULT_TASK_TIMEOUT_SECONDS} seconds"
                    )
                    continue
                except Exception as task_error:
                    logging.error(f"Processing task ERROR for docnumber {docnumber}: {task_error}", exc_info=True)
                    # Requeue with delay instead of immediate fail for transient errors
                    processing_queue.requeue_with_delay(
                        docnumber,
                        attempts=doc.get('attempts', 0) + 1,
                        error=str(task_error)[:200],
                        delay_seconds=20
                    )
                    continue

                db_updated = await db_connector.update_document_processing_status(**result_data)

                status = int(result_data.get("status", 2))
                attempts = int(result_data.get("attempts", 0))
                error = str(result_data.get("error", ""))

                if not db_updated:
                    # Oracle status write failed; requeue so the doc is not stranded
                    # as 'pending' upstream while the local job is closed.
                    logging.error(f"Oracle status update FAILED for docnumber {docnumber}; requeueing.")
                    processing_queue.requeue_with_delay(
                        docnumber,
                        attempts=attempts,
                        error="DB status update failed; will retry",
                        delay_seconds=30
                    )
                    continue

                if status == 3:
                    processing_queue.mark_completed(docnumber)
                elif status == 2 or attempts >= 3:
                    processing_queue.mark_failed(docnumber, error or "Processing failed")
                else:
                    processing_queue.requeue_with_delay(docnumber, attempts=attempts, error=error, delay_seconds=20)

        except Exception as e:
            logging.error(f"Processing queue worker error: {e}", exc_info=True)
            await asyncio.sleep(3)
