import asyncio
import logging
import os
import sqlite3
from threading import Lock
from datetime import datetime, timedelta
from typing import Dict, List

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


_queue_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "processing_queue.sqlite3")
processing_queue = LocalProcessingQueue(_queue_path)

_worker_state_lock = Lock()
_worker_mode = "paused"  # running | draining | paused


def set_worker_paused(paused: bool) -> bool:
    global _worker_mode
    with _worker_state_lock:
        previous_mode = _worker_mode
        _worker_mode = "paused" if bool(paused) else "running"
    if previous_mode != _worker_mode:
        processing_queue.insert_worker_mode_audit(previous_mode, _worker_mode, actor="system", reason="worker pause toggle")
    return _worker_mode == "paused"


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
    while not stop_event.is_set():
        try:
            mode = get_worker_mode()

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

                await db_connector.update_document_processing_status(**result_data)

                status = int(result_data.get("status", 2))
                attempts = int(result_data.get("attempts", 0))
                error = str(result_data.get("error", ""))

                if status == 3:
                    processing_queue.mark_completed(docnumber)
                elif status == 2 or attempts >= 3:
                    processing_queue.mark_failed(docnumber, error or "Processing failed")
                else:
                    processing_queue.requeue_with_delay(docnumber, attempts=attempts, error=error, delay_seconds=20)

        except Exception as e:
            logging.error(f"Processing queue worker error: {e}", exc_info=True)
            await asyncio.sleep(3)
