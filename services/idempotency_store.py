import os
import sqlite3
import logging
from datetime import datetime, timedelta

class IdempotencyStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        # Use WAL mode for better concurrency across workers
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _initialize(self) -> None:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    idempotency_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    doc_number INTEGER,
                    created_at TEXT NOT NULL
                )
                """
            )
            # Cleanup old keys to keep db small
            conn.execute("CREATE INDEX IF NOT EXISTS idx_idempotency_created_at ON idempotency_keys(created_at)")
            conn.commit()
            
    def _cleanup_old_keys(self, conn: sqlite3.Connection):
        try:
            # Delete keys older than 24 hours
            threshold = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            conn.execute("DELETE FROM idempotency_keys WHERE created_at < ?", (threshold,))
        except Exception as e:
            logging.error(f"Error cleaning up old idempotency keys: {e}")

    def claim_key(self, key: str) -> bool:
        """
        Attempts to claim an idempotency key.
        Returns True if successfully claimed, False if it already exists.
        """
        now_iso = datetime.utcnow().isoformat()
        try:
            with self._connect() as conn:
                self._cleanup_old_keys(conn)
                
                existing = conn.execute(
                    "SELECT idempotency_key FROM idempotency_keys WHERE idempotency_key = ?",
                    (key,)
                ).fetchone()
                
                if existing:
                    return False
                    
                conn.execute(
                    """
                    INSERT INTO idempotency_keys (idempotency_key, status, doc_number, created_at)
                    VALUES (?, 'IN_PROGRESS', NULL, ?)
                    """,
                    (key, now_iso)
                )
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            # Handle race condition where another worker inserted it right after our SELECT
            return False
        except Exception as e:
            logging.error(f"Error claiming idempotency key {key}: {e}")
            # If DB fails, we default to True (allow the request) to not break the app entirely
            return True

    def get_result(self, key: str) -> dict:
        """
        Returns the stored result for the key.
        Returns None if key doesn't exist.
        """
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT status, doc_number FROM idempotency_keys WHERE idempotency_key = ?",
                    (key,)
                ).fetchone()
                
                if row:
                    return {"status": row["status"], "doc_number": row["doc_number"]}
                return None
        except Exception as e:
            logging.error(f"Error getting idempotency key {key}: {e}")
            return None

    def mark_success(self, key: str, doc_number: int):
        """
        Marks a key as successful and stores the resulting doc number.
        """
        try:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE idempotency_keys SET status = 'SUCCESS', doc_number = ? WHERE idempotency_key = ?",
                    (doc_number, key)
                )
                conn.commit()
        except Exception as e:
            logging.error(f"Error marking idempotency key {key} success: {e}")

    def mark_failed(self, key: str):
        """
        Removes the key so the client can safely retry from scratch.
        """
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM idempotency_keys WHERE idempotency_key = ?", (key,))
                conn.commit()
        except Exception as e:
            logging.error(f"Error deleting idempotency key {key}: {e}")

    def mark_cancelled(self, key: str):
        """
        Flags an in-progress upload as cancelled.

        This is stored in the shared SQLite database (rather than only in
        process memory) so that a cancel request handled by one worker is
        visible to the worker that is actually streaming the file to the DMS.
        """
        try:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE idempotency_keys SET status = 'CANCELLED' WHERE idempotency_key = ?",
                    (key,)
                )
                conn.commit()
        except Exception as e:
            logging.error(f"Error marking idempotency key {key} cancelled: {e}")

    def is_cancelled(self, key: str) -> bool:
        """Return whether the given key has been flagged as cancelled."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT status FROM idempotency_keys WHERE idempotency_key = ?",
                    (key,)
                ).fetchone()
                return bool(row and row["status"] == "CANCELLED")
        except Exception as e:
            logging.error(f"Error checking cancelled idempotency key {key}: {e}")
            return False

# Global instance
import db_connector

import os
# Put the database file alongside the other database files
db_path = os.path.join(os.path.dirname(os.path.dirname(db_connector.__file__)), 'database', 'idempotency.sqlite3')
idempotency_store = IdempotencyStore(db_path)
