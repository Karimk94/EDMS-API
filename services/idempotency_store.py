import os
import sqlite3
import logging
from datetime import datetime, timedelta

class IdempotencyStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Detect network paths (UNC \\server\share) — WAL mode is unreliable
        # over SMB/NAS because it relies on shared-memory that doesn't work
        # across network filesystems.  Fall back to DELETE (rollback) journal
        # mode for network databases.
        self._is_network = self.db_path.startswith("\\\\") or self.db_path.startswith("//")
        self._journal_mode = "DELETE" if self._is_network else "WAL"
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        # timeout=30 lets SQLite wait up to 30 s for a lock instead of failing
        # immediately with "database is locked" when multiple workers contend.
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        # busy_timeout tells SQLite how long to retry when another connection
        # holds a lock.  30 000 ms = 30 s.
        conn.execute("PRAGMA busy_timeout=30000;")
        # Journal mode is set once during _initialize(); we don't re-set it on
        # every connection because changing journal mode requires an exclusive
        # lock and causes "database is locked" errors under concurrent access
        # (especially over SMB).  Reading the current mode is lock-free.
        try:
            current = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            if str(current).upper() != self._journal_mode:
                conn.execute(f"PRAGMA journal_mode={self._journal_mode};")
        except sqlite3.OperationalError:
            # If we can't change the journal mode (e.g. another process holds
            # the lock), that's fine — _initialize() already set it at startup.
            pass
        return conn

    def _initialize(self) -> None:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        # Set the journal mode ONCE here with an exclusive connection, then
        # never change it again from _connect().
        try:
            init_conn = sqlite3.connect(self.db_path, timeout=30)
            init_conn.execute("PRAGMA busy_timeout=30000;")
            init_conn.execute(f"PRAGMA journal_mode={self._journal_mode};")
            init_conn.close()
        except sqlite3.OperationalError:
            # Database might be locked by another process during startup;
            # the mode will be set by whichever process wins the race.
            pass
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
# Put the database file alongside the other database files.
# In a multi-node (load-balanced) deployment this path MUST point at a shared
# network location (NAS / SMB share) so that idempotency keys are visible across
# all nodes. Set IDEMPOTENCY_DB_PATH in the environment to override the default.
db_path = os.getenv(
    'IDEMPOTENCY_DB_PATH',
    os.path.join(os.path.dirname(os.path.dirname(db_connector.__file__)), 'database', 'idempotency.sqlite3'),
)
idempotency_store = IdempotencyStore(db_path)
