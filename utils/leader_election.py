"""
Lightweight leader-election for multi-node (load-balanced) deployments.

Problem
-------
When the API runs on two (or more) nodes behind a load balancer, certain
background work must run on EXACTLY ONE node at a time:

  * the document processing worker loop (`processing_worker_loop`)
  * the periodic video-cache cleanup (`periodic_cache_cleanup`)
  * the periodic log cleanup (`periodic_log_cleanup`)

If every node ran these, the same queued document would be processed twice,
and cache/log cleanup would race.

Solution
--------
A file-based lease on the SHARED storage (the same NAS/SMB mount that holds
chroma_db, the SQLite queue, and the file caches). The lease is a small JSON
file (`worker_lease.json`) written atomically. The node holding a non-expired
lease is the "leader" and is allowed to run background work.

Two operating modes are supported, controlled by the `WORKER_ROLE` env var:

  * WORKER_ROLE=leader    -> this node ALWAYS runs the background work.
  * WORKER_ROLE=follower  -> this node NEVER runs the background work.
  * WORKER_ROLE=auto      -> (default) the nodes negotiate the lease via the
                             shared lease file. Whichever node renews the lease
                             first becomes leader; if the leader dies, the
                             other node takes over automatically.
"""
import json
import os
import time
import uuid
import logging
import threading

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment) ---
_DEFAULT_LEASE_DIR = os.getenv('CHROMA_DB_PATH', './chroma_db')
LEASE_DIR = os.getenv('WORKER_LEASE_DIR', _DEFAULT_LEASE_DIR)
LEASE_FILE = os.getenv('WORKER_LEASE_FILE', os.path.join(LEASE_DIR, 'worker_lease.json'))

LEASE_TTL = int(os.getenv('WORKER_LEASE_TTL', '30'))            # seconds before lease is stale
LEASE_RENEW_INTERVAL = int(os.getenv('WORKER_LEASE_RENEW_INTERVAL', '10'))

_NODE_ID = f"{os.getenv('COMPUTERNAME', os.getenv('HOSTNAME', 'node'))}-{uuid.uuid4().hex[:8]}"

_ROLE = os.getenv('WORKER_ROLE', 'auto').strip().lower()
if _ROLE not in ('leader', 'follower', 'auto'):
    _ROLE = 'auto'


def _now() -> float:
    return time.time()


def _read_lease() -> dict | None:
    """Read the current lease file, or None if it doesn't exist / is corrupt."""
    try:
        with open(LEASE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Leader election: could not read lease file {LEASE_FILE}: {e}")
        return None


def _write_lease(data: dict) -> bool:
    """Atomically write the lease file. Returns True on success."""
    try:
        os.makedirs(os.path.dirname(LEASE_FILE) or '.', exist_ok=True)
        tmp_path = f"{LEASE_FILE}.{_NODE_ID}.tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        os.replace(tmp_path, LEASE_FILE)
        return True
    except OSError as e:
        logger.warning(f"Leader election: could not write lease file {LEASE_FILE}: {e}")
        return False


def _try_acquire_or_renew() -> bool:
    """
    Attempt to acquire the lease (if none/stale) or renew it (if we own it).
    Returns True if, after this call, THIS node is the leader.
    """
    now = _now()
    current = _read_lease()

    if current is None:
        return _write_lease({
            'node_id': _NODE_ID,
            'acquired_at': now,
            'expires_at': now + LEASE_TTL,
        })

    owner = current.get('node_id')
    expires_at = current.get('expires_at', 0)

    if owner == _NODE_ID:
        return _write_lease({
            'node_id': _NODE_ID,
            'acquired_at': current.get('acquired_at', now),
            'expires_at': now + LEASE_TTL,
        })

    if expires_at < now:
        logger.info(
            f"Leader election: previous lease (node={owner}) expired; "
            f"this node ({_NODE_ID}) is taking over."
        )
        return _write_lease({
            'node_id': _NODE_ID,
            'acquired_at': now,
            'expires_at': now + LEASE_TTL,
        })

    return False


class LeaderElection:
    """
    Tracks whether this node is currently the leader, renewing the lease on a
    background thread when in 'auto' mode.

    Usage:
        election = LeaderElection()
        election.start()                # starts the renewal thread (auto mode)
        if election.is_leader():        # check before doing background work
            ...run worker...
        election.stop()                 # call on shutdown to release the lease
    """

    def __init__(self, role: str = _ROLE):
        self.role = role
        self._is_leader = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        if role == 'leader':
            self._is_leader = True
        elif role == 'follower':
            self._is_leader = False

    def is_leader(self) -> bool:
        with self._lock:
            return self._is_leader

    def start(self) -> None:
        """Start the background renewal thread (only meaningful in 'auto' mode)."""
        if self.role != 'auto':
            logger.info(f"Leader election: role='{self.role}', is_leader={self._is_leader} (static, no renewal thread).")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._renew_loop, daemon=True, name="leader-election")
        self._thread.start()
        logger.info(f"Leader election: role='auto', node_id={_NODE_ID}, lease file={LEASE_FILE}, renewal thread started.")

    def stop(self) -> None:
        """Stop the renewal thread and, if we are the leader, release the lease."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

        if self.is_leader() and self.role == 'auto':
            current = _read_lease()
            if current and current.get('node_id') == _NODE_ID:
                try:
                    os.remove(LEASE_FILE)
                    logger.info("Leader election: released lease on shutdown.")
                except OSError:
                    pass

    def _renew_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                acquired = _try_acquire_or_renew()
                with self._lock:
                    prev = self._is_leader
                    self._is_leader = acquired
                    if acquired and not prev:
                        logger.info(f"Leader election: this node ({_NODE_ID}) BECAME the leader.")
                    elif not acquired and prev:
                        logger.info(f"Leader election: this node ({_NODE_ID}) LOST leadership to another node.")
            except Exception as e:
                logger.warning(f"Leader election: renewal error: {e}")

            self._stop_event.wait(LEASE_RENEW_INTERVAL)


# Module-level singleton used by app.py
leader_election = LeaderElection()

