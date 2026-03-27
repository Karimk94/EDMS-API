import oracledb
import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()

# --- Custom Exception ---
class DatabaseConnectionError(Exception):
    """Raised when a database connection cannot be acquired."""
    pass

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Blocklist Loading ---
BLOCKLIST = {}
try:
    # Adjust path to find blocklist.json in the parent directory (root)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    blocklist_path = os.path.join(base_dir, 'blocklist.json')

    with open(blocklist_path, 'r', encoding='utf-8') as f:
        loaded_blocklist = json.load(f)
        # Combine all meaningless words into a single set for efficient lookup
        meaningless_words = set(loaded_blocklist.get('meaningless_english', []))
        meaningless_words.update(loaded_blocklist.get('meaningless_arabic', []))
        BLOCKLIST['meaningless'] = meaningless_words
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.warning(f"Could not load or parse blocklist.json: {e}")

# --- Connection Pool Setup ---
# Pool is created once at module load and reused for all requests.
_sync_pool = None
_async_pool = None

def _get_dsn():
    return f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"

def _get_sync_pool():
    """Lazily creates and returns a synchronous connection pool."""
    global _sync_pool
    if _sync_pool is None:
        user = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        dsn = _get_dsn()
        if not all([user, password, dsn]):
            raise DatabaseConnectionError("Database connection details missing in environment variables.")
        try:
            _sync_pool = oracledb.create_pool(
                user=user, password=password, dsn=dsn,
                min=2, max=10, increment=1,
                timeout=30, getmode=oracledb.POOL_GETMODE_TIMEDWAIT, wait_timeout=10000
            )
        except oracledb.Error as ex:
            error, = ex.args
            raise DatabaseConnectionError(f"Failed to create sync pool: {error.message}")
    return _sync_pool

async def _get_async_pool():
    """Lazily creates and returns an asynchronous connection pool."""
    global _async_pool
    if _async_pool is None:
        user = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        dsn = _get_dsn()
        if not all([user, password, dsn]):
            raise DatabaseConnectionError("Database connection details missing in environment variables.")
        try:
            _async_pool = oracledb.create_pool_async(
                user=user, password=password, dsn=dsn,
                min=2, max=10, increment=1,
                timeout=30, getmode=oracledb.POOL_GETMODE_TIMEDWAIT, wait_timeout=10000
            )
        except oracledb.Error as ex:
            error, = ex.args
            raise DatabaseConnectionError(f"Failed to create async pool: {error.message}")
    return _async_pool

def get_connection():
    """Acquires a connection from the synchronous pool."""
    pool = _get_sync_pool()
    try:
        return pool.acquire()
    except oracledb.Error as ex:
        error, = ex.args
        raise DatabaseConnectionError(f"DB pool acquire error: {error.message} (Code: {error.code})")

async def get_async_connection():
    """Acquires a connection from the asynchronous pool."""
    pool = await _get_async_pool()
    try:
        return await pool.acquire()
    except oracledb.Error as ex:
        error, = ex.args
        raise DatabaseConnectionError(f"Async DB pool acquire error: {error.message} (Code: {error.code})")