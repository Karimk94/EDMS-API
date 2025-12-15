import oracledb
import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()

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

def get_connection():
    """Establishes a connection to the Oracle database."""
    try:
        dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"
        # Ensure credentials are being fetched correctly
        user = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        if not all([user, password, dsn]):
            logging.error("Database connection details missing in environment variables.")
            return None
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except oracledb.Error as ex:
        error, = ex.args
        # Log the detailed error
        logging.error(f"DB connection error: {error.message} (Code: {error.code}, Context: {error.context})")
        return None