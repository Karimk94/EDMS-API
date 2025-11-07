import oracledb
import os
import sys
import logging
from dotenv import load_dotenv

# --- Setup Environment ---
# Load .env file from the parent directory
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Add parent directory to sys.path to import vector_client
sys.path.append(os.path.dirname(__file__))

import vector_client
import db_connector

# --- Logging Setup ---
# Configure logging to show info-level messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BATCH_SIZE = 100

def get_db_connection():
    """Uses the existing db_connector function to get a connection."""
    try:
        conn = db_connector.get_connection()
        if conn:
            logging.info("Successfully connected to Oracle Database.")
            return conn
        else:
            logging.error("Failed to get Oracle connection.")
            return None
    except Exception as e:
        logging.error(f"Error connecting to Oracle: {e}", exc_info=True)
        return None

def fetch_documents_in_batches(conn, start_docnumber):
    """
    Generator to fetch batches of documents from Oracle.
    """
    last_docnumber = start_docnumber
    
    while True:
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT docnumber, abstract
                FROM PROFILE
                WHERE docnumber > :last_doc
                AND FORM = 2740
                AND abstract IS NOT NULL
                ORDER BY docnumber ASC
                FETCH FIRST :batch_size ROWS ONLY
                """
                logging.info(f"Fetching next batch of {BATCH_SIZE} docs after doc_id {last_docnumber}...")
                cursor.execute(sql, {'last_doc': last_docnumber, 'batch_size': BATCH_SIZE})
                rows = cursor.fetchall()

                if not rows:
                    logging.info("No more documents found to process.")
                    break
                
                logging.info(f"Fetched batch of {len(rows)} documents. Doc IDs from {rows[0][0]} to {rows[-1][0]}.")
                yield rows
                
                # Update last_docnumber with the highest ID from this batch
                last_docnumber = rows[-1][0]
                
        except oracledb.Error as e:
            logging.error(f"Oracle error fetching batch: {e}", exc_info=True)
            # Attempt to re-establish connection if it's a connection error
            if not conn.is_healthy():
                logging.info("DB connection may not be healthy. Attempting to reconnect...")
                conn.close()
                conn = get_db_connection()
                if not conn:
                    logging.critical("DB connection lost and cannot be re-established. Exiting.")
                    break
        except Exception as e:
            logging.error(f"Unexpected error in batch fetch: {e}", exc_info=True)
            break


def main():
    logging.info("--- Starting Vector Backfill Process ---")
    
    # 19677385 is one less than the start number in your queries
    start_docnumber = 19677385 
    logging.info(f"Starting from docnumber > {start_docnumber}")
    
    conn = get_db_connection()
    if not conn:
        return

    total_processed = 0
    try:
        for batch in fetch_documents_in_batches(conn, start_docnumber):
            doc_ids = []
            documents_content = []
            
            for doc_id, abstract in batch:
                if abstract and abstract.strip():
                    doc_ids.append(str(doc_id))
                    documents_content.append(abstract)

            if not doc_ids:
                logging.info(f"Skipping batch starting after {batch[0][0]}, no valid abstracts found.")
                continue

            try:
                # Use the main vector_client's upsert function
                # This will call the embedding service in a batch
                logging.info(f"Indexing batch of {len(doc_ids)} documents (IDs from {doc_ids[0]} to {doc_ids[-1]})...")
                vector_client.collection.upsert(
                    documents=documents_content,
                    ids=doc_ids,
                    metadatas=[{"doc_id": did, "source": "abstract"} for did in doc_ids]
                )
                
                total_processed += len(doc_ids)
                logging.info(f"Successfully indexed batch. Total processed so far: {total_processed}")
                
            except Exception as e:
                logging.error(f"Failed to index batch (IDs from {doc_ids[0]} to {doc_ids[-1]}). Error: {e}", exc_info=True)

    finally:
        if conn:
            conn.close()
            logging.info("Closed Oracle DB connection.")

    logging.info(f"--- Vector Backfill Process Finished. Total documents indexed: {total_processed} ---")

if __name__ == "__main__":
    main()