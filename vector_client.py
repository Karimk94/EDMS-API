import chromadb
import os
import logging
import api_client
from chromadb.utils import embedding_functions
from chromadb.config import Settings

# --- Logging Setup ---
# Set up logging for this module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ChromaDB Setup (Lazy Initialization) ---
CHROMA_DB_PATH = os.getenv('CHROMA_DB_PATH', './chroma_db')
COLLECTION_NAME = os.getenv('CHROMA_COLLECTION_NAME', 'edms_documents')
DISTANCE_THRESHOLD = 1.3

# Use your separate embedding service
# We create a wrapper class that matches what ChromaDB expects
class ExternalEmbeddingFunction(embedding_functions.EmbeddingFunction):
    def __call__(self, input_texts: chromadb.Documents) -> chromadb.Embeddings:
        embeddings = []
        for text in input_texts:
            try:
                logging.info(f"ExternalEmbeddingFunction: Calling embedding service for text: {text[:70]}...")
                embedding = api_client.get_embedding_from_service(text)
                if embedding:
                    embeddings.append(embedding)
                else:
                    embeddings.append([0.0] * 384) 
            except Exception as e:
                embeddings.append([0.0] * 384)
        return embeddings

# --- Lazy ChromaDB client & collection (initialized on first use, not at import time) ---
_client = None
_collection = None
external_embed_fn = ExternalEmbeddingFunction()

def _get_collection():
    """Lazily initializes and returns the ChromaDB collection. Thread-safe via GIL."""
    global _client, _collection
    if _collection is None:
        logging.info(f"Lazily initializing ChromaDB PersistentClient at path: {CHROMA_DB_PATH}")
        _client = chromadb.PersistentClient(path=CHROMA_DB_PATH, settings=Settings(anonymized_telemetry=False))
        _collection = _client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=external_embed_fn
        )
        logging.info(f"ChromaDB collection '{COLLECTION_NAME}' ready.")
    return _collection

def add_or_update_document(doc_id: int, text_content: str):
    """
    Adds or updates a document in the ChromaDB vector store.
    """
    logging.info(f"VectorClient: Attempting to add/update doc_id {doc_id}...")
    if not text_content:
        logging.warning(f"VectorClient: Skipping vector update for doc_id {doc_id}: no text content provided.")
        return

    doc_id_str = str(doc_id)
    try:
        # We don't need to provide the embedding here.
        # ChromaDB will use the collection's embedding_function (ExternalEmbeddingFunction)
        # to call our service and get the embedding for the document.
        logging.info(f"VectorClient: Calling collection.upsert for doc_id {doc_id_str}...")
        _get_collection().upsert(
            documents=[text_content],
            metadatas=[{"doc_id": doc_id_str, "source": "abstract"}],
            ids=[doc_id_str]
        )
        logging.info(f"VectorClient: Successfully upserted vector for doc_id {doc_id}.")
    except Exception as e:
        logging.error(f"VectorClient: Failed to upsert vector for doc_id {doc_id}: {e}", exc_info=True)


def query_documents(search_term: str, n_results: int = 40) -> list[int] | None:
    """
    Queries the vector store for a search term.
    Returns a list of document IDs, or None if the query fails or results are irrelevant.
    """
    # logging.info(f"VectorClient: Querying for term: '{search_term}' (n_results={n_results})...")
    try:
        results = _get_collection().query(
            query_texts=[search_term],
            n_results=n_results,
            include=['documents', 'distances', 'metadatas']
        )

        doc_ids = results.get('ids', [[]])[0]
        distances = results.get('distances', [[]])[0]

        if not doc_ids:
            # logging.info(f"VectorClient: Vector search for '{search_term}' returned 0 results.")
            return []

        # --- Filter Results by Threshold ---
        valid_doc_ids = []

        # logging.info(f"--- DEBUG: Vector Results for '{search_term}' ---")
        for i in range(len(doc_ids)):
            doc_id = doc_ids[i]
            dist = distances[i]

            # Only keep results that are "close enough"
            if dist <= DISTANCE_THRESHOLD:
                valid_doc_ids.append(int(doc_id))
                # logging.info(f"   [KEEP] Match #{i + 1}: DocID {doc_id} | Distance: {dist}")
            else:
                # Log what we are dropping so you can tune the threshold
                logging.info(f"   [DROP] Match #{i + 1}: DocID {doc_id} | Distance: {dist} (Too far)")

        # logging.info(f"-----------------------------------------------")

        # logging.info(f"VectorClient: Returning {len(valid_doc_ids)} valid matches out of {len(doc_ids)} found.")
        return valid_doc_ids

    except Exception as e:
        logging.error(f"VectorClient: Vector search query failed for term '{search_term}': {e}", exc_info=True)
        return None

def delete_document(doc_id: int):
    """
    Deletes a document from the vector store.
    """
    logging.info(f"VectorClient: Attempting to delete doc_id {doc_id}...")
    doc_id_str = str(doc_id)
    try:
        _get_collection().delete(ids=[doc_id_str])
        logging.info(f"VectorClient: Successfully deleted vector for doc_id {doc_id}.")
    except Exception as e:
        logging.error(f"VectorClient: Failed to delete vector for doc_id {doc_id}: {e}", exc_info=True)