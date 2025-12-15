import oracledb
import logging
import math
import os
from database.connection import get_connection
from database.media import (
    dms_system_login,
    get_media_info_from_dms,
    get_media_content_from_dms,
    create_thumbnail,
    thumbnail_cache_dir
)

def get_events(page=1, page_size=20, search=None, fetch_all=False):
    """
    Fetches a paginated list of events.
    If fetch_all is True, it fetches all events.
    Otherwise, it fetches only events with associated documents, including thumbnails.
    """
    conn = get_connection()
    if not conn:
        logging.error("Failed to get DB connection in get_events.")
        return [], 0

    dst = None
    if not fetch_all:
        dst = dms_system_login()
        if not dst:
            logging.error("Could not log into DMS in get_events. Cannot fetch thumbnails.")

    events_dict = {}
    total_rows = 0
    offset = (page - 1) * page_size
    base_params = {}
    where_clauses = []

    if search:
        where_clauses.append("UPPER(e.EVENT_NAME) LIKE :search_term")
        base_params['search_term'] = f"%{search.upper()}%"
        logging.debug(f"Search term applied: {base_params['search_term']}")

    final_where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    try:
        with conn.cursor() as cursor:
            if fetch_all:
                count_query = f"SELECT COUNT(e.SYSTEM_ID) FROM LKP_PHOTO_EVENT e {final_where_clause}"
                cursor.execute(count_query, base_params)
                count_result = cursor.fetchone()
                total_rows = count_result[0] if count_result else 0
                total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1

                if total_rows > 0 and offset < total_rows:
                    fetch_query = f"""
                        SELECT e.SYSTEM_ID, e.EVENT_NAME
                        FROM LKP_PHOTO_EVENT e
                        {final_where_clause}
                        ORDER BY e.SYSTEM_ID DESC
                        OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
                    """
                    fetch_params = base_params.copy()
                    fetch_params['offset'] = offset
                    fetch_params['page_size'] = page_size
                    cursor.execute(fetch_query, fetch_params)

                    events_list = [{"id": row[0], "name": row[1], "thumbnail_urls": []} for row in cursor.fetchall()]
                    return events_list, total_rows
                else:
                    return [], 0

            else:
                # This is the existing logic for fetching events with documents
                count_query = f"""
                    SELECT COUNT(DISTINCT e.SYSTEM_ID)
                    FROM LKP_PHOTO_EVENT e
                    JOIN LKP_EVENT_DOC de ON e.SYSTEM_ID = de.EVENT_ID AND de.DISABLED = '0'
                    {final_where_clause}
                """
                logging.debug(f"Executing count query: {count_query} with params: {base_params}")
                cursor.execute(count_query, base_params)
                count_result = cursor.fetchone()
                total_rows = count_result[0] if count_result else 0
                logging.debug(f"Total events with documents found: {total_rows}")

                total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1

                if total_rows > 0 and offset < total_rows:
                    fetch_query = f"""
                        WITH EventDocsRanked AS (
                            SELECT
                                e.SYSTEM_ID AS event_id,
                                e.EVENT_NAME,
                                de.DOCNUMBER,
                                p.RTADOCDATE, -- Use RTADOCDATE for ordering documents within an event
                                ROW_NUMBER() OVER(PARTITION BY e.SYSTEM_ID ORDER BY p.RTADOCDATE DESC, de.DOCNUMBER DESC) as rn
                            FROM LKP_PHOTO_EVENT e
                            JOIN LKP_EVENT_DOC de ON e.SYSTEM_ID = de.EVENT_ID AND de.DISABLED = '0'
                            JOIN PROFILE p ON de.DOCNUMBER = p.DOCNUMBER -- Join PROFILE to get RTADOCDATE
                            {final_where_clause}
                        ),
                        PaginatedEvents AS (
                            SELECT DISTINCT event_id, EVENT_NAME
                            FROM EventDocsRanked
                            ORDER BY event_id DESC
                            OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
                        )
                        SELECT
                            pe.event_id,
                            pe.EVENT_NAME,
                            edr.DOCNUMBER
                        FROM PaginatedEvents pe
                        JOIN EventDocsRanked edr ON pe.event_id = edr.event_id
                        WHERE edr.rn <= 4 -- Limit to top 4 documents per event
                        ORDER BY pe.event_id DESC, edr.rn ASC -- Ensure docs are ordered correctly for processing
                    """
                    fetch_params = base_params.copy()
                    fetch_params['offset'] = offset
                    fetch_params['page_size'] = page_size

                    logging.debug(f"Executing fetch query: {fetch_query} with params: {fetch_params}")
                    cursor.execute(fetch_query, fetch_params)

                    for event_id, event_name, doc_id in cursor.fetchall():
                        if event_id not in events_dict:
                            events_dict[event_id] = {
                                "id": event_id,
                                "name": event_name,
                                "doc_ids": [],
                                "thumbnail_urls": []
                            }
                        if len(events_dict[event_id]["doc_ids"]) < 4:
                            events_dict[event_id]["doc_ids"].append(doc_id)

                    logging.debug(f"Fetched details for {len(events_dict)} events on page {page}.")

                    if dst:
                        for event_id in events_dict:
                            for doc_id in events_dict[event_id]["doc_ids"]:
                                thumbnail_path = None
                                try:
                                    cached_thumbnail_file = f"{doc_id}.jpg"
                                    cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                                    if os.path.exists(cached_path):
                                        thumbnail_path = f"cache/{cached_thumbnail_file}"
                                    else:
                                        _, media_type, file_ext = get_media_info_from_dms(dst, doc_id)
                                        media_bytes = get_media_content_from_dms(dst, doc_id)
                                        if media_bytes:
                                            thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)
                                        else:
                                            logging.warning(f"Could not retrieve media content for doc {doc_id} to create event thumbnail.")

                                    if thumbnail_path:
                                        events_dict[event_id]["thumbnail_urls"].append(thumbnail_path)
                                    else:
                                        events_dict[event_id]["thumbnail_urls"].append("")

                                except Exception as thumb_e:
                                    logging.error(f"Error processing thumbnail for event doc {doc_id}: {thumb_e}", exc_info=True)
                                    events_dict[event_id]["thumbnail_urls"].append("")

                    events_list = []
                    for event_id in sorted(events_dict.keys(), reverse=True):
                        del events_dict[event_id]["doc_ids"]
                        events_list.append(events_dict[event_id])

                    return events_list, total_rows

                else:
                    return [], 0

    except oracledb.Error as e:
        error_obj, = e.args
        logging.error(f"Oracle Error fetching events: {error_obj.message} (Code: {error_obj.code})", exc_info=True)
        return [], 0
    except Exception as e:
        logging.error(f"Unexpected error fetching events: {e}", exc_info=True)
        return [], 0
    finally:
        if conn:
            try:
                conn.close()
            except oracledb.Error as close_e:
                logging.error(f"Error closing DB connection in get_events: {close_e}")

def create_event(event_name):
    """Creates a new event or returns the ID if it already exists."""
    conn = get_connection()
    if not conn:
        return None, "Database connection failed."
    try:
        with conn.cursor() as cursor:
            # Check if event already exists (case-insensitive)
            cursor.execute("SELECT SYSTEM_ID FROM LKP_PHOTO_EVENT WHERE UPPER(EVENT_NAME) = UPPER(:event_name)", event_name=event_name.strip())
            result = cursor.fetchone()
            if result:
                 # Return existing event ID if found
                 existing_event_id = result[0]
                 # logging.info(f"Event '{event_name}' already exists with ID {existing_event_id}.")
                 return existing_event_id, "Event with this name already exists."

            # Get next SYSTEM_ID
            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PHOTO_EVENT")
            system_id = cursor.fetchone()[0]

            # Insert new event
            cursor.execute("INSERT INTO LKP_PHOTO_EVENT (SYSTEM_ID, EVENT_NAME, LAST_UPDATE, DISABLED) VALUES (:1, :2, SYSDATE, 0)",
                           [system_id, event_name.strip()]) # Trim name before insert
            conn.commit()
            # logging.info(f"Event '{event_name}' created successfully with ID {system_id}.")
            return system_id, "Event created successfully."
    except oracledb.Error as e:
        conn.rollback()
        logging.error(f"Database error creating event '{event_name}': {e}", exc_info=True)
        return None, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def link_document_to_event(doc_id, event_id):
    """Links a document to an event, replacing any existing link for that document."""
    conn = get_connection()
    if not conn:
        logging.error(f"DB connection error linking event for doc {doc_id}")
        return False, "Database connection failed."

    try:
        with conn.cursor() as cursor:
            # Check if document exists
            # logging.info(f"Checking existence for DOCNUMBER = {doc_id}")
            cursor.execute("SELECT 1 FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            doc_exists = cursor.fetchone() is not None
            if not doc_exists:
                logging.warning(f"Document check failed: DOCNUMBER = {doc_id} not found.")
                return False, f"Document with ID {doc_id} not found."
            # logging.info(f"Document check passed for DOCNUMBER = {doc_id}")

            # If event_id is provided, check if it exists and is enabled
            if event_id is not None:
                # logging.info(f"Checking existence for EVENT_ID = {event_id}")
                cursor.execute("SELECT 1 FROM LKP_PHOTO_EVENT WHERE SYSTEM_ID = :1", [event_id])
                event_exists = cursor.fetchone() is not None
                if not event_exists:
                    logging.warning(f"Event check failed: EVENT_ID = {event_id} not found or disabled.")
                    return False, f"Event with ID {event_id} not found or is disabled."
                # logging.info(f"Event check passed for EVENT_ID = {event_id}")


            # Use MERGE to insert or update the link
            # logging.info(f"Executing MERGE for DOCNUMBER={doc_id}, EVENT_ID={event_id}")
            # --- Revised MERGE statement with standard NULL checks ---
            merge_sql = """
            MERGE INTO LKP_EVENT_DOC de
            USING (SELECT :doc_id AS docnumber FROM dual) src ON (de.DOCNUMBER = src.docnumber)
            WHEN MATCHED THEN
                UPDATE SET
                    de.EVENT_ID = :event_id,
                    de.LAST_UPDATE = SYSDATE,
                    de.DISABLED = CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END
                -- Revised WHERE clause using standard NULL comparisons explicitly
                WHERE
                    -- Condition 1: EVENT_ID needs update (handles NULLs explicitly)
                    (
                        (de.EVENT_ID IS NULL AND :event_id IS NOT NULL) OR
                        (de.EVENT_ID IS NOT NULL AND :event_id IS NULL) OR
                        (de.EVENT_ID != :event_id)
                    )
                    -- Condition 2: DISABLED status needs update (handles NULLs explicitly, assumes de.DISABLED could be NULL)
                    OR
                    (
                        (de.DISABLED IS NULL AND (CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END) IS NOT NULL) OR
                        (de.DISABLED IS NOT NULL AND (CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END) IS NULL) OR
                        (de.DISABLED != (CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END))
                    )
            WHEN NOT MATCHED THEN
                INSERT (SYSTEM_ID, DOCNUMBER, EVENT_ID, LAST_UPDATE, DISABLED)
                VALUES (
                    (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EVENT_DOC),
                    :doc_id,
                    :event_id,
                    SYSDATE,
                    '0' -- When inserting a new link, it should not be disabled
                )
            """
            # --- End Revised MERGE statement ---

            cursor.execute(merge_sql, {'doc_id': doc_id, 'event_id': event_id})

            rows_affected = cursor.rowcount
            conn.commit()

            action = "linked to" if event_id is not None else "unlinked from"
            event_desc = f"event {event_id}" if event_id is not None else "any event"
            return True, f"Document event link updated successfully."

    except oracledb.Error as e:
        error_obj, = e.args
        logging.error(f"Oracle error linking document {doc_id} to event {event_id}: {error_obj.message}", exc_info=True)
        try:
            conn.rollback()
        except oracledb.Error:
             logging.error(f"Failed to rollback transaction for doc {doc_id} event link.")
        return False, f"Database error occurred: {error_obj.message}"
    except Exception as e:
        logging.error(f"Unexpected error linking document {doc_id} to event {event_id}: {e}", exc_info=True)
        try:
            conn.rollback()
        except Exception:
             pass
        return False, "An unexpected server error occurred."
    finally:
        if conn:
            try:
                conn.close()
            except oracledb.Error:
                logging.error(f"Error closing DB connection after linking event for doc {doc_id}.")

def get_event_for_document(doc_id):
    """Fetches the currently linked event ID and name for a document."""
    conn = get_connection()
    if not conn:
        logging.error(f"DB connection error fetching event for doc {doc_id}")
        return None

    event_info = None
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT e.SYSTEM_ID, e.EVENT_NAME
                FROM LKP_EVENT_DOC de
                JOIN LKP_PHOTO_EVENT e ON de.EVENT_ID = e.SYSTEM_ID
                WHERE de.DOCNUMBER = :doc_id AND de.DISABLED = '0' AND (e.DISABLED = '0' OR e.DISABLED IS NULL)
            """
            cursor.execute(query, doc_id=doc_id)
            result = cursor.fetchone()
            if result:
                event_info = {"event_id": result[0], "event_name": result[1]}
                logging.debug(f"Found event {result[1]} (ID: {result[0]}) linked to doc {doc_id}")
            else:
                 logging.debug(f"No active event link found for doc {doc_id}")

    except oracledb.Error as e:
        error_obj, = e.args
        logging.error(f"Oracle error fetching event for doc {doc_id}: {error_obj.message}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected error fetching event for doc {doc_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return event_info

def get_documents_for_event(event_id, page=1, page_size=1):
    """Fetches paginated documents linked to a specific event."""
    conn = get_connection()
    if not conn:
        logging.error(f"DB connection error fetching documents for event {event_id}")
        return [], 0, "Database connection failed."

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0
    error_message = None

    try:
        with conn.cursor() as cursor:
            # Count total documents for the event
            count_query = """
                SELECT COUNT(de.DOCNUMBER)
                FROM LKP_EVENT_DOC de
                JOIN LKP_PHOTO_EVENT e ON de.EVENT_ID = e.SYSTEM_ID
                WHERE de.EVENT_ID = :event_id AND de.DISABLED = '0' AND e.DISABLED = '0'
            """
            cursor.execute(count_query, event_id=event_id)
            count_result = cursor.fetchone()
            total_rows = count_result[0] if count_result else 0

            if total_rows == 0:
                return [], 0, None # No documents found

            # Fetch paginated document details
            # Order by RTADOCDATE descending, then DOCNUMBER as fallback
            fetch_query = """
                SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.RTADOCDATE as DOC_DATE, p.DOCNAME
                FROM PROFILE p
                JOIN LKP_EVENT_DOC de ON p.DOCNUMBER = de.DOCNUMBER
                WHERE de.EVENT_ID = :event_id AND de.DISABLED = '0'
                ORDER BY p.RTADOCDATE DESC NULLS LAST, p.DOCNUMBER DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            cursor.execute(fetch_query, event_id=event_id, offset=offset, page_size=page_size)
            rows = cursor.fetchall()

            dst = dms_system_login() # Login to DMS to get media type

            for row in rows:
                doc_id, abstract, author, doc_date, docname = row
                media_type = 'image' # Default
                is_favorite = False # Need user context to determine this, default to false for event view

                if dst:
                    try:
                        _, media_type, _ = get_media_info_from_dms(dst, doc_id)
                         # Could add favorite check here if user_id is passed
                    except Exception as e:
                        logging.error(f"Error getting media info for event doc {doc_id}: {e}")

                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or "", # Use abstract as title here
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": doc_date.strftime('%Y-%m-%d %H:%M:%S') if doc_date else "N/A",
                    "thumbnail_url": f"cache/{doc_id}.jpg", # Assume thumbnail exists for simplicity in this view
                    "media_type": media_type,
                    "is_favorite": is_favorite
                })

            total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
            return documents, total_pages, None

    except oracledb.Error as e:
        error_obj, = e.args
        error_message = f"Oracle error fetching event documents: {error_obj.message}"
        logging.error(f"{error_message} (Code: {error_obj.code})", exc_info=True)
        return [], 0, error_message
    except Exception as e:
        error_message = f"Unexpected error fetching event documents: {e}"
        logging.error(error_message, exc_info=True)
        return [], 0, error_message
    finally:
        if conn:
            conn.close()