import oracledb
import logging
import math
import os
from database.connection import get_async_connection
from database.media import (
    dms_system_login,
    get_media_info_from_dms,
    get_media_content_from_dms,
    create_thumbnail,
    thumbnail_cache_dir
)

async def get_events(page=1, page_size=20, search=None, fetch_all=False):
    """Fetches a paginated list of events."""
    conn = await get_async_connection()
    if not conn:
        logging.error("Failed to get DB connection in get_events.")
        return [], 0

    dst = None
    if not fetch_all:
        dst = dms_system_login()  # Sync call, assume user accepts blocking
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

    final_where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    try:
        async with conn.cursor() as cursor:
            if fetch_all:
                count_query = f"SELECT COUNT(e.SYSTEM_ID) FROM LKP_PHOTO_EVENT e {final_where_clause}"
                await cursor.execute(count_query, base_params)
                count_result = await cursor.fetchone()
                total_rows = count_result[0] if count_result else 0

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
                    await cursor.execute(fetch_query, fetch_params)

                    rows = await cursor.fetchall()
                    events_list = [{"id": row[0], "name": row[1], "thumbnail_urls": []} for row in rows]
                    return events_list, total_rows
                else:
                    return [], 0

            else:
                count_query = f"""
                    SELECT COUNT(DISTINCT e.SYSTEM_ID)
                    FROM LKP_PHOTO_EVENT e
                    JOIN LKP_EVENT_DOC de ON e.SYSTEM_ID = de.EVENT_ID AND de.DISABLED = '0'
                    {final_where_clause}
                """
                await cursor.execute(count_query, base_params)
                count_result = await cursor.fetchone()
                total_rows = count_result[0] if count_result else 0

                if total_rows > 0 and offset < total_rows:
                    fetch_query = f"""
                        WITH EventDocsRanked AS (
                            SELECT
                                e.SYSTEM_ID AS event_id,
                                e.EVENT_NAME,
                                de.DOCNUMBER,
                                p.RTADOCDATE,
                                ROW_NUMBER() OVER(PARTITION BY e.SYSTEM_ID ORDER BY p.RTADOCDATE DESC, de.DOCNUMBER DESC) as rn
                            FROM LKP_PHOTO_EVENT e
                            JOIN LKP_EVENT_DOC de ON e.SYSTEM_ID = de.EVENT_ID AND de.DISABLED = '0'
                            JOIN PROFILE p ON de.DOCNUMBER = p.DOCNUMBER
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
                        WHERE edr.rn <= 4
                        ORDER BY pe.event_id DESC, edr.rn ASC
                    """
                    fetch_params = base_params.copy()
                    fetch_params['offset'] = offset
                    fetch_params['page_size'] = page_size

                    await cursor.execute(fetch_query, fetch_params)
                    rows = await cursor.fetchall()

                    for event_id, event_name, doc_id in rows:
                        if event_id not in events_dict:
                            events_dict[event_id] = {
                                "id": event_id,
                                "name": event_name,
                                "doc_ids": [],
                                "thumbnail_urls": []
                            }
                        if len(events_dict[event_id]["doc_ids"]) < 4:
                            events_dict[event_id]["doc_ids"].append(doc_id)

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
                                        # Async call to get media info
                                        _, media_type, file_ext = await get_media_info_from_dms(dst, doc_id)
                                        # Sync call for content
                                        media_bytes = get_media_content_from_dms(dst, doc_id)
                                        if media_bytes:
                                            thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)
                                        else:
                                            logging.warning(
                                                f"Could not retrieve media content for doc {doc_id} to create event thumbnail.")

                                    if thumbnail_path:
                                        events_dict[event_id]["thumbnail_urls"].append(thumbnail_path)
                                    else:
                                        events_dict[event_id]["thumbnail_urls"].append("")

                                except Exception as thumb_e:
                                    logging.error(f"Error processing thumbnail for event doc {doc_id}: {thumb_e}",
                                                  exc_info=True)
                                    events_dict[event_id]["thumbnail_urls"].append("")

                    events_list = []
                    for event_id in sorted(events_dict.keys(), reverse=True):
                        del events_dict[event_id]["doc_ids"]
                        events_list.append(events_dict[event_id])

                    return events_list, total_rows
                else:
                    return [], 0

    except oracledb.Error as e:
        logging.error(f"Oracle Error fetching events: {e}", exc_info=True)
        return [], 0
    finally:
        if conn:
            await conn.close()

async def create_event(event_name):
    """Creates a new event or returns the ID if it already exists."""
    conn = await get_async_connection()
    if not conn:
        return None, "Database connection failed."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM LKP_PHOTO_EVENT WHERE UPPER(EVENT_NAME) = UPPER(:event_name)",
                                 event_name=event_name.strip())
            result = await cursor.fetchone()
            if result:
                return result[0], "Event with this name already exists."

            await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PHOTO_EVENT")
            row = await cursor.fetchone()
            system_id = row[0]

            await cursor.execute(
                "INSERT INTO LKP_PHOTO_EVENT (SYSTEM_ID, EVENT_NAME, LAST_UPDATE, DISABLED) VALUES (:1, :2, SYSDATE, 0)",
                [system_id, event_name.strip()])
            await conn.commit()
            return system_id, "Event created successfully."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        logging.error(f"Database error creating event: {e}", exc_info=True)
        return None, f"Database error: {e}"
    finally:
        if conn:
            await conn.close()

async def link_document_to_event(doc_id, event_id):
    """Links a document to an event, replacing any existing link for that document."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed."

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            if await cursor.fetchone() is None:
                return False, f"Document with ID {doc_id} not found."

            if event_id is not None:
                await cursor.execute("SELECT 1 FROM LKP_PHOTO_EVENT WHERE SYSTEM_ID = :1", [event_id])
                if await cursor.fetchone() is None:
                    return False, f"Event with ID {event_id} not found or is disabled."

            merge_sql = """
            MERGE INTO LKP_EVENT_DOC de
            USING (SELECT :doc_id AS docnumber FROM dual) src ON (de.DOCNUMBER = src.docnumber)
            WHEN MATCHED THEN
                UPDATE SET
                    de.EVENT_ID = :event_id,
                    de.LAST_UPDATE = SYSDATE,
                    de.DISABLED = CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END
                WHERE
                    ((de.EVENT_ID IS NULL AND :event_id IS NOT NULL) OR (de.EVENT_ID IS NOT NULL AND :event_id IS NULL) OR (de.EVENT_ID != :event_id))
                    OR
                    ((de.DISABLED IS NULL AND (CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END) IS NOT NULL) OR (de.DISABLED IS NOT NULL AND (CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END) IS NULL) OR (de.DISABLED != (CASE WHEN :event_id IS NULL THEN '1' ELSE '0' END)))
            WHEN NOT MATCHED THEN
                INSERT (SYSTEM_ID, DOCNUMBER, EVENT_ID, LAST_UPDATE, DISABLED)
                VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EVENT_DOC), :doc_id, :event_id, SYSDATE, '0')
            """
            await cursor.execute(merge_sql, {'doc_id': doc_id, 'event_id': event_id})
            await conn.commit()
            return True, f"Document event link updated successfully."

    except oracledb.Error as e:
        if conn: await conn.rollback()
        logging.error(f"Oracle error linking document {doc_id} to event {event_id}: {e}", exc_info=True)
        return False, f"Database error occurred: {e}"
    finally:
        if conn:
            await conn.close()

async def get_event_for_document(doc_id):
    """Fetches the currently linked event ID and name for a document."""
    conn = await get_async_connection()
    if not conn: return None

    event_info = None
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT e.SYSTEM_ID, e.EVENT_NAME
                FROM LKP_EVENT_DOC de
                JOIN LKP_PHOTO_EVENT e ON de.EVENT_ID = e.SYSTEM_ID
                WHERE de.DOCNUMBER = :doc_id AND de.DISABLED = '0' AND (e.DISABLED = '0' OR e.DISABLED IS NULL)
            """
            await cursor.execute(query, doc_id=doc_id)
            result = await cursor.fetchone()
            if result:
                event_info = {"event_id": result[0], "event_name": result[1]}
    except oracledb.Error as e:
        logging.error(f"Oracle error fetching event for doc {doc_id}: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
    return event_info

async def get_documents_for_event(event_id, page=1, page_size=1):
    """Fetches paginated documents linked to a specific event."""
    conn = await get_async_connection()
    if not conn:
        return [], 0, "Database connection failed."

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    try:
        async with conn.cursor() as cursor:
            count_query = """
                SELECT COUNT(de.DOCNUMBER)
                FROM LKP_EVENT_DOC de
                JOIN LKP_PHOTO_EVENT e ON de.EVENT_ID = e.SYSTEM_ID
                WHERE de.EVENT_ID = :event_id AND de.DISABLED = '0' AND e.DISABLED = '0'
            """
            await cursor.execute(count_query, event_id=event_id)
            count_result = await cursor.fetchone()
            total_rows = count_result[0] if count_result else 0

            if total_rows == 0:
                return [], 0, None

            fetch_query = """
                SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.RTADOCDATE as DOC_DATE, p.DOCNAME
                FROM PROFILE p
                JOIN LKP_EVENT_DOC de ON p.DOCNUMBER = de.DOCNUMBER
                WHERE de.EVENT_ID = :event_id AND de.DISABLED = '0'
                ORDER BY p.RTADOCDATE DESC NULLS LAST, p.DOCNUMBER DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            await cursor.execute(fetch_query, event_id=event_id, offset=offset, page_size=page_size)
            rows = await cursor.fetchall()

            dst = dms_system_login()

            for row in rows:
                doc_id, abstract, author, doc_date, docname = row
                media_type = 'image'

                if dst:
                    try:
                        _, media_type, _ = await get_media_info_from_dms(dst, doc_id)
                    except Exception as e:
                        logging.error(f"Error getting media info for event doc {doc_id}: {e}")

                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or "",
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": doc_date.strftime('%Y-%m-%d %H:%M:%S') if doc_date else "N/A",
                    "thumbnail_url": f"cache/{doc_id}.jpg",
                    "media_type": media_type,
                    "is_favorite": False
                })

            total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
            return documents, total_pages, None

    except oracledb.Error as e:
        logging.error(f"Oracle error fetching event documents: {e}", exc_info=True)
        return [], 0, str(e)
    finally:
        if conn:
            await conn.close()