import oracledb
import logging
import os
from database.connection import get_async_connection
from database.media import (
    dms_system_login,
    get_media_info_from_dms,
    get_media_content_from_dms,
    create_thumbnail,
    thumbnail_cache_dir
)

async def add_favorite(user_id, doc_id):
    """Adds a document to a user's favorites."""
    conn = await get_async_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)",
                                 username=user_id)
            user_result = await cursor.fetchone()

            if not user_result:
                return False, "User not found in PEOPLE table."

            db_user_id = user_result[0]

            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_FAVORITES_DOC WHERE USER_ID = :user_id AND DOCNUMBER = :doc_id",
                [db_user_id, doc_id])
            count_res = await cursor.fetchone()
            if count_res[0] > 0:
                return True, "Document is already a favorite."

            await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_FAVORITES_DOC")
            sys_id_res = await cursor.fetchone()
            system_id = sys_id_res[0]

            await cursor.execute("INSERT INTO LKP_FAVORITES_DOC (SYSTEM_ID, USER_ID, DOCNUMBER) VALUES (:1, :2, :3)",
                                 [system_id, db_user_id, doc_id])
            await conn.commit()
            return True, "Favorite added."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            await conn.close()

async def remove_favorite(user_id, doc_id):
    """Removes a document from a user's favorites."""
    conn = await get_async_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)",
                                 username=user_id)
            user_result = await cursor.fetchone()

            if not user_result:
                return False, "User not found in PEOPLE table."

            db_user_id = user_result[0]

            await cursor.execute("DELETE FROM LKP_FAVORITES_DOC WHERE USER_ID = :user_id AND DOCNUMBER = :doc_id",
                                 [db_user_id, doc_id])
            await conn.commit()
            if cursor.rowcount > 0:
                return True, "Favorite removed."
            else:
                return False, "Favorite not found."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            await conn.close()

async def get_favorites(user_id, page=1, page_size=20, app_source='unknown'):
    """Fetches a paginated list of a user's favorited documents with app_source filtering."""
    conn = await get_async_connection()
    if not conn:
        return [], 0

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    doc_filter_sql = "AND p.RTA_TEXT1 = 'edms-media'"

    if app_source == 'edms-media':
        doc_filter_sql = "AND p.RTA_TEXT1 = 'edms-media'"
    elif app_source == 'smart-edms':
        smart_edms_floor = 19662092
        doc_filter_sql = f"AND p.DOCNUMBER >= {smart_edms_floor} AND (p.RTA_TEXT1 IS NULL OR p.RTA_TEXT1 != 'edms-media')"

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)",
                                 username=user_id)
            user_result = await cursor.fetchone()

            if not user_result:
                logging.error(f"Could not find user '{user_id}' in PEOPLE table for fetching favorites.")
                return [], 0

            db_user_id = user_result[0]

            count_query = f"""
                SELECT COUNT(f.SYSTEM_ID) 
                FROM LKP_FAVORITES_DOC f
                JOIN PROFILE p ON f.DOCNUMBER = p.DOCNUMBER
                WHERE f.USER_ID = :user_id
                {doc_filter_sql}
            """
            await cursor.execute(count_query, [db_user_id])
            total_rows_res = await cursor.fetchone()
            total_rows = total_rows_res[0]

            query = f"""
                SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.RTADOCDATE as DOC_DATE, p.DOCNAME
                FROM PROFILE p
                JOIN LKP_FAVORITES_DOC f ON p.DOCNUMBER = f.DOCNUMBER
                WHERE f.USER_ID = :user_id
                {doc_filter_sql}
                ORDER BY p.DOCNUMBER DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            await cursor.execute(query, user_id=db_user_id, offset=offset, page_size=page_size)
            rows = await cursor.fetchall()

            dst = dms_system_login()

            for row in rows:
                doc_id, abstract, author, doc_date, docname = row
                thumbnail_path = None
                media_type = 'image'

                if dst:
                    try:
                        _, media_type, file_ext = await get_media_info_from_dms(dst, doc_id)
                        cached_thumbnail_file = f"{doc_id}.jpg"
                        cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                        if os.path.exists(cached_path):
                            thumbnail_path = f"cache/{cached_thumbnail_file}"
                        else:
                            media_bytes = get_media_content_from_dms(dst, doc_id)
                            if media_bytes:
                                thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)
                    except Exception as e:
                        logging.error(f"Error getting media info for favorite {doc_id}: {e}")

                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or "",
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": doc_date.strftime('%Y-%m-%d %H:%M:%S') if doc_date else "N/A",
                    "thumbnail_url": thumbnail_path or "",
                    "media_type": media_type,
                    "is_favorite": True
                })
        return documents, total_rows
    except oracledb.Error as e:
        logging.error(f"Oracle error fetching favorites: {e}", exc_info=True)
        return [], 0
    finally:
        if conn:
            await conn.close()