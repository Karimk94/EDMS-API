import oracledb
import logging
import os
from datetime import datetime
from database.connection import get_async_connection
from database.media import dms_system_login, get_media_info_from_dms, get_media_content_from_dms, create_thumbnail, \
    thumbnail_cache_dir

async def fetch_memories_from_oracle(month, day=None, limit=5):
    """Fetches one representative image document per past year for a given month (and optionally day)."""
    conn = await get_async_connection()
    if not conn: return []

    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS. Aborting memories fetch.")
        if conn: await conn.close()
        return []

    memories = []
    current_year = datetime.now().year

    try:
        month_int = int(month)
        day_int = int(day) if day is not None else None
        limit_int = max(1, min(int(limit), 10))
    except (ValueError, TypeError):
        logging.error(f"Invalid month/day/limit provided for memories: month={month}, day={day}, limit={limit}")
        if conn: await conn.close()
        return []

    if not 1 <= month_int <= 12:
        logging.error(f"Invalid month provided for memories: {month_int}")
        if conn: await conn.close()
        return []

    params = {'month': month_int, 'current_year': current_year, 'limit': limit_int}
    day_filter_sql = ""
    if day_int is not None:
        params['day'] = day_int
        day_filter_sql = "AND EXTRACT(DAY FROM p.RTADOCDATE) = :day"

    sql = f"""
    WITH RankedMemories AS (
        SELECT
            p.DOCNUMBER,
            p.ABSTRACT,
            p.AUTHOR,
            p.RTADOCDATE,
            p.DOCNAME,
            EXTRACT(YEAR FROM p.RTADOCDATE) as memory_year,
            ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM p.RTADOCDATE) ORDER BY p.RTADOCDATE DESC, p.DOCNUMBER DESC) as rn
        FROM
            PROFILE p
        WHERE
            p.FORM = 2740
            AND p.RTADOCDATE IS NOT NULL
            AND EXTRACT(MONTH FROM p.RTADOCDATE) = :month
            {day_filter_sql}
            AND EXTRACT(YEAR FROM p.RTADOCDATE) < :current_year
            AND p.DOCNUMBER >= 19677386
            AND (
                 LOWER(p.DOCNAME) LIKE '%.jpg' OR
                 LOWER(p.DOCNAME) LIKE '%.jpeg' OR
                 LOWER(p.DOCNAME) LIKE '%.png' OR
                 LOWER(p.DOCNAME) LIKE '%.gif' OR
                 LOWER(p.DOCNAME) LIKE '%.bmp'
                 )
    )
    SELECT
        rm.DOCNUMBER,
        rm.ABSTRACT,
        rm.AUTHOR,
        rm.RTADOCDATE,
        rm.DOCNAME
    FROM
        RankedMemories rm
    WHERE
        rm.rn = 1
    ORDER BY
        rm.memory_year DESC
    FETCH FIRST :limit ROWS ONLY
    """

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()

            for row in rows:
                doc_id, abstract, author, rtadocdate, docname = row
                thumbnail_path = None
                media_type = 'image'

                try:
                    cached_thumbnail_file = f"{doc_id}.jpg"
                    cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                    if os.path.exists(cached_path):
                        thumbnail_path = f"cache/{cached_thumbnail_file}"
                    else:
                        _, actual_media_type, actual_file_ext = await get_media_info_from_dms(dst, doc_id)
                        if actual_media_type == 'image':
                            media_bytes = get_media_content_from_dms(dst, doc_id)
                            if media_bytes:
                                thumbnail_path = create_thumbnail(doc_id, actual_media_type, actual_file_ext,
                                                                  media_bytes)
                        else:
                            continue

                except Exception as thumb_e:
                    logging.error(f"Error processing thumbnail for memory doc {doc_id}: {thumb_e}", exc_info=True)

                memories.append({
                    "doc_id": doc_id,
                    "title": abstract or "",
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": rtadocdate.strftime('%d-%m-%Y') if rtadocdate else "N/A",
                    "thumbnail_url": thumbnail_path or "",
                    "media_type": 'image'
                })

    except oracledb.Error as e:
        logging.error(f"Oracle error fetching memories: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    return memories

async def fetch_journey_data():
    """Fetches all events and their associated documents, grouped by year."""
    conn = await get_async_connection()
    if not conn:
        return {}

    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS in fetch_journey_data.")
        if conn: await conn.close()
        return {}

    journey_data = {}
    try:
        async with conn.cursor() as cursor:
            sql = """
                SELECT
                    EXTRACT(YEAR FROM p.RTADOCDATE) as event_year,
                    e.EVENT_NAME,
                    de.DOCNUMBER
                FROM LKP_PHOTO_EVENT e
                JOIN LKP_EVENT_DOC de ON e.SYSTEM_ID = de.EVENT_ID
                JOIN PROFILE p ON de.DOCNUMBER = p.DOCNUMBER
                WHERE e.DISABLED = '0' AND de.DISABLED = '0' AND p.RTADOCDATE IS NOT NULL
                ORDER BY event_year DESC, e.EVENT_NAME
            """
            await cursor.execute(sql)
            rows = await cursor.fetchall()

            events_by_year = {}
            for year, event_name, docnumber in rows:
                if year not in events_by_year:
                    events_by_year[year] = {}
                if event_name not in events_by_year[year]:
                    events_by_year[year][event_name] = []
                events_by_year[year][event_name].append(docnumber)

            for year, events in events_by_year.items():
                if year not in journey_data:
                    journey_data[year] = []

                for event_name, docnumbers in events.items():
                    thumbnail_urls = []
                    for doc_id in docnumbers[:4]:
                        thumbnail_path = f"cache/{doc_id}.jpg"
                        cached_path = os.path.join(thumbnail_cache_dir, f"{doc_id}.jpg")
                        if not os.path.exists(cached_path):
                            _, media_type, file_ext = await get_media_info_from_dms(dst, doc_id)
                            media_bytes = get_media_content_from_dms(dst, doc_id)
                            if media_bytes:
                                create_thumbnail(doc_id, media_type, file_ext, media_bytes)
                        thumbnail_urls.append(thumbnail_path)

                    journey_data[year].append({
                        "title": event_name,
                        "gallery": [f"cache/{doc_id}.jpg" for doc_id in docnumbers],
                        "thumbnail": thumbnail_urls[0] if thumbnail_urls else ""
                    })

    except oracledb.Error as e:
        logging.error(f"Oracle error in fetch_journey_data: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    return journey_data