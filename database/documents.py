import oracledb
import logging
from datetime import datetime
import re
import wsdl_client
from database.connection import get_async_connection
from database.media import (
    dms_system_login,
    get_media_info_from_dms,
    get_media_content_from_dms,
    create_thumbnail,
    thumbnail_cache_dir,
    DOC_IMAGE_EXTS,
    DOC_VIDEO_EXTS,
    DOC_PDF_EXTS
)
import os

# File extensions the AI processing pipeline can actually handle (images, videos, PDFs).
# Office documents (xlsx, pptx, docx...), text files, archives, and any other
# unsupported types must never be queued for AI tagging.
PROCESSABLE_EXTS = (
    DOC_IMAGE_EXTS
    | DOC_VIDEO_EXTS
    | DOC_PDF_EXTS
    | {'m4v', 'mts', 'ts', '3g2'}  # extra video types handled via filename fallback
)


try:
    import vector_client
except ImportError:
    logging.warning("vector_client.py not found. Vector search capabilities will be disabled.")
    vector_client = None

async def fetch_documents_from_oracle(page=1, page_size=20, search_term=None, date_from=None, date_to=None,
                                      persons=None, person_condition='any', tags=None, years=None, sort=None,
                                      memory_month=None, memory_day=None, user_id=None, lang='en',
                                      security_level='Editor', app_source='unknown', media_type=None, scope=None):
    """Fetches a paginated list of documents, applying filters including media_type."""
    
    # --- Pre-fetch WSDL data BEFORE acquiring DB connection ---
    # This is critical to avoid connection pool exhaustion from long-running WSDL calls
    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS. Aborting document fetch.")
        return [], 0

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    params = {}
    where_clauses = []
    shortlist_clause = ""

    # --- Scope Logic - WSDL calls happen BEFORE DB connection ---
    folder_doc_ids = None
    root_doc_ids = None  # For edms-media recents tab
    event_doc_ids = []  # For edms-media event-based search
    event_folder_names = {}  # Map of doc_id -> event name for display
    
    if scope == 'folders':
        # Retrieve all relevant DOCNUMBERS from WSDL first (Async call - NO DB connection held)
        folder_doc_ids = await wsdl_client.get_recursive_doc_ids(dst, media_type)
        if not folder_doc_ids:
            return [], 0

        # Paginate IDs *before* querying DB to handle large lists efficiently
        total_rows = len(folder_doc_ids)

        # If requested page is out of range, return empty
        if offset >= total_rows:
            return [], total_rows

        paginated_ids = folder_doc_ids[offset: offset + page_size]

        # Use these IDs in the WHERE clause
        placeholders = ','.join([f":fid_{i}" for i in range(len(paginated_ids))])
        where_clauses.append(f"p.DOCNUMBER IN ({placeholders})")
        for i, doc_id in enumerate(paginated_ids):
            params[f'fid_{i}'] = doc_id

    else:
        # For edms-media, show documents from the SMART_EDMS_ROOT_ID folder hierarchy
        # This ensures both folders and recents tabs show content from the same root
        if app_source == 'edms-media':
            # Get all document IDs from the SMART_EDMS_ROOT_ID folder hierarchy (Async call - NO DB connection held)
            from wsdl_client.config import SMART_EDMS_ROOT_ID
            root_doc_ids = await wsdl_client.get_recursive_doc_ids(dst, media_type_filter=None, start_node=SMART_EDMS_ROOT_ID)
            logging.info(f"[edms-media recents] Fetched {len(root_doc_ids) if root_doc_ids else 0} doc IDs from SMART_EDMS_ROOT_ID")
            
            if root_doc_ids and len(root_doc_ids) > 0:
                # Limit to 10000 IDs for performance
                limited_ids = root_doc_ids[:10000] if len(root_doc_ids) > 10000 else root_doc_ids
                root_id_placeholders = ','.join([f":root_id_{i}" for i in range(len(limited_ids))])
                
                # Add the doc IDs as parameters - ensure each doc_id is an integer
                for i, doc_id in enumerate(limited_ids):
                    # Handle case where doc_id might be a dict or other type
                    if isinstance(doc_id, dict):
                        doc_id = doc_id.get('docnumber') or doc_id.get('DOCNUMBER') or doc_id.get('id')
                    params[f'root_id_{i}'] = int(doc_id) if doc_id else 0
                
                # Filter to only include documents from the root folder hierarchy
                where_clauses.append(f"p.DOCNUMBER IN ({root_id_placeholders})")
                logging.info(f"[edms-media recents] Added {len(limited_ids)} doc IDs to query")
            else:
                # Fallback: just filter by RTA_TEXT1 if WSDL fetch fails or returns empty
                logging.warning(f"[edms-media recents] WSDL returned no IDs, falling back to RTA_TEXT1 filter only")
                where_clauses.append("p.RTA_TEXT1 = 'edms-media'")
            # NOTE: For edms-media, we do NOT exclude documents in folders - we want to show all documents
            # from the SMART_EDMS_ROOT_ID hierarchy, whether they are in folders or not
        elif app_source == 'smart-edms':
            smart_edms_floor = 19662092
            where_clauses.append(f"p.DOCNUMBER >= {smart_edms_floor} AND (p.RTA_TEXT1 IS NULL OR p.RTA_TEXT1 != 'edms-media')")
            # Exclude documents that are in folders for smart-edms (non-edms-media documents)
            where_clauses.append("NOT EXISTS (SELECT 1 FROM FOLDER_ITEM fi WHERE fi.DOCNUMBER = p.DOCNUMBER AND fi.NODE_TYPE = 'D')")
        
        # --- Event-based search for edms-media (async DB calls, but NO main connection held yet) ---
        if search_term and app_source == 'edms-media':
            try:
                from database.events import search_events_by_name, get_event_folder_docnumbers
                matching_events = await search_events_by_name(search_term, app_source)
                for event in matching_events:
                    event_id = event.get('id')
                    event_name = event.get('name', '')
                    event_docs = await get_event_folder_docnumbers(event_id)
                    for doc_id in event_docs:
                        if doc_id not in event_doc_ids:
                            event_doc_ids.append(doc_id)
                            event_folder_names[doc_id] = event_name
            except Exception as e:
                logging.error(f"Error searching events for document search: {e}")

    # --- NOW acquire DB connection after all WSDL calls are complete ---
    conn = await get_async_connection()
    if not conn: return [], 0

    db_user_id = None
    if user_id:
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)",
                                     username=user_id)
                user_result = await cursor.fetchone()
                if user_result:
                    db_user_id = user_result[0]
        except oracledb.Error as e:
            logging.error(f"Could not fetch user system ID for {user_id}: {e}")

    if media_type:
        try:
            async with conn.cursor() as app_cursor:
                # Step 1: Try to fetch SYSTEM_ID (Numeric) from APPS
                id_column = "SYSTEM_ID"
                try:
                    await app_cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
                    apps_rows = await app_cursor.fetchall()
                except oracledb.DatabaseError:
                    # Fallback if SYSTEM_ID column doesn't exist
                    logging.warning("SYSTEM_ID column not found in APPS, falling back to APPLICATION column.")
                    id_column = "APPLICATION"
                    await app_cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
                    apps_rows = await app_cursor.fetchall()

            # Using the same extended lists as the working reference
            image_exts = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'heic', 'ico', 'jfif'}
            video_exts = {'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm', 'm4v', '3gp', 'ts', 'mts', '3g2'}
            pdf_exts = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'csv', 'zip', 'rar', '7z'}

            target_app_ids = []

            for app_id, ext in apps_rows:
                if not ext: continue
                clean_ext = str(ext).lower().replace('.', '').strip()
                str_id = str(app_id).strip()

                if media_type == 'image' and clean_ext in image_exts:
                    target_app_ids.append(str_id)
                elif media_type == 'video' and clean_ext in video_exts:
                    target_app_ids.append(str_id)
                elif media_type == 'pdf' and clean_ext in pdf_exts:
                    target_app_ids.append(str_id)

            if target_app_ids:
                id_list = ",".join(f"'{x}'" for x in target_app_ids)
                where_clauses.append(f"TRIM(TO_CHAR(p.APPLICATION)) IN ({id_list})")
            else:
                # If no apps found for this type, return nothing
                where_clauses.append("1=0")

        except Exception as e:
            logging.error(f"Error filtering by media_type: {e}")
            # Fallback to failing safe if App ID lookup crashes
            where_clauses.append("1=0")

    base_where = f"WHERE p.FORM = 2740 "
    shortlist_sql = "AND k.SHORTLISTED = '1'" if security_level == 'Viewer' else ""
    vector_doc_ids = None

    use_vector_search = (
            vector_client is not None and
            search_term and
            not memory_month
    )

    # --- Prepare Keyword Search Conditions (Always generate these for Hybrid search) ---
    text_search_conditions = []
    if search_term and not memory_month:
        search_words = [word.strip() for word in search_term.split(' ') if word.strip()]
        if search_words:
            for i, word in enumerate(search_words):
                key = f"search_word_{i}"
                key_upper = f"search_word_{i}_upper"
                params[key] = f"%{word}%"
                params[key_upper] = f"%{word.upper()}%"

                word_condition = f"""
                                        (
                                            p.ABSTRACT LIKE :{key} OR UPPER(p.ABSTRACT) LIKE :{key_upper} OR
                                            p.DOCNAME LIKE :{key} OR UPPER(p.DOCNAME) LIKE :{key_upper} OR
                                            TO_CHAR(p.RTADOCDATE, 'YYYY-MM-DD') LIKE :{key} OR
                                            EXISTS (
                                                SELECT 1 FROM LKP_DOCUMENT_TAGS ldt
                                                JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                                                WHERE ldt.DOCNUMBER = p.DOCNUMBER 
                                                AND (k.DESCRIPTION LIKE :{key} OR UPPER(k.KEYWORD_ID) LIKE :{key_upper}) 
                                                AND ldt.DISABLED = '0'
                                                {shortlist_sql}
                                            ) OR
                                            EXISTS (
                                                SELECT 1 FROM LKP_PERSON p_filter
                                                WHERE (p_filter.NAME_ARABIC LIKE :{key} OR UPPER(p_filter.NAME_ENGLISH) LIKE :{key_upper})
                                                AND (
                                                        UPPER(p.ABSTRACT) LIKE '%' || UPPER(p_filter.NAME_ENGLISH) || '%'
                                                        OR (p_filter.NAME_ARABIC IS NOT NULL AND p.ABSTRACT LIKE '%' || p_filter.NAME_ARABIC || '%')
                                                )
                                            )
                                        )
                                        """
                text_search_conditions.append(word_condition)

    # --- Perform Vector Search (Blocking Call) ---
    if use_vector_search:
        try:
            vector_doc_ids = vector_client.query_documents(search_term, n_results=page * page_size)
        except Exception as e:
            logging.error(f"Vector search failed: {e}")
            vector_doc_ids = None

    if memory_month is not None:
        try:
            month_int = int(memory_month)
            day_int = int(memory_day) if memory_day is not None else None
            current_year = datetime.now().year

            if not 1 <= month_int <= 12: raise ValueError("Invalid month")
            if day_int is not None and not 1 <= day_int <= 31: raise ValueError("Invalid day")

            where_clauses.append("p.RTADOCDATE IS NOT NULL")
            where_clauses.append("EXTRACT(MONTH FROM p.RTADOCDATE) = :memory_month")
            params['memory_month'] = month_int
            if day_int is not None:
                where_clauses.append("EXTRACT(DAY FROM p.RTADOCDATE) = :memory_day")
                params['memory_day'] = day_int
            where_clauses.append("EXTRACT(YEAR FROM p.RTADOCDATE) < :current_year")
            params['current_year'] = current_year
            where_clauses.append("""
                (LOWER(p.DOCNAME) LIKE '%.jpg' OR LOWER(p.DOCNAME) LIKE '%.jpeg' OR LOWER(p.DOCNAME) LIKE '%.png' OR
                 LOWER(p.DOCNAME) LIKE '%.gif' OR LOWER(p.DOCNAME) LIKE '%.bmp')
            """)

        except (ValueError, TypeError) as e:
            logging.error(f"Invalid memory date parameters provided: {e}")
            memory_month = None

    if memory_month is None:
        # --- Combined Hybrid Search Logic ---
        vector_clause = None
        text_clause = None
        event_clause = None

        if vector_doc_ids:
            # logging.info(f"Using {len(vector_doc_ids)} doc_ids from vector search.")
            vector_placeholders = ','.join([f":vec_id_{i}" for i in range(len(vector_doc_ids))])
            vector_clause = f"p.docnumber IN ({vector_placeholders})"
            for i, doc_id in enumerate(vector_doc_ids):
                params[f'vec_id_{i}'] = doc_id

        if text_search_conditions:
            text_clause = f"({' AND '.join(text_search_conditions)})"
        
        # Add event-based document IDs to search (for edms-media)
        if event_doc_ids:
            event_placeholders = ','.join([f":event_id_{i}" for i in range(len(event_doc_ids))])
            event_clause = f"p.docnumber IN ({event_placeholders})"
            for i, doc_id in enumerate(event_doc_ids):
                params[f'event_id_{i}'] = doc_id

        # Combine all search clauses with OR
        all_clauses = [c for c in [vector_clause, text_clause, event_clause] if c]
        if len(all_clauses) > 1:
            where_clauses.append(f"({' OR '.join(all_clauses)})")
        elif len(all_clauses) == 1:
            where_clauses.append(all_clauses[0])

        # --- Standard Filters ---
        if persons:
            person_list = [p.strip() for p in persons.split(',') if p.strip()]
            if person_list:
                op = " OR " if person_condition == 'any' else " AND "
                person_conditions = []
                for i, person in enumerate(person_list):
                    key = f'person_{i}'
                    key_upper = f'person_{i}_upper'

                    # The ABSTRACT always stores English names (enforced by update_abstract_with_vips).
                    # When the UI is in Arabic mode the caller sends the Arabic display name.
                    # We must resolve it to the English form before matching against ABSTRACT.
                    # Strategy:  Arabic input → look up NAME_ENGLISH in LKP_PERSON.
                    #            English input → use as-is.
                    english_person = person  # default: assume English
                    if not person.isascii():
                        # Arabic name — look up the English equivalent inside this same connection
                        try:
                            async with conn.cursor() as _cur:
                                await _cur.execute(
                                    "SELECT NAME_ENGLISH FROM LKP_PERSON "
                                    "WHERE NAME_ARABIC = :1 AND NAME_ENGLISH IS NOT NULL",
                                    [person]
                                )
                                row = await _cur.fetchone()
                                if row:
                                    english_person = row[0]
                        except Exception:
                            pass  # fall back to original value

                    person_conditions.append(f"(p.ABSTRACT LIKE :{key} OR UPPER(p.ABSTRACT) LIKE :{key_upper})")
                    params[key] = f"%{english_person}%"
                    params[key_upper] = f"%{english_person.upper()}%"
                where_clauses.append(f"({op.join(person_conditions)})")


        if date_from:
            try:
                datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S')
                where_clauses.append("p.RTADOCDATE >= TO_DATE(:date_from, 'YYYY-MM-DD HH24:MI:SS')")
                params['date_from'] = date_from
            except ValueError:
                logging.warning(f"Invalid date_from format received: {date_from}")
        if date_to:
            try:
                datetime.strptime(date_to, '%Y-%m-%d %H:%M:%S')
                where_clauses.append("p.RTADOCDATE <= TO_DATE(:date_to, 'YYYY-MM-DD HH24:MI:SS')")
                params['date_to'] = date_to
            except ValueError:
                logging.warning(f"Invalid date_to format received: {date_to}")

        if years:
            year_list_str = years.split(',')
            year_list_int = []
            valid_years = True
            for y_str in year_list_str:
                try:
                    year_int = int(y_str.strip())
                    if 1900 < year_int < 2100:
                        year_list_int.append(year_int)
                    else:
                        valid_years = False;
                        break
                except ValueError:
                    valid_years = False;
                    break
            if valid_years and year_list_int:
                year_placeholders = ', '.join([f":year_{i}" for i in range(len(year_list_int))])
                where_clauses.append(f"EXTRACT(YEAR FROM p.RTADOCDATE) IN ({year_placeholders})")
                for i, year in enumerate(year_list_int): params[f'year_{i}'] = year
            elif not valid_years:
                logging.warning(f"Invalid year format received: {years}")

        if tags:
            tag_list = [t.strip() for t in tags.split(',') if t.strip()]
            if tag_list:
                tag_conditions = []
                keyword_column = "DESCRIPTION" if lang == 'ar' else "KEYWORD_ID"
                person_filter_column = "NAME_ARABIC" if lang == 'ar' else "NAME_ENGLISH"

                for i, tag in enumerate(tag_list):
                    key = f'tag_{i}'
                    key_upper = f'tag_{i}_upper'

                    if lang == 'ar':
                        params[key] = tag
                        keyword_compare = f"TRIM(k.{keyword_column}) = :{key}"
                        person_compare = f"TRIM(p_filter.{person_filter_column}) = :{key}"
                    else:
                        params[key_upper] = tag.upper()
                        keyword_compare = f"UPPER(TRIM(k.{keyword_column})) = :{key_upper}"
                        person_compare = f"UPPER(TRIM(p_filter.{person_filter_column})) = :{key_upper}"

                    keyword_subquery = f"""
                    EXISTS (
                        SELECT 1 FROM LKP_DOCUMENT_TAGS ldt
                        JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                        WHERE ldt.DOCNUMBER = p.DOCNUMBER 
                        AND {keyword_compare} 
                        AND ldt.DISABLED = '0'
                        {shortlist_clause}
                    )
                    """

                    person_subquery = f"""
                    EXISTS (
                        SELECT 1 FROM LKP_PERSON p_filter
                        WHERE {person_compare}
                        AND (
                             UPPER(p.ABSTRACT) LIKE '%' || UPPER(p_filter.NAME_ENGLISH) || '%'
                             OR (p_filter.NAME_ARABIC IS NOT NULL AND p.ABSTRACT LIKE '%' || p_filter.NAME_ARABIC || '%')
                        )
                    )
                    """

                    tag_conditions.append(f"({keyword_subquery} OR {person_subquery})")

                where_clauses.append(" AND ".join(tag_conditions))

    final_where_clause = base_where + ("AND " + " AND ".join(where_clauses) if where_clauses else "")

    order_by_clause = "ORDER BY p.DOCNUMBER DESC"

    # --- Ordering Logic for Hybrid Search ---
    if vector_doc_ids and len(vector_doc_ids) > 0:
        order_case_sql = " ".join([f"WHEN :vec_id_{i} THEN {i + 1}" for i in range(len(vector_doc_ids))])
        order_by_clause = f"ORDER BY CASE p.docnumber {order_case_sql} ELSE {len(vector_doc_ids) + 1} END ASC, p.RTADOCDATE DESC"
    elif memory_month is not None:
        order_by_clause = "ORDER BY p.RTADOCDATE DESC, p.DOCNUMBER DESC"
        if sort == 'rtadocdate_asc':
            order_by_clause = "ORDER BY p.RTADOCDATE ASC, p.DOCNUMBER ASC"
    else:
        if sort == 'date_desc':
            order_by_clause = "ORDER BY p.RTADOCDATE DESC, p.DOCNUMBER DESC"
        elif sort == 'date_asc':
            order_by_clause = "ORDER BY p.RTADOCDATE ASC, p.DOCNUMBER ASC"

    try:
        async with conn.cursor() as cursor:
            # If scope is folders, we already know the total rows from the recursive fetch
            if scope != 'folders':
                count_query = f"SELECT COUNT(p.DOCNUMBER) FROM PROFILE p {final_where_clause}"
                await cursor.execute(count_query, params)
                count_result = await cursor.fetchone()
                total_rows = count_result[0]

            date_column = "p.RTADOCDATE"
            fetch_query = f"""
            SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, {date_column} as DOC_DATE, p.DOCNAME,
                   CASE WHEN f.DOCNUMBER IS NOT NULL THEN 1 ELSE 0 END as IS_FAVORITE
            FROM PROFILE p
            LEFT JOIN LKP_FAVORITES_DOC f ON p.DOCNUMBER = f.DOCNUMBER AND f.USER_ID = :db_user_id
            {final_where_clause}
            {order_by_clause}
            """

            # Add offset/fetch logic only if NOT folder scope
            if scope != 'folders':
                fetch_query += " OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"
                params['offset'] = offset
                params['page_size'] = page_size

            params['db_user_id'] = db_user_id

            await cursor.execute(fetch_query, params)
            rows = await cursor.fetchall()

            for row in rows:
                doc_id, abstract, author, doc_date, docname, is_favorite = row
                thumbnail_path = None
                media_type = 'image'

                final_abstract = abstract or ""
                if security_level == 'Viewer':
                    final_abstract = ""

                try:
                    # Async call to resolve media type/filename
                    original_filename, media_type, file_ext = await get_media_info_from_dms(dst, doc_id)
                    cached_thumbnail_file = f"{doc_id}.jpg"
                    cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                    if os.path.exists(cached_path):
                        thumbnail_path = f"cache/{cached_thumbnail_file}"
                    else:
                        # Create thumbnail on-demand if not cached (same as favorites)
                        media_bytes = get_media_content_from_dms(dst, doc_id)
                        if media_bytes:
                            thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)

                except Exception as media_info_e:
                    logging.error(f"Error processing media info for doc {doc_id}: {media_info_e}",
                                  exc_info=True)

                # Check if this document was found via event search
                event_name = event_folder_names.get(doc_id) if event_folder_names else None
                
                documents.append({
                    "doc_id": doc_id,
                    "title": final_abstract,
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": doc_date.strftime('%Y-%m-%d %H:%M:%S') if doc_date else "N/A",
                    "thumbnail_url": thumbnail_path or "",
                    "media_type": media_type,
                    "is_favorite": bool(is_favorite),
                    "event_name": event_name  # Populated when found via event/folder search
                })
    except oracledb.Error as e:
        logging.error(f"Oracle error fetching documents: {e}", exc_info=True)
        return [], 0
    finally:
        if conn:
            await conn.close()
    return documents, total_rows

def _process_candidates_sql(join_column: str, select_clause: str, extra_clause: str = "") -> str:
    """Builds the processing-candidates SQL restricted to AI-supported file types.

    Joins APPS to resolve each document's extension and only allows extensions
    the AI pipeline can process (images, videos, PDFs). This prevents office
    files (xlsx, pptx, docx...), text files, archives, etc. from ever reaching
    the TAGGING_QUEUE / processing queue.
    """
    ext_list = ",".join(f"'{ext}'" for ext in sorted(PROCESSABLE_EXTS))
    return f"""
            SELECT {select_clause}
            FROM PROFILE p
            LEFT JOIN TAGGING_QUEUE q ON p.docnumber = q.docnumber
            LEFT JOIN APPS a ON p.APPLICATION = a.{join_column}
            WHERE p.form = :form_id
              AND p.docnumber >= 19677386 --19662092
              AND (q.STATUS <> 3 OR q.STATUS IS NULL)
              AND (q.ATTEMPTS <= 3 OR q.ATTEMPTS IS NULL)
              AND TRIM(LOWER(REPLACE(a.DEFAULT_EXTENSION, '.', ''))) IN ({ext_list})
            {extra_clause}
            """


async def _execute_process_candidates_sql(cursor, select_clause: str, extra_clause: str = ""):
    """Runs the candidates query, falling back to the APPLICATION join column
    if the APPS table has no SYSTEM_ID column."""
    try:
        await cursor.execute(
            _process_candidates_sql('SYSTEM_ID', select_clause, extra_clause),
            {'form_id': 2740}
        )
    except oracledb.DatabaseError:
        logging.warning("APPS.SYSTEM_ID join failed, falling back to APPS.APPLICATION column.")
        await cursor.execute(
            _process_candidates_sql('APPLICATION', select_clause, extra_clause),
            {'form_id': 2740}
        )


async def get_documents_to_process():
    """Gets a batch of documents that need AI processing.

    Only file types supported by the AI pipeline (images, videos, PDFs) are
    eligible; office documents and other unsupported types are excluded.
    """
    conn = await get_async_connection()
    if not conn: return []

    select_clause = """p.docnumber, p.abstract,
                   NVL(q.o_detected, 0) as o_detected,
                   NVL(q.OCR, 0) as ocr,
                   NVL(q.face, 0) as face,
                   NVL(q.attempts, 0) as attempts"""

    try:
        async with conn.cursor() as cursor:
            await _execute_process_candidates_sql(cursor, select_clause, "FETCH FIRST 10 ROWS ONLY")
            columns = [col[0].lower() for col in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        if conn:
            await conn.close()



async def get_documents_to_process_count():
    """Gets the total count of Oracle TAGGING_QUEUE candidates pending processing.

    Counts only file types supported by the AI pipeline (images, videos, PDFs),
    matching the filter applied by get_documents_to_process().
    """
    conn = await get_async_connection()
    if not conn:
        return 0

    try:
        async with conn.cursor() as cursor:
            await _execute_process_candidates_sql(cursor, "COUNT(1)")
            row = await cursor.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception as e:
        logging.error(f"Failed to count pending Oracle processing documents: {e}", exc_info=True)
        return 0
    finally:
        if conn:
            await conn.close()


async def update_document_processing_status(docnumber, new_abstract, o_detected, ocr, face, status, error, transcript,
                                            attempts):
    """Updates the processing status of a document in the database with robust transaction handling.

    Returns:
        bool: True if the Oracle write was committed successfully, False otherwise.
    """
    conn = await get_async_connection()
    if not conn:
        logging.error(f"DB_UPDATE_FAILURE: Could not get a database connection for docnumber {docnumber}.")
        return False

    try:
        async with conn.cursor() as cursor:
            # Explicit transaction start not strictly needed with Python DB-API if auto-commit off, but okay.
            # conn.begin() is available in oracledb but async/await context manager handles it usually.

            await cursor.execute("UPDATE PROFILE SET abstract = :1 WHERE docnumber = :2", (new_abstract, docnumber))

            merge_sql = """
            MERGE INTO TAGGING_QUEUE q
            USING (SELECT :docnumber AS docnumber FROM dual) src ON (q.docnumber = src.docnumber)
            WHEN MATCHED THEN
                UPDATE SET q.o_detected = :o_detected, q.OCR = :ocr, q.face = :face,
                           q.status = :status, q.error = :error, q.transcript = :transcript, q.attempts = :attempts, q.LAST_UPDATE = SYSDATE
            WHEN NOT MATCHED THEN
                INSERT (SYSTEM_ID, docnumber, o_detected, OCR, face, status, error, transcript, attempts, LAST_UPDATE, DISABLED)
                VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM TAGGING_QUEUE), :docnumber, :o_detected, :ocr, :face, :status, :error, :transcript, :attempts, SYSDATE, 0)
            """
            await cursor.execute(merge_sql, {
                'docnumber': docnumber, 'o_detected': o_detected, 'ocr': ocr, 'face': face,
                'status': status, 'error': error, 'transcript': transcript, 'attempts': attempts
            })

            await conn.commit()

            # --- VECTOR INDEXING HOOK (Blocking Call) ---
            if status == 3 and vector_client:
                # logging.info(f"Queueing vector update for doc_id {docnumber}.")
                try:
                    vector_client.add_or_update_document(docnumber, new_abstract)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {docnumber}: {e}", exc_info=True)

        return True

    except oracledb.Error as e:
        logging.error(f"DB_UPDATE_ERROR: Oracle error while updating docnumber {docnumber}: {e}", exc_info=True)
        try:
            await conn.rollback()
        except oracledb.Error as rb_e:
            logging.error(f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber}: {rb_e}",
                          exc_info=True)
        return False
    finally:
        if conn:
            await conn.close()

async def _resolve_vip_names_to_english(cursor, vip_names):
    """
    Resolve a list of VIP name strings to their English equivalents from LKP_PERSON.

    When the UI is in Arabic mode the user picks an Arabic name, so we receive
    the Arabic form here.  The tag-matching query in fetch_tags_for_document
    always compares against UPPER(NAME_ENGLISH), so storing an Arabic name in
    the abstract would make the person invisible to that query.

    Strategy for each name:
      1. Try to find it as an exact English match  → keep as-is.
      2. Try to find it as an exact Arabic match   → swap in the English name.
      3. If no match found (brand-new person)      → keep as-is (English assumed).
    """
    if not vip_names:
        return []

    resolved = []
    for name in vip_names:
        name = name.strip()
        if not name:
            continue

        # Check English match first (case-insensitive)
        await cursor.execute(
            "SELECT NAME_ENGLISH FROM LKP_PERSON WHERE UPPER(NAME_ENGLISH) = UPPER(:1)",
            [name]
        )
        row = await cursor.fetchone()
        if row:
            resolved.append(row[0])  # Use the canonical casing from DB
            continue

        # Check Arabic match
        await cursor.execute(
            "SELECT NAME_ENGLISH FROM LKP_PERSON WHERE NAME_ARABIC = :1",
            [name]
        )
        row = await cursor.fetchone()
        if row:
            resolved.append(row[0])  # Swap Arabic → English
            continue

        # Unknown person — keep the original value (assumed English)
        resolved.append(name)

    return resolved


async def update_abstract_with_vips(doc_id, vip_names):
    """Appends or updates VIP names in a document's abstract.

    Names are always stored in English so that the person-tag matching query
    (which filters on UPPER(NAME_ENGLISH)) works regardless of UI language.
    """
    conn = await get_async_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            result = await cursor.fetchone()
            if result is None: return False, f"Document with ID {doc_id} not found."

            current_abstract = result[0] or ""

            base_abstract = re.sub(r'\s*\n*VIPs\s*:.*', '', current_abstract, flags=re.IGNORECASE).strip()

            # Resolve any Arabic names → English before writing to the abstract
            english_names = await _resolve_vip_names_to_english(cursor, vip_names)

            names_str = ", ".join(sorted(list(set(english_names))))

            if names_str:
                vips_section = f"VIPs: {names_str}"
                new_abstract = base_abstract + ("\n\n" if base_abstract else "") + vips_section
            else:
                new_abstract = base_abstract

            await cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
            await conn.commit()

            # --- VECTOR INDEXING HOOK (Blocking Call) ---
            if vector_client:
                try:
                    vector_client.add_or_update_document(doc_id, new_abstract)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {doc_id} after VIP update: {e}",
                                  exc_info=True)

            return True, "Abstract updated successfully."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            await conn.close()

async def update_document_metadata(doc_id, new_abstract=None, new_date_taken=Ellipsis):
    """Updates metadata (abstract and/or RTADOCDATE) for a specific document number in the PROFILE table."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed."

    update_parts = []
    params = {}
    abstract_to_index = None

    if new_abstract is not None:
        update_parts.append("ABSTRACT = :abstract")
        params['abstract'] = new_abstract
        abstract_to_index = new_abstract
    else:
        abstract_to_index = None

    if new_date_taken is not Ellipsis:
        if new_date_taken is None:
            update_parts.append("RTADOCDATE = NULL")
        elif isinstance(new_date_taken, datetime):
            update_parts.append("RTADOCDATE = TO_DATE(:date_taken, 'YYYY-MM-DD HH24:MI:SS')")
            params['date_taken'] = new_date_taken.strftime('%Y-%m-%d %H:%M:%S')
        else:
            logging.error(f"Invalid type for new_date_taken for doc_id {doc_id}: {type(new_date_taken)}")
            if conn: await conn.close()
            return False, "Invalid date format received by database function."

    if not update_parts:
        if conn: await conn.close()
        return False, "No valid fields provided for update."

    sql = f"UPDATE PROFILE SET {', '.join(update_parts)} WHERE DOCNUMBER = :doc_id"
    params['doc_id'] = doc_id

    try:
        async with conn.cursor() as cursor:
            # Check if document exists first
            await cursor.execute("SELECT 1 FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            if await cursor.fetchone() is None:
                return False, f"Document with ID {doc_id} not found."

            if abstract_to_index is None and new_abstract is None:
                await cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                result = await cursor.fetchone()
                if result:
                    abstract_to_index = result[0]

            await cursor.execute(sql, params)

            if cursor.rowcount == 0:
                await conn.rollback()
                return False, f"Update affected 0 rows for Document ID {doc_id}. Check if data actually changed."

            await conn.commit()

            # --- VECTOR INDEXING HOOK (Blocking Call) ---
            if new_abstract is not None and vector_client:
                try:
                    vector_client.add_or_update_document(doc_id, abstract_to_index)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {doc_id} after metadata update: {e}",
                                  exc_info=True)

            return True, "Metadata updated successfully."

    except oracledb.Error as e:
        logging.error(f"Oracle error updating metadata for doc_id {doc_id}: {e}", exc_info=True)
        if conn: await conn.rollback()
        return False, f"Database error occurred: {e}"
    except Exception as e:
        logging.error(f"Unexpected error updating metadata for doc_id {doc_id}: {e}", exc_info=True)
        if conn:
            try:
                await conn.rollback()
            except:
                pass
        return False, "An unexpected server error occurred."
    finally:
        if conn:
            await conn.close()

async def get_specific_documents_for_processing(docnumbers):
    """Gets details for a specific list of docnumbers that need AI processing."""
    if not docnumbers:
        return []

    conn = await get_async_connection()
    if not conn:
        logging.error("Failed to get DB connection in get_specific_documents_for_processing.")
        return []

    try:
        async with conn.cursor() as cursor:
            int_docnumbers = [int(d) for d in docnumbers]
            placeholders = ','.join([f':{i + 1}' for i in range(len(int_docnumbers))])

            sql = f"""
            SELECT p.docnumber, p.abstract,
                   NVL(q.o_detected, 0) as o_detected,
                   NVL(q.OCR, 0) as ocr,
                   NVL(q.face, 0) as face,
                   NVL(q.attempts, 0) as attempts
            FROM PROFILE p
            LEFT JOIN TAGGING_QUEUE q ON p.docnumber = q.docnumber
            WHERE p.docnumber IN ({placeholders})
            """
            await cursor.execute(sql, int_docnumbers)
            columns = [col[0].lower() for col in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except (oracledb.Error, ValueError) as e:
        logging.error(f"Error in get_specific_documents_for_processing: {e}", exc_info=True)
        return []
    finally:
        if conn:
            await conn.close()

async def check_processing_status(docnumbers):
    """Checks the TAGGING_QUEUE for a list of docnumbers and returns those not yet processed."""
    if not docnumbers:
        return []

    conn = await get_async_connection()
    if not conn:
        logging.error("Failed to get DB connection in check_processing_status.")
        return docnumbers

    try:
        int_docnumbers = [int(d) for d in docnumbers]
        async with conn.cursor() as cursor:
            bind_names = [f':doc_{i}' for i in range(len(int_docnumbers))]
            bind_vars = {f'doc_{i}': val for i, val in enumerate(int_docnumbers)}

            sql = f"""
            SELECT COLUMN_VALUE
            FROM TABLE(SYS.ODCINUMBERLIST({','.join(bind_names)})) input_docs
            WHERE input_docs.COLUMN_VALUE NOT IN (
                SELECT docnumber FROM TAGGING_QUEUE WHERE docnumber IN ({','.join(bind_names)}) AND STATUS = 3
            )
            """

            await cursor.execute(sql, bind_vars)
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
    except (oracledb.Error, ValueError) as e:
        logging.error(f"Error in check_processing_status: {e}", exc_info=True)
        return docnumbers
    finally:
        if conn:
            await conn.close()

async def reset_processing_attempts(docnumbers):
    """Resets TAGGING_QUEUE attempts/status for selected docs before retrying jobs."""
    if not docnumbers:
        return 0

    conn = await get_async_connection()
    if not conn:
        logging.error("Failed to get DB connection in reset_processing_attempts.")
        return 0

    try:
        int_docnumbers = [int(d) for d in docnumbers]
        async with conn.cursor() as cursor:
            bind_names = [f":doc_{i}" for i in range(len(int_docnumbers))]
            bind_vars = {f"doc_{i}": val for i, val in enumerate(int_docnumbers)}

            sql = f"""
            UPDATE TAGGING_QUEUE
            SET ATTEMPTS = 0,
                STATUS = 1,
                ERROR = NULL,
                LAST_UPDATE = SYSDATE
            WHERE DOCNUMBER IN ({','.join(bind_names)})
            """

            await cursor.execute(sql, bind_vars)
            updated_rows = cursor.rowcount or 0
            await conn.commit()
            return int(updated_rows)
    except (oracledb.Error, ValueError) as e:
        logging.error(f"Error in reset_processing_attempts: {e}", exc_info=True)
        try:
            await conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        if conn:
            await conn.close()

async def get_folder_system_ids(docnumbers):
    """
    Get SYSTEM_IDs for a list of docnumbers from the FOLDER_ITEM table.
    Returns a dict mapping docnumber -> system_id
    """
    try:
        if not docnumbers:
            # logging.info("No docnumbers provided")
            return {}

        # Validate and convert docnumbers
        valid_docnumbers = []
        for doc in docnumbers:
            try:
                if doc and str(doc).isdigit():
                    valid_docnumbers.append(str(doc))
            except:
                continue

        if not valid_docnumbers:
            # logging.info("No valid docnumbers provided")
            return {}

        # logging.info(f"Looking up SYSTEM_IDs for {len(valid_docnumbers)} docnumbers: {valid_docnumbers}")

        conn = await get_async_connection()
        if not conn:
            logging.error("Failed to get database connection for get_folder_system_ids")
            return {}

        try:
            # Create placeholders for the IN clause
            placeholders = ','.join([f":{i}" for i in range(len(valid_docnumbers))])
            query = f"""
                SELECT DOCNUMBER, SYSTEM_ID 
                FROM FOLDER_ITEM 
                WHERE DOCNUMBER IN ({placeholders})
            """

            # Create parameters dictionary
            params = {str(i): doc_num for i, doc_num in enumerate(valid_docnumbers)}

            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

                # Create mapping dictionary
                result = {str(row[0]): str(row[1]) for row in rows}
                # logging.info(f"Found {len(result)} SYSTEM_IDs")

                return result

        except oracledb.Error as e:
            logging.error(f"Oracle error in get_folder_system_ids: {e}", exc_info=True)
            return {}
        finally:
            await conn.close()

    except Exception as e:
        logging.error(f"Error in get_folder_system_ids: {e}", exc_info=True)
        return {}

async def get_document_by_id(doc_id):
    """Fetches a single document's metadata by ID for sharing purposes."""
    conn = await get_async_connection()
    if not conn: return None

    dst = dms_system_login()
    if not dst:
        if conn: await conn.close()
        return None

    try:
        async with conn.cursor() as cursor:
            # Basic metadata fetch
            sql = """
            SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.RTADOCDATE, p.DOCNAME
            FROM PROFILE p
            WHERE p.DOCNUMBER = :doc_id
            """
            await cursor.execute(sql, {'doc_id': doc_id})
            row = await cursor.fetchone()

            if not row:
                return None

            doc_id, abstract, author, doc_date, docname = row

            # Media info retrieval
            thumbnail_path = None
            media_type = 'image'

            try:
                # Async call to resolve media type/filename
                original_filename, media_type, file_ext = await get_media_info_from_dms(dst, doc_id)
                cached_thumbnail_file = f"{doc_id}.jpg"
                cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                if os.path.exists(cached_path):
                    thumbnail_path = f"cache/{cached_thumbnail_file}"
                else:
                    # Sync call for content retrieval (WSDL)
                    media_bytes = get_media_content_from_dms(dst, doc_id)
                    if media_bytes:
                        thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)
            except Exception as e:
                logging.error(f"Error getting media info for {doc_id}: {e}")

            return {
                "doc_id": doc_id,
                "title": abstract or "",
                "docname": docname or "",
                "author": author or "N/A",
                "date": doc_date.strftime('%Y-%m-%d %H:%M:%S') if doc_date else "N/A",
                "thumbnail_url": thumbnail_path or "",
                "media_type": media_type
            }

    except Exception as e:
        logging.error(f"Error fetching document {doc_id}: {e}")
        return None
    finally:
        await conn.close()