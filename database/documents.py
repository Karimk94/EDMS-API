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
    thumbnail_cache_dir
)
import os

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
    conn = await get_async_connection()
    if not conn: return [], 0

    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS. Aborting document fetch.")
        if conn: await conn.close()
        return [], 0

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

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    params = {}
    where_clauses = []
    shortlist_clause = ""

    # --- Scope Logic ---
    folder_doc_ids = None
    if scope == 'folders':
        # Retrieve all relevant DOCNUMBERS from WSDL first (Sync call)
        folder_doc_ids = wsdl_client.get_recursive_doc_ids(dst, media_type)
        if not folder_doc_ids:
            if conn: await conn.close()
            return [], 0

        # Paginate IDs *before* querying DB to handle large lists efficiently
        total_rows = len(folder_doc_ids)

        # If requested page is out of range, return empty
        if offset >= total_rows:
            if conn: await conn.close()
            return [], total_rows

        paginated_ids = folder_doc_ids[offset: offset + page_size]

        # Use these IDs in the WHERE clause
        placeholders = ','.join([f":fid_{i}" for i in range(len(paginated_ids))])
        where_clauses.append(f"p.DOCNUMBER IN ({placeholders})")
        for i, doc_id in enumerate(paginated_ids):
            params[f'fid_{i}'] = doc_id

    else:
        doc_filter_sql = "AND p.RTA_TEXT1 = 'edms-media'"

        if app_source == 'edms-media':
            doc_filter_sql = "AND p.RTA_TEXT1 = 'edms-media'"
        elif app_source == 'smart-edms':
            smart_edms_floor = 19662092
            doc_filter_sql = f"AND p.DOCNUMBER >= {smart_edms_floor} AND (p.RTA_TEXT1 IS NULL OR p.RTA_TEXT1 != 'edms-media')"

        where_clauses.append(doc_filter_sql.replace('AND ', '', 1))  # Strip first AND if adding to list

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

        if vector_doc_ids:
            logging.info(f"Using {len(vector_doc_ids)} doc_ids from vector search.")
            vector_placeholders = ','.join([f":vec_id_{i}" for i in range(len(vector_doc_ids))])
            vector_clause = f"p.docnumber IN ({vector_placeholders})"
            for i, doc_id in enumerate(vector_doc_ids):
                params[f'vec_id_{i}'] = doc_id

        if text_search_conditions:
            text_clause = f"({' AND '.join(text_search_conditions)})"

        if vector_clause and text_clause:
            where_clauses.append(f"({vector_clause} OR {text_clause})")
        elif vector_clause:
            where_clauses.append(vector_clause)
        elif text_clause:
            where_clauses.append(text_clause)

        # --- Standard Filters ---
        if persons:
            person_list = [p.strip() for p in persons.split(',') if p.strip()]
            if person_list:
                op = " OR " if person_condition == 'any' else " AND "
                person_conditions = []
                for i, person in enumerate(person_list):
                    key = f'person_{i}'
                    key_upper = f'person_{i}_upper'
                    person_conditions.append(f"(p.ABSTRACT LIKE :{key} OR UPPER(p.ABSTRACT) LIKE :{key_upper})")
                    params[key] = f"%{person}%"
                    params[key_upper] = f"%{person.upper()}%"
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
                        # Sync call for content retrieval (WSDL)
                        media_bytes = get_media_content_from_dms(dst, doc_id)
                        if media_bytes:
                            thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)
                        else:
                            logging.warning(f"Could not retrieve media content for doc {doc_id} to create thumbnail.")

                except Exception as media_info_e:
                    logging.error(f"Error processing media info/thumbnail for doc {doc_id}: {media_info_e}",
                                  exc_info=True)

                documents.append({
                    "doc_id": doc_id,
                    "title": final_abstract,
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": doc_date.strftime('%Y-%m-%d %H:%M:%S') if doc_date else "N/A",
                    "thumbnail_url": thumbnail_path or "",
                    "media_type": media_type,
                    "is_favorite": bool(is_favorite)
                })
    except oracledb.Error as e:
        logging.error(f"Oracle error fetching documents: {e}", exc_info=True)
        return [], 0
    finally:
        if conn:
            await conn.close()
    return documents, total_rows

async def get_documents_to_process():
    """Gets a batch of documents that need AI processing."""
    conn = await get_async_connection()
    if not conn: return []

    try:
        async with conn.cursor() as cursor:
            sql = """
            SELECT p.docnumber, p.abstract,
                   NVL(q.o_detected, 0) as o_detected,
                   NVL(q.OCR, 0) as ocr,
                   NVL(q.face, 0) as face,
                   NVL(q.attempts, 0) as attempts
            FROM PROFILE p
            LEFT JOIN TAGGING_QUEUE q ON p.docnumber = q.docnumber
            WHERE p.form = :form_id
              AND p.docnumber >= 19677386 --19662092
              AND (q.STATUS <> 3 OR q.STATUS IS NULL)
              AND (q.ATTEMPTS <= 3 OR q.ATTEMPTS IS NULL)
            FETCH FIRST 10 ROWS ONLY
            """
            await cursor.execute(sql, {'form_id': 2740})
            columns = [col[0].lower() for col in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        if conn:
            await conn.close()

async def update_document_processing_status(docnumber, new_abstract, o_detected, ocr, face, status, error, transcript,
                                            attempts):
    """Updates the processing status of a document in the database with robust transaction handling."""
    conn = await get_async_connection()
    if not conn:
        logging.error(f"DB_UPDATE_FAILURE: Could not get a database connection for docnumber {docnumber}.")
        return

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
                logging.info(f"Queueing vector update for doc_id {docnumber}.")
                try:
                    vector_client.add_or_update_document(docnumber, new_abstract)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {docnumber}: {e}", exc_info=True)

    except oracledb.Error as e:
        logging.error(f"DB_UPDATE_ERROR: Oracle error while updating docnumber {docnumber}: {e}", exc_info=True)
        try:
            await conn.rollback()
        except oracledb.Error as rb_e:
            logging.error(f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber}: {rb_e}",
                          exc_info=True)
    finally:
        if conn:
            await conn.close()

async def update_abstract_with_vips(doc_id, vip_names):
    """Appends or updates VIP names in a document's abstract."""
    conn = await get_async_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            result = await cursor.fetchone()
            if result is None: return False, f"Document with ID {doc_id} not found."

            current_abstract = result[0] or ""

            base_abstract = re.sub(r'\s*\n*VIPs\s*:.*', '', current_abstract, flags=re.IGNORECASE).strip()

            names_str = ", ".join(sorted(list(set(vip_names))))

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