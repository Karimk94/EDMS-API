import oracledb
import os
from dotenv import load_dotenv
from zeep import Client, Settings
from zeep.exceptions import Fault
import re
from PIL import Image
import io
import shutil
import logging
from moviepy import VideoFileClip
import fitz 
from datetime import datetime, timedelta
import wsdl_client
import json

load_dotenv()

# --- Cache Directory Setup ---
thumbnail_cache_dir = os.path.join(os.path.dirname(__file__), 'thumbnail_cache')
if not os.path.exists(thumbnail_cache_dir):
    os.makedirs(thumbnail_cache_dir)

video_cache_dir = os.path.join(os.path.dirname(__file__), 'video_cache')
if not os.path.exists(video_cache_dir):
    os.makedirs(video_cache_dir)

# --- Blocklist Loading ---
BLOCKLIST = {}
try:
    blocklist_path = os.path.join(os.path.dirname(__file__), 'blocklist.json')
    with open(blocklist_path, 'r', encoding='utf-8') as f:
        loaded_blocklist = json.load(f)
        # Combine all meaningless words into a single set for efficient lookup
        meaningless_words = set(loaded_blocklist.get('meaningless_english', []))
        meaningless_words.update(loaded_blocklist.get('meaningless_arabic', []))
        BLOCKLIST['meaningless'] = meaningless_words
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.warning(f"Could not load or parse blocklist.json: {e}")


# --- DMS Communication ---

def dms_system_login():
    """Logs into the DMS SOAP service using system credentials from .env and returns a session token (DST)."""
    return wsdl_client.dms_system_login()


def get_media_info_from_dms(dst, doc_number):
    """
    Efficiently retrieves only the metadata (like filename) for a document from the DMS
    without downloading the full file content.
    """
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl_url = os.getenv("WSDL_URL")
        svc_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMSvc', settings=settings)

        get_doc_call = {
            'call': {
                'dstIn': dst,
                'criteria': {
                    'criteriaCount': 2,
                    'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                    'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}
                }
            }
        }
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)

        if not (doc_reply and doc_reply.resultCode == 0):
            return None, 'image', ''

        filename = f"{doc_number}"  # Default
        if doc_reply.docProperties and doc_reply.docProperties.propertyValues:
            try:
                prop_names = doc_reply.docProperties.propertyNames.string
                if '%VERSION_FILE_NAME' in prop_names:
                    index = prop_names.index('%VERSION_FILE_NAME')
                    version_file_name = doc_reply.docProperties.propertyValues.anyType[index]
                    if version_file_name:
                        filename = str(version_file_name)
            except Exception as e:
                print(f"Could not get filename for {doc_number}, using default. Error: {e}")

        video_extensions = ['.mp4', '.mov', '.avi', '.mkv']
        pdf_extensions = ['.pdf']
        file_ext = os.path.splitext(filename)[1].lower()
        media_type = 'video' if file_ext in video_extensions else 'image'
        if file_ext in pdf_extensions:
            media_type = 'pdf'

        return filename, media_type, file_ext

    except Fault as e:
        print(f"DMS metadata fault for doc {doc_number}: {e}")
        return None, 'image', ''


def get_media_content_from_dms(dst, doc_number):
    """
    Retrieves the full binary content of a media file from the DMS.
    """
    obj_client, content_id, stream_id = None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl_url = os.getenv("WSDL_URL")
        svc_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMObj', settings=settings)

        get_doc_call = {
            'call': {'dstIn': dst,
                     'criteria': {'criteriaCount': 2, 'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                                  'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}}}
        }
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)
        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID):
            return None

        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            return None

        stream_id = stream_reply.streamID
        doc_buffer = bytearray()
        while True:
            read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
            if not read_reply or read_reply.resultCode != 0: break
            chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
            if not chunk_data: break
            doc_buffer.extend(chunk_data)

        return bytes(doc_buffer)
    except Exception as e:
        print(f"Error getting media content for {doc_number}: {e}")
        return None
    finally:
        if obj_client:
            if stream_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception:
                    pass
            if content_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception:
                    pass


def get_dms_stream_details(dst, doc_number):
    """
    Opens a stream to a DMS document and returns the client and stream ID for reading.
    """
    return wsdl_client.get_dms_stream_details(dst, doc_number)

# --- Caching, Streaming, and Thumbnail Logic ---

def stream_and_cache_generator(obj_client, stream_id, content_id, final_cache_path):
    """
    A generator that streams data from DMS, yields it for the user,
    and simultaneously saves it to a cache file.
    """
    temp_cache_path = final_cache_path + ".tmp"
    try:
        with open(temp_cache_path, "wb") as f:
            while True:
                read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
                if not read_reply or read_reply.resultCode != 0:
                    break
                chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
                if not chunk_data:
                    break
                f.write(chunk_data)
                yield chunk_data

        # Once fully downloaded, move the temp file to its final location
        os.rename(temp_cache_path, final_cache_path)
        logging.info(f"Successfully cached file to {final_cache_path}")
    except Exception as e:
        logging.error(f"Error during streaming/caching: {e}")
    finally:
        # CRITICAL: Always release DMS objects to prevent resource leaks
        try:
            if stream_id: obj_client.service.ReleaseObject(call={'objectID': stream_id})
            if content_id: obj_client.service.ReleaseObject(call={'objectID': content_id})
        except Exception as e:
            logging.error(f"Failed to release DMS objects: {e}")
        # Clean up temp file if it still exists on error
        if os.path.exists(temp_cache_path):
            os.remove(temp_cache_path)


def create_thumbnail(doc_number, media_type, file_ext, media_bytes):
    """Creates a thumbnail from media bytes and saves it to the cache."""
    thumbnail_filename = f"{doc_number}.jpg"
    cached_path = os.path.join(thumbnail_cache_dir, thumbnail_filename)
    try:
        if media_type == 'video':
            temp_video_path = os.path.join(thumbnail_cache_dir, f"{doc_number}{file_ext}")
            with open(temp_video_path, 'wb') as f: f.write(media_bytes)
            with VideoFileClip(temp_video_path) as clip: clip.save_frame(cached_path, t=1)
            os.remove(temp_video_path)
        elif media_type == 'pdf':
            with fitz.open(stream=media_bytes, filetype="pdf") as doc:
                page = doc.load_page(0)  # Load the first page
                pix = page.get_pixmap()
                with Image.frombytes("RGB", [pix.width, pix.height], pix.samples) as img:
                    img.save(cached_path, "JPEG", quality=95)
        else:
            with Image.open(io.BytesIO(media_bytes)) as img:
                img.thumbnail((300, 300))
                img.convert("RGB").save(cached_path, "JPEG", quality=95)
        return f"cache/{thumbnail_filename}"
    except Exception as e:
        print(f"Could not create thumbnail for {doc_number}: {e}")
        return None


# --- Oracle Database Interaction ---

def get_connection():
    """Establishes a connection to the Oracle database."""
    try:
        dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"
        return oracledb.connect(user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), dsn=dsn)
    except oracledb.Error as ex:
        error, = ex.args
        print(f"DB connection error: {error.message}")
        return None

def get_user_security_level(username):
    """Fetches the user's security level name from the database using their user ID from the PEOPLE table."""
    conn = get_connection()
    if not conn:
        return "Viewer"  # Default to Viewer if DB connection fails
    
    security_level = "Viewer"  # Default value
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()
            
            if user_result:
                user_id = user_result[0]
                
                # Now, get the security level using the user_id
                query = """
                    SELECT sl.NAME
                    FROM LKP_PTA_USR_SECUR us
                    JOIN LKP_PTA_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                cursor.execute(query, user_id=user_id)
                level_result = cursor.fetchone()
                if level_result:
                    security_level = level_result[0]
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in get_user_security_level: {e}")
    finally:
        if conn:
            conn.close()
    return security_level

def get_app_id_from_extension(extension):
    """
    Looks up the APPLICATION (APP_ID) from the APPS table based on the file extension.
    """
    conn = get_connection()
    if not conn:
        return None

    app_id = None
    try:
        with conn.cursor() as cursor:
            # First, check the DEFAULT_EXTENSION column
            cursor.execute("SELECT APPLICATION FROM APPS WHERE DEFAULT_EXTENSION = :ext", ext=extension)
            result = cursor.fetchone()
            if result:
                app_id = result[0]
            else:
                # If not found, check the FILE_TYPES column
                cursor.execute("SELECT APPLICATION FROM APPS WHERE FILE_TYPES LIKE :ext_like",
                               ext_like=f"%{extension}%")
                result = cursor.fetchone()
                if result:
                    app_id = result[0]
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in get_app_id_from_extension: {e}")
    finally:
        if conn:
            conn.close()
    return app_id


def get_specific_documents_for_processing(docnumbers):
    """Gets details for a specific list of docnumbers that need AI processing."""
    if not docnumbers:
        return []

    conn = get_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cursor:
            # Create placeholders for the IN clause
            placeholders = ','.join([':' + str(i + 1) for i in range(len(docnumbers))])

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
            cursor.execute(sql, docnumbers)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        if conn:
            conn.close()


def check_processing_status(docnumbers):
    """
    Checks the TAGGING_QUEUE for a list of docnumbers and returns those
    that are not yet successfully processed (status != 3).
    """
    if not docnumbers:
        return []

    conn = get_connection()
    if not conn:
        return docnumbers  # Assume still processing if DB is down

    try:
        with conn.cursor() as cursor:
            placeholders = ','.join([':' + str(i + 1) for i in range(len(docnumbers))])

            sql = f"""
            SELECT COLUMN_VALUE FROM TABLE(SYS.ODCINUMBERLIST({placeholders})) input_docs
            WHERE input_docs.COLUMN_VALUE NOT IN (
                SELECT docnumber FROM TAGGING_QUEUE WHERE docnumber IN ({placeholders}) AND STATUS = 3
            )
            """

            cursor.execute(sql, docnumbers + docnumbers)
            still_processing = [row[0] for row in cursor.fetchall()]
            return still_processing
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in check_processing_status: {e}")
        return docnumbers
    finally:
        if conn:
            conn.close()


def fetch_documents_from_oracle(page=1, page_size=10, search_term=None, date_from=None, date_to=None,
                                persons=None, person_condition='any', tags=None):
    """Fetches a paginated list of documents from Oracle, handling filtering and thumbnail logic."""
    conn = get_connection()
    if not conn: return [], 0

    dst = dms_system_login()
    if not dst:
        print("Could not log into DMS. Aborting document fetch.")
        return [], 0

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    base_where = "WHERE p.docnumber >= 19662092 AND p.FORM = 2740 "
    params = {}
    where_clauses = []

    if search_term:
        words = re.findall(r'\w+', search_term.upper())
        search_conditions = []
        for i, word in enumerate(words):
            key = f"search_word_{i}"
            search_conditions.append(f"UPPER(p.ABSTRACT) LIKE :{key}")
            params[key] = f"%{word}%"
        where_clauses.append(" AND ".join(search_conditions))

    if persons:
        person_list = [p.strip().upper() for p in persons.split(',') if p.strip()]
        if person_list:
            op = " OR " if person_condition == 'any' else " AND "
            person_conditions = []
            for i, person in enumerate(person_list):
                key = f'person_{i}'
                person_conditions.append(f"UPPER(p.ABSTRACT) LIKE :{key}")
                params[key] = f"%{person}%"
            where_clauses.append(f"({op.join(person_conditions)})")

    if date_from:
        where_clauses.append("p.CREATION_DATE >= TO_DATE(:date_from, 'YYYY-MM-DD HH24:MI:SS')")
        params['date_from'] = date_from
    if date_to:
        where_clauses.append("p.CREATION_DATE <= TO_DATE(:date_to, 'YYYY-MM-DD HH24:MI:SS')")
        params['date_to'] = date_to

    if tags:
        tag_list = [t.strip().upper() for t in tags.split(',') if t.strip()]
        if tag_list:
            tag_conditions = []
            for i, tag in enumerate(tag_list):
                key = f'tag_{i}'
                keyword_subquery = f"""
                EXISTS (
                    SELECT 1 FROM LKP_DOCUMENT_TAGS ldt
                    JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                    WHERE ldt.DOCNUMBER = p.DOCNUMBER AND UPPER(k.KEYWORD_ID) = :{key}
                )
                """
                person_subquery = f"UPPER(p.ABSTRACT) LIKE '%' || :{key} || '%'"

                tag_conditions.append(f"({keyword_subquery} OR {person_subquery})")
                params[key] = tag

            where_clauses.append(" AND ".join(tag_conditions))

    final_where_clause = base_where + ("AND " + " AND ".join(where_clauses) if where_clauses else "")

    try:
        with conn.cursor() as cursor:
            count_query = f"SELECT COUNT(p.DOCNUMBER) FROM PROFILE p {final_where_clause}"
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            fetch_query = f"SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.CREATION_DATE, p.DOCNAME FROM PROFILE p {final_where_clause}"
            params['offset'] = offset
            params['page_size'] = page_size

            cursor.execute(
                fetch_query + " ORDER BY p.DOCNUMBER DESC OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY",
                params)

            for row in cursor:
                doc_id, abstract, author, creation_date, docname = row
                thumbnail_path = None

                original_filename, media_type, file_ext = get_media_info_from_dms(dst, doc_id)

                cached_thumbnail_file = f"{doc_id}.jpg"
                cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                if os.path.exists(cached_path):
                    thumbnail_path = f"cache/{cached_thumbnail_file}"
                else:
                    media_bytes = get_media_content_from_dms(dst, doc_id)
                    if media_bytes:
                        thumbnail_path = create_thumbnail(doc_id, media_type, file_ext, media_bytes)

                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or "No Title",
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": creation_date.strftime('%Y-%m-%d') if creation_date else "N/A",
                    "thumbnail_url": thumbnail_path or "",
                    "media_type": media_type
                })
    finally:
        conn.close()
    return documents, total_rows


def get_documents_to_process():
    """Gets a batch of documents that need AI processing."""
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        try:
            sql = """
            SELECT p.docnumber, p.abstract,
                   NVL(q.o_detected, 0) as o_detected,
                   NVL(q.OCR, 0) as ocr,
                   NVL(q.face, 0) as face,
                   NVL(q.attempts, 0) as attempts
            FROM PROFILE p
            LEFT JOIN TAGGING_QUEUE q ON p.docnumber = q.docnumber
            WHERE p.form = :form_id
              AND p.docnumber >= 19662092
              AND (q.STATUS <> 3 OR q.STATUS IS NULL)
              AND (q.ATTEMPTS <= 3 OR q.ATTEMPTS IS NULL)
            FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(sql, {'form_id': 2740})
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()
    return []


def update_document_processing_status(docnumber, new_abstract, o_detected, ocr, face, status, error, transcript,
                                      attempts):
    """Updates the processing status of a document in the database with robust transaction handling."""
    conn = get_connection()
    if not conn:
        logging.error(f"DB_UPDATE_FAILURE: Could not get a database connection for docnumber {docnumber}.")
        return

    try:
        with conn.cursor() as cursor:
            conn.begin()

            cursor.execute("UPDATE PROFILE SET abstract = :1 WHERE docnumber = :2", (new_abstract, docnumber))

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
            cursor.execute(merge_sql, {
                'docnumber': docnumber, 'o_detected': o_detected, 'ocr': ocr, 'face': face,
                'status': status, 'error': error, 'transcript': transcript, 'attempts': attempts
            })

            conn.commit()
            logging.info(f"DB_UPDATE_SUCCESS: Successfully updated status for docnumber {docnumber}.")

    except oracledb.Error as e:
        logging.error(f"DB_UPDATE_ERROR: Oracle error while updating docnumber {docnumber}: {e}", exc_info=True)
        try:
            conn.rollback()
            logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} was rolled back.")
        except oracledb.Error as rb_e:
            logging.error(f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber}: {rb_e}",
                          exc_info=True)

    finally:
        if conn:
            conn.close()


def update_abstract_with_vips(doc_id, vip_names):
    """Appends VIP names to a document's abstract."""
    conn = get_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            result = cursor.fetchone()
            if result is None: return False, f"Document with ID {doc_id} not found."
            current_abstract = result[0] or ""

            if "VIPs :" in current_abstract:
                return True, "Abstract already contains VIPs section."

            names_str = ", ".join(vip_names)
            vips_section = f" VIPs : {names_str}"
            new_abstract = current_abstract + (" " if current_abstract else "") + vips_section
            cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
            conn.commit()
            return True, "Abstract updated successfully."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()


def add_person_to_lkp(person_name):
    """Adds a new person to the LKP_PERSON lookup table."""
    conn = get_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON WHERE NAME_ENGLISH = :1", [person_name])
            if cursor.fetchone()[0] > 0:
                return True, f"'{person_name}' already exists in LKP_PERSON."

            insert_query = """
                INSERT INTO LKP_PERSON (NAME_ENGLISH, LAST_UPDATE, DISABLED, SYSTEM_ID)
                VALUES (:1, SYSDATE, 0, (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PERSON))
            """
            cursor.execute(insert_query, [person_name])
            conn.commit()
            return True, f"Successfully added '{person_name}' to LKP_PERSON."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()


def fetch_lkp_persons(page=1, page_size=20, search=''):
    """Fetches a paginated list of people from the LKP_PERSON table."""
    conn = get_connection()
    if not conn: return [], 0

    offset = (page - 1) * page_size
    persons = []
    total_rows = 0
    search_term = f"%{search.upper()}%"
    count_query = "SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON WHERE UPPER(NAME_ENGLISH) LIKE :search OR UPPER(NAME_ARABIC) LIKE :search"
    fetch_query = "SELECT SYSTEM_ID, NAME_ENGLISH, NVL(NAME_ARABIC, '') FROM LKP_PERSON WHERE UPPER(NAME_ENGLISH) LIKE :search OR UPPER(NAME_ARABIC) LIKE :search ORDER BY NAME_ENGLISH OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"

    try:
        with conn.cursor() as cursor:
            cursor.execute(count_query, search=search_term)
            total_rows = cursor.fetchone()[0]

            cursor.execute(fetch_query, search=search_term, offset=offset, page_size=page_size)
            for row in cursor:
                persons.append({"id": row[0], "name_english": row[1], "name_arabic": row[2]})
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_lkp_persons: {e}")
    finally:
        conn.close()
    return persons, total_rows


def fetch_all_tags():
    """Fetches all unique keywords and person names to be used as tags."""
    conn = get_connection()
    if not conn: return []

    tags = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT KEYWORD_ID FROM KEYWORD k JOIN LKP_DOCUMENT_TAGS ldt ON ldt.TAG_ID = k.SYSTEM_ID")
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())

            cursor.execute("SELECT NAME_ENGLISH FROM LKP_PERSON")
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_all_tags: {e}")
    finally:
        if conn:
            conn.close()

    return sorted(list(tags))


def fetch_tags_for_document(doc_id):
    """Fetches all keyword and person tags for a single document."""
    conn = get_connection()
    if not conn:
        return []

    doc_tags = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :doc_id", {'doc_id': doc_id})
            result = cursor.fetchone()
            abstract = result[0] if result else None

            tag_query = """
                SELECT k.KEYWORD_ID
                FROM LKP_DOCUMENT_TAGS ldt
                JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                WHERE ldt.DOCNUMBER = :doc_id
            """
            cursor.execute(tag_query, {'doc_id': doc_id})
            for tag_row in cursor:
                if tag_row[0]:
                    doc_tags.add(tag_row[0])

            if abstract:
                person_query = """
                    SELECT NAME_ENGLISH
                    FROM LKP_PERSON
                    WHERE :abstract LIKE '%' || UPPER(NAME_ENGLISH) || '%'
                """
                cursor.execute(person_query, {'abstract': abstract.upper()})
                for person_row in cursor:
                    if person_row[0]:
                        doc_tags.add(person_row[0])
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_tags_for_document for doc_id {doc_id}: {e}")
    finally:
        if conn:
            conn.close()

    return sorted(list(doc_tags))


def clear_thumbnail_cache():
    """Deletes all files in the thumbnail cache directory."""
    if os.path.exists(thumbnail_cache_dir):
        shutil.rmtree(thumbnail_cache_dir)
    os.makedirs(thumbnail_cache_dir)


def clear_video_cache():
    """Deletes all files in the video cache directory."""
    if os.path.exists(video_cache_dir):
        shutil.rmtree(video_cache_dir)
    os.makedirs(video_cache_dir)


def insert_keywords_and_tags(docnumber, keywords):
    """
    Inserts keywords and links them to a document. Handles duplicates gracefully.
    'keywords' is a list of dictionaries, with each dictionary having 'english' and 'arabic' keys.
    """
    conn = get_connection()
    if not conn:
        logging.error(f"DB_KEYWORD_FAILURE: Could not get a database connection for docnumber {docnumber}.")
        return

    try:
        with conn.cursor() as cursor:
            processed_keywords = set()

            for keyword in keywords:
                english_keyword_orig = keyword.get('english')
                arabic_keyword = keyword.get('arabic')

                if not english_keyword_orig or not arabic_keyword:
                    continue

                # --- Keyword Validation ---
                if len(english_keyword_orig.strip()) < 2:
                    logging.warning(f"Skipping short keyword '{english_keyword_orig}' for docnumber {docnumber}.")
                    continue

                if ' ' not in english_keyword_orig and english_keyword_orig.lower() in BLOCKLIST.get('meaningless', set()):
                    logging.warning(f"Skipping meaningless keyword '{english_keyword_orig}' for docnumber {docnumber}.")
                    continue

                english_keyword = english_keyword_orig.lower()

                if english_keyword in processed_keywords:
                    continue

                if len(english_keyword) > 30:
                    logging.warning(
                        f"Skipping keyword '{english_keyword_orig}' for docnumber {docnumber} because its length ({len(english_keyword_orig)}) exceeds the 30-character limit.")
                    continue

                keyword_system_id = None

                cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :keyword_id",
                               keyword_id=english_keyword)
                result = cursor.fetchone()

                if result:
                    keyword_system_id = result[0]
                else:
                    try:
                        cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                        keyword_system_id = cursor.fetchone()[0]

                        cursor.execute("""
                            INSERT INTO KEYWORD (KEYWORD_ID, DESCRIPTION, SYSTEM_ID)
                            VALUES (:keyword_id, :description, :system_id)
                        """, keyword_id=english_keyword, description=arabic_keyword,
                                       system_id=keyword_system_id)

                    except oracledb.IntegrityError as ie:
                        error, = ie.args
                        if "ORA-00001" in error.message:
                            logging.warning(
                                f"Keyword '{english_keyword}' was inserted by another process. Fetching existing ID.")
                            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :keyword_id",
                                           keyword_id=english_keyword)
                            result = cursor.fetchone()
                            if result:
                                keyword_system_id = result[0]
                            else:
                                logging.error(
                                    f"Failed to fetch SYSTEM_ID for '{english_keyword}' after integrity error.")
                                continue
                        else:
                            raise

                if keyword_system_id:
                    cursor.execute(
                        "SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :docnumber AND TAG_ID = :tag_id",
                        docnumber=docnumber, tag_id=keyword_system_id)

                    if cursor.fetchone()[0] == 0:
                        cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_DOCUMENT_TAGS")
                        lkp_system_id = cursor.fetchone()[0]
                        cursor.execute("""
                            INSERT INTO LKP_DOCUMENT_TAGS (DOCNUMBER, TAG_ID, SYSTEM_ID, LAST_UPDATE, DISABLED)
                            VALUES (:docnumber, :tag_id, :system_id, SYSDATE, 0)
                        """, docnumber=docnumber, tag_id=keyword_system_id, system_id=lkp_system_id)

                processed_keywords.add(english_keyword)

            conn.commit()
            logging.info(f"DB_KEYWORD_SUCCESS: Successfully processed keywords for docnumber {docnumber}.")

    except oracledb.Error as e:
        logging.error(f"DB_KEYWORD_ERROR: Oracle error while processing keywords for docnumber {docnumber}: {e}",
                      exc_info=True)
        try:
            conn.rollback()
            logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} keywords was rolled back.")
        except oracledb.Error as rb_e:
            logging.error(
                f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber} keywords: {rb_e}",
                exc_info=True)

    finally:
        if conn:
            conn.close()


def add_tag_to_document(doc_id, tag):
    """Adds a new tag to a document, handling existing keywords and validating the tag."""
    # --- Tag Validation ---
    if not tag or len(tag.strip()) < 2:
        return False, "Tag cannot be empty or less than 2 characters."

    # If the tag is a single word, check if it's in the blocklist
    if ' ' not in tag and tag.lower() in BLOCKLIST.get('meaningless', set()):
        return False, f"Tag '{tag}' is a meaningless word and cannot be added."

    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Check if the keyword already exists
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [tag.lower()])
            result = cursor.fetchone()
            if result:
                keyword_id = result[0]
            else:
                # Insert new keyword if it doesn't exist
                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                keyword_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)",
                               [tag.lower(), keyword_id])

            # Check if the document is already tagged with this keyword
            cursor.execute("SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :1 AND TAG_ID = :2",
                           [doc_id, keyword_id])
            if cursor.fetchone()[0] > 0:
                return True, "Tag already exists on this document."

            # Link the keyword to the document
            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_DOCUMENT_TAGS")
            lkp_system_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT INTO LKP_DOCUMENT_TAGS (DOCNUMBER, TAG_ID, SYSTEM_ID, LAST_UPDATE, DISABLED)
                VALUES (:docnumber, :tag_id, :system_id, SYSDATE, 0)
            """, docnumber=doc_id, tag_id=keyword_id, system_id=lkp_system_id)
            conn.commit()
            return True, "Tag added successfully."
    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()


def update_tag_for_document(doc_id, old_tag, new_tag):
    """Updates a tag for a document."""
    # --- Tag Validation ---
    if not new_tag or len(new_tag.strip()) < 2:
        return False, "New tag cannot be empty or less than 2 characters."

    if ' ' not in new_tag and new_tag.lower() in BLOCKLIST.get('meaningless', set()):
        return False, f"Tag '{new_tag}' is a meaningless word and cannot be added."

    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Find the old keyword ID
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [old_tag.lower()])
            result = cursor.fetchone()
            if not result:
                return False, "Old tag not found."
            old_keyword_id = result[0]

            # Check if the new keyword exists
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [new_tag.lower()])
            result = cursor.fetchone()
            if result:
                new_keyword_id = result[0]
            else:
                # Insert new keyword
                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                new_keyword_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)",
                               [new_tag.lower(), new_keyword_id])

            # Update the link in LKP_DOCUMENT_TAGS
            cursor.execute("""
                UPDATE LKP_DOCUMENT_TAGS
                SET TAG_ID = :new_tag_id
                WHERE DOCNUMBER = :doc_id AND TAG_ID = :old_tag_id
            """, new_tag_id=new_keyword_id, doc_id=doc_id, old_tag_id=old_keyword_id)
            conn.commit()
            return True, "Tag updated successfully."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()


def delete_tag_from_document(doc_id, tag):
    """Deletes a tag from a document. The tag can be a keyword or a person's name."""
    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."

    try:
        with conn.cursor() as cursor:
            # First, assume the tag is a keyword and try to delete it.
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [tag.lower()])
            keyword_result = cursor.fetchone()

            if keyword_result:
                keyword_id = keyword_result[0]
                # Delete the link from LKP_DOCUMENT_TAGS
                cursor.execute("""
                    DELETE FROM LKP_DOCUMENT_TAGS
                    WHERE DOCNUMBER = :doc_id AND TAG_ID = :tag_id
                """, doc_id=doc_id, tag_id=keyword_id)
                
                # Check if any rows were deleted
                if cursor.rowcount > 0:
                    conn.commit()
                    return True, "Tag deleted successfully."

            # If the tag was not found as a keyword or no rows were deleted, check if it's a person.
            cursor.execute("SELECT NAME_ENGLISH FROM LKP_PERSON WHERE UPPER(NAME_ENGLISH) = :1", [tag.upper()])
            person_result = cursor.fetchone()

            if person_result:
                # It's a person, so we need to modify the abstract.
                cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                abstract_result = cursor.fetchone()
                if not abstract_result or not abstract_result[0]:
                    return False, "Document abstract not found or is empty."

                current_abstract = abstract_result[0]
                # Find the VIPs section
                vips_match = re.search(r'VIPs:\s*(.*)', current_abstract, re.IGNORECASE)
                if vips_match:
                    vips_str = vips_match.group(1)
                    vips_list = [name.strip() for name in vips_str.split(',')]
                    
                    # Remove the person from the list (case-insensitive)
                    original_len = len(vips_list)
                    vips_list = [name for name in vips_list if name.lower() != tag.lower()]

                    if len(vips_list) < original_len:
                        # Reconstruct the abstract
                        if vips_list:
                            new_vips_str = "VIPs: " + ", ".join(vips_list)
                            new_abstract = current_abstract.replace(vips_match.group(0), new_vips_str)
                        else:
                            # If no VIPs are left, remove the entire VIPs line
                            new_abstract = current_abstract.replace(vips_match.group(0), '').strip()

                        cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
                        conn.commit()
                        return True, "Person tag removed from abstract successfully."

            conn.commit()
            return False, f"Tag '{tag}' not found for this document."

    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

# --- Archiving Database Functions ---

def get_dashboard_counts():
    conn = get_connection()
    if not conn:
        return {
            "total_employees": 0,
            "active_employees": 0,
            "judicial_warrants": 0,
            "expiring_soon": 0,
        }

    counts = {}
    try:
        with conn.cursor() as cursor:
            # Total employees
            cursor.execute("SELECT COUNT(*) FROM LKP_PTA_EMP_ARCH")
            counts["total_employees"] = cursor.fetchone()[0]

            # Active employees
            cursor.execute("""
                SELECT COUNT(*)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
                WHERE TRIM(stat.NAME_ENGLISH) = 'Active'
            """)
            counts["active_employees"] = cursor.fetchone()[0]

            # Judicial Warrants
            cursor.execute("""
                SELECT COUNT(DISTINCT arch.SYSTEM_ID)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID
                JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                WHERE (TRIM(dt.NAME) LIKE '%Warrant Decisions%' OR TRIM(dt.NAME) LIKE '%القرارات الخاصة بالضبطية%') AND doc.DISABLED = '0'
            """)
            counts["judicial_warrants"] = cursor.fetchone()[0]

            # Expiring Soon (in the next 30 days)
            cursor.execute("""
                SELECT COUNT(DISTINCT arch.SYSTEM_ID)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID
                WHERE doc.EXPIRY BETWEEN SYSDATE AND SYSDATE + 30 AND doc.DISABLED = '0'
            """)
            counts["expiring_soon"] = cursor.fetchone()[0]
    finally:
        if conn:
            conn.close()
    return counts


def fetch_archived_employees(page=1, page_size=20, search_term=None, status=None, filter_type=None):
    conn = get_connection()
    if not conn: return [], 0
    offset = (page - 1) * page_size
    employees, total_rows = [], 0
    base_query = """
        FROM LKP_PTA_EMP_ARCH arch
        JOIN lkp_hr_employees hr ON arch.EMPLOYEE_ID = hr.SYSTEM_ID
        LEFT JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
    """
    where_clauses, params = [], {}
    if search_term:
        where_clauses.append(
            "(UPPER(TRIM(hr.FULLNAME_EN)) LIKE :search OR UPPER(TRIM(hr.FULLNAME_AR)) LIKE :search OR TRIM(hr.EMPNO) LIKE :search)")
        params['search'] = f"%{search_term.upper()}%"
    if status:
        where_clauses.append("TRIM(stat.NAME_ENGLISH) = :status")
        params['status'] = status

    if filter_type == 'judicial_warrant':
        base_query += " JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID"
        where_clauses.append(
            "(TRIM(dt.NAME) LIKE '%Warrant Decisions%' OR TRIM(dt.NAME) LIKE '%القرارات الخاصة بالضبطية%') AND doc.DISABLED = '0'")
    elif filter_type == 'expiring_soon':
        base_query += " JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID"
        where_clauses.append("doc.EXPIRY BETWEEN SYSDATE AND SYSDATE + 30 AND doc.DISABLED = '0'")

    final_where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    try:
        with conn.cursor() as cursor:
            count_query = f"SELECT COUNT(DISTINCT arch.SYSTEM_ID) {base_query} {final_where_clause}"
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            fetch_query = f"""
                SELECT DISTINCT arch.SYSTEM_ID, TRIM(hr.FULLNAME_EN) as FULLNAME_EN, TRIM(hr.FULLNAME_AR) as FULLNAME_AR, TRIM(hr.EMPNO) as EMPNO, TRIM(hr.DEPARTEMENT) as DEPARTMENT, TRIM(hr.SECTION) as SECTION,
                       TRIM(stat.NAME_ENGLISH) as STATUS_EN, TRIM(stat.NAME_ARABIC) as STATUS_AR
                {base_query} {final_where_clause} ORDER BY arch.SYSTEM_ID DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            params.update({'offset': offset, 'page_size': page_size})
            cursor.execute(fetch_query, params)

            columns = [c[0].lower() for c in cursor.description]
            employees = []
            for row in cursor.fetchall():
                emp = dict(zip(columns, row))

                cursor.execute("""
                    SELECT 1
                    FROM LKP_PTA_EMP_DOCS doc
                    JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                    WHERE doc.PTA_EMP_ARCH_ID = :1 AND (TRIM(dt.NAME) LIKE '%Warrant Decisions%' OR TRIM(dt.NAME) LIKE '%القرارات الخاصة بالضبطية%') AND doc.DISABLED = '0'
                    AND ROWNUM = 1
                """, [emp['system_id']])
                warrant_decision_doc = cursor.fetchone()
                emp['warrant_status'] = 'توجد / Yes' if warrant_decision_doc else 'لا توجد / No'

                cursor.execute("""
                    SELECT doc.EXPIRY
                    FROM LKP_PTA_EMP_DOCS doc
                    JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                    WHERE doc.PTA_EMP_ARCH_ID = :1 AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%') AND doc.DISABLED = '0'
                """, [emp['system_id']])
                judicial_card = cursor.fetchone()

                if judicial_card:
                    emp['card_status'] = 'توجد / Yes'
                    expiry_date = judicial_card[0]
                    if expiry_date:
                        emp['card_expiry'] = expiry_date.strftime('%Y-%m-%d')
                        if expiry_date < datetime.now():
                            emp['card_status_class'] = 'expired'
                        elif expiry_date < datetime.now() + timedelta(days=30):
                            emp['card_status_class'] = 'expiring-soon'
                        else:
                            emp['card_status_class'] = 'valid'
                    else:
                        emp['card_expiry'] = 'N/A'
                        emp['card_status_class'] = 'valid'
                else:
                    emp['card_status'] = 'لا توجد / No'
                    emp['card_expiry'] = 'N/A'
                    emp['card_status_class'] = ''

                employees.append(emp)
    finally:
        if conn: conn.close()
    return employees, total_rows


def fetch_hr_employees_paginated(search_term="", page=1, page_size=10):
    conn = get_connection()
    if not conn: return [], 0
    offset = (page - 1) * page_size
    employees, total_rows = [], 0
    base_query = "FROM lkp_hr_employees hr WHERE hr.SYSTEM_ID NOT IN (SELECT EMPLOYEE_ID FROM LKP_PTA_EMP_ARCH WHERE EMPLOYEE_ID IS NOT NULL)"
    params = {}
    search_clause = ""
    if search_term:
        search_clause = " AND (UPPER(TRIM(hr.FULLNAME_EN)) LIKE :search OR UPPER(TRIM(hr.FULLNAME_AR)) LIKE :search OR TRIM(hr.EMPNO) LIKE :search)"
        params['search'] = f"%{search_term.upper()}%"
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(hr.SYSTEM_ID) {base_query} {search_clause}", params)
            total_rows = cursor.fetchone()[0]
            query = f"SELECT SYSTEM_ID, TRIM(FULLNAME_EN) as FULLNAME_EN, TRIM(FULLNAME_AR) as FULLNAME_AR, TRIM(EMPNO) as EMPNO {base_query} {search_clause} ORDER BY hr.FULLNAME_EN OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"
            params.update({'offset': offset, 'page_size': page_size})
            cursor.execute(query, params)
            employees = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in cursor.fetchall()]
    finally:
        if conn: conn.close()
    return employees, total_rows


def fetch_hr_employee_details(employee_id):
    conn = get_connection()
    if not conn: return None
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(FULLNAME_EN) as FULLNAME_EN, TRIM(FULLNAME_AR) as FULLNAME_AR, TRIM(EMPNO) as EMPNO, TRIM(DEPARTEMENT) as DEPARTMENT, TRIM(SECTION) as SECTION, TRIM(EMAIL) as EMAIL, TRIM(MOBILE) as MOBILE, TRIM(SUPERVISORNAME) as SUPERVISORNAME, TRIM(NATIONALITY) as NATIONALITY, TRIM(JOB_NAME) as JOB_NAME FROM lkp_hr_employees WHERE SYSTEM_ID = :1",
                [employee_id])
            columns = [col[0].lower() for col in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
    finally:
        if conn: conn.close()


def fetch_statuses():
    conn = get_connection()
    if not conn: return {}
    statuses = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(NAME_ENGLISH) as NAME_ENGLISH, TRIM(NAME_ARABIC) as NAME_ARABIC FROM LKP_PTA_EMP_STATUS WHERE DISABLED='0'")
            statuses['employee_status'] = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in
                                           cursor.fetchall()]
    finally:
        if conn: conn.close()
    return statuses


def fetch_document_types():
    conn = get_connection()
    if not conn: return {"all_types": [], "types_with_expiry": []}
    doc_types = {"all_types": [], "types_with_expiry": []}
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(NAME) as NAME, HAS_EXPIRY FROM LKP_PTA_DOC_TYPES WHERE DISABLED = '0' ORDER BY SYSTEM_ID")
            for row in cursor:
                doc_type_obj = {'system_id': row[0], 'name': row[1]}
                doc_types['all_types'].append(doc_type_obj)
                if row[2] and str(row[2]).strip() == '1':
                    doc_types['types_with_expiry'].append(doc_type_obj)
    finally:
        if conn: conn.close()
    return doc_types


def fetch_legislations():
    conn = get_connection()
    if not conn: return []
    legislations = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(NAME) as NAME FROM LKP_PTA_LEGISL WHERE DISABLED = '0' ORDER BY NAME")
            legislations = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in
                            cursor.fetchall()]
    finally:
        if conn: conn.close()
    return legislations


def fetch_single_archived_employee(archive_id):
    conn = get_connection()
    if not conn: return None
    employee_details = {}
    try:
        with conn.cursor() as cursor:
            query = "SELECT arch.SYSTEM_ID as ARCHIVE_ID, arch.EMPLOYEE_ID, arch.STATUS_ID, arch.HIRE_DATE, TRIM(hr.FULLNAME_EN) as FULLNAME_EN, TRIM(hr.FULLNAME_AR) as FULLNAME_AR, TRIM(hr.EMPNO) as EMPNO, TRIM(hr.DEPARTEMENT) as DEPARTMENT, TRIM(hr.SECTION) as SECTION, TRIM(hr.EMAIL) as EMAIL, TRIM(hr.MOBILE) as MOBILE, TRIM(hr.SUPERVISORNAME) as SUPERVISORNAME, TRIM(hr.NATIONALITY) as NATIONALITY, TRIM(hr.JOB_NAME) as JOB_NAME FROM LKP_PTA_EMP_ARCH arch JOIN lkp_hr_employees hr ON arch.EMPLOYEE_ID = hr.SYSTEM_ID WHERE arch.SYSTEM_ID = :1"
            cursor.execute(query, [archive_id])
            columns = [col[0].lower() for col in cursor.description]
            row = cursor.fetchone()
            if not row: return None
            employee_details = dict(zip(columns, row))

            doc_query = """
                SELECT d.SYSTEM_ID, d.DOCNUMBER, d.DOC_TYPE_ID, d.EXPIRY, TRIM(dt.NAME) as DOC_NAME
                FROM LKP_PTA_EMP_DOCS d
                JOIN LKP_PTA_DOC_TYPES dt ON d.DOC_TYPE_ID = dt.SYSTEM_ID
                WHERE d.PTA_EMP_ARCH_ID = :1 AND d.DISABLED = '0'
            """
            cursor.execute(doc_query, [archive_id])
            doc_columns = [col[0].lower() for col in cursor.description]
            documents = []
            
            for doc_row in cursor.fetchall():
                doc_dict = dict(zip(doc_columns, doc_row))
                if doc_dict.get('expiry') and hasattr(doc_dict['expiry'], 'strftime'):
                    doc_dict['expiry'] = doc_dict['expiry'].strftime('%Y-%m-%d')
                
                doc_dict['legislation_ids'] = []
                doc_dict['legislation_names'] = []
                leg_query = """
                    SELECT dl.LEGISLATION_ID, TRIM(l.NAME)
                    FROM LKP_PTA_DOC_LEGISL dl
                    JOIN LKP_PTA_LEGISL l ON dl.LEGISLATION_ID = l.SYSTEM_ID
                    WHERE dl.DOC_ID = :1
                """
                cursor.execute(leg_query, [doc_dict['system_id']])
                for leg_row in cursor.fetchall():
                    doc_dict['legislation_ids'].append(leg_row[0])
                    doc_dict['legislation_names'].append(leg_row[1])
                
                documents.append(doc_dict)
            
            employee_details['documents'] = documents
    finally:
        if conn: conn.close()
    return employee_details


def add_employee_archive_with_docs(dst, dms_user, employee_data, documents):
    conn = get_connection()
    if not conn: return False, "Database connection failed."
    try:
        conn.begin()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM LKP_PTA_EMP_ARCH WHERE EMPLOYEE_ID = :1",
                           [employee_data['employee_id']])
            if cursor.fetchone()[0] > 0: return False, "This employee is already in the archive."

            # Update lkp_hr_employees with any changes from the form
            hr_update_query = """
                UPDATE lkp_hr_employees
                SET JOB_NAME = :jobTitle, NATIONALITY = :nationality, EMAIL = :email,
                    MOBILE = :phone, SUPERVISORNAME = :manager,
                    DEPARTEMENT = :department, SECTION = :section
                WHERE SYSTEM_ID = :employee_id
            """
            cursor.execute(hr_update_query, {
                'jobTitle': employee_data.get('jobTitle'),
                'nationality': employee_data.get('nationality'),
                'email': employee_data.get('email'),
                'phone': employee_data.get('phone'),
                'manager': employee_data.get('manager'),
                'department': employee_data.get('department'),
                'section': employee_data.get('section'),
                'employee_id': employee_data['employee_id']
            })

            doc_types_to_add = [doc.get('doc_type_id') for doc in documents]
            if len(doc_types_to_add) != len(set(doc_types_to_add)):
                raise Exception("Cannot add the same document type twice.")

            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_ARCH")
            new_archive_id = cursor.fetchone()[0]

            archive_query = "INSERT INTO LKP_PTA_EMP_ARCH (SYSTEM_ID, EMPLOYEE_ID, STATUS_ID, HIRE_DATE, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), '0', SYSDATE)"
            cursor.execute(archive_query, [new_archive_id, employee_data['employee_id'],
                                            employee_data['status_id'],
                                            employee_data.get('hireDate') if employee_data.get(
                                                'hireDate') else None])

            for doc in documents:
                file_stream = doc['file'].stream
                file_stream.seek(0)
                
                sanitized_doc_type = re.sub(r'[^a-zA-Z0-9]', '_', doc['doc_type_name'])
                safe_docname = f"Archive_{employee_data['employeeNumber']}_{sanitized_doc_type}"

                _, file_extension = os.path.splitext(doc['file'].filename)
                app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'

                dms_metadata = {
                    "docname": safe_docname,
                    "abstract": f"{doc['doc_type_name']} for {employee_data['name_en']}",
                    "filename": doc['file'].filename,
                    "dms_user": dms_user,
                    "app_id": app_id
                }

                docnumber = wsdl_client.upload_archive_document_to_dms(dst, file_stream, dms_metadata)
                if not docnumber: raise Exception(f"Failed to upload {doc['doc_type_name']}")

                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_DOCS")
                new_doc_table_id = cursor.fetchone()[0]
                doc_query = "INSERT INTO LKP_PTA_EMP_DOCS (SYSTEM_ID, PTA_EMP_ARCH_ID, DOCNUMBER, DOC_TYPE_ID, EXPIRY, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), '0', SYSDATE)"
                cursor.execute(doc_query, [new_doc_table_id, new_archive_id, docnumber, doc.get('doc_type_id'),
                                            doc.get('expiry') or None])
                
                # Handle multiple legislations
                legislation_ids = doc.get('legislation_ids')
                if legislation_ids and isinstance(legislation_ids, list):
                    for leg_id in legislation_ids:
                        if leg_id: # Ensure not empty
                            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_DOC_LEGISL")
                            new_leg_link_id = cursor.fetchone()[0]
                            leg_query = "INSERT INTO LKP_PTA_DOC_LEGISL (SYSTEM_ID, DOC_ID, LEGISLATION_ID) VALUES (:1, :2, :3)"
                            cursor.execute(leg_query, [new_leg_link_id, new_doc_table_id, leg_id])

        conn.commit()
        return True, "Employee and documents archived successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Transaction failed: {e}"
    finally:
        if conn: conn.close()


def update_archived_employee(dst, dms_user, archive_id, employee_data, new_documents, deleted_doc_ids, updated_documents):
    conn = get_connection()
    if not conn: return False, "Database connection failed."
    try:
        conn.begin()
        with conn.cursor() as cursor:
            # Update the archive status table
            update_query = "UPDATE LKP_PTA_EMP_ARCH SET STATUS_ID = :status_id, HIRE_DATE = TO_DATE(:hireDate, 'YYYY-MM-DD'), LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :archive_id"
            cursor.execute(update_query, {'status_id': employee_data['status_id'],
                                           'hireDate': employee_data.get('hireDate') if employee_data.get(
                                               'hireDate') else None, 'archive_id': archive_id})

            # Update the main employee details table
            hr_update_query = """
                UPDATE lkp_hr_employees
                SET JOB_NAME = :jobTitle, NATIONALITY = :nationality, EMAIL = :email,
                    MOBILE = :phone, SUPERVISORNAME = :manager,
                    DEPARTEMENT = :department, SECTION = :section
                WHERE SYSTEM_ID = :employee_id
            """
            cursor.execute(hr_update_query, {
                'jobTitle': employee_data.get('jobTitle'),
                'nationality': employee_data.get('nationality'),
                'email': employee_data.get('email'),
                'phone': employee_data.get('phone'),
                'manager': employee_data.get('manager'),
                'department': employee_data.get('department'),
                'section': employee_data.get('section'),
                'employee_id': employee_data['employee_id']
            })

            if deleted_doc_ids:
                # First delete from the junction table to maintain referential integrity
                for doc_id in deleted_doc_ids:
                    cursor.execute("DELETE FROM LKP_PTA_DOC_LEGISL WHERE DOC_ID = :1", [doc_id])
                
                # Then mark the document as disabled
                cursor.executemany(
                    "UPDATE LKP_PTA_EMP_DOCS SET DISABLED = '1', LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :1",
                    [[doc_id] for doc_id in deleted_doc_ids])

            # Handle updated documents' legislations
            if updated_documents:
                for doc in updated_documents:
                    doc_id = doc.get('system_id')
                    legislation_ids = doc.get('legislation_ids', [])

                    # Clear existing legislations for this document
                    cursor.execute("DELETE FROM LKP_PTA_DOC_LEGISL WHERE DOC_ID = :1", [doc_id])

                    # Add the new set of legislations
                    for leg_id in legislation_ids:
                        if leg_id:
                            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_DOC_LEGISL")
                            new_leg_link_id = cursor.fetchone()[0]
                            leg_query = "INSERT INTO LKP_PTA_DOC_LEGISL (SYSTEM_ID, DOC_ID, LEGISLATION_ID) VALUES (:1, :2, :3)"
                            cursor.execute(leg_query, [new_leg_link_id, doc_id, leg_id])

            cursor.execute("SELECT DOC_TYPE_ID FROM LKP_PTA_EMP_DOCS WHERE PTA_EMP_ARCH_ID = :1 AND DISABLED = '0'",
                           [archive_id])
            existing_doc_type_ids = {row[0] for row in cursor.fetchall()}

            for doc in new_documents:
                if int(doc['doc_type_id']) in existing_doc_type_ids:
                    raise Exception(f"Document type '{doc['doc_type_name']}' already exists for this employee.")

                file_stream = doc['file'].stream
                file_stream.seek(0)
                
                sanitized_doc_type = re.sub(r'[^a-zA-Z0-9]', '_', doc['doc_type_name'])
                safe_docname = f"Archive_{employee_data['employeeNumber']}_{sanitized_doc_type}"

                _, file_extension = os.path.splitext(doc['file'].filename)
                app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'

                dms_metadata = {"docname": safe_docname,
                                "abstract": f"Updated document for {employee_data['name_en']}",
                                "filename": doc['file'].filename, 
                                "dms_user": dms_user,
                                "app_id": app_id
                                }

                docnumber = wsdl_client.upload_archive_document_to_dms(dst, file_stream, dms_metadata)
                if not docnumber: raise Exception(f"Failed to upload new document {doc['doc_type_name']}")

                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_DOCS")
                new_doc_table_id = cursor.fetchone()[0]

                doc_query = "INSERT INTO LKP_PTA_EMP_DOCS (SYSTEM_ID, PTA_EMP_ARCH_ID, DOCNUMBER, DOC_TYPE_ID, EXPIRY, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), '0', SYSDATE)"
                cursor.execute(doc_query,
                               [new_doc_table_id, archive_id, docnumber, doc.get('doc_type_id'),
                                doc.get('expiry') or None])
                
                # Handle multiple legislations
                legislation_ids = doc.get('legislation_ids')
                if legislation_ids and isinstance(legislation_ids, list):
                    for leg_id in legislation_ids:
                        if leg_id: # Ensure not empty
                            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_DOC_LEGISL")
                            new_leg_link_id = cursor.fetchone()[0]
                            leg_query = "INSERT INTO LKP_PTA_DOC_LEGISL (SYSTEM_ID, DOC_ID, LEGISLATION_ID) VALUES (:1, :2, :3)"
                            cursor.execute(leg_query, [new_leg_link_id, new_doc_table_id, leg_id])


        conn.commit()
        return True, "Employee archive updated successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Update transaction failed: {e}"
    finally:
        if conn: conn.close()