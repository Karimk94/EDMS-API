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
from moviepy.video.io.VideoFileClip import VideoFileClip
import fitz
from datetime import datetime, timedelta
import wsdl_client
import json
import math
try:
    import vector_client
except ImportError:
    logging.warning("vector_client.py not found. Vector search capabilities will be disabled.")
    vector_client = None

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

def get_exif_date(image_stream):
    """Extracts the 'Date Taken' from image EXIF data."""
    try:
        # Reset stream position just in case
        image_stream.seek(0)
        with Image.open(image_stream) as img:
            # exif = img.getexif() # Use the recommended method
            # exif_ifd = exif.get_ifd(ExifTags.IFD.Exif)
            # Use the numeric tag directly if getexif() returns the raw dictionary
            raw_exif = img._getexif()
            if raw_exif and 36867 in raw_exif: # 36867 is the tag for DateTimeOriginal
                date_str = raw_exif[36867]
                # Ensure the date string is not empty or just spaces
                if date_str and date_str.strip():
                     # Attempt to parse known formats
                    for fmt in ('%Y:%m:%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'):
                        try:
                            return datetime.strptime(date_str, fmt)
                        except ValueError:
                            continue
                    logging.warning(f"Could not parse EXIF date string '{date_str}' with known formats.")
                else:
                    logging.info("EXIF date tag found but empty.")

    except AttributeError:
         # Handle cases where _getexif might not be available or returns None
         logging.warning("Could not retrieve EXIF data using _getexif.")
    except Exception as e:
        logging.warning(f"Could not extract or parse EXIF date: {e}")
    finally:
        # It's good practice to seek back to the beginning if the stream might be reused
        image_stream.seek(0)
    return None

# --- DMS Communication ---
def dms_system_login():
    """Logs into the DMS SOAP service using system credentials from .env and returns a session token (DST)."""
    return wsdl_client.dms_system_login()

def get_media_info_from_dms(dst, doc_number):
    """
    Efficiently retrieves only the metadata (like filename) for a document from the DMS
    without downloading the full file content. Uses DB resolution for media type.
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

        # Resolve media type strictly from DB first
        media_type = 'file'  # Default
        try:
            resolved_map = resolve_media_types_from_db([doc_number])
            media_type = resolved_map.get(str(doc_number), 'file')
        except Exception as e:
            logging.error(f"Error resolving media type from DB for {doc_number}: {e}")
            # Only fallback to extension if DB lookup failed entirely
            video_extensions = [
                '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm',
                '.m4v', '.3gp', '.mts', '.ts', '.3g2'
            ]
            pdf_extensions = ['.pdf']
            image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff', '.webp', '.heic']
            text_extensions = ['.txt', '.csv', '.json', '.xml', '.log', '.md', '.yml', '.yaml', '.ini', '.conf']

            file_ext = os.path.splitext(filename)[1].lower()

            if file_ext in video_extensions:
                media_type = 'video'
            elif file_ext in pdf_extensions:
                media_type = 'pdf'
            elif file_ext in image_extensions:
                media_type = 'image'
            elif file_ext in text_extensions:
                media_type = 'text'
            else:
                media_type = 'file'

        file_ext = os.path.splitext(filename)[1].lower()
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
                     'criteria': {'criteriaCount': 2,
                                  'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
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
        # logging.info(f"Successfully cached file to {final_cache_path}")
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
            # Use moviepy.editor
            with VideoFileClip(temp_video_path) as clip: clip.save_frame(cached_path, t=1)
            os.remove(temp_video_path)
        elif media_type == 'pdf':
            # Use fitz (PyMuPDF)
            with fitz.open(stream=media_bytes, filetype="pdf") as doc:
                page = doc.load_page(0)  # Load the first page
                pix = page.get_pixmap()
                # Create PIL image from pixmap samples
                with Image.frombytes("RGB", [pix.width, pix.height], pix.samples) as img:
                    img.save(cached_path, "JPEG", quality=95)
        else: # Assumed image
            # Use Pillow
            with Image.open(io.BytesIO(media_bytes)) as img:
                img.thumbnail((300, 300))
                # Ensure image is RGB before saving as JPEG
                img.convert("RGB").save(cached_path, "JPEG", quality=95)
        return f"cache/{thumbnail_filename}" # Return relative path for URL
    except Exception as e:
        print(f"Could not create thumbnail for {doc_number}: {e}")
        # Consider logging the error more formally
        # logging.error(f"Thumbnail creation failed for {doc_number}", exc_info=True)
        return None

# --- Oracle Database Interaction ---
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

def get_user_security_level(username):
    """Fetches the user's security level name from the database using their user ID from the PEOPLE table."""
    conn = get_connection()
    if not conn:
        return None  # Return None if DB connection fails

    security_level = None  # Default value is now None
    try:
        with conn.cursor() as cursor:
            # Use upper for case-insensitive comparison
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                # Now, get the security level using the user_id
                query = """
                    SELECT sl.NAME
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                cursor.execute(query, user_id=user_id)
                level_result = cursor.fetchone()
                if level_result:
                    security_level = level_result[0]
                else:
                    logging.warning(f"No security level found for user_id {user_id} (DMS user: {username})")
            else:
                 logging.warning(f"No PEOPLE record found for DMS user: {username}")
                 # If level_result is None, security_level remains None and will be returned
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_security_level for {username}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return security_level

def get_app_id_from_extension(extension):
    """
    Looks up the APPLICATION (APP_ID) from the APPS table based on the file extension.
    Converts extension to uppercase for comparison.
    """
    conn = get_connection()
    if not conn:
        return None

    app_id = None
    upper_extension = extension.upper() if extension else ''
    try:
        with conn.cursor() as cursor:
            # First, check the DEFAULT_EXTENSION column (case-insensitive)
            cursor.execute("SELECT APPLICATION FROM APPS WHERE UPPER(DEFAULT_EXTENSION) = :ext", ext=upper_extension)
            result = cursor.fetchone()
            if result:
                app_id = result[0]
            else:
                # If not found, check the FILE_TYPES column (case-insensitive, using LIKE)
                cursor.execute("SELECT APPLICATION FROM APPS WHERE UPPER(FILE_TYPES) LIKE :ext_like",
                               ext_like=f"%{upper_extension}%")
                result = cursor.fetchone()
                if result:
                    app_id = result[0]
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_app_id_from_extension for '{extension}': {e}", exc_info=True)
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
         logging.error("Failed to get DB connection in get_specific_documents_for_processing.")
         return []

    try:
        with conn.cursor() as cursor:
            # Ensure docnumbers are integers for binding
            int_docnumbers = [int(d) for d in docnumbers]

            # Create placeholders for the IN clause
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
            cursor.execute(sql, int_docnumbers)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except (oracledb.Error, ValueError) as e: # Catch potential int conversion error
        logging.error(f"Error in get_specific_documents_for_processing: {e}", exc_info=True)
        return []
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
        logging.error("Failed to get DB connection in check_processing_status.")
        return docnumbers  # Assume still processing if DB is down

    try:
         # Ensure docnumbers are integers
        int_docnumbers = [int(d) for d in docnumbers]
        with conn.cursor() as cursor:
            # Using bind variables is generally safer and more performant
            bind_names = [f':doc_{i}' for i in range(len(int_docnumbers))]
            bind_vars = {f'doc_{i}': val for i, val in enumerate(int_docnumbers)}

            # Construct the SQL using bind names
            # Using SYS.ODCINUMBERLIST or similar collection type for IN clause binding
            # NOTE: For very large lists, consider alternative approaches like temporary tables
            # if performance becomes an issue. SYS.ODCINUMBERLIST is generally fine for moderate lists.
            sql = f"""
            SELECT COLUMN_VALUE
            FROM TABLE(SYS.ODCINUMBERLIST({','.join(bind_names)})) input_docs
            WHERE input_docs.COLUMN_VALUE NOT IN (
                SELECT docnumber FROM TAGGING_QUEUE WHERE docnumber IN ({','.join(bind_names)}) AND STATUS = 3
            )
            """

            # Execute with the original bind_vars dictionary.
            # The driver handles using the same bind names multiple times.
            cursor.execute(sql, bind_vars)

            still_processing = [row[0] for row in cursor.fetchall()]
            return still_processing
    except (oracledb.Error, ValueError) as e:
        # Log the specific Oracle error or ValueError (from int conversion)
        error_message = f"Error in check_processing_status: {e}"
        if isinstance(e, oracledb.Error):
             error_obj, = e.args
             error_message = f"Oracle Error in check_processing_status: {error_obj.message} (Code: {error_obj.code})"
        logging.error(error_message, exc_info=True)
        return docnumbers # Return original list on error to be safe
    finally:
        if conn:
            conn.close()

def fetch_documents_from_oracle(page=1, page_size=20, search_term=None, date_from=None, date_to=None,
                                persons=None, person_condition='any', tags=None, years=None, sort=None,
                                memory_month=None, memory_day=None, user_id=None, lang='en',
                                security_level='Editor', app_source='unknown', media_type=None, scope=None):
    """Fetches a paginated list of documents, applying filters including media_type."""
    conn = get_connection()
    if not conn: return [], 0

    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS. Aborting document fetch.")
        return [], 0

    db_user_id = None
    if user_id:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=user_id)
                user_result = cursor.fetchone()
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

    # --- Scope Logic (New) ---
    folder_doc_ids = None
    if scope == 'folders':
        # Retrieve all relevant DOCNUMBERS from WSDL first
        folder_doc_ids = wsdl_client.get_recursive_doc_ids(dst, media_type)
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

        # Clear media_type here because we already filtered by it in WSDL
        # But we keep it in the function arg for other logic if needed

    else:
        # --- Existing Dynamic Filtering Logic ---
        range_start = 19677386
        range_end = 19679115

        # Default filter (fallback)
        doc_filter_sql = f"AND p.DOCNUMBER >= {range_start}"

        if app_source == 'edms-media':
            doc_filter_sql = f"AND p.DOCNUMBER BETWEEN {range_start} AND {range_end}"
        elif app_source == 'smart-edms':
            smart_edms_floor = 19662092
            doc_filter_sql = f"AND p.DOCNUMBER >= {smart_edms_floor} AND (p.DOCNUMBER < {range_start} OR p.DOCNUMBER > {range_end})"

        where_clauses.append(doc_filter_sql.replace('AND ', '', 1))  # Strip first AND if adding to list
        # -------------------------------

        if media_type:
            try:
                with conn.cursor() as app_cursor:
                    # Step 1: Try to fetch SYSTEM_ID (Numeric) from APPS
                    id_column = "SYSTEM_ID"
                    try:
                        app_cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
                        apps_rows = app_cursor.fetchall()
                    except oracledb.DatabaseError:
                        # Fallback if SYSTEM_ID column doesn't exist
                        logging.warning("SYSTEM_ID column not found in APPS, falling back to APPLICATION column.")
                        id_column = "APPLICATION"
                        app_cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
                        apps_rows = app_cursor.fetchall()

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
                    # Use TRIM(TO_CHAR(...)) to safely compare against string IDs
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

    if use_vector_search:
        vector_doc_ids = vector_client.query_documents(search_term, n_results=page_size)

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
        if vector_doc_ids is None or len(vector_doc_ids) == 0:
            if use_vector_search:
                logging.info(
                    f"Vector search returned no results or failed. Falling back to keyword search for: {search_term}")

            if search_term:
                search_words = [word.strip() for word in search_term.split(' ') if word.strip()]
                if search_words:
                    word_conditions = []
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
                        word_conditions.append(word_condition)
                    where_clauses.append(f"({' AND '.join(word_conditions)})")

        else:
            logging.info(f"Using {len(vector_doc_ids)} doc_ids from vector search.")
            vector_placeholders = ','.join([f":vec_id_{i}" for i in range(len(vector_doc_ids))])
            where_clauses.append(f"p.docnumber IN ({vector_placeholders})")
            for i, doc_id in enumerate(vector_doc_ids):
                params[f'vec_id_{i}'] = doc_id

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
    if vector_doc_ids and len(vector_doc_ids) > 0:
        order_case_sql = " ".join([f"WHEN :vec_id_{i} THEN {i + 1}" for i in range(len(vector_doc_ids))])
        order_by_clause = f"ORDER BY CASE p.docnumber {order_case_sql} END"
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
        with conn.cursor() as cursor:
            # If scope is folders, we already know the total rows from the recursive fetch
            if scope != 'folders':
                count_query = f"SELECT COUNT(p.DOCNUMBER) FROM PROFILE p {final_where_clause}"
                cursor.execute(count_query, params)
                total_rows = cursor.fetchone()[0]

            date_column = "p.RTADOCDATE"
            fetch_query = f"""
            SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, {date_column} as DOC_DATE, p.DOCNAME,
                   CASE WHEN f.DOCNUMBER IS NOT NULL THEN 1 ELSE 0 END as IS_FAVORITE
            FROM PROFILE p
            LEFT JOIN LKP_FAVORITES_DOC f ON p.DOCNUMBER = f.DOCNUMBER AND f.USER_ID = :db_user_id
            {final_where_clause}
            {order_by_clause}
            """

            # Add offset/fetch logic only if NOT folder scope (since we already paginated IDs)
            if scope != 'folders':
                fetch_query += " OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"
                params['offset'] = offset
                params['page_size'] = page_size

            params['db_user_id'] = db_user_id

            cursor.execute(fetch_query, params)
            rows = cursor.fetchall()

            for row in rows:
                doc_id, abstract, author, doc_date, docname, is_favorite = row
                thumbnail_path = None
                # Default, will update below
                media_type = 'image'

                final_abstract = abstract or ""
                if security_level == 'Viewer':
                    final_abstract = ""

                try:
                    # Optimization: If scope=folders, we already resolved media type in WSDL, but DB check is fine
                    original_filename, media_type, file_ext = get_media_info_from_dms(dst, doc_id)
                    cached_thumbnail_file = f"{doc_id}.jpg"
                    cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                    if os.path.exists(cached_path):
                        thumbnail_path = f"cache/{cached_thumbnail_file}"
                    else:
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
            try:
                conn.close()
            except oracledb.Error:
                logging.error("Error closing DB connection.")
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
              AND p.docnumber >= 19677386 --19662092
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
            # logging.info(f"DB_UPDATE_SUCCESS: Successfully updated status for docnumber {docnumber}.")

            # --- NEW: VECTOR INDEXING HOOK ---
            if status == 3 and vector_client: # If processing was successful
                logging.info(f"Queueing vector update for doc_id {docnumber}.")
                try:
                    # Run in a thread? For now, run inline.
                    vector_client.add_or_update_document(docnumber, new_abstract)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {docnumber}: {e}", exc_info=True)
            # --- END: VECTOR INDEXING HOOK ---

    except oracledb.Error as e:
        logging.error(f"DB_UPDATE_ERROR: Oracle error while updating docnumber {docnumber}: {e}", exc_info=True)
        try:
            conn.rollback()
            # logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} was rolled back.")
        except oracledb.Error as rb_e:
            logging.error(f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber}: {rb_e}",
                          exc_info=True)

    finally:
        if conn:
            conn.close()

def update_abstract_with_vips(doc_id, vip_names):
    """Appends or updates VIP names in a document's abstract."""
    conn = get_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            result = cursor.fetchone()
            if result is None: return False, f"Document with ID {doc_id} not found."
            
            current_abstract = result[0] or ""

            base_abstract = re.sub(r'\s*\n*VIPs\s*:.*', '', current_abstract, flags=re.IGNORECASE).strip()

            names_str = ", ".join(sorted(list(set(vip_names))))
            
            if names_str:
                vips_section = f"VIPs: {names_str}"
                
                new_abstract = base_abstract + ("\n\n" if base_abstract else "") + vips_section
            else:
                new_abstract = base_abstract

            cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
            conn.commit()

            # --- NEW: VECTOR INDEXING HOOK ---
            if vector_client:
                try:
                    vector_client.add_or_update_document(doc_id, new_abstract)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {doc_id} after VIP update: {e}", exc_info=True)
            # --- END: VECTOR INDEXING HOOK ---

            return True, "Abstract updated successfully."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def add_person_to_lkp(person_name_english, person_name_arabic=None):
    """Adds a new person to the LKP_PERSON lookup table."""
    conn = get_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON WHERE NAME_ENGLISH = :1", [person_name_english])
            if cursor.fetchone()[0] > 0:
                return True, f"'{person_name_english}' already exists in LKP_PERSON."

            insert_query = """
                INSERT INTO LKP_PERSON (NAME_ENGLISH, NAME_ARABIC, LAST_UPDATE, DISABLED, SYSTEM_ID)
                VALUES (:1, :2, SYSDATE, 0, (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PERSON))
            """
            cursor.execute(insert_query, [person_name_english, person_name_arabic])
            conn.commit()
            return True, f"Successfully added '{person_name_english}' to LKP_PERSON."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def fetch_lkp_persons(page=1, page_size=20, search='', lang='en'):
    """Fetches a paginated list of people from the LKP_PERSON table."""
    conn = get_connection()
    if not conn: return [], 0

    offset = (page - 1) * page_size
    persons = []
    total_rows = 0
    
    search_term_upper = f"%{search.upper()}%"
    search_term_normal = f"%{search}%"
    
    # Search both English and Arabic columns regardless of lang
    search_clause = "WHERE (UPPER(NAME_ENGLISH) LIKE :search_upper OR NAME_ARABIC LIKE :search_normal)"
    params = {'search_upper': search_term_upper, 'search_normal': search_term_normal}

    count_query = f"SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON {search_clause}"
    
    # Determine sort order
    order_by_column = "NAME_ENGLISH" if lang == 'en' else "NAME_ARABIC"

    fetch_query = f"""
        SELECT SYSTEM_ID, NAME_ENGLISH, NVL(NAME_ARABIC, '')
        FROM LKP_PERSON
        {search_clause}
        ORDER BY {order_by_column}
        OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
    """
    
    params_paginated = params.copy()
    params_paginated['offset'] = offset
    params_paginated['page_size'] = page_size

    try:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            cursor.execute(fetch_query, params_paginated)
            for row in cursor:
                # The frontend will construct the label. Send all data.
                persons.append({"id": row[0], "name_english": row[1], "name_arabic": row[2]})
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_lkp_persons: {e}")
    finally:
        if conn:
            conn.close()
    return persons, total_rows

def fetch_all_tags(lang='en', security_level='Editor', app_source='unknown'):
    """Fetches all unique tags (keywords and persons) considering security level and app source visibility."""
    conn = get_connection()
    if not conn: return []

    # --- Dynamic Filtering Logic (Same as fetch_documents) ---
    range_start = 19677386
    range_end = 19679115
    smart_edms_floor = 19662092

    # Default filter (fallback)
    doc_filter_sql = f"p.DOCNUMBER >= {range_start}"

    if app_source == 'edms-media':
        doc_filter_sql = f"p.DOCNUMBER BETWEEN {range_start} AND {range_end}"
    elif app_source == 'smart-edms':
        doc_filter_sql = f"p.DOCNUMBER >= {smart_edms_floor} AND (p.DOCNUMBER < {range_start} OR p.DOCNUMBER > {range_end})"

    # -------------------------------

    tags = set()
    try:
        with conn.cursor() as cursor:
            keyword_column = "k.DESCRIPTION" if lang == 'ar' else "k.KEYWORD_ID"
            person_column = "p.NAME_ARABIC" if lang == 'ar' else "p.NAME_ENGLISH"

            shortlist_clause = "AND k.SHORTLISTED = '1'" if security_level == 'Viewer' else ""

            keyword_query = f"""
                SELECT DISTINCT {keyword_column} 
                FROM KEYWORD k 
                JOIN LKP_DOCUMENT_TAGS ldt ON ldt.TAG_ID = k.SYSTEM_ID 
                JOIN PROFILE p ON ldt.DOCNUMBER = p.DOCNUMBER
                WHERE {keyword_column} IS NOT NULL 
                {shortlist_clause}
                AND p.FORM = 2740
                AND {doc_filter_sql}
            """
            cursor.execute(keyword_query)
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())

            person_query = f"""
                SELECT DISTINCT {person_column} 
                FROM LKP_PERSON p 
                WHERE {person_column} IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM PROFILE pr
                    WHERE pr.FORM = 2740
                    AND {doc_filter_sql.replace('p.', 'pr.')}
                    AND (
                        UPPER(pr.ABSTRACT) LIKE '%' || UPPER(p.NAME_ENGLISH) || '%'
                        OR 
                        (p.NAME_ARABIC IS NOT NULL AND pr.ABSTRACT LIKE '%' || p.NAME_ARABIC || '%')
                    )
                )
            """
            cursor.execute(person_query)
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_all_tags: {e}")
    finally:
        if conn:
            conn.close()

    return sorted(list(tags))

def fetch_tags_for_document(doc_id, lang='en', security_level='Editor'):
    """
    Fetches all keyword and person tags for a single document.
    Returns a list of dictionaries: {'text': 'TagName', 'shortlisted': 1/0, 'type': 'keyword'/'person'}
    """
    conn = get_connection()
    if not conn:
        return []

    doc_tags = []
    seen_tags = set()

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :doc_id", {'doc_id': doc_id})
            result = cursor.fetchone()
            abstract = result[0] if result else None

            keyword_column = "k.DESCRIPTION" if lang == 'ar' else "k.KEYWORD_ID"
            person_column = "p.NAME_ARABIC" if lang == 'ar' else "p.NAME_ENGLISH"

            shortlist_clause = "AND k.SHORTLISTED = '1'" if security_level == 'Viewer' else ""

            tag_query = f"""
                SELECT {keyword_column}, k.SHORTLISTED
                FROM LKP_DOCUMENT_TAGS ldt
                JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                WHERE ldt.DOCNUMBER = :doc_id AND {keyword_column} IS NOT NULL {shortlist_clause}
            """
            cursor.execute(tag_query, {'doc_id': doc_id})
            for row in cursor:
                tag_text = row[0].strip() if row[0] else ""
                is_shortlisted = row[1] if row[1] else '0'

                if tag_text and tag_text not in seen_tags:
                    seen_tags.add(tag_text)
                    doc_tags.append({
                        'text': tag_text,
                        'shortlisted': 1 if str(is_shortlisted) == '1' else 0,
                        'type': 'keyword'
                    })

            if abstract:
                person_query = f"""
                    SELECT {person_column}
                    FROM LKP_PERSON p
                    WHERE :abstract LIKE '%' || UPPER(NAME_ENGLISH) || '%' AND {person_column} IS NOT NULL
                """
                cursor.execute(person_query, {'abstract': abstract.upper()})
                for row in cursor:
                    person_name = row[0].strip() if row[0] else ""
                    if person_name and person_name not in seen_tags:
                        seen_tags.add(person_name)
                        doc_tags.append({
                            'text': person_name,
                            'shortlisted': 0,
                            'type': 'person'
                        })

    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_tags_for_document for doc_id {doc_id}: {e}")
    finally:
        if conn:
            conn.close()

    # Sort by text
    return sorted(doc_tags, key=lambda x: x['text'].lower())

def toggle_tag_shortlist(tag, lang='en'):
    """
    Toggles the SHORTLISTED status of a keyword (0 -> 1 or 1 -> 0).
    Identifies the keyword by name (English or Arabic).
    """
    conn = get_connection()
    if not conn: return False, "Database connection failed."

    try:
        with conn.cursor() as cursor:
            # 1. Find the Keyword ID and current status
            # Check both English ID and Arabic Description
            cursor.execute("""
                           SELECT SYSTEM_ID, SHORTLISTED
                           FROM KEYWORD
                           WHERE UPPER(KEYWORD_ID) = :tag_upper
                              OR DESCRIPTION = :tag_normal
                           """, tag_upper=tag.upper(), tag_normal=tag)

            result = cursor.fetchone()
            if not result:
                return False, "Tag not found in keywords (cannot shortlist Persons)."

            keyword_id = result[0]
            current_status = str(result[1]) if result[1] else '0'

            # Toggle status
            new_status = '0' if current_status == '1' else '1'

            # 2. Update the keyword
            cursor.execute("UPDATE KEYWORD SET SHORTLISTED = :new_status WHERE SYSTEM_ID = :id",
                           new_status=new_status, id=keyword_id)
            conn.commit()

            return True, {"new_status": int(new_status)}

    except oracledb.Error as e:
        if conn: conn.rollback()
        logging.error(f"Database error toggling shortlist: {e}", exc_info=True)
        return False, f"Database error: {e}"
    finally:
        if conn: conn.close()

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

                cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE UPPER(KEYWORD_ID) = UPPER(:keyword_id)",
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
            # logging.info(f"DB_KEYWORD_SUCCESS: Successfully processed keywords for docnumber {docnumber}.")

    except oracledb.Error as e:
        logging.error(f"DB_KEYWORD_ERROR: Oracle error while processing keywords for docnumber {docnumber}: {e}",
                      exc_info=True)
        try:
            conn.rollback()
            # logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} keywords was rolled back.")
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
            # --- 1. Check if the tag is a KEYWORD (English or Arabic) ---
            # Checks KEYWORD_ID (English, case-insensitive) OR DESCRIPTION (Arabic, case-sensitive)
            cursor.execute("""
                SELECT SYSTEM_ID 
                FROM KEYWORD 
                WHERE UPPER(KEYWORD_ID) = :tag_upper OR DESCRIPTION = :tag_normal
            """, tag_upper=tag.upper(), tag_normal=tag)
            
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

            # --- 2. If not a keyword, check if it's a PERSON (English or Arabic) ---
            # Checks NAME_ENGLISH (case-insensitive) OR NAME_ARABIC (case-sensitive)
            cursor.execute("""
                SELECT NAME_ENGLISH 
                FROM LKP_PERSON 
                WHERE UPPER(NAME_ENGLISH) = :tag_upper OR NAME_ARABIC = :tag_normal
            """, tag_upper=tag.upper(), tag_normal=tag)
            
            person_result = cursor.fetchone()

            if person_result:
                # It's a person, so we need to modify the abstract.
                cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                abstract_result = cursor.fetchone()
                if not abstract_result or not abstract_result[0]:
                    return False, "Document abstract not found or is empty."

                current_abstract = abstract_result[0]
                # Find the VIPs section
                vips_match = re.search(r'VIPs\s*:\s*(.*)', current_abstract, re.IGNORECASE)
                if vips_match:
                    vips_str = vips_match.group(1)
                    vips_list = [name.strip() for name in vips_str.split(',')]
                    
                    # Remove the person from the list (case-insensitive for both English and Arabic)
                    # .upper() works for Arabic (returns the same string) and English
                    original_len = len(vips_list)
                    tag_upper = tag.upper()
                    vips_list = [name for name in vips_list if name.upper() != tag_upper]

                    if len(vips_list) < original_len:
                        # Reconstruct the abstract
                        if vips_list:
                            new_vips_str = "VIPs: " + ", ".join(vips_list)
                            # Use regex sub to replace the old vips_match group 0
                            new_abstract = re.sub(re.escape(vips_match.group(0)), new_vips_str, current_abstract, flags=re.IGNORECASE)
                        else:
                            # If no VIPs are left, remove the entire VIPs line
                            new_abstract = re.sub(r'\s*\n*VIPs\s*:.*', '', current_abstract, flags=re.IGNORECASE).strip()

                        cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
                        conn.commit()

                        if vector_client:
                            try:
                                vector_client.add_or_update_document(doc_id, new_abstract)
                            except Exception as e:
                                logging.error(f"Failed to update vector index for doc_id {doc_id} after tag delete: {e}", exc_info=True)

                        return True, "Person tag removed from abstract successfully."

            # --- 3. If not found as keyword or person ---
            conn.commit() 
            return False, f"Tag '{tag}' not found for this document."

    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

# --- Archiving Database Functions ---
def fetch_memories_from_oracle(month, day=None, limit=5):
    """Fetches one representative image document per past year for a given month (and optionally day)."""
    conn = get_connection()
    if not conn: return []

    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS. Aborting memories fetch.")
        return []

    memories = []
    current_year = datetime.now().year
    # Ensure month and day are integers if provided
    try:
        month_int = int(month)
        day_int = int(day) if day is not None else None
        limit_int = int(limit)
    except (ValueError, TypeError):
        logging.error(f"Invalid month/day/limit provided for memories: month={month}, day={day}, limit={limit}")
        return []

    if not 1 <= month_int <= 12:
         logging.error(f"Invalid month provided for memories: {month_int}")
         return []
    if day_int is not None and not 1 <= day_int <= 31:
         logging.error(f"Invalid day provided for memories: {day_int}")
         return []
    limit_int = max(1, min(limit_int, 10)) # Clamp limit between 1 and 10

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
            AND p.DOCNUMBER >= 19677386 --19662092
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
        with conn.cursor() as cursor:
            logging.debug(f"Memories Query: {sql}")
            logging.debug(f"Memories Params: {params}")
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            for row in rows:
                doc_id, abstract, author, rtadocdate, docname = row
                thumbnail_path = None
                media_type = 'image'
                file_ext = '.jpg'

                try:
                    # Check cache first
                    cached_thumbnail_file = f"{doc_id}.jpg"
                    cached_path = os.path.join(thumbnail_cache_dir, cached_thumbnail_file)

                    if os.path.exists(cached_path):
                        thumbnail_path = f"cache/{cached_thumbnail_file}"
                    else:
                        # Get actual media info only if not cached, to verify type and create thumb
                        _, actual_media_type, actual_file_ext = get_media_info_from_dms(dst, doc_id)
                        if actual_media_type == 'image':
                            media_bytes = get_media_content_from_dms(dst, doc_id)
                            if media_bytes:
                                # Ensure create_thumbnail exists and handles images
                                thumbnail_path = create_thumbnail(doc_id, actual_media_type, actual_file_ext, media_bytes)
                            else:
                                logging.warning(f"Could not get content for memory doc {doc_id} to create thumbnail.")
                        else:
                             # This case should be less likely due to the SQL filter, but good to keep
                             logging.warning(f"Memory query returned non-image doc {doc_id} (Type: {actual_media_type}). Skipping.")
                             continue # Skip non-image results

                except Exception as thumb_e:
                     logging.error(f"Error processing thumbnail for memory doc {doc_id}: {thumb_e}", exc_info=True)

                memories.append({
                    "doc_id": doc_id,
                    "title": abstract or "",
                    "docname": docname or "",
                    "author": author or "N/A",
                    "date": rtadocdate.strftime('%d-%m-%Y') if rtadocdate else "N/A",
                    "thumbnail_url": thumbnail_path or "", # Use empty string if no thumbnail
                    "media_type": 'image' # Hardcode as image for memories component
                })

    except oracledb.Error as e:
        logging.error(f"Oracle error fetching memories: {e}", exc_info=True)
    finally:
        if conn:
            try:
                conn.close()
            except oracledb.Error:
                logging.error("Error closing database connection in fetch_memories_from_oracle.")

    return memories

def update_document_metadata(doc_id, new_abstract=None, new_date_taken=Ellipsis):
    """
    Updates metadata (abstract and/or RTADOCDATE) for a specific document number in the PROFILE table.
    - If new_abstract is provided, updates the ABSTRACT column.
    - If new_date_taken is provided (not Ellipsis), updates the RTADOCDATE column.
      - If new_date_taken is None, sets RTADOCDATE to NULL.
      - If new_date_taken is a datetime object, sets RTADOCDATE accordingly.
    - Ellipsis for new_date_taken means "do not change the date".
    """
    conn = get_connection()
    if not conn:
        return False, "Database connection failed."

    update_parts = []
    params = {}
    abstract_to_index = None # For vector indexing

    # Build the SET part of the UPDATE statement dynamically
    if new_abstract is not None:
        update_parts.append("ABSTRACT = :abstract")
        params['abstract'] = new_abstract
        abstract_to_index = new_abstract # Store for indexing
    else:
        abstract_to_index = None # Will need to fetch it if only date changes

    if new_date_taken is not Ellipsis: # Check against Ellipsis to see if date update is requested
        if new_date_taken is None:
             update_parts.append("RTADOCDATE = NULL")
             # No parameter needed for NULL
        elif isinstance(new_date_taken, datetime):
             # Oracle TO_DATE expects a string in the specified format
             update_parts.append("RTADOCDATE = TO_DATE(:date_taken, 'YYYY-MM-DD HH24:MI:SS')")
             params['date_taken'] = new_date_taken.strftime('%Y-%m-%d %H:%M:%S')
        else:
             # This case should ideally be caught by the API layer parsing
             logging.error(f"Invalid type for new_date_taken for doc_id {doc_id}: {type(new_date_taken)}")
             return False, "Invalid date format received by database function."

    # Check if there's anything to update
    if not update_parts:
        return False, "No valid fields provided for update."

    # Finalize SQL statement
    sql = f"UPDATE PROFILE SET {', '.join(update_parts)} WHERE DOCNUMBER = :doc_id"
    params['doc_id'] = doc_id

    logging.debug(f"Executing metadata update SQL: {sql}")
    logging.debug(f"With params: {params}")

    try:
        with conn.cursor() as cursor:
            # Check if document exists first
            cursor.execute("SELECT 1 FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            if cursor.fetchone() is None:
                return False, f"Document with ID {doc_id} not found."

            # --- MODIFICATION ---
            # If abstract isn't being updated, but we need it for re-indexing
            if abstract_to_index is None and new_abstract is None:
                cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                result = cursor.fetchone()
                if result:
                    abstract_to_index = result[0]
            # --- END MODIFICATION ---

            # Perform the update
            cursor.execute(sql, params)

            # Check if any row was updated
            if cursor.rowcount == 0:
                conn.rollback() # Rollback if no rows affected
                # This could happen if the doc_id exists but the update didn't change anything (e.g., same abstract/date)
                # Or potentially a race condition. Treat as failure for clarity.
                return False, f"Update affected 0 rows for Document ID {doc_id}. Check if data actually changed."

            conn.commit()

            # --- NEW: VECTOR INDEXING HOOK ---
            # Only re-index if the abstract was actually changed
            if new_abstract is not None and vector_client:
                try:
                    vector_client.add_or_update_document(doc_id, abstract_to_index)
                except Exception as e:
                    logging.error(f"Failed to update vector index for doc_id {doc_id} after metadata update: {e}", exc_info=True)
            # --- END: VECTOR INDEXING HOOK ---

            return True, "Metadata updated successfully."

    except oracledb.Error as e:
        error_obj, = e.args
        logging.error(f"Oracle error updating metadata for doc_id {doc_id}: {error_obj.message}", exc_info=True)
        try:
            conn.rollback()
        except oracledb.Error:
             logging.error(f"Failed to rollback metadata update transaction for doc_id {doc_id}.")
        return False, f"Database error occurred: {error_obj.message}"
    except Exception as e:
         logging.error(f"Unexpected error updating metadata for doc_id {doc_id}: {e}", exc_info=True)
         try:
            conn.rollback()
         except Exception:
             pass # Ignore rollback error if main operation failed unexpectedly
         return False, "An unexpected server error occurred."
    finally:
        if conn:
            try:
                conn.close()
            except oracledb.Error:
                logging.error(f"Error closing DB connection after metadata update for doc_id {doc_id}.")

def add_favorite(user_id, doc_id):
    """Adds a document to a user's favorites."""
    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Get the numeric SYSTEM_ID from the PEOPLE table using the username
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=user_id)
            user_result = cursor.fetchone()

            if not user_result:
                return False, "User not found in PEOPLE table."
            
            db_user_id = user_result[0]

            # Check if it's already a favorite
            cursor.execute("SELECT COUNT(*) FROM LKP_FAVORITES_DOC WHERE USER_ID = :user_id AND DOCNUMBER = :doc_id",
                           [db_user_id, doc_id])
            if cursor.fetchone()[0] > 0:
                return True, "Document is already a favorite."

            # Get the next SYSTEM_ID for the favorites table
            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_FAVORITES_DOC")
            system_id = cursor.fetchone()[0]

            # Insert the new favorite record
            cursor.execute("INSERT INTO LKP_FAVORITES_DOC (SYSTEM_ID, USER_ID, DOCNUMBER) VALUES (:1, :2, :3)",
                           [system_id, db_user_id, doc_id])
            conn.commit()
            return True, "Favorite added."
    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def remove_favorite(user_id, doc_id):
    """Removes a document from a user's favorites."""
    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Get the numeric SYSTEM_ID from the PEOPLE table
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=user_id)
            user_result = cursor.fetchone()

            if not user_result:
                return False, "User not found in PEOPLE table."

            db_user_id = user_result[0]

            cursor.execute("DELETE FROM LKP_FAVORITES_DOC WHERE USER_ID = :user_id AND DOCNUMBER = :doc_id",
                           [db_user_id, doc_id])
            conn.commit()
            if cursor.rowcount > 0:
                return True, "Favorite removed."
            else:
                return False, "Favorite not found."
    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def get_favorites(user_id, page=1, page_size=20, app_source='unknown'):
    """Fetches a paginated list of a user's favorited documents with app_source filtering."""
    conn = get_connection()
    if not conn:
        return [], 0

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    range_start = 19677386
    range_end = 19679115

    # Default filter
    doc_filter_sql = f"AND p.DOCNUMBER >= {range_start}"

    if app_source == 'edms-media':
        doc_filter_sql = f"AND p.DOCNUMBER BETWEEN {range_start} AND {range_end}"
    elif app_source == 'smart-edms':
        smart_edms_floor = 19662092
        doc_filter_sql = f"AND p.DOCNUMBER >= {smart_edms_floor} AND (p.DOCNUMBER < {range_start} OR p.DOCNUMBER > {range_end})"

    try:
        with conn.cursor() as cursor:
            # Get the numeric SYSTEM_ID from the PEOPLE table
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=user_id)
            user_result = cursor.fetchone()

            if not user_result:
                logging.error(f"Could not find user '{user_id}' in PEOPLE table for fetching favorites.")
                return [], 0

            db_user_id = user_result[0]

            # Count total favorites (Updated to join PROFILE and apply filters)
            count_query = f"""
                SELECT COUNT(f.SYSTEM_ID) 
                FROM LKP_FAVORITES_DOC f
                JOIN PROFILE p ON f.DOCNUMBER = p.DOCNUMBER
                WHERE f.USER_ID = :user_id
                {doc_filter_sql}
            """
            cursor.execute(count_query, [db_user_id])
            total_rows = cursor.fetchone()[0]

            # Fetch paginated favorites
            query = f"""
                SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.RTADOCDATE as DOC_DATE, p.DOCNAME
                FROM PROFILE p
                JOIN LKP_FAVORITES_DOC f ON p.DOCNUMBER = f.DOCNUMBER
                WHERE f.USER_ID = :user_id
                {doc_filter_sql}
                ORDER BY p.DOCNUMBER DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            cursor.execute(query, user_id=db_user_id, offset=offset, page_size=page_size)

            rows = cursor.fetchall()
            dst = dms_system_login()  # Login to DMS to get thumbnails

            for row in rows:
                doc_id, abstract, author, doc_date, docname = row
                thumbnail_path = None
                media_type = 'image'

                if dst:
                    try:
                        _, media_type, file_ext = get_media_info_from_dms(dst, doc_id)
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
                    "is_favorite": True  # Mark as favorite
                })
        return documents, total_rows
    except oracledb.Error as e:
        logging.error(f"Oracle error fetching favorites: {e}", exc_info=True)
        return [], 0
    finally:
        if conn:
            conn.close()

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

def fetch_journey_data():
    """Fetches all events and their associated documents, grouped by year."""
    conn = get_connection()
    if not conn:
        logging.error("Failed to get DB connection in fetch_journey_data.")
        return {}

    dst = dms_system_login()
    if not dst:
        logging.error("Could not log into DMS in fetch_journey_data.")
        return {}

    journey_data = {}
    try:
        with conn.cursor() as cursor:
            # Fetch all events with at least one document, ordered by date
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
            cursor.execute(sql)

            # Group documents by year and event name
            events_by_year = {}
            for year, event_name, docnumber in cursor.fetchall():
                if year not in events_by_year:
                    events_by_year[year] = {}
                if event_name not in events_by_year[year]:
                    events_by_year[year][event_name] = []
                events_by_year[year][event_name].append(docnumber)

            # Process each event to get thumbnails
            for year, events in events_by_year.items():
                if year not in journey_data:
                    journey_data[year] = []

                for event_name, docnumbers in events.items():
                    thumbnail_urls = []
                    # Fetch up to 4 thumbnails for each event
                    for doc_id in docnumbers[:4]:
                        thumbnail_path = f"cache/{doc_id}.jpg"
                        cached_path = os.path.join(thumbnail_cache_dir, f"{doc_id}.jpg")
                        if not os.path.exists(cached_path):
                            # Create thumbnail if it doesn't exist
                            _, media_type, file_ext = get_media_info_from_dms(dst, doc_id)
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
            conn.close()

    return journey_data

def get_user_details(username):
    """Fetches user details including security level, language, and theme preference."""
    conn = get_connection()
    if not conn:
        return None

    user_details = None
    try:
        with conn.cursor() as cursor:
            # First, get the USER_ID from the PEOPLE table
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                # Now, get details from the EDMS security table
                query = """
                    SELECT sl.NAME, us.LANG, us.THEME
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                cursor.execute(query, user_id=user_id)
                details_result = cursor.fetchone()

                if details_result:
                    security_level, lang, theme = details_result
                    user_details = {
                        'username': username,
                        'security_level': security_level,
                        'lang': lang or 'en',  # Default to 'en'
                        'theme': theme or 'light' # Default to 'light'
                    }
                else:
                    logging.warning(f"No security details found for user_id {user_id} (DMS user: {username})")
            else:
                logging.warning(f"No PEOPLE record found for DMS user: {username}")

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_details for {username}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    return user_details

def update_user_language(username, lang):
    """Updates the language preference for a user."""
    conn = get_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if not user_result:
                logging.error(f"Cannot update language. User '{username}' not found in PEOPLE table.")
                return False

            user_id = user_result[0]

            # Update the LANG in the security table
            update_query = """
                UPDATE LKP_EDMS_USR_SECUR
                SET LANG = :lang
                WHERE USER_ID = :user_id
            """
            cursor.execute(update_query, lang=lang, user_id=user_id)

            if cursor.rowcount == 0:
                logging.warning(f"No rows updated for user '{username}' (user_id: {user_id}). They may not have a security record.")
                return False

            conn.commit()
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_language for {username}: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def update_user_theme(username, theme):
    """Updates the theme preference for a user."""
    conn = get_connection()
    if not conn:
        return False

    if theme not in ['light', 'dark']:
        logging.error(f"Invalid theme value '{theme}' for user '{username}'.")
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if not user_result:
                logging.error(f"Cannot update theme. User '{username}' not found in PEOPLE table.")
                return False

            user_id = user_result[0]

            # Update the THEME in the security table
            update_query = """
                UPDATE LKP_EDMS_USR_SECUR
                SET THEME = :theme
                WHERE USER_ID = :user_id
            """
            cursor.execute(update_query, theme=theme, user_id=user_id)

            if cursor.rowcount == 0:
                logging.warning(f"No rows updated for user '{username}' (user_id: {user_id}). They may not have a security record.")
                return False # Or True, if you consider "not having a record" a non-failure

            conn.commit()
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_theme for {username}: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def get_user_system_id(username):
    """Fetches the SYSTEM_ID from the PEOPLE table for a given username."""
    conn = get_connection()
    if not conn:
        return None

    system_id = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            result = cursor.fetchone()
            if result:
                system_id = result[0]
            else:
                logging.warning(f"No SYSTEM_ID found for user: {username}")
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_system_id for {username}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return system_id

def get_media_type_counts(app_source='unknown', scope=None):
    """
    Counts documents by media type.
    If scope is 'folders', it counts items specifically in the root folder via DMS.
    Otherwise, it counts from the Oracle database using App IDs from the APPS table.
    """

    # --- Logic for Folders Scope ---
    if scope == 'folders':
        dst = dms_system_login()
        if dst:
            return wsdl_client.get_root_folder_counts(dst)
        else:
            logging.error("Failed to login to DMS for folder counts")
            return {"images": 0, "videos": 0, "files": 0}
    # -------------------------------

    conn = get_connection()
    if not conn:
        logging.error("Failed to get DB connection in get_media_type_counts.")
        return None

    range_start = 19677386
    range_end = 19679115

    doc_filter_sql = f"AND p.DOCNUMBER >= {range_start}"

    if app_source == 'edms-media':
        doc_filter_sql = f"AND p.DOCNUMBER BETWEEN {range_start} AND {range_end}"
    elif app_source == 'smart-edms':
        smart_edms_floor = 19662092
        doc_filter_sql = f"AND p.DOCNUMBER >= {smart_edms_floor} AND (p.DOCNUMBER < {range_start} OR p.DOCNUMBER > {range_end})"

    try:
        with conn.cursor() as cursor:
            # Step 1: Try to fetch SYSTEM_ID (Numeric) from APPS
            id_column = "SYSTEM_ID"
            try:
                cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
                apps_rows = cursor.fetchall()
            except oracledb.DatabaseError:
                # Fallback if SYSTEM_ID column doesn't exist
                logging.warning("SYSTEM_ID column not found in APPS, falling back to APPLICATION column.")
                id_column = "APPLICATION"
                cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
                apps_rows = cursor.fetchall()

            image_exts = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'heic', 'ico', 'jfif'}
            video_exts = {'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm', 'm4v', '3gp', 'ts', 'mts', '3g2'}
            pdf_exts = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'csv', 'zip', 'rar', '7z'}

            image_app_ids = []
            video_app_ids = []
            pdf_app_ids = []

            for app_id, ext in apps_rows:
                if not ext: continue
                # Clean extension string for categorization
                clean_ext = str(ext).lower().replace('.', '').strip()
                # Treat ID as a string for safe SQL injection and comparison
                str_id = str(app_id).strip()

                if clean_ext in image_exts:
                    image_app_ids.append(str_id)
                elif clean_ext in video_exts:
                    video_app_ids.append(str_id)
                elif clean_ext in pdf_exts:
                    pdf_app_ids.append(str_id)

            # Helper to create SQL IN clauses
            def build_app_id_clause(ids):
                if not ids: return "1=0"
                id_list = ",".join(f"'{x}'" for x in ids)
                return f"TRIM(TO_CHAR(p.APPLICATION)) IN ({id_list})"

            img_sql = build_app_id_clause(image_app_ids)
            vid_sql = build_app_id_clause(video_app_ids)
            pdf_sql = build_app_id_clause(pdf_app_ids)

            # Step 2: Execute Count Query
            sql = f"""
                  SELECT 
                      SUM(CASE WHEN {img_sql} THEN 1 ELSE 0 END) as image_count,
                      SUM(CASE WHEN {vid_sql} THEN 1 ELSE 0 END) as video_count,
                      SUM(CASE WHEN {pdf_sql} THEN 1 ELSE 0 END) as pdf_count
                  FROM PROFILE p
                  WHERE p.FORM = '2740' 
                  {doc_filter_sql}
                  """

            cursor.execute(sql)
            result = cursor.fetchone()

            if result:
                return {
                    "images": result[0] or 0,
                    "videos": result[1] or 0,
                    "files": result[2] or 0
                }
            else:
                return {"images": 0, "videos": 0, "files": 0}

    except oracledb.Error as e:
        logging.error(f"Oracle error in get_media_type_counts: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

def resolve_media_types_from_db(doc_ids):
    """
    Queries the database to find the media type (image, video, pdf, file, text) for a list of document IDs
    by checking their Application ID and default extension.
    """
    if not doc_ids:
        return {}

    conn = get_connection()
    if not conn:
        return {}

    resolved_map = {}

    try:
        with conn.cursor() as cursor:
            # Prepare comma-separated string for IN clause (safe for ints)
            ids_str = ",".join(str(int(did)) for did in doc_ids)

            # Query PROFILE and APPS tables
            sql = f"""
                SELECT p.DOCNUMBER, a.DEFAULT_EXTENSION
                FROM PROFILE p
                LEFT JOIN APPS a ON p.APPLICATION = a.SYSTEM_ID
                WHERE p.DOCNUMBER IN ({ids_str})
            """

            cursor.execute(sql)
            rows = cursor.fetchall()

            image_exts = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'heic'}
            video_exts = {'mp4', 'mov', 'avi', 'wmv', 'mkv', 'flv', 'webm', '3gp'}
            pdf_exts = {'pdf'}
            text_exts = {'txt', 'csv', 'json', 'xml', 'log', 'md', 'yml', 'yaml', 'ini', 'conf'}

            for doc_id, ext in rows:
                media_type = 'file'  # Default to generic file

                if ext:
                    clean_ext = str(ext).lower().replace('.', '').strip()
                    if clean_ext in image_exts:
                        media_type = 'image'
                    elif clean_ext in video_exts:
                        media_type = 'video'
                    elif clean_ext in pdf_exts:
                        media_type = 'pdf'
                    elif clean_ext in text_exts:
                        media_type = 'text'
                    # else it remains 'file' (docx, xlsx, zip, etc.)

                resolved_map[str(doc_id)] = media_type

    except Exception as e:
        logging.error(f"Error resolving media types: {e}")
    finally:
        if conn:
            conn.close()

    return resolved_map