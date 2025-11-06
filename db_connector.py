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
import math

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

def get_pta_user_security_level(username):
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
                    FROM LKP_PTA_USR_SECUR us
                    JOIN LKP_PTA_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
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
                                memory_month=None, memory_day=None, user_id=None, lang='en'):
    """Fetches a paginated list of documents from Oracle, handling filtering, memories, and thumbnail logic."""
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

    base_where = "WHERE p.docnumber >= 19677386 AND p.FORM = 2740 " #19662092
    params = {}
    where_clauses = []

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
        
        if search_term:
            key = "search_term"
            key_upper = "search_term_upper"
            
            params[key] = f"%{search_term}%"
            params[key_upper] = f"%{search_term.upper()}%"
            
            base_search = f"""
            (
                p.ABSTRACT LIKE :{key} OR UPPER(p.ABSTRACT) LIKE :{key_upper} OR
                p.DOCNAME LIKE :{key} OR UPPER(p.DOCNAME) LIKE :{key_upper}
            )
            """
            
            search_conditions = [base_search] 

            if lang == 'ar':
                keyword_subquery = f"""
                EXISTS (
                    SELECT 1 FROM LKP_DOCUMENT_TAGS ldt
                    JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                    WHERE ldt.DOCNUMBER = p.DOCNUMBER AND k.DESCRIPTION LIKE :{key} AND ldt.DISABLED = '0'
                )
                """
                search_conditions.append(keyword_subquery)
                
                person_subquery = f"""
                EXISTS (
                    SELECT 1 FROM LKP_PERSON p_filter
                    WHERE p_filter.NAME_ARABIC LIKE :{key}
                    AND (
                         UPPER(p.ABSTRACT) LIKE '%' || UPPER(p_filter.NAME_ENGLISH) || '%'
                         OR (p_filter.NAME_ARABIC IS NOT NULL AND p.ABSTRACT LIKE '%' || p_filter.NAME_ARABIC || '%')
                    )
                )
                """
                search_conditions.append(person_subquery)
            
            where_clauses.append(f"({ ' OR '.join(search_conditions) })")

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
                    if 1900 < year_int < 2100: year_list_int.append(year_int)
                    else: valid_years = False; break
                except ValueError: valid_years = False; break
            if valid_years and year_list_int:
                 year_placeholders = ', '.join([f":year_{i}" for i in range(len(year_list_int))])
                 where_clauses.append(f"EXTRACT(YEAR FROM p.RTADOCDATE) IN ({year_placeholders})")
                 for i, year in enumerate(year_list_int): params[f'year_{i}'] = year
            elif not valid_years: logging.warning(f"Invalid year format received: {years}")


        if tags:
            tag_list = [t.strip() for t in tags.split(',') if t.strip()]
            if tag_list:
                tag_conditions = []
                keyword_column = "DESCRIPTION" if lang == 'ar' else "KEYWORD_ID"
                person_filter_column = "NAME_ARABIC" if lang == 'ar' else "NAME_ENGLISH"

                for i, tag in enumerate(tag_list):
                    key = f'tag_{i}'
                    key_upper = f'tag_{i}_upper'
                    params[key] = tag

                    if lang == 'ar':
                        keyword_compare = f"k.{keyword_column} = :{key}"
                    else:
                        params[key_upper] = tag.upper()
                        keyword_compare = f"UPPER(k.{keyword_column}) = :{key_upper}"

                    keyword_subquery = f"""
                    EXISTS (
                        SELECT 1 FROM LKP_DOCUMENT_TAGS ldt
                        JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                        WHERE ldt.DOCNUMBER = p.DOCNUMBER AND {keyword_compare} AND ldt.DISABLED = '0'
                    )
                    """
                    
                    if lang == 'ar':
                        person_compare = f"p_filter.{person_filter_column} = :{key}"
                    else:
                        person_compare = f"UPPER(p_filter.{person_filter_column}) = :{key_upper}"
                    
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

    # --- Sorting Logic ---
    order_by_clause = "ORDER BY p.DOCNUMBER DESC"
    if memory_month is not None:
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
            count_query = f"SELECT COUNT(p.DOCNUMBER) FROM PROFILE p {final_where_clause}"
            logging.debug(f"Count Query: {count_query}")
            logging.debug(f"Count Params: {params}")
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            date_column = "p.RTADOCDATE" if memory_month is not None else "p.RTADOCDATE"
            fetch_query = f"""
            SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, {date_column} as DOC_DATE, p.DOCNAME,
                   CASE WHEN f.DOCNUMBER IS NOT NULL THEN 1 ELSE 0 END as IS_FAVORITE
            FROM PROFILE p
            LEFT JOIN LKP_FAVORITES_DOC f ON p.DOCNUMBER = f.DOCNUMBER AND f.USER_ID = :db_user_id
            {final_where_clause}
            {order_by_clause}
            OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            params_paginated = params.copy()
            params_paginated['db_user_id'] = db_user_id
            params_paginated['offset'] = offset
            params_paginated['page_size'] = page_size

            logging.debug(f"Fetch Query: {fetch_query}")
            logging.debug(f"Fetch Params: {params_paginated}")
            cursor.execute(fetch_query, params_paginated)

            rows = cursor.fetchall()
            logging.info(f"Fetched {len(rows)} rows for page {page}.")

            for row in rows:
                doc_id, abstract, author, doc_date, docname, is_favorite = row
                thumbnail_path = None
                media_type = 'image'
                file_ext = '.jpg'

                try:
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
                     logging.error(f"Error processing media info/thumbnail for doc {doc_id}: {media_info_e}", exc_info=True)


                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or "",
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
            try: conn.close()
            except oracledb.Error: logging.error("Error closing DB connection.")
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

def fetch_all_tags(lang='en'):
    """Fetches all unique keywords and person names to be used as tags, based on language."""
    conn = get_connection()
    if not conn: return []

    tags = set()
    try:
        with conn.cursor() as cursor:
            keyword_column = "k.DESCRIPTION" if lang == 'ar' else "k.KEYWORD_ID"
            person_column = "p.NAME_ARABIC" if lang == 'ar' else "p.NAME_ENGLISH"

            cursor.execute(
                f"SELECT {keyword_column} FROM KEYWORD k JOIN LKP_DOCUMENT_TAGS ldt ON ldt.TAG_ID = k.SYSTEM_ID WHERE {keyword_column} IS NOT NULL"
            )
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())

            cursor.execute(f"SELECT {person_column} FROM LKP_PERSON p WHERE {person_column} IS NOT NULL")
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_all_tags: {e}")
    finally:
        if conn:
            conn.close()

    return sorted(list(tags))

def fetch_tags_for_document(doc_id, lang='en'):
    """Fetches all keyword and person tags for a single document, based on language."""
    conn = get_connection()
    if not conn:
        return []

    doc_tags = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :doc_id", {'doc_id': doc_id})
            result = cursor.fetchone()
            abstract = result[0] if result else None

            # Determine which columns to select based on language
            keyword_column = "k.DESCRIPTION" if lang == 'ar' else "k.KEYWORD_ID"
            person_column = "p.NAME_ARABIC" if lang == 'ar' else "p.NAME_ENGLISH"

            tag_query = f"""
                SELECT {keyword_column}
                FROM LKP_DOCUMENT_TAGS ldt
                JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                WHERE ldt.DOCNUMBER = :doc_id AND {keyword_column} IS NOT NULL
            """
            cursor.execute(tag_query, {'doc_id': doc_id})
            for tag_row in cursor:
                if tag_row[0]:
                    doc_tags.add(tag_row[0])

            if abstract:
                person_query = f"""
                    SELECT {person_column}
                    FROM LKP_PERSON p
                    WHERE :abstract LIKE '%' || UPPER(NAME_ENGLISH) || '%' AND {person_column} IS NOT NULL
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
def get_dashboard_counts():
    conn = get_connection()
    if not conn:
        return {
            "total_employees": 0,
            "active_employees": 0,
            "inactive_employees": 0,
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
               SELECT COUNT(*)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
                WHERE TRIM(stat.NAME_ENGLISH) = 'Inactive'
            """)
            counts["inactive_employees"] = cursor.fetchone()[0]

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

    # Use RTADOCDATE for filtering memories
    # Rank documents within each past year for the given month/day and pick the latest one (rn=1)
    # Filter for common image types in DOCNAME (case-insensitive)
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
                media_type = 'image' # Assume image based on query filter
                file_ext = '.jpg'   # Default extension for thumbnail

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

    # Build the SET part of the UPDATE statement dynamically
    if new_abstract is not None:
        update_parts.append("ABSTRACT = :abstract")
        params['abstract'] = new_abstract

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

            # Perform the update
            cursor.execute(sql, params)

            # Check if any row was updated
            if cursor.rowcount == 0:
                conn.rollback() # Rollback if no rows affected
                # This could happen if the doc_id exists but the update didn't change anything (e.g., same abstract/date)
                # Or potentially a race condition. Treat as failure for clarity.
                return False, f"Update affected 0 rows for Document ID {doc_id}. Check if data actually changed."

            conn.commit()
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

def get_favorites(user_id, page=1, page_size=20):
    """Fetches a paginated list of a user's favorited documents."""
    conn = get_connection()
    if not conn:
        return [], 0

    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    try:
        with conn.cursor() as cursor:
            # Get the numeric SYSTEM_ID from the PEOPLE table
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=user_id)
            user_result = cursor.fetchone()

            if not user_result:
                logging.error(f"Could not find user '{user_id}' in PEOPLE table for fetching favorites.")
                return [], 0
            
            db_user_id = user_result[0]

            # Count total favorites
            cursor.execute("SELECT COUNT(*) FROM LKP_FAVORITES_DOC WHERE USER_ID = :user_id", [db_user_id])
            total_rows = cursor.fetchone()[0]

            # Fetch paginated favorites
            query = """
                SELECT p.DOCNUMBER, p.ABSTRACT, p.AUTHOR, p.RTADOCDATE as DOC_DATE, p.DOCNAME
                FROM PROFILE p
                JOIN LKP_FAVORITES_DOC f ON p.DOCNUMBER = f.DOCNUMBER
                WHERE f.USER_ID = :user_id
                ORDER BY p.DOCNUMBER DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            cursor.execute(query, user_id=db_user_id, offset=offset, page_size=page_size)

            rows = cursor.fetchall()
            dst = dms_system_login() # Login to DMS to get thumbnails

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
                    "is_favorite": True # Mark as favorite
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
                 logging.info(f"Event '{event_name}' already exists with ID {existing_event_id}.")
                 return existing_event_id, "Event with this name already exists."

            # Get next SYSTEM_ID
            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PHOTO_EVENT")
            system_id = cursor.fetchone()[0]

            # Insert new event
            cursor.execute("INSERT INTO LKP_PHOTO_EVENT (SYSTEM_ID, EVENT_NAME, LAST_UPDATE, DISABLED) VALUES (:1, :2, SYSDATE, 0)",
                           [system_id, event_name.strip()]) # Trim name before insert
            conn.commit()
            logging.info(f"Event '{event_name}' created successfully with ID {system_id}.")
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
            logging.info(f"Checking existence for DOCNUMBER = {doc_id}")
            cursor.execute("SELECT 1 FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            doc_exists = cursor.fetchone() is not None
            if not doc_exists:
                logging.warning(f"Document check failed: DOCNUMBER = {doc_id} not found.")
                return False, f"Document with ID {doc_id} not found."
            logging.info(f"Document check passed for DOCNUMBER = {doc_id}")

            # If event_id is provided, check if it exists and is enabled
            if event_id is not None:
                logging.info(f"Checking existence for EVENT_ID = {event_id}")
                cursor.execute("SELECT 1 FROM LKP_PHOTO_EVENT WHERE SYSTEM_ID = :1", [event_id])
                event_exists = cursor.fetchone() is not None
                if not event_exists:
                    logging.warning(f"Event check failed: EVENT_ID = {event_id} not found or disabled.")
                    return False, f"Event with ID {event_id} not found or is disabled."
                logging.info(f"Event check passed for EVENT_ID = {event_id}")


            # Use MERGE to insert or update the link
            logging.info(f"Executing MERGE for DOCNUMBER={doc_id}, EVENT_ID={event_id}")
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
    """Fetches user details including security level and language preference."""
    conn = get_connection()
    if not conn:
        return None

    user_details = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                query = """
                    SELECT sl.NAME, us.LANG
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                cursor.execute(query, user_id=user_id)
                details_result = cursor.fetchone()

                if details_result:
                    security_level, lang = details_result
                    user_details = {
                        'username': username,
                        'security_level': security_level,
                        'lang': lang or 'en'
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

def get_pta_user_details(username):
    """Fetches user details including security level and language preference."""
    conn = get_connection()
    if not conn:
        return None

    user_details = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                query = """
                    SELECT sl.NAME
                    FROM LKP_PTA_USR_SECUR us
                    JOIN LKP_PTA_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                cursor.execute(query, user_id=user_id)
                details_result = cursor.fetchone()

                if details_result:
                    security_level = details_result
                    user_details = {
                        'username': username,
                        'security_level': security_level,
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
            logging.info(f"Successfully updated language to '{lang}' for user '{username}'.")
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_language for {username}: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()