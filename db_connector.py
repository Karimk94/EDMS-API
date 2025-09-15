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

load_dotenv()

# --- Cache Directory Setup ---
thumbnail_cache_dir = os.path.join(os.path.dirname(__file__), 'thumbnail_cache')
if not os.path.exists(thumbnail_cache_dir):
    os.makedirs(thumbnail_cache_dir)

video_cache_dir = os.path.join(os.path.dirname(__file__), 'video_cache')
if not os.path.exists(video_cache_dir):
    os.makedirs(video_cache_dir)

# --- DMS Communication ---

def dms_login():
    """Logs into the DMS SOAP service and returns a session token (DST)."""
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl_url = os.getenv("WSDL_URL")
        client = Client(wsdl_url, settings=settings)
        login_info_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        dms_user, dms_password = os.getenv("DMS_USER"), os.getenv("DMS_PASSWORD")
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=dms_user, password=dms_password)
        array_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])
        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}
        response = client.service.LoginSvr5(**call_data)
        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        return None
    except Exception:
        return None

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
            'call': {'dstIn': dst, 'criteria': {'criteriaCount': 2, 'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']}, 'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}}}
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
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass
            if content_id: 
                try: obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception: pass


def get_dms_stream_details(dst, doc_number):
    """
    Opens a stream to a DMS document and returns the client and stream ID for reading.
    """
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl_url = os.getenv("WSDL_URL")
        svc_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMObj', settings=settings)

        get_doc_call = {'call': {'dstIn': dst, 'criteria': {'criteriaCount': 2, 'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']}, 'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}}}}
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)
        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID):
            return None

        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            obj_client.service.ReleaseObject(call={'objectID': content_id})
            return None
        
        return {
            "obj_client": obj_client,
            "stream_id": stream_reply.streamID,
            "content_id": content_id
        }
    except Exception as e:
        print(f"Error opening DMS stream for {doc_number}: {e}")
        return None

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
                cursor.execute("SELECT APPLICATION FROM APPS WHERE FILE_TYPES LIKE :ext_like", ext_like=f"%{extension}%")
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
            placeholders = ','.join([':' + str(i+1) for i in range(len(docnumbers))])
            
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
        return docnumbers # Assume still processing if DB is down

    try:
        with conn.cursor() as cursor:
            placeholders = ','.join([':' + str(i+1) for i in range(len(docnumbers))])
            
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


def fetch_documents_from_oracle(page=1, page_size=10, search_term=None, date_from=None, date_to=None, persons=None, person_condition='any', tags=None):
    """Fetches a paginated list of documents from Oracle, handling filtering and thumbnail logic."""
    conn = get_connection()
    if not conn: return [], 0

    dst = dms_login()
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
            
            cursor.execute(fetch_query + " ORDER BY p.DOCNUMBER DESC OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY", params)
            
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

def update_document_processing_status(docnumber, new_abstract, o_detected, ocr, face, status, error, transcript, attempts):
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
                'status': status, 'error': error, 'transcript': transcript, 'attempts' : attempts
            })
            
            conn.commit()
            logging.info(f"DB_UPDATE_SUCCESS: Successfully updated status for docnumber {docnumber}.")
            
    except oracledb.Error as e:
        logging.error(f"DB_UPDATE_ERROR: Oracle error while updating docnumber {docnumber}: {e}", exc_info=True)
        try:
            conn.rollback()
            logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} was rolled back.")
        except oracledb.Error as rb_e:
            logging.error(f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber}: {rb_e}", exc_info=True)
            
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
            cursor.execute("SELECT KEYWORD_ID FROM KEYWORD k JOIN LKP_DOCUMENT_TAGS ldt ON ldt.TAG_ID = k.SYSTEM_ID")
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

                english_keyword = english_keyword_orig.lower()

                if english_keyword in processed_keywords:
                    continue

                if len(english_keyword) > 30:
                    logging.warning(f"Skipping keyword '{english_keyword_orig}' for docnumber {docnumber} because its length ({len(english_keyword_orig)}) exceeds the 30-character limit.")
                    continue

                keyword_system_id = None

                cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :keyword_id", keyword_id=english_keyword)
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
                        """, keyword_id=english_keyword, description=arabic_keyword, system_id=keyword_system_id)

                    except oracledb.IntegrityError as ie:
                        error, = ie.args
                        if "ORA-00001" in error.message:
                            logging.warning(f"Keyword '{english_keyword}' was inserted by another process. Fetching existing ID.")
                            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :keyword_id", keyword_id=english_keyword)
                            result = cursor.fetchone()
                            if result:
                                keyword_system_id = result[0]
                            else:
                                logging.error(f"Failed to fetch SYSTEM_ID for '{english_keyword}' after integrity error.")
                                continue
                        else:
                            raise

                if keyword_system_id:
                    cursor.execute("SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :docnumber AND TAG_ID = :tag_id",
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
        logging.error(f"DB_KEYWORD_ERROR: Oracle error while processing keywords for docnumber {docnumber}: {e}", exc_info=True)
        try:
            conn.rollback()
            logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} keywords was rolled back.")
        except oracledb.Error as rb_e:
            logging.error(f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber} keywords: {rb_e}", exc_info=True)

    finally:
        if conn:
            conn.close()

def add_tag_to_document(doc_id, tag):
    """Adds a new tag to a document, handling existing keywords."""
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
                cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)", [tag.lower(), keyword_id])

            # Check if the document is already tagged with this keyword
            cursor.execute("SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :1 AND TAG_ID = :2", [doc_id, keyword_id])
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
                cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)", [new_tag.lower(), new_keyword_id])

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
    """Deletes a tag from a document."""
    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Find the keyword ID
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [tag.lower()])
            result = cursor.fetchone()
            if not result:
                return False, "Tag not found."
            keyword_id = result[0]

            # Delete the link from LKP_DOCUMENT_TAGS
            cursor.execute("""
                DELETE FROM LKP_DOCUMENT_TAGS
                WHERE DOCNUMBER = :doc_id AND TAG_ID = :tag_id
            """, doc_id=doc_id, tag_id=keyword_id)
            conn.commit()
            return True, "Tag deleted successfully."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()