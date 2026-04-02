import os
import io
import shutil
import logging
import time
import oracledb
from datetime import datetime
from PIL import Image
import fitz
from moviepy.video.io.VideoFileClip import VideoFileClip
from zeep import Client, Settings
from zeep.exceptions import Fault
import wsdl_client
from database.connection import get_connection, get_async_connection

APPS_BUCKET_TTL_SECONDS = int(os.getenv('APPS_CACHE_TTL_SECONDS', '600'))
ORACLE_IN_CLAUSE_LIMIT = 900

IMAGE_EXTS = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'heic', 'ico', 'jfif'}
VIDEO_EXTS = {'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm', 'm4v', '3gp', 'ts', 'mts', '3g2'}
PDF_EXTS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'csv', 'zip', 'rar', '7z'}

DOC_IMAGE_EXTS = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'heic'}
DOC_VIDEO_EXTS = {'mp4', 'mov', 'avi', 'wmv', 'mkv', 'flv', 'webm', '3gp'}
DOC_PDF_EXTS = {'pdf'}
DOC_WORD_EXTS = {'doc', 'docx'}
DOC_EXCEL_EXTS = {'xls', 'xlsx', 'ods', 'xlsm'}
DOC_PPT_EXTS = {'ppt', 'pptx', 'odp', 'pps', 'ppsx'}
DOC_TEXT_EXTS = {'txt', 'csv', 'json', 'xml', 'log', 'md'}
DOC_ZIP_EXTS = {'zip', 'rar', '7z', 'tar', 'gz'}

_apps_bucket_cache = {
    'expires_at': 0.0,
    'image_ids': [],
    'video_ids': [],
    'pdf_ids': []
}


def _build_app_buckets(apps_rows):
    image_app_ids = []
    video_app_ids = []
    pdf_app_ids = []

    for app_id, ext in apps_rows:
        if not ext:
            continue
        clean_ext = str(ext).lower().replace('.', '').strip()
        str_id = str(app_id).strip()
        if clean_ext in IMAGE_EXTS:
            image_app_ids.append(str_id)
        elif clean_ext in VIDEO_EXTS:
            video_app_ids.append(str_id)
        elif clean_ext in PDF_EXTS:
            pdf_app_ids.append(str_id)

    return image_app_ids, video_app_ids, pdf_app_ids


def _get_cached_app_buckets(cursor):
    now = time.time()
    if _apps_bucket_cache['expires_at'] > now:
        return (
            _apps_bucket_cache['image_ids'],
            _apps_bucket_cache['video_ids'],
            _apps_bucket_cache['pdf_ids']
        )

    id_column = 'SYSTEM_ID'
    try:
        cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
        apps_rows = cursor.fetchall()
    except oracledb.DatabaseError:
        id_column = 'APPLICATION'
        cursor.execute(f"SELECT {id_column}, DEFAULT_EXTENSION FROM APPS")
        apps_rows = cursor.fetchall()

    image_app_ids, video_app_ids, pdf_app_ids = _build_app_buckets(apps_rows)
    _apps_bucket_cache['image_ids'] = image_app_ids
    _apps_bucket_cache['video_ids'] = video_app_ids
    _apps_bucket_cache['pdf_ids'] = pdf_app_ids
    _apps_bucket_cache['expires_at'] = now + APPS_BUCKET_TTL_SECONDS

    return image_app_ids, video_app_ids, pdf_app_ids

# --- Cache Directory Setup ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
thumbnail_cache_dir = os.path.join(BASE_DIR, 'thumbnail_cache')
video_cache_dir = os.path.join(BASE_DIR, 'video_cache')
temp_thumbnail_cache_dir = os.path.join(BASE_DIR, 'temp_thumbnail_cache')
if not os.path.exists(thumbnail_cache_dir): os.makedirs(thumbnail_cache_dir)
if not os.path.exists(video_cache_dir): os.makedirs(video_cache_dir)
if not os.path.exists(temp_thumbnail_cache_dir): os.makedirs(temp_thumbnail_cache_dir)

def dms_system_login():
    """Logs into the DMS SOAP service using system credentials."""
    return wsdl_client.dms_system_login()

def stream_document_from_dms(dst, doc_number):
    """
    Returns a generator that yields file content chunks and the filename.
    Returns: (generator, filename)
    """
    return wsdl_client.stream_document_content(dst, doc_number)

def get_exif_date(image_stream):
    """Extracts the 'Date Taken' from image EXIF data."""
    try:
        image_stream.seek(0)
        with Image.open(image_stream) as img:
            raw_exif = img._getexif()
            if raw_exif and 36867 in raw_exif:
                date_str = raw_exif[36867]
                if date_str and date_str.strip():
                    for fmt in ('%Y:%m:%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'):
                        try:
                            return datetime.strptime(date_str, fmt)
                        except ValueError:
                            continue
    except Exception as e:
        logging.warning(f"Could not extract EXIF date: {e}")
    finally:
        image_stream.seek(0)
    return None

async def get_document_metadata_from_db(doc_number):
    """Fetches DOCNAME and DEFAULT_EXTENSION from Oracle DB."""
    conn = await get_async_connection()
    if not conn: return None, None
    try:
        async with conn.cursor() as cursor:
            # Query PROFILE and APPS. 
            # Note: APPS join based on resolve_media_types_from_db logic
            sql = """
                SELECT p.DOCNAME, a.DEFAULT_EXTENSION
                FROM PROFILE p
                LEFT JOIN APPS a ON p.APPLICATION = a.SYSTEM_ID
                WHERE p.DOCNUMBER = :doc_number
            """
            await cursor.execute(sql, {'doc_number': doc_number})
            row = await cursor.fetchone()
            if row:
                return row[0], row[1]
    except Exception as e:
        logging.error(f"Error fetching DB metadata for {doc_number}: {e}")
    finally:
        await conn.close()
    return None, None

async def get_media_info_from_dms(dst, doc_number):
    """
    Efficiently retrieves metadata. Prioritizes Oracle DB for Filename/DocName.
    """
    # 1. Fetch metadata from Oracle DB (Async)
    db_docname, db_def_ext = await get_document_metadata_from_db(doc_number)
    
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
        system_filename = None

        if doc_reply.docProperties and doc_reply.docProperties.propertyValues:
            try:
                prop_names = doc_reply.docProperties.propertyNames.string
                prop_values = doc_reply.docProperties.propertyValues.anyType
                
                if '%VERSION_FILE_NAME' in prop_names:
                     index = prop_names.index('%VERSION_FILE_NAME')
                     val = prop_values[index]
                     if val: system_filename = str(val).strip()
            except Exception as e:
                 logging.error(f"Error parsing DMS properties for {doc_number}: {e}")

        # --- FILENAME RESOLUTION ---
        # Priority 1: Oracle DOCNAME
        if db_docname:
            filename = str(db_docname).strip()
        # Priority 2: DMS Version Filename
        elif system_filename:
            filename = system_filename
        
        # --- EXTENSION RESOLUTION ---
        # Ensure filename has an extension
        _, current_ext = os.path.splitext(filename)
        if not current_ext:
            if db_def_ext:
                # Use default extension from DB APPS table
                clean_ext = str(db_def_ext).strip().replace('.', '')
                filename = f"{filename}.{clean_ext}"
            elif system_filename:
                # Fallback to extension from system filename
                _, sys_ext = os.path.splitext(system_filename)
                if sys_ext:
                    filename = f"{filename}{sys_ext}"

        # Resolve media type strictly from DB first
        media_type = 'file'  # Default
        try:
            # 'resolve_media_types_from_db' is a coroutine, so we must await it
            resolved_map = await resolve_media_types_from_db([doc_number])
            media_type = resolved_map.get(str(doc_number), 'file')
        except Exception as e:
            logging.error(f"Error resolving media type from DB for {doc_number}: {e}")

        # Fallback to extension if DB lookup failed entirely or returned generic file
        if media_type == 'file':
            video_extensions = [
                '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm',
                '.m4v', '.3gp', '.mts', '.ts', '.3g2'
            ]
            pdf_extensions = ['.pdf']
            image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff', '.webp', '.heic']
            text_extensions = ['.txt', '.csv', '.json', '.xml', '.log', '.md', '.yml', '.yaml', '.ini', '.conf']
            excel_extensions = ['.xls', '.xlsx', '.ods', '.xlsm']
            ppt_extensions = ['.ppt', '.pptx', '.odp', '.pps', '.ppsx']
            zip_extensions = ['.zip', '.rar', '.7z', '.tar', '.gz']

            file_ext = os.path.splitext(filename)[1].lower()

            if file_ext in video_extensions:
                media_type = 'video'
            elif file_ext in pdf_extensions:
                media_type = 'pdf'
            elif file_ext in image_extensions:
                media_type = 'image'
            elif file_ext in text_extensions:
                media_type = 'text'
            elif file_ext in excel_extensions:
                media_type = 'excel'
            elif file_ext in ppt_extensions:
                media_type = 'powerpoint'
            elif file_ext in zip_extensions:
                media_type = 'zip'
            else:
                media_type = 'file'

        file_ext = os.path.splitext(filename)[1].lower()
        return filename, media_type, file_ext

    except Fault as e:
        logging.error(f"DMS metadata fault for doc {doc_number}: {e}")
        return None, 'image', ''
    except Exception as e:
        logging.error(f"Unexpected error in get_media_info_from_dms: {e}")
        return None, 'image', ''

def get_media_content_from_dms(dst, doc_number):
    """Retrieves the full binary content (Synchronous SOAP)."""
    # wsdl_client.get_image_by_docnumber is likely synchronous in your wsdl_client.py
    # If you changed it to async, await it here. Assuming sync for now as it's pure SOAP.
    return wsdl_client.get_image_by_docnumber(dst, doc_number)[0]

def get_dms_stream_details(dst, doc_number):
    return wsdl_client.get_dms_stream_details(dst, doc_number)

def stream_and_cache_generator(obj_client, stream_id, content_id, final_cache_path):
    """Generator for streaming data."""
    temp_cache_path = final_cache_path + ".tmp"
    try:
        with open(temp_cache_path, "wb") as f:
            while True:
                read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
                if not read_reply or read_reply.resultCode != 0: break
                chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
                if not chunk_data: break
                f.write(chunk_data)
                yield chunk_data
        os.rename(temp_cache_path, final_cache_path)
    except Exception as e:
        logging.error(f"Error streaming: {e}")
    finally:
        try:
            if stream_id: obj_client.service.ReleaseObject(call={'objectID': stream_id})
            if content_id: obj_client.service.ReleaseObject(call={'objectID': content_id})
        except:
            pass
        if os.path.exists(temp_cache_path): os.remove(temp_cache_path)

def create_thumbnail(doc_number, media_type, file_ext, media_bytes, is_temp=False):
    """Creates a thumbnail from media bytes and saves it to the cache."""
    if media_type in ['excel', 'powerpoint', 'text', 'file', 'zip']:
        return None

    thumbnail_filename = f"{doc_number}.jpg"
    target_dir = temp_thumbnail_cache_dir if is_temp else thumbnail_cache_dir
    cached_path = os.path.join(target_dir, thumbnail_filename)
    try:
        if media_type == 'video':
            temp_video_path = os.path.join(target_dir, f"{doc_number}{file_ext}")
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
        return f"temp_thumbnail/{doc_number}" if is_temp else f"cache/{thumbnail_filename}" # Return appropriate path for URL
    except Exception as e:
        logging.error(f"Could not create thumbnail for {doc_number}: {e}")
        return None

def clear_thumbnail_cache():
    if os.path.exists(thumbnail_cache_dir): shutil.rmtree(thumbnail_cache_dir)
    os.makedirs(thumbnail_cache_dir)

def clear_video_cache():
    if os.path.exists(video_cache_dir): shutil.rmtree(video_cache_dir)
    os.makedirs(video_cache_dir)

async def get_app_id_from_extension(extension):
    """Looks up the APPLICATION (APP_ID) from the APPS table (Async)."""
    conn = get_connection()
    if not conn: return None

    app_id = None
    upper_extension = extension.upper() if extension else ''
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT APPLICATION FROM APPS WHERE UPPER(DEFAULT_EXTENSION) = :ext", ext=upper_extension)
            result = cursor.fetchone()
            if result:
                app_id = result[0]
            else:
                cursor.execute("SELECT APPLICATION FROM APPS WHERE UPPER(FILE_TYPES) LIKE :ext_like",
                               ext_like=f"%{upper_extension}%")
                result = cursor.fetchone()
                if result: app_id = result[0]
    except oracledb.Error as e:
        logging.error(f"Oracle error in get_app_id_from_extension: {e}")
    finally:
        if conn: conn.close()
    return app_id

async def get_media_type_counts(app_source='unknown', scope=None, username=None):
    """Counts documents by media type (Async)."""
    if scope == 'folders':
        dst = dms_system_login()
        if dst:
            return await wsdl_client.get_root_folder_counts(dst, username=username)
        return {"images": 0, "videos": 0, "files": 0}

    conn = get_connection()
    if not conn: return None

    doc_filter_sql = "AND p.RTA_TEXT1 = 'edms-media'"

    if app_source == 'edms-media':
        doc_filter_sql = "AND p.RTA_TEXT1 = 'edms-media'"
    elif app_source == 'smart-edms':
        smart_edms_floor = 19662092
        doc_filter_sql = f"AND p.DOCNUMBER >= {smart_edms_floor} AND (p.RTA_TEXT1 IS NULL OR p.RTA_TEXT1 != 'edms-media')"

    try:
        with conn.cursor() as cursor:
            image_app_ids, video_app_ids, pdf_app_ids = _get_cached_app_buckets(cursor)

            def build_app_id_clause(ids):
                if not ids: return "1=0"
                id_list = ",".join(f"'{x}'" for x in ids)
                return f"TRIM(TO_CHAR(p.APPLICATION)) IN ({id_list})"

            img_sql = build_app_id_clause(image_app_ids)
            vid_sql = build_app_id_clause(video_app_ids)
            pdf_sql = build_app_id_clause(pdf_app_ids)

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
            return {"images": result[0] or 0, "videos": result[1] or 0, "files": result[2] or 0}

    except oracledb.Error as e:
        logging.error(f"Oracle error in get_media_type_counts: {e}")
        return None
    finally:
        if conn: conn.close()

async def resolve_media_types_from_db(doc_ids):
    """Queries the database to find the media type (Async)."""
    if not doc_ids: return {}

    conn = get_connection()
    if not conn: return {}

    resolved_map = {}
    try:
        with conn.cursor() as cursor:
            valid_doc_ids = []
            for did in doc_ids:
                try:
                    valid_doc_ids.append(int(did))
                except Exception:
                    continue

            if not valid_doc_ids:
                return {}

            join_column = 'SYSTEM_ID'

            for start in range(0, len(valid_doc_ids), ORACLE_IN_CLAUSE_LIMIT):
                chunk = valid_doc_ids[start:start + ORACLE_IN_CLAUSE_LIMIT]
                bind_names = [f"id{i}" for i in range(len(chunk))]
                in_clause = ",".join(f":{name}" for name in bind_names)
                bind_params = {name: value for name, value in zip(bind_names, chunk)}

                sql = f"""
                    SELECT p.DOCNUMBER, a.DEFAULT_EXTENSION
                    FROM PROFILE p
                    LEFT JOIN APPS a ON p.APPLICATION = a.{join_column}
                    WHERE p.DOCNUMBER IN ({in_clause})
                """

                try:
                    cursor.execute(sql, bind_params)
                except oracledb.DatabaseError:
                    if join_column == 'SYSTEM_ID':
                        join_column = 'APPLICATION'
                        sql = f"""
                            SELECT p.DOCNUMBER, a.DEFAULT_EXTENSION
                            FROM PROFILE p
                            LEFT JOIN APPS a ON p.APPLICATION = a.{join_column}
                            WHERE p.DOCNUMBER IN ({in_clause})
                        """
                        cursor.execute(sql, bind_params)
                    else:
                        raise

                rows = cursor.fetchall()

                for doc_id, ext in rows:
                    media_type = 'file'
                    if ext:
                        clean_ext = str(ext).lower().replace('.', '').strip()
                        if clean_ext in DOC_IMAGE_EXTS:
                            media_type = 'image'
                        elif clean_ext in DOC_VIDEO_EXTS:
                            media_type = 'video'
                        elif clean_ext in DOC_PDF_EXTS:
                            media_type = 'pdf'
                        elif clean_ext in DOC_WORD_EXTS:
                            media_type = 'docx'
                        elif clean_ext in DOC_EXCEL_EXTS:
                            media_type = 'excel'
                        elif clean_ext in DOC_PPT_EXTS:
                            media_type = 'powerpoint'
                        elif clean_ext in DOC_TEXT_EXTS:
                            media_type = 'text'
                        elif clean_ext in DOC_ZIP_EXTS:
                            media_type = 'zip'
                    resolved_map[str(doc_id)] = media_type
    except Exception as e:
        logging.error(f"Error resolving media types: {e}")
    finally:
        if conn: conn.close()
    return resolved_map