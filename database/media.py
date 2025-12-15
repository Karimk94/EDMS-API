import os
import io
import shutil
import logging
import oracledb
from datetime import datetime
from PIL import Image
import fitz
from moviepy.video.io.VideoFileClip import VideoFileClip
from zeep import Client, Settings
from zeep.exceptions import Fault
import wsdl_client
from database.connection import get_connection

# --- Cache Directory Setup ---
# Setup cache directories in the root folder (parent of database/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

thumbnail_cache_dir = os.path.join(BASE_DIR, 'thumbnail_cache')
if not os.path.exists(thumbnail_cache_dir):
    os.makedirs(thumbnail_cache_dir)

video_cache_dir = os.path.join(BASE_DIR, 'video_cache')
if not os.path.exists(video_cache_dir):
    os.makedirs(video_cache_dir)

def dms_system_login():
    """Logs into the DMS SOAP service using system credentials from .env and returns a session token (DST)."""
    return wsdl_client.dms_system_login()

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
            # Fallback to extension if DB lookup failed entirely
            video_extensions = [
                '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm',
                '.m4v', '.3gp', '.mts', '.ts', '.3g2'
            ]
            pdf_extensions = ['.pdf']
            image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff', '.webp', '.heic']
            text_extensions = ['.txt', '.csv', '.json', '.xml', '.log', '.md', '.yml', '.yaml', '.ini', '.conf']
            excel_extensions = ['.xls', '.xlsx', '.ods', '.xlsm']
            ppt_extensions = ['.ppt', '.pptx', '.odp', '.pps', '.ppsx']

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

def get_app_id_from_extension(extension):
    """
    Looks up the APPLICATION (APP_ID) from the APPS table based on the file extension.
    Converts extension to uppercase for comparison.
    """
    conn = get_connection()
    if not conn: return None

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
    Queries the database to find the media type (image, video, pdf, file, text, excel, powerpoint) for a list of document IDs
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
            excel_exts = {'xls', 'xlsx', 'ods', 'xlsm'}
            ppt_exts = {'ppt', 'pptx', 'odp', 'pps', 'ppsx'}

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
                    elif clean_ext in excel_exts:
                        media_type = 'excel'
                    elif clean_ext in ppt_exts:
                        media_type = 'powerpoint'
                    # else it remains 'file' (zip, rar, etc.)

                resolved_map[str(doc_id)] = media_type

    except Exception as e:
        logging.error(f"Error resolving media types: {e}")
    finally:
        if conn:
            conn.close()

    return resolved_map