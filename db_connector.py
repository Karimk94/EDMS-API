import oracledb
import os
from dotenv import load_dotenv
from zeep import Client, Settings
from zeep.exceptions import Fault
import re
from PIL import Image
import io
import shutil

load_dotenv()

thumbnail_cache_dir = os.path.join(os.path.dirname(__file__), 'thumbnail_cache')
if not os.path.exists(thumbnail_cache_dir):
    os.makedirs(thumbnail_cache_dir)

def get_connection():
    try:
        dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"
        connection = oracledb.connect(user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), dsn=dsn)
        return connection
    except oracledb.Error as ex:
        error, = ex.args
        print(f"Database connection error: {error.message}")
        return None

def get_documents_to_process():
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
              AND q.attempts <= 3
            FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(sql, {'form_id': 2740})
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()
    return []

def update_document_processing_status(docnumber, new_abstract, o_detected, ocr, face, status, error, transcript, attempts):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        try:
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
        finally:
            conn.close()

def fetch_documents_from_oracle(page=1, page_size=10, search_term=None, date_from=None, date_to=None, persons=None, person_condition='any'):
    conn = get_connection()
    if not conn: return [], 0
    offset = (page - 1) * page_size
    documents = []
    total_rows = 0

    base_where = "WHERE docnumber >= 19662092 and FORM = 2740 "
    count_query = f"SELECT COUNT(DOCNUMBER) FROM PROFILE {base_where}"
    fetch_query = f"SELECT DOCNUMBER, ABSTRACT, AUTHOR, CREATION_DATE, DOCNAME FROM PROFILE {base_where}"

    where_clause = ""
    params = {}
    if search_term:
        words = re.findall(r'\w+', search_term.upper())
        conditions = [f"UPPER(ABSTRACT) LIKE :search_word_{i}" for i in range(len(words))]
        where_clause += "AND " + " AND ".join(conditions)
        for i, word in enumerate(words):
            params[f"search_word_{i}"] = f"%{word}%"

    if persons:
        person_list = [p.strip().upper() for p in persons.split(',') if p.strip()]
        if person_list:
            logical_operator = " OR " if person_condition == 'any' else " AND "
            person_conditions = [f"UPPER(ABSTRACT) LIKE :person_{i}" for i in range(len(person_list))]
            where_clause += " AND (" + logical_operator.join(person_conditions) + ")"
            for i, person in enumerate(person_list):
                params[f'person_{i}'] = f"%{person}%"

    if date_from:
        where_clause += " AND CREATION_DATE >= TO_DATE(:date_from, 'YYYY-MM-DD HH24:MI:SS')"
        params['date_from'] = date_from

    if date_to:
        where_clause += " AND CREATION_DATE <= TO_DATE(:date_to, 'YYYY-MM-DD HH24:MI:SS')"
        params['date_to'] = date_to

    try:
        with conn.cursor() as cursor:
            cursor.execute(count_query + where_clause, params)
            total_rows = cursor.fetchone()[0]
            params['offset'] = offset
            params['page_size'] = page_size
            cursor.execute(fetch_query + where_clause + " ORDER BY DOCNUMBER DESC OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY", params)
            for row in cursor:
                doc_id = row[0]
                thumbnail_path = get_thumbnail_from_edms(doc_id)
                documents.append({
                    "doc_id": doc_id,
                    "title": row[1] or "No Title",
                    "docname": row[4] or "",
                    "author": row[2] or "N/A",
                    "date": row[3].strftime('%Y-%m-%d') if row[3] else "N/A",
                    "thumbnail_url": thumbnail_path or "https://placehold.co/100x100/e9ecef/6c757d?text=No+Image"
                })
    finally:
        conn.close()
    return documents, total_rows

def get_image_from_edms(doc_number):
    """
    Retrieves a single document's full-size image bytes from the DMS.
    """
    dst = dms_login()
    if not dst:
        return None

    svc_client, obj_client, content_id, stream_id = None, None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl_url = os.getenv("WSDL_URL")
        svc_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(wsdl_url, port_name='BasicHttpBinding_IDMObj', settings=settings)

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

        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID):
            return None

        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})

        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            raise Exception("Failed to get read stream.")

        stream_id = stream_reply.streamID
        doc_buffer = bytearray()
        while True:
            read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
            if not read_reply or read_reply.resultCode != 0: break
            chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
            if not chunk_data: break
            doc_buffer.extend(chunk_data)

        return bytes(doc_buffer)

    except Fault as e:
        print(f"DMS server fault for doc: {doc_number}. Error: {e}")
        return None
    finally:
        if obj_client:
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass
            if content_id:
                try: obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception: pass

def get_thumbnail_from_edms(doc_number):
    thumbnail_filename = f"{doc_number}.jpg"
    cached_path = os.path.join(thumbnail_cache_dir, thumbnail_filename)
    if os.path.exists(cached_path):
        return f"cache/{thumbnail_filename}"
    image_bytes = get_image_from_edms(doc_number)
    if not image_bytes: return None
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            img.thumbnail((300, 300))
            img.convert("RGB").save(cached_path, "JPEG", quality=95)
            return f"cache/{thumbnail_filename}"
    except Exception as e:
        print(f"Error creating thumbnail for {doc_number}: {e}")
        return None
    
def clear_thumbnail_cache():
    shutil.rmtree(thumbnail_cache_dir)
    os.makedirs(thumbnail_cache_dir)

def update_abstract_with_vips(doc_id, vip_names):
    conn = get_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
            result = cursor.fetchone()
            if result is None: return False, f"Document with ID {doc_id} not found."
            current_abstract = result[0] or ""
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
    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
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
    conn = get_connection()
    if not conn:
        return [], 0

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
                persons.append({
                    "id": row[0],
                    "name_english": row[1],
                    "name_arabic": row[2]
                })
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_lkp_persons: {e}")
    finally:
        conn.close()

    return persons, total_rows

def dms_login():
    """Logs into the DMS SOAP service and returns a session token (DST)."""
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        wsdl_url = os.getenv("WSDL_URL")
        client = Client(wsdl_url, settings=settings)

        login_info_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        dms_user = os.getenv("DMS_USER")
        dms_password = os.getenv("DMS_PASSWORD")
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=dms_user, password=dms_password)

        array_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])

        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}

        response = client.service.LoginSvr5(**call_data)

        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        else:
            return None
    except Exception as e:
        return None