import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

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
            SELECT p.docnumber, p.image_path, p.abstract,
                   NVL(q.o_detection, 0) as o_detection, NVL(q.OCR, 0) as ocr, NVL(q.face, 0) as face
            FROM PROFILE p
            LEFT JOIN TAGGING_QUEUE q ON p.docnumber = q.docnumber
            WHERE p.form = :form_id AND (q.status IS NULL OR q.status != '4')
            FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(sql, {'form_id': 2740})
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()
    return []

def update_document_processing_status(docnumber, new_abstract, o_detection, ocr, face, status, error, transcript):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        try:
            conn.begin()
            cursor.execute("UPDATE PROFILE SET abstract = :1 WHERE docnumber = :2", (new_abstract, docnumber))
            merge_sql = """
            MERGE INTO TAGGING_QUEUE q
            USING (SELECT :docnumber AS docnumber FROM dual) src ON (q.docnumber = src.docnumber)
            WHEN MATCHED THEN
                UPDATE SET q.o_detection = :o_detection, q.OCR = :ocr, q.face = :face,
                           q.status = :status, q.error = :error, q.transcript = :transcript
            WHEN NOT MATCHED THEN
                INSERT (docnumber, o_detection, OCR, face, status, error, transcript)
                VALUES (:docnumber, :o_detection, :ocr, :face, :status, :error, :transcript)
            """
            cursor.execute(merge_sql, {
                'docnumber': docnumber, 'o_detection': o_detection, 'ocr': ocr, 'face': face,
                'status': status, 'error': error, 'transcript': transcript
            })
            conn.commit()
        except oracledb.Error as ex:
            error, = ex.args
            print(f"Database transaction error: {error.message}")
            conn.rollback()
        finally:
            conn.close()
