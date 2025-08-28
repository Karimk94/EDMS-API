from flask import Flask, jsonify
import db_connector
import api_client
import wsdl_client
import logging
from werkzeug.serving import run_simple
import re

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

def process_document(doc, dms_session_token):
    """
    Processes a single document using the provided DMS session token.
    """
    docnumber = doc['docnumber']
    logging.info(f"Processing document: {docnumber}")

    original_abstract = doc.get('abstract') or ''
    new_abstract = original_abstract  # Ensure new_abstract always has a value

    ai_abstract_parts = {}
    
    parts = re.split(r'\n\n(Caption:|OCR:|VIPs:)', original_abstract, flags=re.IGNORECASE)
    base_abstract = parts[0].strip()

    if len(parts) > 1:
        it = iter(parts[1:])
        for tag in it:
            key = tag.strip().replace(':', '')
            value = next(it, '').strip()
            ai_abstract_parts[key.upper()] = value

    o_detected_status = doc.get('o_detected', 0)
    ocr_status = doc.get('ocr', 0)
    face_status = doc.get('face', 0)
    final_status = doc.get('status', 1)
    attempts = doc.get('attempts', 0)
    error_message = ''
    transcript_text = ''

    try:
        image_bytes, filename = wsdl_client.get_image_by_docnumber(dms_session_token, docnumber)
        if not image_bytes:
            raise Exception(f"Failed to retrieve image for docnumber {docnumber} from WSDL service.")
        logging.info(f"Image for {docnumber} ({filename}) fetched successfully.")

        if not o_detected_status:
            captions = api_client.get_captions(image_bytes, filename)
            if captions is not None:
                o_detected_status = 1
                ai_abstract_parts['CAPTION'] = captions.strip()

        if not ocr_status:
            ocr_text = api_client.get_ocr_text(image_bytes, filename)
            if ocr_text is not None:
                ocr_status = 1
                ai_abstract_parts['OCR'] = ocr_text.strip()

        if not face_status:
            recognized_faces = api_client.recognize_faces(image_bytes, filename)
            if recognized_faces is not None:
                face_status = 1
                known_face_names = [
                    face.get('name').replace('_', ' ').title()
                    for face in recognized_faces
                    if face.get('name') and face.get('name') != 'Unknown'
                ]
                if known_face_names:
                    ai_abstract_parts['VIPS'] = ", ".join(known_face_names)

        final_abstract_parts = [base_abstract]
        if ai_abstract_parts.get('CAPTION'):
            final_abstract_parts.append(f"Caption: {ai_abstract_parts['CAPTION']}")
        if ai_abstract_parts.get('OCR'):
            final_abstract_parts.append(f"OCR: {ai_abstract_parts['OCR']}")
        if ai_abstract_parts.get('VIPS'):
            final_abstract_parts.append(f"VIPs: {ai_abstract_parts['VIPS']}")
        
        new_abstract = "\n\n".join(filter(None, final_abstract_parts))
        
        if o_detected_status and ocr_status and face_status:
            final_status = 3

    except Exception as e:
        logging.error(f"Error processing document {docnumber}", exc_info=True)
        error_message = str(e)
        final_status = 2

    finally:
        attempts += 1
        db_connector.update_document_processing_status(
            docnumber=docnumber, new_abstract=new_abstract, o_detected=o_detected_status,
            ocr=ocr_status, face=face_status, status=final_status,
            error=error_message, transcript=transcript_text, attempts= attempts
        )
        logging.info(f"Finished processing document {docnumber} with status: {final_status}")


@app.route('/process-batch', methods=['POST'])
def process_batch():
    """API endpoint to trigger the processing of a batch of documents."""
    logging.info("'/process-batch' endpoint called.")
    
    dms_session_token = wsdl_client.dms_login()
    if not dms_session_token:
        logging.critical("Could not log into DMS. Aborting batch.")
        return jsonify({"status": "error", "message": "Failed to authenticate with DMS."}), 500

    logging.info("DMS login successful. Fetching documents from database.")
    processed_count = 0
    try:
        documents = db_connector.get_documents_to_process()
        if not documents:
            logging.info("No new documents to process.")
            return jsonify({"status": "success", "message": "No new documents to process.", "processed_count": 0}), 200

        for doc in documents:
            try:
                # This inner try-except block ensures that a single document failure
                # does not stop the entire batch.
                process_document(doc, dms_session_token)
                processed_count += 1
            except Exception as e:
                # Log the specific document that failed and continue the loop
                docnumber = doc.get('docnumber', 'N/A')
                logging.error(f"A critical error occurred while processing docnumber {docnumber}. Skipping to next document.", exc_info=True)

        logging.info(f"Successfully processed a batch of {processed_count} out of {len(documents)} documents.")
        return jsonify({"status": "success", "message": f"Processed {processed_count} documents.", "processed_count": processed_count}), 200

    except Exception as e:
        logging.error("An unhandled error occurred in the /process-batch endpoint.", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    run_simple(
        '127.0.0.1',
        5006,
        app,
        use_reloader=False,
        use_debugger=True,
        threaded=True,
        exclude_patterns=['*venv*', '*__pycache__*']
    )