from flask import Flask, jsonify
import db_connector
import api_client
import wsdl_client
import logging
from werkzeug.serving import run_simple

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

def process_document(doc, dms_session_token):
    """
    Processes a single document using the provided DMS session token.
    """
    docnumber = doc['docnumber']
    logging.info(f"Processing document: {docnumber}")
    
    new_abstract = doc.get('abstract') or ''
    o_detected_status = doc.get('o_detected', 0)
    ocr_status = doc.get('ocr', 0)
    face_status = doc.get('face', 0)
    final_status = '1'
    error_message = ''
    transcript_text = ''

    try:
        # Step 1: Fetch the image using the WSDL client and the session token
        image_bytes, filename = wsdl_client.get_image_by_docnumber(dms_session_token, docnumber)

        if not image_bytes:
            raise Exception(f"Failed to retrieve image for docnumber {docnumber} from WSDL service.")

        logging.info(f"Image for {docnumber} ({filename}) fetched successfully.")

        # Step 2: Send image data to AI services
        if not o_detected_status:
            captions = api_client.get_captions(image_bytes, filename)
            if captions is not None: 
                o_detected_status = 1
                if captions and captions.strip():
                    new_abstract += f"\n {captions}"

        if not ocr_status:
            ocr_text = api_client.get_ocr_text(image_bytes, filename)
            if ocr_text is not None:
                ocr_status = 1
                if ocr_text.strip(): new_abstract += f"\n\n, OCR Text: {ocr_text}"

        if not face_status:
            recognized_faces = api_client.recognize_faces(image_bytes, filename)
            if recognized_faces is not None:
                face_status = 1
                if recognized_faces:
                    known_face_names = [
                        face.get('name').replace('_', ' ').title() 
                        for face in recognized_faces 
                        if face.get('name') and face.get('name') != 'Unknown'
                    ]
                    
                    if known_face_names:
                        vips = ", ".join(known_face_names)
                        new_abstract += f", VIPs: {vips}"
        
        if o_detected_status and ocr_status and face_status:
            final_status = '4'

    except Exception as e:
        logging.error(f"Error processing document {docnumber}", exc_info=True)
        error_message = str(e)
        final_status = '99'

    finally:
        db_connector.update_document_processing_status(
            docnumber=docnumber, new_abstract=new_abstract, o_detected=o_detected_status,
            ocr=ocr_status, face=face_status, status=final_status,
            error=error_message, transcript=transcript_text
        )
        logging.info(f"Finished processing document {docnumber} with status: {final_status}")


@app.route('/process-batch', methods=['POST'])
def process_batch():
    """API endpoint to trigger the processing of a batch of documents."""
    logging.info("'/process-batch' endpoint called.")
    
    # First, log in to the DMS to get a session token
    dms_session_token = wsdl_client.dms_login()
    if not dms_session_token:
        logging.critical("Could not log into DMS. Aborting batch.")
        return jsonify({"status": "error", "message": "Failed to authenticate with DMS."}), 500

    logging.info("DMS login successful. Fetching documents from database.")
    try:
        documents = db_connector.get_documents_to_process()
        if not documents:
            logging.info("No new documents to process.")
            return jsonify({"status": "success", "message": "No new documents to process.", "processed_count": 0}), 200

        for doc in documents:
            process_document(doc, dms_session_token)
            
        logging.info(f"Successfully processed a batch of {len(documents)} documents.")
        return jsonify({"status": "success", "message": f"Processed {len(documents)} documents.", "processed_count": len(documents)}), 200

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