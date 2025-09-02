from flask import Flask, jsonify, request, Response, send_from_directory, stream_with_context, send_file
from flask_cors import CORS
import db_connector
import api_client
import wsdl_client
import logging
from werkzeug.serving import run_simple
import re
import math
import os

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

# --- AI Processing Routes ---
def process_document(doc, dms_session_token):
    """
    Processes a single document using the provided DMS session token.
    """
    docnumber = doc['docnumber']
    logging.info(f"Processing document: {docnumber}")

    original_abstract = doc.get('abstract') or ''
    
    # Safely extract existing AI parts from the abstract
    ai_abstract_parts = {}
    parts = re.split(r'\n\n(Caption:|OCR:|VIPs:)', original_abstract, flags=re.IGNORECASE)
    base_abstract = parts[0].strip()
    if len(parts) > 1:
        it = iter(parts[1:])
        for tag in it:
            key = tag.strip().replace(':', '').upper()
            value = next(it, '').strip()
            ai_abstract_parts[key] = value

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
        if ai_abstract_parts.get('CAPTION'): final_abstract_parts.append(f"Caption: {ai_abstract_parts['CAPTION']}")
        if ai_abstract_parts.get('OCR'): final_abstract_parts.append(f"OCR: {ai_abstract_parts['OCR']}")
        if ai_abstract_parts.get('VIPS'): final_abstract_parts.append(f"VIPs: {ai_abstract_parts['VIPS']}")

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

    dms_session_token = db_connector.dms_login()
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
                process_document(doc, dms_session_token)
                processed_count += 1
            except Exception as e:
                docnumber = doc.get('docnumber', 'N/A')
                logging.error(f"A critical error occurred while processing docnumber {docnumber}. Skipping.", exc_info=True)

        logging.info(f"Successfully processed a batch of {processed_count} out of {len(documents)} documents.")
        return jsonify({"status": "success", "message": f"Processed {processed_count} documents.", "processed_count": processed_count}), 200

    except Exception as e:
        logging.error("An unhandled error occurred in the /process-batch endpoint.", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# --- Viewer API Routes ---
@app.route('/api/documents')
def api_get_documents():
    """Handles fetching documents for the frontend viewer."""
    page = request.args.get('page', 1, type=int)
    search_term = request.args.get('search', None, type=str)
    date_from = request.args.get('date_from', None, type=str)
    date_to = request.args.get('date_to', None, type=str)
    persons = request.args.get('persons', None, type=str)
    person_condition = request.args.get('person_condition', 'any', type=str)

    page_size = 20
    documents, total_rows = db_connector.fetch_documents_from_oracle(
        page=page,
        page_size=page_size,
        search_term=search_term,
        date_from=date_from,
        date_to=date_to,
        persons=persons,
        person_condition=person_condition
    )

    total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1

    return jsonify({
        "documents": documents, "page": page,
        "total_pages": total_pages, "total_documents": total_rows
    })

@app.route('/api/image/<doc_id>')
def api_get_image(doc_id):
    """Serves the full image content for a given document ID."""
    dst = db_connector.dms_login()
    if not dst: return jsonify({'error': 'DMS login failed.'}), 500
    
    image_bytes = db_connector.get_media_content_from_dms(dst, doc_id)
    if image_bytes:
        return Response(image_bytes, mimetype='image/jpeg')
    return jsonify({'error': 'Image not found in EDMS.'}), 404

@app.route('/api/video/<doc_id>')
def api_get_video(doc_id):
    """
    Handles video requests using a hybrid stream-through cache model.
    """
    dst = db_connector.dms_login()
    if not dst:
        return jsonify({'error': 'DMS login failed.'}), 500

    # Determine the expected path of the cached file
    original_filename, media_type, file_ext = db_connector.get_media_info_from_dms(dst, doc_id)
    if not original_filename:
        return jsonify({'error': 'Video metadata not found in EDMS.'}), 404
    
    if media_type != 'video':
        return jsonify({'error': 'Requested document is not a video.'}), 400

    if not file_ext: file_ext = '.mp4' # Default extension
    cached_video_path = os.path.join(db_connector.video_cache_dir, f"{doc_id}{file_ext}")
    
    # If the file is already cached, serve it directly and quickly.
    if os.path.exists(cached_video_path):
        logging.info(f"Serving video {doc_id} from cache.")
        return send_file(cached_video_path, as_attachment=False)

    # If not cached, initiate the stream-and-cache process.
    logging.info(f"Video {doc_id} not in cache. Streaming from DMS and caching simultaneously.")
    stream_details = db_connector.get_dms_stream_details(dst, doc_id)
    if not stream_details:
        return jsonify({'error': 'Could not open stream from DMS.'}), 500

    # Create the generator that will stream to the user and save to a file
    stream_generator = db_connector.stream_and_cache_generator(
        obj_client=stream_details['obj_client'], 
        stream_id=stream_details['stream_id'], 
        content_id=stream_details['content_id'],
        final_cache_path=cached_video_path
    )
    
    # Return a streaming response
    return Response(stream_with_context(stream_generator), mimetype="video/mp4")


@app.route('/cache/<path:filename>')
def serve_cached_thumbnail(filename):
    """Serves cached thumbnail images."""
    return send_from_directory(db_connector.thumbnail_cache_dir, filename)

@app.route('/api/clear_cache', methods=['POST'])
def api_clear_cache():
    """Clears the thumbnail and video cache."""
    try:
        db_connector.clear_thumbnail_cache()
        db_connector.clear_video_cache()
        return jsonify({"message": "All caches cleared successfully."})
    except Exception as e:
        return jsonify({"error": f"Failed to clear cache: {e}"}), 500

@app.route('/api/update_abstract', methods=['POST'])
def api_update_abstract():
    """Updates a document's abstract with VIP names."""
    data = request.get_json()
    doc_id = data.get('doc_id')
    names = data.get('names')
    if not doc_id or not isinstance(names, list):
        return jsonify({'error': 'Invalid data provided.'}), 400
    success, message = db_connector.update_abstract_with_vips(doc_id, names)
    if success:
        return jsonify({'message': message})
    else:
        return jsonify({'error': message}), 500

@app.route('/api/add_person', methods=['POST'])
def api_add_person():
    """Adds a person to the lookup table."""
    data = request.get_json()
    name = data.get('name')
    if not name:
        return jsonify({'error': 'Invalid data provided.'}), 400
    success, message = db_connector.add_person_to_lkp(name)
    if success:
        return jsonify({'message': message})
    else:
        return jsonify({'error': message}), 500

@app.route('/api/persons')
def api_get_persons():
    """Fetches people from the lookup table for autocomplete."""
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '', type=str)
    persons, total_rows = db_connector.fetch_lkp_persons(page=page, search=search)
    return jsonify({
        'options': persons,
        'hasMore': (page * 20) < total_rows
    })


if __name__ == '__main__':
    run_simple(
        '127.0.0.1',
        5000,
        app,
        use_reloader=False,
        use_debugger=True,
        threaded=True,
        exclude_patterns=['*venv*', '*__pycache__*']
    )