from flask import Flask, jsonify, request, Response, send_from_directory, stream_with_context, send_file
from flask_cors import CORS
import db_connector
import api_client
import wsdl_client
import logging
from werkzeug.serving import run_simple
import math
import os
import json
import re
from threading import Thread

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# --- AI Processing Routes ---
def process_document(doc, dms_session_token):
    """
    Processes a single document, handling its own errors and returning a dictionary
    ready for database update.
    """
    docnumber = doc['docnumber']
    logging.info(f"Starting processing for document: {docnumber}")

    # Initialize variables with data from the document
    original_abstract = doc.get('abstract') or ''
    base_abstract = original_abstract.split('\n\nCaption:')[0].strip()
    ai_abstract_parts = {}

    results = {
        "docnumber": docnumber,
        "new_abstract": original_abstract,
        "o_detected": doc.get('o_detected', 0),
        "ocr": doc.get('ocr', 0),
        "face": doc.get('face', 0),
        "transcript": '',
        "status": 1, # Default status: In Progress
        "error": '',
        "attempts": doc.get('attempts', 0) + 1
    }

    try:
        # Step 1: Fetch media and determine type
        media_bytes, filename = wsdl_client.get_image_by_docnumber(dms_session_token, docnumber)
        if not media_bytes:
            raise Exception(f"Failed to retrieve media for docnumber {docnumber} from WSDL service.")
        logging.info(f"Media for {docnumber} ({filename}) fetched successfully.")

        video_extensions = ['.mp4', '.mov', '.avi', '.mkv']
        is_video = any(filename.lower().endswith(ext) for ext in video_extensions)

        # Step 2: Execute AI workflows based on media type
        if is_video:
            video_summary = api_client.summarize_video(media_bytes, filename)
            caption_parts = []
            keywords_to_insert = []

            if video_summary.get('objects'):
                caption_parts.extend(video_summary['objects'])
                results['o_detected'] = 1 # Mark as success if objects are found
                for obj in video_summary['objects']:
                    arabic_translation = api_client.translate_text(obj)
                    keywords_to_insert.append({'english': obj, 'arabic': arabic_translation})

            if video_summary.get('faces'):
                recognized_faces = api_client.recognize_faces_from_list(video_summary['faces'])
                # Use a set to automatically handle duplicates
                unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if f.get('name') and f.get('name') != 'Unknown'}
                if unique_known_faces:
                    ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
                results['face'] = 1 # Mark as success if face analysis was run

            if video_summary.get('transcript'):
                tokenized_json_str = api_client.tokenize_transcript(video_summary['transcript'])
                english_tags = []
                try:
                    # "Happy Path": The response is valid JSON
                    tokenized_data = json.loads(tokenized_json_str)
                    english_tags = tokenized_data.get('english_tags', [])

                except json.JSONDecodeError:
                    # "Smart Fallback": The response is broken, so we parse the raw string
                    logging.warning(f"Could not decode tokenized transcript for {docnumber} as JSON. Attempting to salvage tags.")

                    # Use regex to find the content within "english_tags": [...]
                    english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str, re.IGNORECASE)
                    if english_match:
                        raw_english = english_match.group(1)
                        # Clean the extracted string and split into a list
                        english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if tag.strip()]
                        logging.info(f"Salvaged English tags: {english_tags}")
                    else:
                        logging.warning(f"Could not salvage any English tags from the malformed response for {docnumber}.")

                if english_tags:
                    caption_parts.extend(english_tags) # Add to abstract
                    # Translate each tag for keyword insertion
                    for tag in english_tags:
                        arabic_translation = api_client.translate_text(tag)
                        keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})


            if keywords_to_insert:
                db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)

            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

            results['ocr'] = 1 # OCR is not applicable for videos, so mark as complete

        else: # Is an image
            captions = api_client.get_captions(media_bytes, filename)
            if captions:
                ai_abstract_parts['CAPTION'] = captions
                results['o_detected'] = 1

            ocr_text = api_client.get_ocr_text(media_bytes, filename)
            if ocr_text:
                ai_abstract_parts['OCR'] = ocr_text
                results['ocr'] = 1

            recognized_faces = api_client.recognize_faces(media_bytes, filename)
            if recognized_faces:
                # Use a set to automatically handle duplicates
                unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if f.get('name') and f.get('name') != 'Unknown'}
                if unique_known_faces:
                    ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
                results['face'] = 1

        # Step 3: Assemble the final abstract and set success status
        final_abstract_parts = [base_abstract]
        if ai_abstract_parts.get('CAPTION'): final_abstract_parts.append(f"Caption: {ai_abstract_parts['CAPTION']} ")
        if ai_abstract_parts.get('OCR'): final_abstract_parts.append(f"OCR: {ai_abstract_parts['OCR']} ")
        if ai_abstract_parts.get('VIPS'): final_abstract_parts.append(f"VIPs: {ai_abstract_parts['VIPS']}")

        results['new_abstract'] = "\n\n".join(filter(None, final_abstract_parts))

        # Final status is success only if all relevant parts succeeded
        if results['o_detected'] and results['ocr'] and results['face']:
            results['status'] = 3 # Success

    except Exception as e:
        # If any error occurs, log it and set the error status
        logging.error(f"Error processing document {docnumber}", exc_info=True)
        results['status'] = 2 # Error status
        results['error'] = str(e)

    return results
@app.route('/process-batch', methods=['POST'])
def process_batch():
    """API endpoint to trigger the processing of a batch of documents."""
    logging.info("'/process-batch' endpoint called.")

    dms_session_token = db_connector.dms_login()
    if not dms_session_token:
        logging.critical("Could not log into DMS. Aborting batch.")
        return jsonify({"status": "error", "message": "Failed to authenticate with DMS."}), 500

    logging.info("DMS login successful. Fetching documents from database.")
    documents = db_connector.get_documents_to_process()
    if not documents:
        logging.info("No new documents to process.")
        return jsonify({"status": "success", "message": "No new documents to process.", "processed_count": 0}), 200

    processed_count = 0
    for doc in documents:
        result_data = process_document(doc, dms_session_token)

        # --- New Timeout Logic ---
        # Run the database update in a thread so it can't freeze the main loop
        db_thread = Thread(
            target=db_connector.update_document_processing_status,
            kwargs=result_data
        )
        db_thread.start()
        db_thread.join(timeout=20.0) # Wait a maximum of 20 seconds for the DB to respond

        if db_thread.is_alive():
            # If the thread is still running after 20 seconds, it's hung.
            logging.critical(f"DATABASE HANG: The update for doc {doc['docnumber']} timed out. Skipping to next document.")
        else:
            # If the thread finished, the update was successful.
            if result_data['status'] == 3: # Success
                processed_count += 1
                logging.info(f"Successfully processed and updated DB for document {doc['docnumber']}.")
            else:
                logging.warning(f"Failed to process document {doc['docnumber']}. Error has been logged to the database.")

    logging.info(f"Batch finished. Successfully processed {processed_count} out of {len(documents)} documents.")
    return jsonify({"status": "success", "message": f"Processed {processed_count} documents.", "processed_count": processed_count}), 200

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

    image_data, _ = wsdl_client.get_image_by_docnumber(dst, doc_id)
    if image_data:
        return Response(bytes(image_data), mimetype='image/jpeg')
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