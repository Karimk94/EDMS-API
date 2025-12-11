from flask import Flask, jsonify, request, Response, send_from_directory, stream_with_context, send_file, session, abort
from flask_cors import CORS
import db_connector
import api_client
import wsdl_client
import logging
from waitress import serve
from werkzeug.utils import secure_filename
import math
import os
import json
import re
from threading import Thread
import mimetypes
from functools import wraps
import io
from datetime import datetime, timedelta
from moviepy.editor import VideoFileClip, ImageClip, CompositeVideoClip
from PIL import Image, ImageDraw, ImageFont
import fitz
import tempfile

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=60)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
CORS(app, supports_credentials=True, resources={r"/api/*": {"origins": "*"}})

# --- Security Decorator ---
def editor_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session['user'].get('security_level') != 'Editor':
            abort(403)  # Forbidden
        return f(*args, **kwargs)

    return decorated_function

@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    dst = wsdl_client.dms_user_login(username, password)

    if dst:
        # Fetch full user details including security level, lang, and theme
        user_details = db_connector.get_user_details(username)

        if user_details is None or 'security_level' not in user_details:
            logging.warning(
                f"User '{username}' authenticated via DMS but has no security level assigned in middleware DB.")
            return jsonify({"error": "User not authorized for this application"}), 401

        # Store full user details in session
        session['user'] = user_details
        # session['dst'] = dst
        session.permanent = True

        # logging.info(f"User '{username}' logged in successfully with security level '{user_details.get('security_level')}' and theme '{user_details.get('theme')}'.")
        return jsonify({"message": "Login successful", "user": user_details}), 200
    else:
        logging.warning(f"DMS login failed for user '{username}'.")
        return jsonify({"error": "Invalid DMS credentials"}), 401

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    username = session.get('user', {}).get('username', 'Unknown user')
    session.pop('user', None)
    logging.info(f"User '{username}' logged out.")
    return jsonify({"message": "Logout successful"}), 200

@app.route('/api/auth/user', methods=['GET'])
def get_user():
    user_session = session.get('user')
    if user_session and 'username' in user_session:
        # Re-fetch from DB to ensure details are fresh
        user_details = db_connector.get_user_details(user_session['username'])
        if user_details:
            session['user'] = user_details  # Update session
            return jsonify({'user': user_details}), 200
        else:
            # User was in session but not in DB? Log them out.
            session.pop('user', None)
            # session.pop('dst', None)
            return jsonify({'error': 'User not found'}), 401
    else:
        return jsonify({'error': 'Not authenticated'}), 401

@app.route('/api/user/language', methods=['PUT'])
def update_user_language():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    lang = data.get('lang')

    if lang not in ['en', 'ar']:
        return jsonify({"error": "Invalid language"}), 400

    success = db_connector.update_user_language(session['user']['username'], lang)

    if success:
        # Update session
        user_session = session['user']
        user_session['lang'] = lang
        session['user'] = user_session
        return jsonify({"message": "Language updated"}), 200
    else:
        return jsonify({"error": "Failed to update language"}), 500

@app.route('/api/user/theme', methods=['PUT'])
def api_update_user_theme():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    theme = data.get('theme')

    if theme not in ['light', 'dark']:
        return jsonify({"error": "Invalid theme"}), 400

    username = session['user']['username']
    success = db_connector.update_user_theme(username, theme)

    if success:
        # Update session
        user_session = session['user']
        user_session['theme'] = theme
        session['user'] = user_session
        # logging.info(f"User '{username}' updated theme to '{theme}'.")
        return jsonify({"message": "Theme updated"}), 200
    else:
        logging.error(f"Failed to update theme for user '{username}'.")
        return jsonify({"error": "Failed to update theme"}), 500

# --- Watermarking Functions ---
def apply_watermark_to_image(image_bytes, watermark_text):
    """Applies the provided text watermark to the bottom-right of an image."""
    try:
        base_image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
        width, height = base_image.size

        # Create a transparent layer for text
        txt_layer = Image.new('RGBA', base_image.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(txt_layer)

        # Calculate font size relative to image width
        font_size = max(10, int(width * 0.015))

        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except IOError:
            font = ImageFont.load_default()

        # Get text size
        if hasattr(draw, 'textbbox'):
            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        else:
            text_width, text_height = draw.textsize(watermark_text, font=font)

        # Position: Bottom Right with padding
        padding_x = 20
        padding_y = 20
        x = width - text_width - padding_x
        y = height - text_height - padding_y

        # Draw Shadow (Black)
        draw.text((x + 1, y + 1), watermark_text, font=font, fill=(0, 0, 0, 160))
        # Draw Main Text (White)
        draw.text((x, y), watermark_text, font=font, fill=(255, 255, 255, 200))

        # Composite
        watermarked = Image.alpha_composite(base_image, txt_layer)

        output_buffer = io.BytesIO()
        watermarked.convert("RGB").save(output_buffer, format="JPEG", quality=95)
        return output_buffer.getvalue(), "image/jpeg"
    except Exception as e:
        logging.error(f"Error watermarking image: {e}", exc_info=True)
        return image_bytes, "image/jpeg"

def apply_watermark_to_pdf(pdf_bytes, watermark_text):
    """Applies the provided text watermark to the bottom-right of every PDF page."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        for page in doc:
            rect = page.rect
            fontsize = 8

            # Estimate text width
            text_width = fitz.get_text_length(watermark_text, fontname="helv", fontsize=fontsize)

            x = rect.width - text_width - 20
            y = rect.height - 20

            # Insert Shadow (Black)
            page.insert_text((x + 0.5, y + 0.5), watermark_text, fontsize=fontsize, fontname="helv", color=(0, 0, 0),
                             fill_opacity=0.5)

            # Insert Main Text (White)
            page.insert_text((x, y), watermark_text, fontsize=fontsize, fontname="helv", color=(1, 1, 1),
                             fill_opacity=0.8)

        output_buffer = io.BytesIO()
        doc.save(output_buffer)
        return output_buffer.getvalue(), "application/pdf"
    except Exception as e:
        logging.error(f"Error watermarking PDF: {e}", exc_info=True)
        return pdf_bytes, "application/pdf"

def apply_watermark_to_video(video_bytes, watermark_text, filename):
    """
    Applies text watermark to the ENTIRE duration of the video.
    Uses Pillow to generate the text image to avoid ImageMagick dependencies.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_video_path = os.path.join(temp_dir, filename)
            output_video_path = os.path.join(temp_dir, f"watermarked_{filename}")
            watermark_img_path = os.path.join(temp_dir, "watermark.png")

            # Write original video to temp file
            with open(temp_video_path, 'wb') as f:
                f.write(video_bytes)

            video_clip = None
            watermark_clip = None
            final_clip = None

            try:
                video_clip = VideoFileClip(temp_video_path)

                # --- Generate Watermark Image using Pillow (Avoids ImageMagick) ---
                font_size = max(20, int(video_clip.w * 0.025))

                try:
                    font = ImageFont.truetype("arial.ttf", font_size)
                except IOError:
                    font = ImageFont.load_default()

                # Measure text size
                dummy_draw = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
                if hasattr(dummy_draw, 'textbbox'):
                    bbox = dummy_draw.textbbox((0, 0), watermark_text, font=font)
                    text_width, text_height = bbox[2] - bbox[0], bbox[3] - bbox[1]
                else:
                    text_width, text_height = dummy_draw.textsize(watermark_text, font=font)

                # Create transparent image for text
                img_width = text_width + 20
                img_height = text_height + 20
                txt_img = Image.new('RGBA', (img_width, img_height), (255, 255, 255, 0))
                draw = ImageDraw.Draw(txt_img)

                # Draw Text (White with Black Outline/Shadow)
                draw.text((2, 2), watermark_text, font=font, fill=(0, 0, 0, 160))
                draw.text((0, 0), watermark_text, font=font, fill=(255, 255, 255, 200))

                # Save text image
                txt_img.save(watermark_img_path, "PNG")

                # --- Create MoviePy Clips ---
                # Set duration to match video duration (Full Video Watermark)
                watermark_clip = (
                    ImageClip(watermark_img_path)
                    .set_duration(video_clip.duration)
                    .set_position(('right', 'bottom'))
                    .set_opacity(0.8)
                    .margin(right=20, bottom=20, opacity=0)
                )

                final_clip = CompositeVideoClip([video_clip, watermark_clip])

                # Write to a file (temp) using ultrafast preset
                final_clip.write_videofile(
                    output_video_path,
                    codec='libx264',
                    audio_codec='aac',
                    temp_audiofile=os.path.join(temp_dir, 'temp-audio.m4a'),
                    remove_temp=True,
                    verbose=False,
                    logger=None,
                    preset='ultrafast'
                )

                # Read the result back into memory
                with open(output_video_path, 'rb') as f:
                    return f.read(), "video/mp4"

            finally:
                # CRITICAL: Close clips explicitly to release file handles
                if final_clip:
                    final_clip.close()
                if watermark_clip:
                    watermark_clip.close()
                if video_clip:
                    video_clip.close()

    except Exception as e:
        logging.error(f"Error watermarking video: {e}", exc_info=True)
        return video_bytes, "video/mp4"

# --- AI Processing Routes ---
def process_document(doc, dms_session_token):
    # ... (Existing process_document logic unchanged)
    """
    Processes a single document, handling its own errors and returning a dictionary
    ready for database update.
    """
    docnumber = doc['docnumber']
    logging.info(f"Starting processing for document: {docnumber}")

    # Initialize variables with data from the document
    original_abstract = doc.get('abstract') or ''
    base_abstract = re.split(r'\s*\n*\s*Caption:', original_abstract, 1, flags=re.IGNORECASE)[0].strip()
    ai_abstract_parts = {}

    results = {
        "docnumber": docnumber,
        "new_abstract": original_abstract,
        "o_detected": doc.get('o_detected', 0),
        "ocr": doc.get('ocr', 0),
        "face": doc.get('face', 0),
        "transcript": '',  # Assuming transcript is not stored long-term in DB separately
        "status": 1,  # Default status: In Progress
        "error": '',
        "attempts": doc.get('attempts', 0) + 1
    }

    try:
        # Step 1: Fetch media and determine type from DMS
        media_bytes, filename = wsdl_client.get_image_by_docnumber(dms_session_token,
                                                                   docnumber)  # Reusing this function for all types
        if not media_bytes:
            raise Exception(f"Failed to retrieve media for docnumber {docnumber} from WSDL service.")

        # Re-fetch metadata reliably here
        _, media_type, _ = db_connector.get_media_info_from_dms(dms_session_token, docnumber)
        logging.info(f"Media for {docnumber} ({filename}) fetched successfully. Type: {media_type}")

        # Step 2: Execute AI workflows based on media type
        if media_type == 'video':
            video_summary = api_client.summarize_video(media_bytes, filename)
            caption_parts = []
            keywords_to_insert = []

            if video_summary.get('objects'):
                caption_parts.extend(video_summary['objects'])
                results['o_detected'] = 1  # Mark as success if objects are found
                for obj in video_summary['objects']:
                    arabic_translation = api_client.translate_text(obj)
                    keywords_to_insert.append({'english': obj, 'arabic': arabic_translation})

            if video_summary.get('faces'):
                recognized_faces = api_client.recognize_faces_from_list(video_summary['faces'])
                # Use a set to automatically handle duplicates
                unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if
                                      f.get('name') and f.get('name') != 'Unknown'}
                if unique_known_faces:
                    ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
                results['face'] = 1  # Mark as success if face analysis was run

            if video_summary.get('transcript'):
                tokenized_json_str = api_client.tokenize_transcript(video_summary['transcript'])
                english_tags = []
                try:
                    # "Happy Path": The response is valid JSON
                    tokenized_data = json.loads(tokenized_json_str)
                    english_tags = tokenized_data.get('english_tags', [])

                except json.JSONDecodeError:
                    # "Smart Fallback": The response is broken, so we parse the raw string
                    logging.warning(
                        f"Could not decode tokenized transcript for {docnumber} as JSON. Attempting to salvage tags.")

                    # Use regex to find the content within "english_tags": [...]
                    english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str,
                                              re.IGNORECASE)
                    if english_match:
                        raw_english = english_match.group(1)
                        # Clean the extracted string and split into a list
                        english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if
                                        tag.strip()]
                        logging.info(f"Salvaged English tags: {english_tags}")
                    else:
                        logging.warning(
                            f"Could not salvage any English tags from the malformed response for {docnumber}.")

                if english_tags:
                    caption_parts.extend(english_tags)  # Add to abstract
                    # Translate each tag for keyword insertion
                    for tag in english_tags:
                        arabic_translation = api_client.translate_text(tag)
                        keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})

            # --- Process OCR texts from video ---
            if video_summary.get('ocr_texts'):
                results['ocr'] = 1  # Mark OCR as successful since we have data
                for ocr_text in video_summary['ocr_texts']:
                    if not ocr_text: continue

                    tokenized_json_str = api_client.tokenize_transcript(ocr_text)
                    english_tags = []
                    try:
                        tokenized_data = json.loads(tokenized_json_str)
                        english_tags = tokenized_data.get('english_tags', [])
                    except json.JSONDecodeError:
                        logging.warning(
                            f"Could not decode tokenized video OCR for {docnumber} as JSON. Attempting to salvage tags.")
                        english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str,
                                                  re.IGNORECASE)
                        if english_match:
                            raw_english = english_match.group(1)
                            english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if
                                            tag.strip()]
                            logging.info(f"Salvaged English tags from video OCR: {english_tags}")
                        else:
                            logging.warning(
                                f"Could not salvage any English tags from the malformed video OCR response for {docnumber}.")

                    if english_tags:
                        caption_parts.extend(english_tags)
                        for tag in english_tags:
                            arabic_translation = api_client.translate_text(tag)
                            keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            else:
                # If video has no text, still mark as completed for processing purposes.
                results['ocr'] = 1
            # --- END OF OCR LOGIC ---

            if keywords_to_insert:
                db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)

            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

        elif media_type == 'pdf':
            logging.info(f"Processing PDF document: {docnumber}")
            keywords_to_insert = []
            caption_parts = []

            # Perform OCR on the PDF
            ocr_text = api_client.get_ocr_text_from_pdf(media_bytes, filename)
            if ocr_text:
                results['ocr'] = 1

                # Tokenize the OCR text to get keywords
                tokenized_json_str = api_client.tokenize_transcript(ocr_text)
                english_tags = []
                try:
                    tokenized_data = json.loads(tokenized_json_str)
                    english_tags = tokenized_data.get('english_tags', [])
                except json.JSONDecodeError:
                    logging.warning(
                        f"Could not decode tokenized transcript for PDF {docnumber} as JSON. Attempting to salvage tags.")
                    english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str,
                                              re.IGNORECASE)
                    if english_match:
                        raw_english = english_match.group(1)
                        english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if
                                        tag.strip()]
                        logging.info(f"Salvaged English tags from PDF OCR: {english_tags}")
                    else:
                        logging.warning(
                            f"Could not salvage any English tags from the malformed response for PDF {docnumber}.")

                if english_tags:
                    caption_parts.extend(english_tags)
                    # Translate each tag for keyword insertion
                    for tag in english_tags:
                        arabic_translation = api_client.translate_text(tag)
                        keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            else:  # If no OCR text found in PDF
                results['ocr'] = 1  # Still mark as OCR processed

            if keywords_to_insert:
                db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)

            # As per requirements, these steps are considered complete for PDFs
            results['o_detected'] = 1
            results['face'] = 1

            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

        else:  # Is an image
            keywords_to_insert = []
            result = api_client.get_captions(media_bytes, filename)
            if result:
                raw_caption = result.get('caption', '')
                # Clean the stuttering words using the new helper function
                cleaned_caption = clean_repeated_words(raw_caption)
                # Assign the cleaned caption to the abstract
                ai_abstract_parts['CAPTION'] = cleaned_caption
                results['o_detected'] = 1
                tags = result.get('tags', [])
                for tag in tags:
                    arabic_translation = api_client.translate_text(tag)
                    keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            else:  # No caption result
                results['o_detected'] = 0  # Mark as not detected if service failed/returned empty

            ocr_text = api_client.get_ocr_text(media_bytes, filename)

            if ocr_text:  # Check if OCR actually returned text
                results['ocr'] = 1
                ai_abstract_parts['OCR'] = ocr_text
            else:
                results['ocr'] = 1  # Mark OCR as processed even if no text found

            recognized_faces = api_client.recognize_faces(media_bytes, filename)
            if recognized_faces is not None:  # Check if recognition ran (even if no faces found)
                results['face'] = 1
                # Use a set to automatically handle duplicates
                unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if
                                      f.get('name') and f.get('name') != 'Unknown'}
                if unique_known_faces:
                    ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
            else:  # Face recognition service failed
                results['face'] = 0

            if keywords_to_insert:
                db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)

        # Step 3: Assemble the final abstract
        final_abstract_parts = [base_abstract]
        if ai_abstract_parts.get('CAPTION'): final_abstract_parts.append(f"Caption: {ai_abstract_parts['CAPTION']} ")
        if ai_abstract_parts.get('OCR'): final_abstract_parts.append(f"OCR: {ai_abstract_parts['OCR']} ")
        if ai_abstract_parts.get('VIPS'): final_abstract_parts.append(f"VIPs: {ai_abstract_parts['VIPS']}")

        # Only update if there are AI parts to add
        if len(ai_abstract_parts) > 0:
            results['new_abstract'] = "\n\n".join(filter(None, final_abstract_parts)).strip()
        else:
            results['new_abstract'] = base_abstract  # Keep original if no AI data

        # Step 4: Set success status based on media type and results
        if media_type == 'pdf':
            results['status'] = 3 if results['ocr'] == 1 else results['status']  # Success for PDF is just OCR attempted
        else:  # Image or Video
            # Success requires all *attempted* steps to have run (indicated by 1)
            if results['o_detected'] == 1 and results['ocr'] == 1 and results['face'] == 1:
                results['status'] = 3
            else:
                # Check if any step failed (returned 0 after being attempted)
                if results['o_detected'] == 0 or results['ocr'] == 0 or results['face'] == 0:
                    logging.warning(f"One or more AI steps failed for {docnumber}. Status not set to Success.")
                    # Keep status as 1 (In Progress) or let error handling set it to 2

    except Exception as e:
        # If any error occurs during the main processing, log it and set the error status
        logging.error(f"Error processing document {docnumber}: {e}", exc_info=True)
        results['status'] = 2  # Error status
        results['error'] = str(e)[:2000]  # Truncate error message if needed

    # Final check before returning results
    if results['status'] != 3 and results['status'] != 2:
        # If it's not Success or Error, but attempts are high, mark as Error
        if results['attempts'] >= 3:
            logging.warning(
                f"Document {docnumber} reached max attempts ({results['attempts']}) without full success. Marking as Error.")
            results['status'] = 2
            results['error'] = results['error'] or "Max processing attempts reached without full success."
        else:
            # Otherwise, keep it as In Progress (status 1)
            results['status'] = 1

    return results

@app.route('/process-batch', methods=['POST'])
def process_batch():
    """API endpoint to trigger the processing of a batch of documents."""
    logging.info("'/process-batch' endpoint called.")

    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        logging.critical("Could not log into DMS. Aborting batch.")
        return jsonify({"status": "error", "message": "Failed to authenticate with DMS."}), 500

    logging.info("DMS login successful. Fetching documents from database.")
    documents = db_connector.get_documents_to_process()
    if not documents:
        logging.info("No new documents to process.")
        return jsonify(
            {"status": "success", "message": "No new documents to process.", "processed_count": 0}), 200

    processed_count = 0
    for doc in documents:
        result_data = process_document(doc, dms_session_token)

        # Run the database update in a thread so it can't freeze the main loop
        db_thread = Thread(
            target=db_connector.update_document_processing_status,
            kwargs=result_data
        )
        db_thread.start()
        db_thread.join(timeout=30.0)  # Increased timeout

        if db_thread.is_alive():
            # If the thread is still running after 30 seconds, it's hung.
            logging.critical(
                f"DATABASE HANG: The update for doc {doc['docnumber']} timed out. Skipping to next document.")
        else:
            # If the thread finished, check the result status
            if result_data['status'] == 3:  # Success
                processed_count += 1
                logging.info(f"Successfully processed and updated DB for document {doc['docnumber']}.")
            elif result_data['status'] == 2:  # Error
                logging.error(f"Failed to process document {doc['docnumber']}. Error logged: {result_data['error']}")
            else:  # Still In Progress (status 1)
                logging.warning(
                    f"Processing for document {doc['docnumber']} not fully complete in this run (Status: {result_data['status']}). Will retry on next batch if attempts < 3.")

    logging.info(f"Batch finished. Successfully processed {processed_count} out of {len(documents)} documents.")
    return jsonify(
        {"status": "success", "message": f"Processed {processed_count} documents.",
         "processed_count": processed_count}), 200

@app.route('/api/upload_document', methods=['POST'])
def api_upload_document():
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "error": "No selected file"}), 400

    file_stream = file.stream
    # Read the content once to pass to EXIF and WSDL upload
    file_bytes = file_stream.read()
    file_stream.seek(0)  # Reset stream pointer for the actual upload

    # --- Date Taken Logic ---
    doc_date_taken = None
    date_taken_str = request.form.get('date_taken')  # Expected format: YYYY-MM-DD HH:MM:SS
    if date_taken_str:
        try:
            doc_date_taken = datetime.strptime(date_taken_str, '%Y-%m-%d %H:%M:%S')
            logging.info(f"Using date_taken from form data: {doc_date_taken}")
        except ValueError:
            logging.warning(f"Could not parse date_taken '{date_taken_str}' from form.")
    else:
        # Try extracting from EXIF if not provided in form
        doc_date_taken = db_connector.get_exif_date(io.BytesIO(file_bytes))
        logging.info(f"Using extracted EXIF date: {doc_date_taken}")
    # --- End Date Taken Logic ---

    # Secure original filename, get extension
    original_filename = secure_filename(file.filename)
    file_extension = os.path.splitext(original_filename)[1].lstrip('.').upper()

    app_id = db_connector.get_app_id_from_extension(file_extension)
    if not app_id:
        logging.warning(f"Could not find APP_ID for extension: {file_extension}. Defaulting to 'UNKNOWN'.")
        app_id = 'UNKNOWN'

    # --- Docname Logic ---
    docname = request.form.get('docname')
    if not docname or not docname.strip():
        docname = os.path.splitext(original_filename)[0]  # Fallback to original filename base
    else:
        docname = docname.strip()  # Use stripped name from form
        name_base, name_ext = os.path.splitext(docname)
        if name_ext.lstrip('.').upper() == file_extension:
            docname = name_base
    logging.info(f"Using docname: {docname}")
    # --- End Docname Logic ---

    abstract = request.form.get('abstract', 'Uploaded via EDMS Viewer')  # Keep default if needed

    # --- Event ID ---
    event_id_str = request.form.get('event_id')
    event_id = None
    if event_id_str:
        try:
            event_id = int(event_id_str)
            logging.info(f"Event ID from form: {event_id}")
        except ValueError:
            logging.warning(f"Invalid event_id '{event_id_str}' received from form.")
    # --- End Event ID ---

    logging.info(f"Upload request received for file: {original_filename}. Mapped to APP_ID: {app_id}")

    dst = wsdl_client.dms_system_login()
    if not dst:
        return jsonify({"success": False, "error": "Failed to authenticate with DMS."}), 500

    metadata = {
        "docname": docname,
        "abstract": abstract,
        "app_id": app_id,
        "filename": original_filename,
        "doc_date": doc_date_taken,
        "event_id": event_id  # Pass event_id to wsdl client
    }

    # Pass the original file stream (reset pointer) for the actual upload
    new_doc_number = wsdl_client.upload_document_to_dms(dst, file_stream, metadata)

    if new_doc_number:
        logging.info(f"Successfully uploaded {original_filename} as docnumber {new_doc_number}.")
        # Optionally trigger immediate processing for the uploaded doc
        # process_single_doc_async(new_doc_number) # Needs implementation
        return jsonify({"success": True, "docnumber": new_doc_number, "filename": original_filename})
    else:
        logging.error(f"Failed to upload {original_filename} to DMS.")
        return jsonify({"success": False, "error": "Failed to upload file to DMS."}), 500

@app.route('/api/process_uploaded_documents', methods=['POST'])
def api_process_uploaded_documents():
    data = request.get_json()
    docnumbers = data.get('docnumbers')

    if not docnumbers or not isinstance(docnumbers, list):
        return jsonify(
            {"status": "error", "message": "Invalid data provided. 'docnumbers' list is required."}), 400

    logging.info(f"Processing request for docnumbers: {docnumbers}")

    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        logging.critical("Could not log into DMS for processing. Aborting.")
        return jsonify({"status": "error", "message": "Failed to authenticate with DMS."}), 500

    results = {"processed": [], "failed": [], "in_progress": []}

    docs_to_process = db_connector.get_specific_documents_for_processing(docnumbers)

    for doc in docs_to_process:
        result_data = process_document(doc, dms_session_token)

        db_thread = Thread(
            target=db_connector.update_document_processing_status,
            kwargs=result_data
        )
        db_thread.start()
        db_thread.join(timeout=30.0)  # Increased timeout

        if db_thread.is_alive():
            logging.critical(f"DATABASE HANG: The update for doc {doc['docnumber']} timed out.")
            results["failed"].append(doc['docnumber'])
        else:
            if result_data['status'] == 3:  # Success
                results["processed"].append(doc['docnumber'])
                logging.info(f"Successfully processed uploaded doc {doc['docnumber']}.")
            elif result_data['status'] == 2:  # Error
                results["failed"].append(doc['docnumber'])
                logging.error(f"Failed to process uploaded doc {doc['docnumber']}. Error: {result_data.get('error')}")
            else:  # Still In Progress (status 1)
                results["in_progress"].append(doc['docnumber'])
                logging.warning(f"Processing for uploaded doc {doc['docnumber']} not fully complete.")

    return jsonify(results), 200

def clean_repeated_words(text):
    """Removes consecutive repeated words from a string, keeping the last one's punctuation."""
    if not text:
        return ""
    words = text.split()
    if not words:
        return ""

    result_words = [words[0]]
    for i in range(1, len(words)):
        # Normalize current word and the last word in the result for comparison
        # Keep case for comparison now
        current_word_norm = re.sub(r'[^\w]', '', words[i])
        last_result_word_norm = re.sub(r'[^\w]', '', result_words[-1])

        # Check that the normalized words are not empty and are identical (case-sensitive)
        if current_word_norm and current_word_norm == last_result_word_norm:
            # Overwrite the last word with the current one to keep the punctuation of the final word
            result_words[-1] = words[i]
        else:
            # It's a new word, so append it
            result_words.append(words[i])

    return " ".join(result_words)

# --- Viewer API Routes ---
@app.route('/api/documents')
def api_get_documents():
    """Handles fetching documents for the frontend viewer, including full memory view."""
    try:
        user = session.get('user')
        username = user.get('username') if user else None
        security_level = user.get('security_level', 'Viewer') if user else 'Viewer'

        app_source = request.headers.get('X-App-Source', 'unknown')

        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('pageSize', 20, type=int)
        search_term = request.args.get('search', None, type=str)
        date_from = request.args.get('date_from', None, type=str)
        date_to = request.args.get('date_to', None, type=str)
        persons = request.args.get('persons', None, type=str)
        person_condition = request.args.get('person_condition', 'any', type=str)
        tags = request.args.get('tags', None, type=str)
        years = request.args.get('years', None, type=str)
        sort = request.args.get('sort', None, type=str)
        lang = request.args.get('lang', 'en', type=str)

        media_type = request.args.get('media_type', None, type=str)
        scope = request.args.get('scope', None, type=str)  # Capture scope

        memory_month = request.args.get('memoryMonth', None, type=str)
        memory_day = request.args.get('memoryDay', None, type=str)

        if page < 1: page = 1
        if page_size < 1: page_size = 20
        if page_size > 100: page_size = 100

        documents, total_rows = db_connector.fetch_documents_from_oracle(
            page=page,
            page_size=page_size,
            search_term=search_term,
            date_from=date_from,
            date_to=date_to,
            persons=persons,
            person_condition=person_condition,
            tags=tags,
            years=years,
            sort=sort,
            memory_month=memory_month,
            memory_day=memory_day,
            user_id=username,
            lang=lang,
            security_level=security_level,
            app_source=app_source,
            media_type=media_type,
            scope=scope  # Pass scope to db_connector
        )

        total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1

        return jsonify({
            "documents": documents,
            "page": page,
            "total_pages": total_pages,
            "total_documents": total_rows
        })
    except Exception as e:
        logging.error(f"Error in /api/documents endpoint: {e}", exc_info=True)
        return jsonify({"error": "Failed to fetch documents due to server error."}), 500

@app.route('/api/document/<int:doc_id>/event', methods=['PUT'])
def link_document_event(doc_id):
    logging.info(f"--- HIT ROUTE: PUT /api/document/{doc_id}/event ---")
    # print(f"--- HIT ROUTE: PUT /api/document/{doc_id}/event ---")
    # -----------------------
    data = request.get_json()
    if data is None:
        logging.error(f"Request for PUT /api/document/{doc_id}/event did not contain valid JSON data.")
        return jsonify({"error": "Missing or invalid JSON data in request body."}), 400
    logging.info(f"Received data: {data}")  # Log the received data
    # --- END ADD ---

    event_id = data.get('event_id')  # Can be None to unlink

    # Add extra logging around event_id processing
    logging.info(f"Extracted event_id: {event_id} (Type: {type(event_id)})")

    if event_id is not None:
        try:
            event_id = int(event_id)
            logging.info(f"Converted event_id to int: {event_id}")
        except (ValueError, TypeError):
            logging.error(f"Invalid non-integer event_id received: {event_id}")
            return jsonify({"error": "Invalid event_id provided. Must be an integer or null."}), 400

    success, message = db_connector.link_document_to_event(doc_id, event_id)

    if success:
        return jsonify({"message": message}), 200
    else:
        # Determine appropriate status code
        status_code = 404 if "not found" in message.lower() else 500
        return jsonify({"error": message}), status_code

@app.route('/api/document/<int:doc_id>/event', methods=['GET'])
def get_document_event(doc_id):
    """Fetches the event linked to a specific document."""
    event_info = db_connector.get_event_for_document(doc_id)
    if event_info:
        return jsonify(event_info), 200
    else:
        return jsonify(None), 200

@app.route('/api/image/<doc_id>')
def api_get_image(doc_id):
    """Serves the full image content for a given document ID."""
    dst = db_connector.dms_system_login()
    if not dst: return jsonify({'error': 'DMS login failed.'}), 500

    image_data, _ = wsdl_client.get_image_by_docnumber(dst, doc_id)  # Reusing this
    if image_data:
        return Response(bytes(image_data), mimetype='image/jpeg')  # Assuming jpeg, might need dynamic type
    logging.warning(f"Image not found in DMS for doc_id: {doc_id}")
    return jsonify({'error': 'Image not found in EDMS.'}), 404

@app.route('/api/pdf/<doc_id>')
def api_get_pdf(doc_id):
    """Serves the full PDF content for a given document ID."""
    dst = db_connector.dms_system_login()
    if not dst: return jsonify({'error': 'DMS login failed.'}), 500

    pdf_data, _ = wsdl_client.get_image_by_docnumber(dst, doc_id)  # Reusing this
    if pdf_data:
        return Response(bytes(pdf_data), mimetype='application/pdf')
    logging.warning(f"PDF not found in DMS for doc_id: {doc_id}")
    return jsonify({'error': 'PDF not found in EDMS.'}), 404

@app.route('/api/video/<doc_id>')
def api_get_video(doc_id):
    """
    Handles video requests using a hybrid stream-through cache model.
    """
    dst = db_connector.dms_system_login()
    if not dst:
        return jsonify({'error': 'DMS login failed.'}), 500

    # Determine the expected path of the cached file
    original_filename, media_type, file_ext = db_connector.get_media_info_from_dms(dst, doc_id)
    if not original_filename:
        logging.warning(f"Video metadata not found in DMS for doc_id: {doc_id}")
        return jsonify({'error': 'Video metadata not found in EDMS.'}), 404

    if media_type != 'video':
        logging.warning(f"Requested doc_id {doc_id} is not a video (type: {media_type}).")
        return jsonify({'error': 'Requested document is not a video.'}), 400

    if not file_ext: file_ext = '.mp4'  # Default extension if DMS doesn't provide one
    cached_video_path = os.path.join(db_connector.video_cache_dir, f"{doc_id}{file_ext}")

    # If the file is already cached, serve it directly and quickly.
    if os.path.exists(cached_video_path):
        logging.info(f"Serving video {doc_id} from cache.")
        return send_file(cached_video_path, as_attachment=False)

    # If not cached, initiate the stream-and-cache process.
    logging.info(f"Video {doc_id} not in cache. Streaming from DMS and caching simultaneously.")
    stream_details = db_connector.get_dms_stream_details(dst, doc_id)
    if not stream_details:
        logging.error(f"Could not open stream from DMS for doc_id: {doc_id}")
        return jsonify({'error': 'Could not open stream from DMS.'}), 500

    # Create the generator that will stream to the user and save to a file
    stream_generator = db_connector.stream_and_cache_generator(
        obj_client=stream_details['obj_client'],
        stream_id=stream_details['stream_id'],
        content_id=stream_details['content_id'],
        final_cache_path=cached_video_path
    )

    # Return a streaming response
    # Guess mimetype, default to video/mp4
    mimetype, _ = mimetypes.guess_type(cached_video_path)
    return Response(stream_with_context(stream_generator), mimetype=mimetype or "video/mp4")

@app.route('/cache/<path:filename>')
def serve_cached_thumbnail(filename):
    """Serves cached thumbnail images."""
    return send_from_directory(db_connector.thumbnail_cache_dir, filename)

@app.route('/api/clear_cache', methods=['POST'])
@editor_required
def api_clear_cache():
    """Clears the thumbnail and video cache."""
    try:
        username = session.get('user', {}).get('username', 'Unknown user')
        logging.info(f"User '{username}' initiated cache clearing.")
        db_connector.clear_thumbnail_cache()
        db_connector.clear_video_cache()
        logging.info("Thumbnail and video caches cleared successfully.")
        return jsonify({"message": "All caches cleared successfully."})
    except Exception as e:
        logging.error(f"Failed to clear cache: {e}", exc_info=True)
        return jsonify({"error": f"Failed to clear cache: {e}"}), 500

@app.route('/api/update_abstract', methods=['POST'])
def api_update_abstract():
    """Updates a document's abstract with VIP names."""
    data = request.get_json()
    doc_id = data.get('doc_id')
    names = data.get('names')
    if not doc_id or not isinstance(names, list):
        return jsonify({'error': 'Invalid data provided.'}), 400

    username = session.get('user', {}).get('username', 'Unknown user')
    logging.info(f"User '{username}' updating abstract for doc_id {doc_id} with names: {names}")

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
    lang = data.get('lang', 'en')

    if not name or not isinstance(name, str) or len(name.strip()) < 2:
        return jsonify({'error': 'Invalid data provided. Name must be a string with at least 2 characters.'}), 400

    username = session.get('user', {}).get('username', 'Unknown user')
    logging.info(f"User '{username}' adding person: {name} (lang: {lang})")

    try:
        name_english = ""
        name_arabic = ""

        is_arabic = (lang == 'ar') or (not name.strip().isascii())

        if is_arabic:
            name_arabic = name.strip()
            name_english = api_client.translate_text(name_arabic)
            if not name_english:
                logging.error(
                    f"Failed to add person: Could not get English translation for Arabic name '{name_arabic}'.")
                return jsonify({'error': 'Failed to get English translation for Arabic name.'}), 500
        else:
            name_english = name.strip()
            name_arabic = api_client.translate_text(name_english)
            if not name_arabic:
                logging.warning(f"Could not translate name '{name_english}' to Arabic. Storing as NULL.")
                name_arabic = None

    except Exception as e:
        logging.error(f"Error during translation for person '{name}': {e}")
        return jsonify({'error': f'Translation service error: {e}'}), 500

    success, message = db_connector.add_person_to_lkp(name_english, name_arabic)
    if success:
        return jsonify({'message': message})
    else:
        return jsonify({'error': message}), 500

@app.route('/api/persons')
def api_get_persons():
    """Fetches people from the lookup table for autocomplete."""
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '', type=str)
    lang = request.args.get('lang', 'en', type=str)

    persons, total_rows = db_connector.fetch_lkp_persons(page=page, search=search, lang=lang)

    return jsonify({
        'options': persons,
        'hasMore': (page * 20) < total_rows
    })

@app.route('/api/tags')
def api_get_tags():
    """Fetches all unique tags (keywords and persons) for the filter dropdown."""
    lang = request.args.get('lang', 'en', type=str)
    user = session.get('user')
    security_level = user.get('security_level', 'Viewer') if user else 'Viewer'
    app_source = request.headers.get('X-App-Source', 'unknown')

    tags = db_connector.fetch_all_tags(lang=lang, security_level=security_level, app_source=app_source)
    return jsonify(tags)

@app.route('/api/tags/<int:doc_id>')
def api_get_tags_for_document(doc_id):
    """Fetches all tags for a specific document ID."""
    lang = request.args.get('lang', 'en', type=str)
    user = session.get('user')
    security_level = user.get('security_level', 'Viewer') if user else 'Viewer'

    tags = db_connector.fetch_tags_for_document(doc_id, lang=lang, security_level=security_level)
    return jsonify({"tags": tags})

@app.route('/api/tags/shortlist', methods=['POST'])
@editor_required
def api_toggle_shortlist():
    """Toggles the shortlisted status of a tag."""
    data = request.get_json()
    tag = data.get('tag')

    if not tag:
        return jsonify({'error': 'Tag is required'}), 400

    success, result = db_connector.toggle_tag_shortlist(tag)

    if success:
        return jsonify(result)
    else:
        return jsonify({'error': result}), 400

@app.route('/api/processing_status', methods=['POST'])
def api_processing_status():
    """Checks the processing status of a list of documents."""
    data = request.get_json()
    docnumbers = data.get('docnumbers')

    if not docnumbers or not isinstance(docnumbers, list):
        return jsonify(
            {"status": "error", "message": "Invalid data provided. 'docnumbers' list is required."}), 400

    still_processing = db_connector.check_processing_status(docnumbers)

    return jsonify({"processing": still_processing})

@app.route('/api/tags/<int:doc_id>', methods=['POST'])
def api_add_tag(doc_id):
    """Adds a new tag to a document, handling translation."""
    data = request.get_json()
    tag = data.get('tag')
    if not tag or len(tag.strip()) < 2:
        return jsonify({'error': 'Invalid data provided. Tag must be at least 2 characters.'}), 400

    username = session.get('user', {}).get('username', 'Unknown user')
    logging.info(f"User '{username}' adding tag '{tag}' to doc_id {doc_id}")

    try:
        # --- Language Detection & Translation ---
        # A simple check: if it contains non-ASCII, assume Arabic.
        is_arabic = not tag.isascii()

        english_keyword = ""
        arabic_keyword = ""

        if is_arabic:
            arabic_keyword = tag
            english_keyword = api_client.translate_text(tag)
            if not english_keyword:
                logging.warning(f"Could not translate Arabic tag '{tag}' to English.")
                return jsonify({'error': 'Failed to get English translation for Arabic tag.'}), 500
        else:
            english_keyword = tag
            arabic_keyword = api_client.translate_text(tag)
            if not arabic_keyword:
                logging.warning(f"Could not translate English tag '{tag}' to Arabic.")
                return jsonify({'error': 'Failed to get Arabic translation for English tag.'}), 500

        # --- End Translation ---

        # We now have both versions. Use the correct batch-insertion function.
        keyword_to_insert = {
            'english': english_keyword,
            'arabic': arabic_keyword
        }

        # This function is designed to handle duplicates and link the tag.
        db_connector.insert_keywords_and_tags(doc_id, [keyword_to_insert])

        return jsonify({'message': 'Tag added successfully.'}), 201

    except Exception as e:
        logging.error(f"Error in api_add_tag for doc {doc_id} with tag '{tag}': {e}", exc_info=True)
        return jsonify({'error': f'Server error: {e}'}), 500

@app.route('/api/tags/<int:doc_id>/<tag>', methods=['PUT'])
def api_update_tag(doc_id, tag):
    """Updates a tag for a document (not typically needed, usually add/delete)."""
    return jsonify({'error': 'Tag update not implemented. Use delete and add instead.'}), 501

@app.route('/api/tags/<int:doc_id>/<tag>', methods=['DELETE'])
def api_delete_tag(doc_id, tag):
    """Deletes a tag from a document."""
    username = session.get('user', {}).get('username', 'Unknown user')
    logging.info(f"User '{username}' deleting tag '{tag}' from doc_id {doc_id}")
    success, message = db_connector.delete_tag_from_document(doc_id, tag)
    if success:
        return jsonify({'message': message})
    else:
        # Don't return 500 for "not found", just inform the user
        status_code = 404 if "not found" in message.lower() else 500
        return jsonify({'error': message}), status_code

@app.route('/api/document/<int:docnumber>', methods=['GET'])
def get_document_file(docnumber):
    """
    Securely streams a document from the DMS to the client.
    """
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    dst = wsdl_client.dms_system_login()
    if not dst:
        return jsonify({"error": "Failed to get system-level DMS token"}), 500

    file_bytes, filename = wsdl_client.get_document_from_dms(dst, docnumber)

    if file_bytes and filename:
        mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        return Response(file_bytes, mimetype=mimetype, headers={"Content-Disposition": f"inline; filename={filename}"})
    else:
        logging.warning(f"Document not found or retrieval failed for docnumber: {docnumber}")
        return jsonify({"error": "Document not found or could not be retrieved from DMS."}), 404

@app.route('/api/memories', methods=['GET'])
def api_get_memories():
    """Fetches representative memory documents (images) from past years for the current month."""
    try:
        current_dt = datetime.now()
        # Get month from query param, default to current month
        month_str = request.args.get('month')
        month = int(month_str) if month_str and month_str.isdigit() else current_dt.month
        # print(f"month is: {month}")

        # Get day from query param, optional
        day_str = request.args.get('day')
        day = int(day_str) if day_str and day_str.isdigit() else None

        limit_str = request.args.get('limit', '5')  # Default limit for stack view
        limit = int(limit_str) if limit_str.isdigit() else 5
        limit = max(1, min(limit, 10))  # Ensure limit is reasonable (1-10)

        if not 1 <= month <= 12:
            return jsonify({"error": "Invalid month provided."}), 400
        if day is not None and not 1 <= day <= 31:
            return jsonify({"error": "Invalid day provided."}), 400

        # logging.info(f"Fetching memories for Month: {month}, Day: {day}, Limit: {limit}")
        memories = db_connector.fetch_memories_from_oracle(month=month, day=day, limit=limit)

        return jsonify({"memories": memories})

    except Exception as e:
        logging.error(f"Error fetching memories via API: {e}", exc_info=True)
        return jsonify({"error": "Failed to fetch memories due to server error."}), 500

@app.route('/api/update_metadata', methods=['PUT'])
def api_update_metadata():
    """Updates specific metadata fields for a document (abstract and/or date_taken)."""
    data = request.get_json()
    doc_id = data.get('doc_id')

    if not doc_id:
        return jsonify({'error': 'Document ID (doc_id) is required.'}), 400

    # Extract potential fields to update
    new_abstract = data.get('abstract')  # Can be None if only date is updated
    date_taken_str = data.get('date_taken')  # Expected format: YYYY-MM-DD HH:MM:SS or null

    # --- Validation ---
    # Check if at least one field is provided for update
    if new_abstract is None and date_taken_str is None:
        return jsonify({'error': 'At least one field (abstract or date_taken) must be provided for update.'}), 400

    # --- Parse Date Taken String ---
    new_date_taken = None
    update_date = False  # Flag to indicate if date needs updating
    if date_taken_str is not None:  # Check if the key exists (even if value is null)
        update_date = True  # Intent to update the date is present
        if date_taken_str:  # If the string is not empty or null
            try:
                # Attempt to parse the date string from the form
                new_date_taken = datetime.strptime(date_taken_str, '%Y-%m-%d %H:%M:%S')
                logging.info(f"Parsed date_taken from request: {new_date_taken}")
            except (ValueError, TypeError):
                logging.warning(
                    f"Could not parse date_taken '{date_taken_str}' from request. Date will not be updated.")
                # Return an error if parsing fails for a non-null string
                return jsonify({
                                   'error': f"Invalid date_taken format provided: '{date_taken_str}'. Expected YYYY-MM-DD HH:MM:SS."}), 400
        # If date_taken_str is explicitly null or empty string, new_date_taken remains None, indicating clear/set to null
        else:
            logging.info(f"Received request to set date_taken to NULL for doc_id {doc_id}.")

    username = session.get('user', {}).get('username', 'Unknown user')
    logging.info(
        f"User '{username}' updating metadata for doc_id {doc_id}. Abstract provided: {new_abstract is not None}, Date provided: {date_taken_str is not None}")

    # Call the updated database function, passing the parsed date or None
    success, message = db_connector.update_document_metadata(
        doc_id,
        new_abstract=new_abstract,
        new_date_taken=new_date_taken if update_date else Ellipsis  # Use Ellipsis to signal "don't update date"
    )

    if success:
        logging.info(f"Successfully updated metadata for doc_id {doc_id}.")
        return jsonify({'message': message}), 200
    else:
        logging.error(f"Failed to update metadata for doc_id {doc_id}: {message}")
        # Determine appropriate status code based on error
        status_code = 404 if "not found" in message.lower() else 500
        return jsonify({'error': message}), status_code

@app.route('/api/download_watermarked/<int:doc_id>', methods=['GET'])
def api_download_watermarked(doc_id):
    """Downloads a document with a watermark applied."""
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    # Get username from session
    username = session.get('user', {}).get('username', 'Unknown')

    # 1. Fetch System ID for the user
    system_id = db_connector.get_user_system_id(username)
    if not system_id:
        system_id = "UNKNOWN"  # Fallback

    dst = wsdl_client.dms_system_login()
    if not dst:
        return jsonify({"error": "Failed to get system-level DMS token"}), 500

    # Get original info
    filename, media_type, file_ext = db_connector.get_media_info_from_dms(dst, doc_id)
    if not filename:
        return jsonify({"error": "Document not found"}), 404

    # Get docname from DB for filename
    download_filename = filename
    try:
        conn = db_connector.get_connection()
        if conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DOCNAME FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                result = cursor.fetchone()
                if result and result[0]:
                    safe_docname = "".join(
                        [c for c in result[0] if c.isalpha() or c.isdigit() or c in (' ', '-', '_')]).rstrip()
                    if safe_docname:
                        download_filename = f"{safe_docname}{file_ext}"
            conn.close()
    except Exception as e:
        logging.error(f"Error getting docname for download: {e}")

    # Get content
    file_bytes = db_connector.get_media_content_from_dms(dst, doc_id)
    if not file_bytes:
        return jsonify({"error": "Failed to retrieve document content"}), 500

    processed_bytes = file_bytes
    mimetype = 'application/octet-stream'

    # 2. Construct the watermark text: system_id - docnumber | date
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    watermark_text = f"{system_id} - {doc_id} | {current_time}"

    # 3. Apply watermark using new format
    if media_type == 'image':
        processed_bytes, mimetype = apply_watermark_to_image(file_bytes, watermark_text)
    elif media_type == 'pdf':
        processed_bytes, mimetype = apply_watermark_to_pdf(file_bytes, watermark_text)
    elif media_type == 'video':
        # WARNING: Video processing is slow.
        processed_bytes, mimetype = apply_watermark_to_video(file_bytes, watermark_text, filename)

    return Response(
        processed_bytes,
        mimetype=mimetype,
        headers={"Content-Disposition": f"attachment; filename={download_filename}"}
    )

# --- Favorites API Routes ---
@app.route('/api/favorites/<int:doc_id>', methods=['POST'])
def add_favorite_route(doc_id):
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401
    user_id = session['user'].get('username')  # Assuming username is the user ID from PEOPLE table
    success, message = db_connector.add_favorite(user_id, doc_id)
    if success:
        return jsonify({"message": message}), 201
    else:
        return jsonify({"error": message}), 500

@app.route('/api/favorites/<int:doc_id>', methods=['DELETE'])
def remove_favorite_route(doc_id):
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401
    user_id = session['user'].get('username')
    success, message = db_connector.remove_favorite(user_id, doc_id)
    if success:
        return jsonify({"message": message}), 200
    else:
        return jsonify({"error": message}), 500

@app.route('/api/favorites', methods=['GET'])
def get_favorites_route():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    user_id = session['user'].get('username')

    app_source = request.headers.get('X-App-Source', 'unknown')

    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('pageSize', 20, type=int)

    documents, total_rows = db_connector.get_favorites(user_id, page, page_size, app_source=app_source)

    total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1

    return jsonify({
        "documents": documents,
        "page": page,
        "total_pages": total_pages,
        "total_documents": total_rows
    })

# --- Events API Routes ---
@app.route('/api/events', methods=['GET'])
def get_events_route():
    """Fetches paginated events."""
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', None, type=str)
    page_size = request.args.get('pageSize', 20, type=int)
    fetch_all = request.args.get('fetch_all', 'false', type=str).lower() == 'true'

    if page < 1: page = 1
    if page_size < 1: page_size = 20
    if page_size > 100: page_size = 100

    logging.debug(f"Fetching events - Page: {page}, PageSize: {page_size}, Search: '{search}', Fetch all: {fetch_all}")

    events_list, total_rows = db_connector.get_events(page=page, page_size=page_size, search=search,
                                                      fetch_all=fetch_all)

    total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
    has_more = (page * page_size) < total_rows

    return jsonify({
        "events": events_list,
        "page": page,
        "total_pages": total_pages,
        "hasMore": has_more
    })

@app.route('/api/events', methods=['POST'])
def create_event_route():
    data = request.get_json()
    event_name = data.get('name')
    if not event_name:
        return jsonify({"error": "Event name is required."}), 400
    event_id, message = db_connector.create_event(event_name)
    if event_id:
        return jsonify({"id": event_id, "message": message}), 201
    else:
        return jsonify({"error": message}), 400

@app.route('/api/events/<int:event_id>/documents', methods=['GET'])
def get_event_documents_route(event_id):
    """Fetches paginated documents associated with a specific event."""
    page = request.args.get('page', 1, type=int)
    # Use a page size of 1 for the slider modal
    page_size = 1  # Fetch one document at a time for the modal slider

    if page < 1: page = 1

    logging.debug(f"Fetching documents for Event ID: {event_id} - Page: {page}")

    documents, total_pages, error_message = db_connector.get_documents_for_event(
        event_id=event_id,
        page=page,
        page_size=page_size
    )

    if error_message:
        # Determine appropriate status code based on error message if needed
        status_code = 500  # Default to internal server error
        if "not found" in error_message.lower():
            status_code = 404
        return jsonify({"error": error_message}), status_code

    # The function now returns a list (usually with one item due to page_size=1)
    # Return the first document if available, or null
    current_doc = documents[0] if documents else None

    return jsonify({
        "document": current_doc,  # Send the single document object for the current page
        "page": page,
        "total_pages": total_pages,
        # Optionally, include total_documents count if needed by frontend
        # "total_documents": total_rows # Need get_documents_for_event to return total_rows too
    })

@app.route('/api/journey', methods=['GET'])
def get_journey_data():
    """Fetches all events grouped by year for the journey timeline."""
    try:
        journey_data = db_connector.fetch_journey_data()
        return jsonify(journey_data)
    except Exception as e:
        logging.error(f"Error in /api/journey endpoint: {e}", exc_info=True)
        return jsonify({"error": "Failed to fetch journey data due to server error."}), 500

@app.route('/api/media_counts', methods=['GET'])
def get_media_counts():
    try:
        app_source = request.headers.get('X-App-Source', 'unknown')
        scope = request.args.get('scope')

        counts = db_connector.get_media_type_counts(app_source=app_source, scope=scope)

        if counts:
            return jsonify(counts), 200
        else:
            return jsonify({"images": 0, "videos": 0, "files": 0}), 200
    except Exception as e:
        logging.error(f"Error fetching media counts: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/folders', methods=['GET'])
def api_list_folders():
    """
    Lists the contents of a specific folder or the root.
    Query Param: parent_id (optional), scope (optional), media_type (optional), search (optional)
    """
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    app_source = request.headers.get('X-App-Source', 'unknown')
    scope = request.args.get('scope')

    media_type = request.args.get('media_type')
    if media_type == '': media_type = None  # Ensure empty string is None

    search_term = request.args.get('search')
    if search_term == '': search_term = None

    parent_id = request.args.get('parent_id')
    # Treat string "null" or empty as None
    if parent_id in ['null', 'undefined', '']:
        parent_id = None

    dst = wsdl_client.dms_system_login()
    if not dst:
        return jsonify({"error": "Failed to authenticate with DMS"}), 500

    try:
        # Pass media_type and search_term to wsdl_client
        contents = wsdl_client.list_folder_contents(
            dst,
            parent_id,
            app_source,
            scope=scope,
            media_type=media_type,
            search_term=search_term
        )

        return jsonify({"contents": contents}), 200
    except Exception as e:
        logging.error(f"API Error listing folders: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/folders', methods=['POST'])
def api_create_folder():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    folder_name = data.get('name')
    description = data.get('description', '')

    # Extract parent_id. If it's an empty string or 0, treat as None (Root folder)
    parent_id = data.get('parent_id')
    if not parent_id or str(parent_id).strip() == "":
        parent_id = None

    if not folder_name:
        return jsonify({"error": "Folder name is required"}), 400

    username = session['user'].get('username')
    dst = wsdl_client.dms_system_login()
    if not dst:
        return jsonify({"error": "Failed to authenticate with DMS system"}), 500

    try:
        new_folder_id = wsdl_client.create_dms_folder(
            dst=dst,
            folder_name=folder_name,
            description=description,
            parent_id=parent_id,
            user_id=username
        )

        if new_folder_id:
            return jsonify({
                "message": "Folder created successfully",
                "folder_id": new_folder_id,
                "name": folder_name,
                "type": "subfolder" if parent_id else "root"
            }), 201
        else:
            return jsonify({"error": "Failed to create folder in DMS"}), 500

    except Exception as e:
        logging.error(f"API Error creating folder: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/folders/<folder_id>', methods=['PUT'])
def api_rename_folder(folder_id):
    """
    Renames a folder (or document).
    JSON Body: { "name": "New Name" }
    """
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    new_name = data.get('name')

    if not new_name:
        return jsonify({"error": "New name is required"}), 400

    dst = wsdl_client.dms_system_login()
    if not dst:
        return jsonify({"error": "Failed to authenticate with DMS"}), 500

    success = wsdl_client.rename_document(dst, folder_id, new_name)

    if success:
        return jsonify({"message": "Folder renamed successfully", "id": folder_id, "new_name": new_name}), 200
    else:
        return jsonify({"error": "Failed to rename folder in DMS"}), 500

@app.route('/api/folders/<folder_id>', methods=['DELETE'])
def api_delete_folder(folder_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401

    force_delete = request.args.get('force', 'false').lower() == 'true'
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to authenticate"}), 500

    # Unpack tuple (success, message)
    success, message = wsdl_client.delete_document(dst, folder_id, force=force_delete)

    if success:
        return jsonify({"message": "Folder deleted successfully", "id": folder_id}), 200
    else:
        # Return the actual SOAP error message so frontend can detect "referenced"
        return jsonify({"error": message}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # logging.info(f"Starting server on host 0.0.0.0 port {port}")
    serve(app, host='0.0.0.0', port=port, threads=100)