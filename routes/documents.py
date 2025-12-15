from flask import Blueprint, request, jsonify, session, Response
from werkzeug.utils import secure_filename
from threading import Thread
import math
import os
import io
import mimetypes
import logging
from datetime import datetime
import db_connector
import wsdl_client
from services.processor import process_document
from utils.watermark import apply_watermark_to_image, apply_watermark_to_pdf, apply_watermark_to_video

documents_bp = Blueprint('documents', __name__)


@documents_bp.route('/api/documents')
def api_get_documents():
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
        scope = request.args.get('scope', None, type=str)
        memory_month = request.args.get('memoryMonth', None, type=str)
        memory_day = request.args.get('memoryDay', None, type=str)

        if page < 1: page = 1
        if page_size < 1: page_size = 20
        if page_size > 100: page_size = 100

        documents, total_rows = db_connector.fetch_documents_from_oracle(
            page=page, page_size=page_size, search_term=search_term, date_from=date_from,
            date_to=date_to, persons=persons, person_condition=person_condition, tags=tags,
            years=years, sort=sort, memory_month=memory_month, memory_day=memory_day,
            user_id=username, lang=lang, security_level=security_level, app_source=app_source,
            media_type=media_type, scope=scope
        )
        total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
        return jsonify(
            {"documents": documents, "page": page, "total_pages": total_pages, "total_documents": total_rows})
    except Exception as e:
        logging.error(f"Error in /api/documents endpoint: {e}", exc_info=True)
        return jsonify({"error": "Failed to fetch documents due to server error."}), 500


@documents_bp.route('/process-batch', methods=['POST'])
def process_batch():
    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        return jsonify({"status": "error", "message": "Failed to authenticate with DMS."}), 500
    documents = db_connector.get_documents_to_process()
    if not documents:
        return jsonify({"status": "success", "message": "No new documents to process.", "processed_count": 0}), 200
    processed_count = 0
    for doc in documents:
        result_data = process_document(doc, dms_session_token)
        db_thread = Thread(target=db_connector.update_document_processing_status, kwargs=result_data)
        db_thread.start()
        db_thread.join(timeout=30.0)
        if not db_thread.is_alive():
            if result_data['status'] == 3: processed_count += 1
    return jsonify({"status": "success", "message": f"Processed {processed_count} documents.",
                    "processed_count": processed_count}), 200


@documents_bp.route('/api/upload_document', methods=['POST'])
def api_upload_document():
    if 'file' not in request.files: return jsonify({"success": False, "error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"success": False, "error": "No selected file"}), 400
    file_stream = file.stream
    file_bytes = file_stream.read()
    file_stream.seek(0)

    date_taken_str = request.form.get('date_taken')
    if date_taken_str:
        try:
            doc_date_taken = datetime.strptime(date_taken_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            doc_date_taken = None
    else:
        doc_date_taken = db_connector.get_exif_date(io.BytesIO(file_bytes))

    original_filename = secure_filename(file.filename)
    file_extension = os.path.splitext(original_filename)[1].lstrip('.').upper()
    app_id = db_connector.get_app_id_from_extension(file_extension) or 'UNKNOWN'

    docname = request.form.get('docname')
    if not docname or not docname.strip():
        docname = os.path.splitext(original_filename)[0]
    else:
        docname = docname.strip()

    abstract = request.form.get('abstract', 'Uploaded via EDMS Viewer')
    event_id_str = request.form.get('event_id')
    event_id = int(event_id_str) if event_id_str else None
    parent_id = request.form.get('parent_id')
    if parent_id and parent_id.lower() in ['null', 'undefined', '']: parent_id = None

    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"success": False, "error": "Failed to authenticate with DMS."}), 500

    metadata = {"docname": docname, "abstract": abstract, "app_id": app_id, "filename": original_filename,
                "doc_date": doc_date_taken, "event_id": event_id}
    new_doc_number = wsdl_client.upload_document_to_dms(dst, file_stream, metadata, parent_id=parent_id)

    if new_doc_number:
        return jsonify({"success": True, "docnumber": new_doc_number, "filename": original_filename})
    else:
        return jsonify({"success": False, "error": "Failed to upload file to DMS."}), 500


@documents_bp.route('/api/process_uploaded_documents', methods=['POST'])
def api_process_uploaded_documents():
    data = request.get_json()
    docnumbers = data.get('docnumbers')
    if not docnumbers or not isinstance(docnumbers, list):
        return jsonify({"status": "error", "message": "Invalid data provided."}), 400
    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token: return jsonify({"status": "error", "message": "Failed to authenticate."}), 500
    results = {"processed": [], "failed": [], "in_progress": []}
    docs_to_process = db_connector.get_specific_documents_for_processing(docnumbers)
    for doc in docs_to_process:
        result_data = process_document(doc, dms_session_token)
        db_thread = Thread(target=db_connector.update_document_processing_status, kwargs=result_data)
        db_thread.start()
        db_thread.join(timeout=30.0)
        if db_thread.is_alive():
            results["failed"].append(doc['docnumber'])
        else:
            if result_data['status'] == 3:
                results["processed"].append(doc['docnumber'])
            elif result_data['status'] == 2:
                results["failed"].append(doc['docnumber'])
            else:
                results["in_progress"].append(doc['docnumber'])
    return jsonify(results), 200


@documents_bp.route('/api/document/<int:docnumber>', methods=['GET'])
def get_document_file(docnumber):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to get token"}), 500
    file_bytes, filename = wsdl_client.get_document_from_dms(dst, docnumber)
    if file_bytes and filename:
        mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        return Response(file_bytes, mimetype=mimetype, headers={"Content-Disposition": f"inline; filename={filename}"})
    else:
        return jsonify({"error": "Document not found"}), 404


@documents_bp.route('/api/update_metadata', methods=['PUT'])
def api_update_metadata():
    data = request.get_json()
    doc_id = data.get('doc_id')
    if not doc_id: return jsonify({'error': 'Document ID is required.'}), 400
    new_abstract = data.get('abstract')
    date_taken_str = data.get('date_taken')
    if new_abstract is None and date_taken_str is None:
        return jsonify({'error': 'At least one field must be provided.'}), 400
    new_date_taken = None
    update_date = False
    if date_taken_str is not None:
        update_date = True
        if date_taken_str:
            try:
                new_date_taken = datetime.strptime(date_taken_str, '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                return jsonify({'error': f"Invalid date format."}), 400
    success, message = db_connector.update_document_metadata(doc_id, new_abstract=new_abstract,
                                                             new_date_taken=new_date_taken if update_date else Ellipsis)
    if success:
        return jsonify({'message': message}), 200
    else:
        return jsonify({'error': message}), 500


@documents_bp.route('/api/download_watermarked/<int:doc_id>', methods=['GET'])
def api_download_watermarked(doc_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    username = session.get('user', {}).get('username', 'Unknown')
    system_id = db_connector.get_user_system_id(username) or "UNKNOWN"
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to get token"}), 500
    filename, media_type, file_ext = db_connector.get_media_info_from_dms(dst, doc_id)
    if not filename: return jsonify({"error": "Document not found"}), 404
    file_bytes = db_connector.get_media_content_from_dms(dst, doc_id)
    if not file_bytes: return jsonify({"error": "Failed to retrieve content"}), 500

    watermark_text = f"{system_id} - {doc_id} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    if media_type == 'image':
        processed_bytes, mimetype = apply_watermark_to_image(file_bytes, watermark_text)
    elif media_type == 'pdf':
        processed_bytes, mimetype = apply_watermark_to_pdf(file_bytes, watermark_text)
    elif media_type == 'video':
        processed_bytes, mimetype = apply_watermark_to_video(file_bytes, watermark_text, filename)
    else:
        processed_bytes, mimetype = file_bytes, 'application/octet-stream'

    return Response(processed_bytes, mimetype=mimetype,
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


@documents_bp.route('/api/update_abstract', methods=['POST'])
def api_update_abstract():
    data = request.get_json()
    doc_id = data.get('doc_id')
    names = data.get('names')
    if not doc_id or not isinstance(names, list): return jsonify({'error': 'Invalid data provided.'}), 400
    success, message = db_connector.update_abstract_with_vips(doc_id, names)
    if success:
        return jsonify({'message': message})
    else:
        return jsonify({'error': message}), 500


@documents_bp.route('/api/document/<int:doc_id>/event', methods=['PUT'])
def link_document_event(doc_id):
    data = request.get_json()
    if data is None: return jsonify({"error": "Invalid JSON."}), 400
    event_id = data.get('event_id')
    if event_id is not None:
        try:
            event_id = int(event_id)
        except:
            return jsonify({"error": "Invalid event_id"}), 400
    success, message = db_connector.link_document_to_event(doc_id, event_id)
    if success:
        return jsonify({"message": message}), 200
    else:
        return jsonify({"error": message}), 500


@documents_bp.route('/api/document/<int:doc_id>/event', methods=['GET'])
def get_document_event(doc_id):
    event_info = db_connector.get_event_for_document(doc_id)
    return jsonify(event_info), 200