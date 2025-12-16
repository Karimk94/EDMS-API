from fastapi import APIRouter, Request, HTTPException, Depends, UploadFile, File, Form, Response, BackgroundTasks, \
    Header, Query
from fastapi.responses import StreamingResponse
from werkzeug.utils import secure_filename
from typing import List, Optional
from datetime import datetime
import os
import io
import mimetypes
import logging
from pydantic import BaseModel

import db_connector
import wsdl_client
from services.processor import process_document
from utils.watermark import apply_watermark_to_image, apply_watermark_to_pdf, apply_watermark_to_video

router = APIRouter()


# --- Pydantic Models ---
class ProcessUploadRequest(BaseModel):
    docnumbers: List[int]


class UpdateMetadataRequest(BaseModel):
    doc_id: int
    abstract: Optional[str] = None
    date_taken: Optional[str] = None


class UpdateAbstractRequest(BaseModel):
    doc_id: int
    names: List[str]


class LinkEventRequest(BaseModel):
    event_id: Optional[int]


# --- Helper for Background Task ---
def run_db_update(result_data):
    db_connector.update_document_processing_status(**result_data)


# --- Routes ---

@router.get('/api/documents')
def api_get_documents(
        request: Request,
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        page: int = 1,
        pageSize: int = 20,
        search: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        persons: Optional[str] = None,
        person_condition: str = 'any',
        tags: Optional[str] = None,
        years: Optional[str] = None,
        sort: Optional[str] = None,
        lang: str = 'en',
        media_type: Optional[str] = None,
        scope: Optional[str] = None,
        memoryMonth: Optional[str] = None,
        memoryDay: Optional[str] = None
):
    try:
        user = request.session.get('user')
        username = user.get('username') if user else None
        security_level = user.get('security_level', 'Viewer') if user else 'Viewer'

        if page < 1: page = 1
        if pageSize < 1: pageSize = 20
        if pageSize > 100: pageSize = 100

        documents, total_rows = db_connector.fetch_documents_from_oracle(
            page=page, page_size=pageSize, search_term=search, date_from=date_from,
            date_to=date_to, persons=persons, person_condition=person_condition, tags=tags,
            years=years, sort=sort, memory_month=memoryMonth, memory_day=memoryDay,
            user_id=username, lang=lang, security_level=security_level, app_source=x_app_source,
            media_type=media_type, scope=scope
        )
        # Handle simple math here since math.ceil requires importing math
        total_pages = (total_rows + pageSize - 1) // pageSize if total_rows > 0 else 1

        return {
            "documents": documents,
            "page": page,
            "total_pages": total_pages,
            "total_documents": total_rows
        }
    except Exception as e:
        logging.error(f"Error in /api/documents endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch documents due to server error.")


@router.post('/process-batch')
def process_batch(background_tasks: BackgroundTasks):
    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS.")

    documents = db_connector.get_documents_to_process()
    if not documents:
        return {"status": "success", "message": "No new documents to process.", "processed_count": 0}

    processed_count = 0
    # In FastAPI, we can dispatch background tasks.
    # Since we have a list, we iterate and add them.
    for doc in documents:
        result_data = process_document(doc, dms_session_token)
        background_tasks.add_task(run_db_update, result_data)
        if result_data.get('status') == 3:
            processed_count += 1

    return {"status": "success", "message": f"Processing started for {len(documents)} documents.",
            "processed_count": processed_count}


@router.post('/api/upload_document')
def api_upload_document(
        file: UploadFile = File(...),
        docname: Optional[str] = Form(None),
        abstract: str = Form("Uploaded via EDMS Viewer"),
        event_id: Optional[str] = Form(None),
        parent_id: Optional[str] = Form(None),
        date_taken: Optional[str] = Form(None)
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No selected file")

    file_bytes = file.file.read()
    file.file.seek(0)

    if date_taken:
        try:
            doc_date_taken = datetime.strptime(date_taken, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            doc_date_taken = None
    else:
        doc_date_taken = db_connector.get_exif_date(io.BytesIO(file_bytes))

    original_filename = secure_filename(file.filename)
    file_extension = os.path.splitext(original_filename)[1].lstrip('.').upper()
    app_id = db_connector.get_app_id_from_extension(file_extension) or 'UNKNOWN'

    final_docname = docname.strip() if docname and docname.strip() else os.path.splitext(original_filename)[0]

    parsed_event_id = int(event_id) if event_id else None
    if parent_id and parent_id.lower() in ['null', 'undefined', '']:
        parent_id = None

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS.")

    metadata = {
        "docname": final_docname,
        "abstract": abstract,
        "app_id": app_id,
        "filename": original_filename,
        "doc_date": doc_date_taken,
        "event_id": parsed_event_id
    }

    # Passing file.file (SpooledTemporaryFile) acts like a stream
    new_doc_number = wsdl_client.upload_document_to_dms(dst, file.file, metadata, parent_id=parent_id)

    if new_doc_number:
        return {"success": True, "docnumber": new_doc_number, "filename": original_filename}
    else:
        raise HTTPException(status_code=500, detail="Failed to upload file to DMS.")


@router.post('/api/process_uploaded_documents')
def api_process_uploaded_documents(data: ProcessUploadRequest, background_tasks: BackgroundTasks):
    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        raise HTTPException(status_code=500, detail="Failed to authenticate.")

    results = {"processed": [], "failed": [], "in_progress": []}
    docs_to_process = db_connector.get_specific_documents_for_processing(data.docnumbers)

    for doc in docs_to_process:
        result_data = process_document(doc, dms_session_token)
        # For uploaded processing, we add to background tasks to update DB
        background_tasks.add_task(run_db_update, result_data)

        # Immediate status reporting (prediction based on process_document result)
        if result_data['status'] == 3:
            results["processed"].append(doc['docnumber'])
        elif result_data['status'] == 2:
            results["failed"].append(doc['docnumber'])
        else:
            results["in_progress"].append(doc['docnumber'])

    return results


@router.get('/api/document/{docnumber}')
def get_document_file(docnumber: int, request: Request):
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to get token")

    file_bytes, filename = wsdl_client.get_document_from_dms(dst, docnumber)
    if file_bytes and filename:
        mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        # Convert bytes to stream for StreamingResponse
        return StreamingResponse(
            io.BytesIO(file_bytes),
            media_type=mimetype,
            headers={"Content-Disposition": f"inline; filename={filename}"}
        )
    else:
        raise HTTPException(status_code=404, detail="Document not found")


@router.put('/api/update_metadata')
def api_update_metadata(data: UpdateMetadataRequest):
    if data.abstract is None and data.date_taken is None:
        raise HTTPException(status_code=400, detail='At least one field must be provided.')

    new_date_taken = None
    update_date = False

    if data.date_taken is not None:
        update_date = True
        if data.date_taken:
            try:
                new_date_taken = datetime.strptime(data.date_taken, '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail="Invalid date format.")

    # Use Ellipsis (...) to indicate no update in DB connector if logic follows that pattern
    date_arg = new_date_taken if update_date else Ellipsis

    success, message = db_connector.update_document_metadata(
        data.doc_id,
        new_abstract=data.abstract,
        new_date_taken=date_arg
    )

    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)


@router.get('/api/download_watermarked/{doc_id}')
def api_download_watermarked(doc_id: int, request: Request):
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    username = request.session['user'].get('username', 'Unknown')
    system_id = db_connector.get_user_system_id(username) or "UNKNOWN"

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to get token")

    filename, media_type, file_ext = db_connector.get_media_info_from_dms(dst, doc_id)
    if not filename:
        raise HTTPException(status_code=404, detail="Document not found")

    file_bytes = db_connector.get_media_content_from_dms(dst, doc_id)
    if not file_bytes:
        raise HTTPException(status_code=500, detail="Failed to retrieve content")

    watermark_text = f"{system_id} - {doc_id} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    if media_type == 'image':
        processed_bytes, mimetype = apply_watermark_to_image(file_bytes, watermark_text)
    elif media_type == 'pdf':
        processed_bytes, mimetype = apply_watermark_to_pdf(file_bytes, watermark_text)
    elif media_type == 'video':
        # apply_watermark_to_video is expensive/blocking. Consider background task or warning.
        processed_bytes, mimetype = apply_watermark_to_video(file_bytes, watermark_text, filename)
    else:
        processed_bytes, mimetype = file_bytes, 'application/octet-stream'

    return StreamingResponse(
        io.BytesIO(processed_bytes),
        media_type=mimetype,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.post('/api/update_abstract')
def api_update_abstract(data: UpdateAbstractRequest):
    success, message = db_connector.update_abstract_with_vips(data.doc_id, data.names)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)


@router.put('/api/document/{doc_id}/event')
def link_document_event(doc_id: int, data: LinkEventRequest):
    success, message = db_connector.link_document_to_event(doc_id, data.event_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=500, detail=message)


@router.get('/api/document/{doc_id}/event')
def get_document_event(doc_id: int):
    event_info = db_connector.get_event_for_document(doc_id)
    # Return empty dict if None to match JSON behavior or handle strictly
    return event_info if event_info else {}