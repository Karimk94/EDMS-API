from fastapi import APIRouter, Request, HTTPException, UploadFile, File, Form, BackgroundTasks, Header, Response, Depends
from fastapi.responses import StreamingResponse
from werkzeug.utils import secure_filename
from typing import Optional
from datetime import datetime
import os
import io
import mimetypes
import logging
import db_connector
from utils.common import get_current_user, get_session_token
import wsdl_client
from services.processor import process_document
from utils.watermark import apply_watermark_to_image, apply_watermark_to_pdf, apply_watermark_to_video, apply_watermark_to_video_async
from schemas.documents import ProcessUploadRequest, UpdateMetadataRequest, UpdateAbstractRequest, LinkEventRequest, SetTrusteesRequest
from database import user_data

router = APIRouter()

# --- Helper for Background Task ---
async def run_db_update(result_data):
    await db_connector.update_document_processing_status(**result_data)

# --- Routes ---

@router.get('/api/documents')
async def api_get_documents(
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

        documents, total_rows = await db_connector.fetch_documents_from_oracle(
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
async def process_batch(background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS.")

    documents = await db_connector.get_documents_to_process()
    if not documents:
        return {"status": "success", "message": "No new documents to process.", "processed_count": 0}

    # Process documents in parallel with a concurrency limit of 3
    import asyncio
    semaphore = asyncio.Semaphore(3)

    async def process_with_limit(doc):
        async with semaphore:
            return await process_document(doc, dms_session_token)

    results = await asyncio.gather(*[process_with_limit(doc) for doc in documents], return_exceptions=True)

    processed_count = 0
    for result_data in results:
        if isinstance(result_data, Exception):
            logging.error(f"Batch processing error: {result_data}")
            continue
        background_tasks.add_task(run_db_update, result_data)
        if result_data.get('status') == 3:
            processed_count += 1

    return {"status": "success", "message": f"Processing completed for {len(documents)} documents.",
            "processed_count": processed_count}


@router.post('/api/upload_document')
async def api_upload_document(request: Request, file: UploadFile = File(...), docname: Optional[str] = Form(None),
                              abstract: str = Form("Uploaded via EDMS Viewer"),
                              event_id: Optional[str] = Form(None), parent_id: Optional[str] = Form(None),
                              date_taken: Optional[str] = Form(None),
                              x_app_source: str = Header("unknown", alias="X-App-Source")):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No selected file")

    file_bytes = file.file.read()
    file.file.seek(0)
    file_size = len(file_bytes)

    # --- Quota Check ---
    user = request.session.get('user')
    if user:
        username = user.get('username')
        people_id = await db_connector.get_user_system_id(username)
        if people_id:
            edms_user_id = await user_data.get_edms_user_id(people_id)
            if edms_user_id:
                current_quota = await user_data.get_user_quota(edms_user_id)
                if file_size > current_quota:
                    raise HTTPException(status_code=400, detail=f"Upload exceeds remaining quota. Remaining: {current_quota} bytes, File: {file_size} bytes.")
    # -------------------

    if date_taken:
        try:
            doc_date_taken = datetime.strptime(date_taken, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            doc_date_taken = None
    else:
        doc_date_taken = db_connector.get_exif_date(io.BytesIO(file_bytes))

    original_filename = secure_filename(file.filename)
    file_extension = os.path.splitext(original_filename)[1].lstrip('.').upper()
    app_id = await db_connector.get_app_id_from_extension(file_extension) or 'UNKNOWN'

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
        "event_id": parsed_event_id,
        "app_source": x_app_source
    }

    # Passing file.file (SpooledTemporaryFile) acts like a stream
    new_doc_number = await wsdl_client.upload_document_to_dms(dst, file.file, metadata, parent_id=parent_id)

    if new_doc_number:
        # --- Deduct Quota ---
        if user and 'edms_user_id' in locals():
            await user_data.deduct_user_quota(edms_user_id, file_size)
        # --------------------
        return {"success": True, "docnumber": new_doc_number, "filename": original_filename}
    else:
        raise HTTPException(status_code=500, detail="Failed to upload file to DMS.")

@router.post('/api/process_uploaded_documents')
async def api_process_uploaded_documents(data: ProcessUploadRequest, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    dms_session_token = db_connector.dms_system_login()
    if not dms_session_token:
        raise HTTPException(status_code=500, detail="Failed to authenticate.")

    results = {"processed": [], "failed": [], "in_progress": []}
    docs_to_process = await db_connector.get_specific_documents_for_processing(data.docnumbers)

    for doc in docs_to_process:
        result_data = await process_document(doc, dms_session_token)
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
async def get_document_file(docnumber: int, request: Request):
    """
    View document inline.
    Updated to use db_connector for better media type resolution and correct file extensions.
    """
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to get token")

    # Use db_connector to get accurate media info including type
    try:
        filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, docnumber)

        # Ensure filename has extension
        if file_ext and not filename.lower().endswith(file_ext.lower()):
            filename = f"{filename}{file_ext}"

        # Get content
        # Get content stream
        # file_bytes = db_connector.get_media_content_from_dms(dst, docnumber)
        stream_generator, stream_filename = db_connector.stream_document_from_dms(dst, docnumber)

        if stream_generator:
            # Determine correct content type
            mimetype = 'application/octet-stream'
            disposition = 'inline'

            if media_type == 'pdf':
                mimetype = "application/pdf"
            elif media_type == 'image':
                mimetype = f"image/{file_ext.replace('.', '')}"
            elif media_type == 'video':
                mimetype = f"video/{file_ext.replace('.', '')}"
            elif media_type == 'text':
                mimetype = "text/plain"
            elif media_type == 'zip':
                mimetype = "application/zip"
                disposition = 'attachment'
            elif media_type == 'excel':
                mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                disposition = 'attachment'  # Browsers often can't preview Excel inline
            elif media_type == 'powerpoint':
                mimetype = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                disposition = 'attachment'
            
            # Use filename from stream if DB filename was missing (unlikely given get_media_info check)
            if not filename and stream_filename:
                filename = stream_filename

            return StreamingResponse(
                stream_generator,
                media_type=mimetype,
                headers={"Content-Disposition": f"{disposition}; filename={filename}"}
            )
        else:
            # Fallback to old method if streaming setup fails
            file_bytes, filename = wsdl_client.get_document_from_dms(dst, docnumber)
            if file_bytes:
                mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
                return StreamingResponse(
                    io.BytesIO(file_bytes),
                    media_type=mimetype,
                    headers={"Content-Disposition": f"inline; filename={filename}"}
                )
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        logging.error(f"Error in get_document_file: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.put('/api/update_metadata')
async def api_update_metadata(data: UpdateMetadataRequest):
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

    success, message = await db_connector.update_document_metadata(
        data.doc_id,
        new_abstract=data.abstract,
        new_date_taken=date_arg
    )

    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.get('/api/download_watermarked/{doc_id}')
async def api_download_watermarked(doc_id: int, request: Request):
    """
    Downloads document.
    Updated to correct filename extensions and Content-Types for Office documents.
    """
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    username = request.session['user'].get('username', 'Unknown')
    system_id = await db_connector.get_user_system_id(username) or "UNKNOWN"

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to get token")

    # Await async media info
    filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, doc_id)
    if not filename:
        raise HTTPException(status_code=404, detail="Document not found")

    # FIX: Ensure filename has the correct extension
    if file_ext and not filename.lower().endswith(file_ext.lower()):
        filename = f"{filename}{file_ext}"

    # Determine if watermarking is needed
    needs_watermark = media_type in ['image', 'pdf', 'video']

    if not needs_watermark:
        # --- STREAMING PATH (No Watermark) ---
        stream_generator, stream_filename = db_connector.stream_document_from_dms(dst, doc_id)
        if stream_generator:
            mimetype = 'application/octet-stream'
            if media_type == 'excel':
                mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif media_type == 'zip':
                mimetype = "application/zip"
            elif media_type == 'powerpoint':
                mimetype = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            elif media_type == 'text':
                mimetype = "text/plain"
            
            # Use filename from stream if DB filename was missing
            if not filename and stream_filename:
                filename = stream_filename

            return StreamingResponse(
                stream_generator,
                media_type=mimetype,
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Access-Control-Expose-Headers": "Content-Disposition"
                }
            )
    
    # --- PROCESSING PATH (Watermark Required or Fallback) ---
    file_bytes = db_connector.get_media_content_from_dms(dst, doc_id)
    if not file_bytes:
        raise HTTPException(status_code=500, detail="Failed to retrieve content")

    watermark_text = f"{system_id} - {doc_id} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    # Determine mime type
    mimetype = 'application/octet-stream'
    processed_bytes = file_bytes

    if media_type == 'image':
        processed_bytes, mimetype = apply_watermark_to_image(file_bytes, watermark_text)
    elif media_type == 'pdf':
        processed_bytes, mimetype = apply_watermark_to_pdf(file_bytes, watermark_text)
    elif media_type == 'video':
        processed_bytes, mimetype = await apply_watermark_to_video_async(file_bytes, watermark_text, filename)
    elif media_type == 'excel':
        # Fallback if streaming failed for some reason, though unlikely to reach here if logic holds
        mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif media_type == 'zip':
        mimetype = "application/zip"
    elif media_type == 'powerpoint':
        mimetype = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    elif media_type == 'text':
        mimetype = "text/plain"

    return StreamingResponse(
        io.BytesIO(processed_bytes),
        media_type=mimetype,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )

@router.post('/api/update_abstract')
async def api_update_abstract(data: UpdateAbstractRequest):
    success, message = await db_connector.update_abstract_with_vips(data.doc_id, data.names)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.put('/api/document/{doc_id}/event')
async def link_document_event(doc_id: int, data: LinkEventRequest):
    success, message = await db_connector.link_document_to_event(doc_id, data.event_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.get('/api/document/{doc_id}/event')
async def get_document_event(doc_id: int):
    event_info = await db_connector.get_event_for_document(doc_id)
    # Return empty dict if None to match JSON behavior or handle strictly
    return event_info if event_info else {}

@router.post('/api/document/{doc_id}/security')
def set_document_security(doc_id: str, data: SetTrusteesRequest, request: Request):
    """
    Sets the trustees for a specific document or folder using the unified session token.
    """
    token = get_session_token(request)

    success, message = wsdl_client.set_trustees(
        dst=token,
        doc_id=doc_id,
        library=data.library,
        trustees=data.trustees,
        security_enabled=data.security_enabled
    )

    if not success:
        raise HTTPException(status_code=500, detail=message)

    return {"status": "success", "message": "Trustees updated successfully"}