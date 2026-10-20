from fastapi import APIRouter, Request, HTTPException, UploadFile, File, Form, Header, Response, Depends
from fastapi.responses import FileResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool
from werkzeug.utils import secure_filename
from typing import Optional
from datetime import datetime
import os
import io
import time
import asyncio
import uuid
import mimetypes
import logging
import db_connector
from utils.common import get_current_user, get_session_token, get_mimetype_for_media, build_content_disposition, is_editor
import wsdl_client
from services.processing_queue import processing_queue
from utils.watermark import apply_watermark_to_image, apply_watermark_to_pdf, apply_watermark_to_video, apply_watermark_to_video_async
from slowapi import Limiter
from slowapi.util import get_remote_address
from utils.sanitize import sanitize_text, sanitize_filename
from schemas.documents import ProcessUploadRequest, UpdateMetadataRequest, UpdateAbstractRequest, LinkEventRequest, SetTrusteesRequest, ReviewStatusRequest
from database import user_data
from services.upload_cancel import upload_cancel_registry
from wsdl_client.documents import UploadCancelled

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


def _write_cached_download_fallback(cache_path, content_bytes):
    temp_path = f"{cache_path}.tmp"
    with open(temp_path, 'wb') as cache_file:
        cache_file.write(content_bytes)
    os.replace(temp_path, cache_path)

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
        memoryDay: Optional[str] = None,
        user=Depends(get_current_user)
):
    try:
        username = user.get('username')
        security_level = user.get('security_level', 'Viewer')

        if page < 1: page = 1
        if pageSize < 1: pageSize = 20
        if pageSize > 100: pageSize = 100

        documents, total_rows, search_meta = await db_connector.fetch_documents_from_oracle(
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
            "total_documents": total_rows,
            "search_meta": search_meta
        }
    except Exception as e:
        logging.error(f"Error in /api/documents endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch documents due to server error.")

@router.post('/process-batch')
@limiter.limit("3/minute")
async def process_batch(request: Request, user=Depends(get_current_user)):
    documents = await db_connector.get_documents_to_process()
    if not documents:
        return {"status": "success", "message": "No new documents to queue.", "queued_count": 0}

    docnumbers = [int(doc['docnumber']) for doc in documents if doc.get('docnumber')]
    queue_result = processing_queue.enqueue_docnumbers(docnumbers, source="process-batch")

    return {
        "status": "success",
        "message": "Documents queued for background processing.",
        "queued_count": int(queue_result.get("inserted", 0)) + int(queue_result.get("requeued", 0)),
        "details": queue_result,
    }


@router.post('/api/upload_document')
@limiter.limit("100/minute")
async def api_upload_document(request: Request, file: UploadFile = File(...), docname: Optional[str] = Form(None),
                              abstract: str = Form(""),
                              event_id: Optional[str] = Form(None), parent_id: Optional[str] = Form(None),
                              date_taken: Optional[str] = Form(None),
                              x_app_source: str = Header("unknown", alias="X-App-Source"),
                              idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
                              user=Depends(get_current_user)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No selected file")

    original_filename = secure_filename(file.filename)

    from services.idempotency_store import idempotency_store
    if idempotency_key:
        result = idempotency_store.get_result(idempotency_key)
        if result:
            if result["status"] == "SUCCESS":
                return {
                    "success": True, 
                    "docnumber": result["doc_number"], 
                    "filename": original_filename, 
                    "message": "Idempotent hit: Returning cached success response"
                }
            elif result["status"] == "IN_PROGRESS":
                raise HTTPException(status_code=409, detail="Upload already in progress. Please wait.")
            elif result["status"] == "CANCELLED":
                # This upload was cancelled before the handler got here — clear
                # the key so a fresh retry is possible and stop immediately.
                idempotency_store.mark_failed(idempotency_key)
                raise HTTPException(status_code=499, detail="Upload cancelled.")

        if not idempotency_store.claim_key(idempotency_key):
            raise HTTPException(status_code=409, detail="Upload already in progress. Please wait.")

    # Read the uploaded file. If the client aborts/cancels mid-transfer this read
    # will be interrupted, so release the idempotency key and bail out cleanly.
    try:
        file_bytes = file.file.read()
        file.file.seek(0)
    except Exception as read_err:
        if idempotency_key:
            idempotency_store.mark_failed(idempotency_key)
        logging.warning(f"Upload aborted during file transfer (client likely cancelled): {read_err}")
        raise HTTPException(status_code=499, detail="Upload cancelled by client during transfer.")
    file_size = len(file_bytes)

    # --- Quota Check ---
    username = user.get('username')
    if username:
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

    # Sanitize user-provided text to prevent stored XSS
    final_docname = sanitize_filename(final_docname) or final_docname
    abstract = sanitize_text(abstract) or abstract

    try:
        parsed_event_id = int(event_id) if event_id else None
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid event_id. Must be a number.")
    if parent_id and str(parent_id).strip().lower() in ['null', 'undefined', '']:
        parent_id = None

    # If the client has disconnected/cancelled, abort BEFORE creating anything in the
    # DMS so we don't leave an orphaned document the user never asked for.
    if await request.is_disconnected():
        if idempotency_key:
            idempotency_store.mark_failed(idempotency_key)
        # logging.info(f"Client disconnected before DMS upload for '{original_filename}'; aborting.")
        raise HTTPException(status_code=499, detail="Client disconnected; upload aborted.")

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS.")

    metadata = {
        "docname": final_docname,
        "abstract": abstract,
        "app_id": app_id,
        "filename": original_filename,
        "doc_date": doc_date_taken,
        "event_id": parsed_event_id,
        "app_source": x_app_source,
        # The actual logged-in user — used as AUTHOR_ID / TYPIST_ID in the DMS
        # so the document history shows the real uploader, not the service account.
        "author_id": username,
    }

    # --- Cancellation support for the (potentially long) DMS transfer ---
    # The flag is keyed by the idempotency key so the client can target this
    # exact upload via POST /api/upload_document/cancel.
    cancel_key = idempotency_key or f"_anon_{uuid.uuid4().hex}"
    upload_cancel_registry.register(cancel_key)

    # Throttled shared-store check so a cancel request that lands on a different
    # worker process (uvicorn --workers N) still stops this upload.
    _shared_check_state = {"last_check": 0.0}

    def cancel_check():
        # Fast path: in-memory flag (same-worker cancel or disconnect watcher)
        if upload_cancel_registry.is_cancelled(cancel_key):
            return True
        # Slower path: shared SQLite flag, checked at most once per second
        if idempotency_key:
            now = time.monotonic()
            if now - _shared_check_state["last_check"] >= 1.0:
                _shared_check_state["last_check"] = now
                if idempotency_store.is_cancelled(idempotency_key):
                    upload_cancel_registry.cancel(cancel_key)  # latch locally
                    return True
        return False

    # Belt-and-suspenders: also watch for a client disconnect (tab closed /
    # XHR aborted) and flag cancellation so the DMS transfer stops promptly,
    # even if the explicit cancel request never arrives.
    async def _watch_disconnect():
        try:
            while True:
                await asyncio.sleep(0.25)
                if await request.is_disconnected():
                    upload_cancel_registry.cancel(cancel_key)
                    return
        except asyncio.CancelledError:
            pass

    disconnect_watcher = asyncio.create_task(_watch_disconnect())

    # Passing file.file (SpooledTemporaryFile) acts like a stream
    try:
        new_doc_number = await wsdl_client.upload_document_to_dms(
            dst, file.file, metadata, parent_id=parent_id, cancel_check=cancel_check
        )
    except UploadCancelled as ce:
        if idempotency_key:
            idempotency_store.mark_failed(idempotency_key)
        # Clean up the partially-created document now that the write stream has
        # been released inside upload_document_to_dms.
        if ce.doc_number:
            try:
                deleted, del_msg = await run_in_threadpool(
                    wsdl_client.delete_document, dst, ce.doc_number, True
                )
                if not deleted:
                    logging.warning(
                        f"Could not delete cancelled partial document {ce.doc_number}: {del_msg}"
                    )
            except Exception as cleanup_err:
                logging.warning(
                    f"Error cleaning up cancelled partial document {ce.doc_number}: {cleanup_err}"
                )
        raise HTTPException(status_code=499, detail="Upload cancelled.")
    except Exception as e:
        if idempotency_key:
            idempotency_store.mark_failed(idempotency_key)
        raise e
    finally:
        disconnect_watcher.cancel()
        upload_cancel_registry.unregister(cancel_key)

    if new_doc_number:
        if idempotency_key:
            idempotency_store.mark_success(idempotency_key, int(new_doc_number))
            
        # --- Deduct Quota ---
        if user and 'edms_user_id' in locals():
            await user_data.deduct_user_quota(edms_user_id, file_size)
        # --------------------

        # --- Set security: inherit parent folder trustees + ensure uploader has full control ---
        if username:
            trustees = []
            if parent_id:
                try:
                    parent_trustees = await run_in_threadpool(wsdl_client.get_object_trustees, dst, str(parent_id))
                    if parent_trustees:
                        trustees = list(parent_trustees)
                except Exception as te:
                    logging.warning(f"Could not fetch parent trustees for document {new_doc_number}: {te}")

            # Ensure the uploader is present with full control
            uploader_in_list = any(
                str(t.get('username', '')).upper() == str(username).upper()
                for t in trustees
            )
            if not uploader_in_list:
                trustees.append({'username': username, 'rights': 255, 'flag': 2})

            success, message = await run_in_threadpool(
                wsdl_client.set_trustees,
                dst,
                str(new_doc_number),
                'RTA_MAIN',
                trustees,
                '1'
            )
            if not success:
                logging.warning(f"Failed to set security on uploaded document {new_doc_number}: {message}")
        # -------------------------------------------------------

        # --- Auto-enqueue for AI analysis when uploaded from edms-media ---
        # Only edms-media triggers immediate analysis; other sources rely on
        # the scheduled batch crawler (/process-batch).
        if x_app_source == "edms-media":
            try:
                from services.processing_queue import processing_queue
                processing_queue.enqueue_docnumbers([int(new_doc_number)], source="edms-media")
            except Exception as eq_err:
                logging.warning(f"Failed to enqueue doc {new_doc_number} for processing: {eq_err}")
        # -------------------------------------------------------------------

        return {"success": True, "docnumber": new_doc_number, "filename": original_filename}
    else:
        if idempotency_key:
            idempotency_store.mark_failed(idempotency_key)
        raise HTTPException(status_code=500, detail="Failed to upload file to DMS.")


@router.post('/api/upload_document/cancel')
async def api_cancel_upload_document(request: Request, user=Depends(get_current_user)):
    """
    Cancels an in-progress upload identified by its Idempotency-Key.

    The flag is checked between chunks while the file is being streamed to the
    DMS, so the transfer stops shortly after this call and the partially-created
    document is removed. Safe to call even if the upload has already finished.

    The signal is written to both the in-memory registry (fast path, same
    worker) and the shared idempotency store (works across worker processes).
    """
    try:
        body = await request.json()
    except Exception:
        body = {}

    key = body.get('idempotency_key') or body.get('idempotencyKey')
    if not key:
        raise HTTPException(status_code=400, detail="idempotency_key is required.")

    key = str(key)
    cancelled_in_worker = upload_cancel_registry.cancel(key)

    # Persist the cancellation so the worker streaming the file can see it even
    # if this request was handled by a different worker process.
    from services.idempotency_store import idempotency_store
    idempotency_store.mark_cancelled(key)

    return {"cancelled": cancelled_in_worker, "idempotency_key": key}


@router.post('/api/process_uploaded_documents')
async def api_process_uploaded_documents(data: ProcessUploadRequest, user=Depends(get_current_user)):
    # --- AUTO AI PROCESSING DISABLED ---
    # AI analysis on upload is disabled. Documents will only be processed
    # when triggered by the scheduled Crawler Task via /process-batch.
    # To re-enable, uncomment the enqueue call below.
    # ---
    if not data.docnumbers:
        return {"processed": [], "failed": [], "in_progress": []}

    if data.source == "edms-media":
        processing_queue.enqueue_docnumbers(data.docnumbers, source="edms-media")
        return {
            "processed": [],
            "failed": [],
            "in_progress": data.docnumbers,
            "message": "Queued for processing.",
        }

    return {
        "processed": [],
        "failed": [],
        "in_progress": [],
        "message": "Auto AI processing is disabled. Documents will be processed by the scheduled crawler.",
    }

@router.get('/api/document/{docnumber}')
async def get_document_file(docnumber: int, request: Request, user=Depends(get_current_user)):

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to get token")

    # Use db_connector to get accurate media info including type
    try:
        filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, docnumber)

        # Ensure filename has extension
        if file_ext and not filename.lower().endswith(file_ext.lower()):
            filename = f"{filename}{file_ext}"

        mimetype, disposition = get_mimetype_for_media(media_type, file_ext)

        if disposition == 'attachment':
            cache_path = db_connector.get_download_cache_path(docnumber, file_ext)
            if not os.path.exists(cache_path):
                did_cache = await db_connector.cache_document_stream_to_file_async(dst, docnumber, cache_path)
                if not did_cache:
                    file_bytes = await db_connector.get_media_content_from_dms_async(dst, docnumber)
                    if not file_bytes:
                        raise HTTPException(status_code=404, detail="Document not found")
                    _write_cached_download_fallback(cache_path, file_bytes)

            return FileResponse(
                cache_path,
                media_type=mimetype,
                headers={
                    "Content-Disposition": build_content_disposition(filename, disposition),
                    "Access-Control-Expose-Headers": "Content-Disposition",
                    "Accept-Ranges": "bytes"
                }
            )

        file_bytes = await db_connector.get_media_content_from_dms_async(dst, docnumber)
        if not file_bytes:
            raise HTTPException(status_code=404, detail="Document not found")

        # For HEIC/HEIF images viewed inline in browser, standard browsers (Chrome/Edge/Firefox)
        # cannot decode HEIF natively. Convert to high-quality JPEG on-the-fly using pillow_heif.
        clean_ext = (file_ext or '').lower().replace('.', '')
        if disposition == 'inline' and clean_ext in ('heic', 'heif'):
            preview_cache_path = os.path.join(db_connector.thumbnail_cache_dir, f"preview_{docnumber}.jpg")
            if os.path.exists(preview_cache_path):
                return FileResponse(
                    preview_cache_path,
                    media_type="image/jpeg",
                    headers={
                        "Content-Disposition": build_content_disposition(f"{os.path.splitext(filename)[0]}.jpg", disposition),
                        "Access-Control-Expose-Headers": "Content-Disposition",
                        "Accept-Ranges": "bytes"
                    }
                )
            try:
                from PIL import Image as PILImage
                with PILImage.open(io.BytesIO(file_bytes)) as pil_img:
                    pil_img.convert("RGB").save(preview_cache_path, "JPEG", quality=95)
                    return FileResponse(
                        preview_cache_path,
                        media_type="image/jpeg",
                        headers={
                            "Content-Disposition": build_content_disposition(f"{os.path.splitext(filename)[0]}.jpg", disposition),
                            "Access-Control-Expose-Headers": "Content-Disposition",
                            "Accept-Ranges": "bytes"
                        }
                    )
            except Exception as conv_err:
                logging.warning(f"Failed to convert {file_ext} to JPEG for inline display (doc {docnumber}): {conv_err}")

        return StreamingResponse(
            io.BytesIO(file_bytes),
            media_type=mimetype,
            headers={
                "Content-Disposition": build_content_disposition(filename, disposition),
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    except Exception as e:
        logging.error(f"Error in get_document_file: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get('/api/document/{doc_id}/metadata')
async def api_get_document_metadata(doc_id: int, user=Depends(get_current_user)):
    """Fetches the latest metadata (abstract, author, date, docname) for a single document."""
    meta = await db_connector.get_document_metadata(doc_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Document not found")
    return meta

@router.put('/api/update_metadata')
async def api_update_metadata(data: UpdateMetadataRequest, user=Depends(get_current_user)):
    if data.abstract is None and data.date_taken is None:
        raise HTTPException(status_code=400, detail='At least one field must be provided.')

    # Sanitize abstract text to prevent stored XSS
    if data.abstract is not None:
        data.abstract = sanitize_text(data.abstract)

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
        # Also sync to OpenText DM DEF_PROF best-effort if abstract changed
        if data.abstract is not None:
            try:
                dst = await run_in_threadpool(wsdl_client.dms_system_login)
                if dst:
                    await run_in_threadpool(
                        wsdl_client.update_document_profile_abstract,
                        dst,
                        data.doc_id,
                        data.abstract
                    )
            except Exception as sync_e:
                logging.warning(f"WSDL sync failed after manual metadata update for doc {data.doc_id}: {sync_e}")

        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.get('/api/download_watermarked/{doc_id}')
async def api_download_watermarked(doc_id: int, request: Request, user=Depends(get_current_user)):

    username = request.session['user'].get('username', 'Unknown')
    system_id = await db_connector.get_user_system_id(username) or "UNKNOWN"

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
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
        mimetype, _ = get_mimetype_for_media(media_type, file_ext)
        cache_path = db_connector.get_download_cache_path(doc_id, file_ext)
        if not os.path.exists(cache_path):
            did_cache = await db_connector.cache_document_stream_to_file_async(dst, doc_id, cache_path)
            if not did_cache:
                file_bytes = await db_connector.get_media_content_from_dms_async(dst, doc_id)
                if not file_bytes:
                    raise HTTPException(status_code=500, detail="Failed to retrieve content")
                _write_cached_download_fallback(cache_path, file_bytes)

        try:
            sys_id_int = int(system_id)
            from database.activity import log_activity
            await log_activity(doc_id, 'Download', sys_id_int)
        except (ValueError, TypeError):
            pass

        return FileResponse(
            cache_path,
            media_type=mimetype,
            headers={
                "Content-Disposition": build_content_disposition(filename, 'attachment'),
                "Access-Control-Expose-Headers": "Content-Disposition",
                "Accept-Ranges": "bytes"
            }
        )
    
    # --- PROCESSING PATH (Watermark Required or Fallback) ---
    file_bytes = await db_connector.get_media_content_from_dms_async(dst, doc_id)
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

    try:
        sys_id_int = int(system_id)
        from database.activity import log_activity
        await log_activity(doc_id, 'Download', sys_id_int)
    except (ValueError, TypeError):
        pass

    return StreamingResponse(
        io.BytesIO(processed_bytes),
        media_type=mimetype,
        headers={
            "Content-Disposition": build_content_disposition(filename, 'attachment'),
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )

@router.post('/api/update_abstract')
async def api_update_abstract(data: UpdateAbstractRequest, user=Depends(get_current_user)):
    success, message = await db_connector.update_abstract_with_vips(data.doc_id, data.names)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.put('/api/document/{doc_id}/event')
async def link_document_event(doc_id: int, data: LinkEventRequest, user=Depends(get_current_user)):
    success, message = await db_connector.link_document_to_event(doc_id, data.event_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.get('/api/document/{doc_id}/event')
async def get_document_event(doc_id: int, user=Depends(get_current_user)):
    event_info = await db_connector.get_event_for_document(doc_id)
    # Return empty dict if None to match JSON behavior or handle strictly
    return event_info if event_info else {}

@router.post('/api/document/{doc_id}/security')
async def set_document_security(doc_id: str, data: SetTrusteesRequest, request: Request, user=Depends(get_current_user)):
    """
    Sets the trustees for a specific document or folder.
    Falls back to system token if user session token is expired.
    """
    # Try user's session token first; fall back to system token if missing/expired
    token = None
    try:
        token = get_session_token(request)
    except Exception:
        pass

    if not token:
        token = await run_in_threadpool(wsdl_client.dms_system_login)
        if not token:
            raise HTTPException(status_code=500, detail="Failed to authenticate with DMS.")

    success, message = await run_in_threadpool(
        wsdl_client.set_trustees,
        dst=token,
        doc_id=doc_id,
        library=data.library,
        trustees=data.trustees,
        security_enabled=data.security_enabled
    )

    # If user token failed, retry with a fresh system token
    if not success:
        logging.warning(f"set_trustees failed with user token for doc {doc_id}: {message}. Retrying with system token.")
        sys_token = await run_in_threadpool(wsdl_client.dms_system_login, True)
        if sys_token and sys_token != token:
            success, message = await run_in_threadpool(
                wsdl_client.set_trustees,
                dst=sys_token,
                doc_id=doc_id,
                library=data.library,
                trustees=data.trustees,
                security_enabled=data.security_enabled
            )

    if not success:
        logging.error(f"Failed to set trustees for doc {doc_id}: {message}")
        raise HTTPException(status_code=500, detail=message)

    return {"status": "success", "message": "Trustees updated successfully"}

@router.post('/api/documents/{doc_id}/rerun-analysis')
async def rerun_analysis(doc_id: int, user=Depends(get_current_user)):
    """
    Resets the document's TAGGING_QUEUE status to pending (1) and attempts to 0,
    then adds it back to the AI processing queue.
    """
    if not is_editor(user):
        raise HTTPException(status_code=403, detail="Not authorized to rerun analysis")

    try:
        await db_connector.reset_processing_attempts([doc_id])
    except Exception as reset_err:
        logging.warning(f"Could not reset TAGGING_QUEUE for doc {doc_id}: {reset_err}")

    processing_queue.enqueue_docnumbers([doc_id], source="rerun-analysis")
    
    return {"status": "success", "message": "Document analysis restarted."}

@router.get('/api/documents/review-status')
async def get_document_review_statuses(doc_ids: str, user=Depends(get_current_user)):
    """Fetches review statuses for a comma-separated list of document IDs.

    Read access is granted to ALL authenticated users (including Viewers).
    Viewers rely on these statuses client-side to filter the document grid so
    only "Completed" (verified) documents are visible. The write endpoints
    (set review status) remain editor-only. Gating this read on is_editor
    previously caused viewers to receive an empty statuses map, which left the
    Recent/Favorites/Folders tabs stuck on a loading skeleton and showed no
    documents — even when every document was marked Completed.
    """
    ids = [int(i.strip()) for i in doc_ids.split(',') if i.strip().isdigit()]
    if not ids:
        return {"statuses": {}}
    from database.documents import get_review_statuses
    statuses = await get_review_statuses(ids)
    return {"statuses": statuses}

@router.put('/api/documents/{doc_id}/review-status')
async def set_document_review_status(doc_id: int, data: ReviewStatusRequest, user=Depends(get_current_user)):
    """Sets the review status for a single document."""
    if not is_editor(user):
        raise HTTPException(status_code=403, detail="Only Editors can change review status.")
    if data.status not in ['verified', 'pending']:
        raise HTTPException(status_code=400, detail="Invalid status value.")
    from database.documents import set_review_status
    success = await set_review_status(doc_id, data.status)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update review status.")
    return {"status": "success", "message": "Review status updated."}
