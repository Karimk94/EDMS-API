from fastapi import APIRouter, Response, HTTPException, Header, Request, Depends
from fastapi.responses import FileResponse, StreamingResponse
from typing import Optional
import os
import time
import logging
import mimetypes
import threading
import db_connector
import wsdl_client
from utils.common import verify_editor, get_current_user

router = APIRouter()

# Per-doc-id locks so concurrent requests for the same missing thumbnail don't
# race to generate it multiple times in parallel.
_thumbnail_gen_locks: dict[int, threading.Lock] = {}
_thumbnail_gen_locks_guard = threading.Lock()


def _get_thumbnail_gen_lock(doc_id: int) -> threading.Lock:
    with _thumbnail_gen_locks_guard:
        lock = _thumbnail_gen_locks.get(doc_id)
        if lock is None:
            lock = threading.Lock()
            _thumbnail_gen_locks[doc_id] = lock
        return lock

@router.get('/api/image/{doc_id}')
async def api_get_image(doc_id: int, user=Depends(get_current_user)):
    dst = await db_connector.dms_system_login_async()
    if not dst:
        raise HTTPException(status_code=500, detail='DMS login failed.')

    image_data = await db_connector.get_media_content_from_dms_async(dst, doc_id)
    if image_data:
        data_bytes = bytes(image_data)
        try:
            from PIL import Image as PILImage
            with PILImage.open(io.BytesIO(data_bytes)) as pil_img:
                if getattr(pil_img, 'format', None) in ('HEIF', 'HEIC'):
                    out_io = io.BytesIO()
                    pil_img.convert("RGB").save(out_io, format="JPEG", quality=95)
                    return Response(content=out_io.getvalue(), media_type='image/jpeg')
        except Exception as e:
            logging.debug(f"Pillow check/conversion in api_get_image for doc {doc_id}: {e}")
        return Response(content=data_bytes, media_type='image/jpeg')
    raise HTTPException(status_code=404, detail='Image not found in EDMS.')

@router.get('/api/pdf/{doc_id}')
async def api_get_pdf(doc_id: int, user=Depends(get_current_user)):
    dst = await db_connector.dms_system_login_async()
    if not dst:
        raise HTTPException(status_code=500, detail='DMS login failed.')

    pdf_data = await db_connector.get_media_content_from_dms_async(dst, doc_id)
    if pdf_data:
        return Response(content=bytes(pdf_data), media_type='application/pdf')
    raise HTTPException(status_code=404, detail='PDF not found in EDMS.')

@router.get('/api/video/{doc_id}')
async def api_get_video(doc_id: int, user=Depends(get_current_user)):
    dst = await db_connector.dms_system_login_async()
    if not dst:
        raise HTTPException(status_code=500, detail='DMS login failed.')

    # ASYNC DB CALL
    original_filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, doc_id)
    if not original_filename:
        raise HTTPException(status_code=404, detail='Video metadata not found.')
    if media_type != 'video':
        raise HTTPException(status_code=400, detail='Not a video.')

    if not file_ext: file_ext = '.mp4'
    cached_video_path = os.path.join(db_connector.video_cache_dir, f"{doc_id}{file_ext}")

    if os.path.exists(cached_video_path):
        return FileResponse(cached_video_path)

    stream_details = await db_connector.get_dms_stream_details_async(dst, doc_id)
    if not stream_details:
        raise HTTPException(status_code=500, detail='Could not open stream.')

    stream_generator = db_connector.stream_and_cache_generator(
        obj_client=stream_details['obj_client'],
        stream_id=stream_details['stream_id'],
        content_id=stream_details['content_id'],
        final_cache_path=cached_video_path
    )

    mimetype, _ = mimetypes.guess_type(cached_video_path)
    return StreamingResponse(stream_generator, media_type=mimetype or "video/mp4")

@router.get('/cache/{filename}')
async def serve_cached_thumbnail(filename: str, user=Depends(get_current_user)):
    file_path = os.path.join(db_connector.thumbnail_cache_dir, filename)

    # Fast path: thumbnail already exists on disk -> serve it immediately.
    if os.path.exists(file_path):
        return FileResponse(file_path, headers={
            "Cache-Control": "public, max-age=86400, immutable",
            "ETag": f'"{filename}"',
        })

    # On-demand generation path: derive the doc_id from the filename (expected
    # form: "{doc_id}.jpg") and generate the thumbnail from the DMS media.
    # This mirrors the /api/temp_thumbnail/{doc_id} logic but persists into the
    # main thumbnail_cache_dir so subsequent requests hit the fast path.
    stem, _ext = os.path.splitext(filename)
    try:
        doc_id = int(stem)
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="File not found")

    # Re-check after acquiring the lock (another request may have just built it).
    gen_lock = _get_thumbnail_gen_lock(doc_id)
    with gen_lock:
        if os.path.exists(file_path):
            return FileResponse(file_path, headers={
                "Cache-Control": "public, max-age=86400, immutable",
                "ETag": f'"{filename}"',
            })

        dst = await db_connector.dms_system_login_async()
        if not dst:
            raise HTTPException(status_code=500, detail='DMS login failed.')

        try:
            original_filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, doc_id)
        except Exception as e:
            logging.error(f"Error resolving media info for doc {doc_id} during thumbnail on-demand gen: {e}", exc_info=True)
            raise HTTPException(status_code=404, detail="File not found")

        # Media types that have no thumbnail (office docs, archives, text, ...).
        if not media_type or media_type in ['excel', 'powerpoint', 'text', 'file', 'zip']:
            raise HTTPException(status_code=404, detail="File not found")

        try:
            media_bytes = await db_connector.get_media_content_from_dms_async(dst, doc_id)
        except Exception as e:
            logging.error(f"Error fetching media content for doc {doc_id} during thumbnail on-demand gen: {e}", exc_info=True)
            raise HTTPException(status_code=404, detail="File not found")

        if media_bytes:
            # is_temp=False so the result is persisted in thumbnail_cache_dir
            # (not the temp dir) and future requests hit the fast path.
            thumbnail_path = db_connector.create_thumbnail(doc_id, media_type, file_ext, media_bytes, is_temp=False)
            if thumbnail_path and os.path.exists(file_path):
                return FileResponse(file_path, headers={
                    "Cache-Control": "public, max-age=86400, immutable",
                    "ETag": f'"{filename}"',
                })

    raise HTTPException(status_code=404, detail="File not found")


@router.get('/api/thumbnail-exists/{doc_id}')
async def check_thumbnail_exists(doc_id: int, user=Depends(get_current_user)):
    """
    Checks whether a cached thumbnail exists on disk for the given document ID.
    Used by the frontend to detect when a thumbnail has been generated after
    processing completes, so it can bust the browser cache and re-request.
    """
    filename = f"{doc_id}.jpg"
    file_path = os.path.join(db_connector.thumbnail_cache_dir, filename)
    return {"exists": os.path.exists(file_path)}

@router.get('/api/temp_thumbnail/{doc_id}')
async def serve_temp_thumbnail(doc_id: int, user=Depends(get_current_user)):
    filename = f"{doc_id}.jpg"
    file_path = os.path.join(db_connector.temp_thumbnail_cache_dir, filename)
    
    if os.path.exists(file_path):
        if time.time() - os.path.getmtime(file_path) > 86400:
            try:
                os.remove(file_path)
            except OSError:
                pass
        else:
            return FileResponse(file_path)
            
    dst = await db_connector.dms_system_login_async()
    if not dst:
        raise HTTPException(status_code=500, detail='DMS login failed.')
        
    original_filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, doc_id)
    if not media_type or media_type in ['excel', 'powerpoint', 'text', 'file', 'zip']:
        raise HTTPException(status_code=404, detail='Thumbnail not applicable for this media type.')
        
    media_bytes = await db_connector.get_media_content_from_dms_async(dst, doc_id)
    if media_bytes:
        thumbnail_path = db_connector.create_thumbnail(doc_id, media_type, file_ext, media_bytes, is_temp=True)
        if thumbnail_path and os.path.exists(file_path):
            return FileResponse(file_path)
            
    raise HTTPException(status_code=404, detail="Failed to generate temporary thumbnail")

@router.post('/api/clear_cache', dependencies=[Depends(verify_editor)])
async def api_clear_cache():
    try:
        db_connector.clear_thumbnail_cache()
        db_connector.clear_video_cache()
        return {"message": "All caches cleared successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to clear cache.")

@router.get('/api/media_counts')
async def get_media_counts(
        request: Request,
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        scope: Optional[str] = None,
        user=Depends(get_current_user)
):
    try:
        username = user.get('username')
        
        counts = await db_connector.get_media_type_counts(app_source=x_app_source, scope=scope, username=username)
        if counts:
            return counts
        else:
            return {"images": 0, "videos": 0, "files": 0}
    except Exception as e:
        logging.error(f"Error in /api/media_counts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch media counts.")