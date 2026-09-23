import io
import logging
import os
import zipfile

from fastapi import APIRouter, Request, HTTPException, Header, Depends
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Optional
import wsdl_client
from schemas.folders import CreateFolderRequest, RenameFolderRequest, MoveItemsRequest
from utils.common import get_current_user, build_content_disposition
from database import user_data, folders as db_folders
import db_connector

ZIP_PART_SIZE_LIMIT_BYTES = 500 * 1024 * 1024  # 500 MB per partition
ZIP_TOTAL_SIZE_LIMIT_BYTES = 1 * 1024 * 1024 * 1024  # 1 GB total limit unless bypassed

router = APIRouter()

async def _resolve_edms_user_id(username):
    """Helper to resolve the EDMS user ID from a username for quota operations."""
    if not username:
        return None
    try:
        people_id = await db_connector.get_user_system_id(username)
        if people_id:
            return await user_data.get_edms_user_id(people_id)
    except Exception as e:
        logging.warning(f"Could not resolve EDMS user ID for {username}: {e}")
    return None

async def _get_file_size_from_dms(dst, doc_number):
    """
    Gets the file size of a document from the DMS by reading its content.
    Returns the size in bytes, or 0 if the size cannot be determined.
    """
    try:
        content_bytes = await db_connector.get_media_content_from_dms_async(dst, doc_number)
        if content_bytes:
            return len(content_bytes)
    except Exception as e:
        logging.warning(f"Could not determine file size for doc {doc_number}: {e}")
    return 0

async def _get_folder_direct_file_size_sum(dst, folder_id):
    """
    Sums the sizes of direct file children for a folder.
    Subfolders are handled separately by the delete guard.
    """
    total_size = 0

    try:
        folder_files = await db_folders.get_folder_files(folder_id)
        for file_item in folder_files:
            file_id = file_item.get('id')
            if file_id:
                total_size += await _get_file_size_from_dms(dst, file_id)
    except Exception as e:
        logging.warning(f"Could not calculate direct file size sum for folder {folder_id}: {e}")

    return total_size

@router.get('/api/folders')
async def api_list_folders(
        request: Request,
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        scope: Optional[str] = None,
        media_type: Optional[str] = None,
        search: Optional[str] = None,
        parent_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 60,
        user=Depends(get_current_user)
):
    if parent_id in ['null', 'undefined', '']:
        parent_id = None

    # Get the current logged-in user's username for permission filtering
    username = user.get('username')

    # Clamp pagination parameters to sane bounds
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 60
    if page_size > 200:
        page_size = 200

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS")

    try:
        contents = await wsdl_client.list_folder_contents(
            dst, parent_id, x_app_source, scope=scope, media_type=media_type, search_term=search, username=username
        )

        # --- Server-side pagination ---
        # Standard folders (Images/Videos/Files) are navigational containers and
        # must always be returned in full so the UI can render them. Only the
        # regular user files/folders are paginated.
        standard_items = [c for c in contents if c.get('is_standard')]
        regular_items = [c for c in contents if not c.get('is_standard')]

        total_items = len(regular_items)
        total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1

        offset = (page - 1) * page_size
        if offset >= total_items and total_items > 0:
            # Requested page is out of range — return the last available page
            page = total_pages
            offset = (page - 1) * page_size

        paginated_regular = regular_items[offset: offset + page_size]
        paged_contents = standard_items + paginated_regular

        return {
            "contents": paged_contents,
            "page": page,
            "page_size": page_size,
            "total_items": total_items,
            "total_pages": total_pages
        }
    except Exception as e:
        logging.error(f"Error listing folders: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list folder contents.")


@router.get('/api/folders/processing-status')
async def api_get_folders_processing_status(
        folder_ids: Optional[str] = None,
        user=Depends(get_current_user)
):
    """
    Batch endpoint: returns processing summaries for multiple folders.
    Query param: folder_ids=1234,5678,9012 (comma-separated folder DOCNUMBERs)

    Returns:
        { "folders": { "1234": { total, processing, done, is_complete, processing_doc_ids }, ... } }
    """
    if not folder_ids:
        return {"folders": {}}

    try:
        folder_id_list = [fid.strip() for fid in folder_ids.split(',') if fid.strip()]
        if not folder_id_list:
            return {"folders": {}}

        summaries = await db_folders.get_folders_processing_summary_batch(folder_id_list)
        return {"folders": summaries}
    except Exception as e:
        logging.error(f"Error getting batch folder processing status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get folder processing status.")


@router.get('/api/folders/{folder_id}/processing-status')
async def api_get_folder_processing_status(
        folder_id: str,
        user=Depends(get_current_user)
):
    """
    Returns the processing summary for a single folder's direct file children.

    Returns:
        { folder_id, total, processing, done, is_complete, processing_doc_ids }
    """
    if not folder_id:
        raise HTTPException(status_code=400, detail="folder_id is required.")

    try:
        summary = await db_folders.get_folder_processing_summary(folder_id)
        return summary
    except Exception as e:
        logging.error(f"Error getting folder processing status for {folder_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get folder processing status.")


@router.post('/api/folders')
async def api_create_folder(request: Request, data: CreateFolderRequest, user=Depends(get_current_user)):
    username = user.get('username')

    parent_id = data.parent_id
    if not parent_id or str(parent_id).strip() == "":
        parent_id = None

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    try:
        # wsdl_client.create_dms_folder is synchronous (SOAP only), so no await needed
        new_folder_id = await run_in_threadpool(
            wsdl_client.create_dms_folder,
            dst,
            data.name,
            data.description,
            parent_id,
            username
        )
        if new_folder_id:
            # Build trustee list: start from parent folder's trustees then add/ensure creator
            trustees = []
            if parent_id:
                try:
                    parent_trustees = await run_in_threadpool(wsdl_client.get_object_trustees, dst, str(parent_id))
                    if parent_trustees:
                        trustees = list(parent_trustees)
                except Exception as te:
                    logging.warning(f"Could not fetch parent trustees for folder {parent_id}: {te}")

            # Ensure the creator is present with full control
            creator_in_list = any(
                str(t.get('username', '')).upper() == str(username).upper()
                for t in trustees
            )
            if not creator_in_list:
                trustees.append({'username': username, 'rights': 255, 'flag': 2})

            success, message = await run_in_threadpool(
                wsdl_client.set_trustees,
                dst,
                str(new_folder_id),
                'RTA_MAIN',
                trustees,
                '1'
            )
            if not success:
                logging.warning(f"Failed to set security on folder {new_folder_id}: {message}")

            return {"message": "Folder created", "folder_id": new_folder_id, "name": data.name}
        else:
            raise HTTPException(status_code=500, detail="Failed to create folder")
    except Exception as e:
        logging.error(f"Error creating folder: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create folder.")

@router.get('/api/folders/{folder_id}/preview-thumbnails')
async def api_get_folder_preview_thumbnails(folder_id: str, user=Depends(get_current_user)):
    try:
        preview_ids = await db_folders.get_folder_preview_ids(folder_id, limit=4)
        return {"thumbnails": preview_ids}
    except Exception as e:
        logging.error(f"Error fetching preview thumbnails for folder {folder_id}: {e}", exc_info=True)
        return {"thumbnails": []}

@router.put('/api/folders/{folder_id}')
async def api_rename_folder(folder_id: str, request: Request, data: RenameFolderRequest, user=Depends(get_current_user)):

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    try:
        if not hasattr(data, 'system_id') or not data.system_id:
            raise HTTPException(status_code=400, detail="system_id is required for renaming")

        success = await run_in_threadpool(wsdl_client.rename_folder_display, dst, data.system_id, data.name)

        if success:
            return {"message": "Renamed", "id": folder_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to rename")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in rename: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to rename folder.")

@router.delete('/api/folders/{folder_id}')
async def api_delete_folder(folder_id: str, request: Request, force: bool = False, user=Depends(get_current_user)):
    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    # --- Resolve user for quota restoration ---
    username = user.get('username')
    edms_user_id = await _resolve_edms_user_id(username)

    folder_info = await db_folders.get_folder_by_id(folder_id)
    is_folder = folder_info is not None

    # --- Get total bytes to restore before deletion ---
    bytes_freed = 0
    if is_folder:
        child_folders = await db_folders.get_folder_children(folder_id)
        if child_folders:
            raise HTTPException(
                status_code=409,
                detail="This folder cannot be deleted because it contains subfolders."
            )

        bytes_freed = await _get_folder_direct_file_size_sum(dst, folder_id)
    else:
        bytes_freed = await _get_file_size_from_dms(dst, folder_id)

    # logging.info(f"Delete {folder_id}: is_folder={is_folder}, edms_user_id={edms_user_id}, bytes_freed={bytes_freed}")

    try:
        # Try standard delete first
        success, message = await run_in_threadpool(wsdl_client.delete_document, dst, folder_id)

        if success:
            if edms_user_id and bytes_freed > 0:
                restore_ok, restore_msg = await user_data.restore_user_quota(edms_user_id, bytes_freed)
                # logging.info(f"Quota restore for user {edms_user_id} after deleting {folder_id}: {restore_ok}, {restore_msg}")
            else:
                logging.warning(f"Skipped quota restore for {folder_id}: edms_user_id={edms_user_id}, bytes_freed={bytes_freed}")
            return {"message": "Folder deleted", "id": folder_id}

        # Check for specific "Referenced by" error to prompt user
        if "referenced by one or more folders" in str(message).lower() or "referenced" in str(message).lower():
            total_bytes_deleted = await wsdl_client.delete_folder_contents(dst, folder_id, delete_root=True)

            if total_bytes_deleted is False:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to clear folder contents. Some items may still be referenced elsewhere. Check logs for details."
                )

            if is_folder:
                bytes_freed = total_bytes_deleted if isinstance(total_bytes_deleted, int) else 0

            if edms_user_id and bytes_freed > 0:
                await user_data.restore_user_quota(edms_user_id, bytes_freed)

            return {"message": "Folder and contents deleted", "id": folder_id}

        raise HTTPException(status_code=500, detail="Failed to delete folder. It may be referenced elsewhere.")

    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"Error in api_delete_folder: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete folder.")


@router.post('/api/folders/move-items')
async def api_move_items_to_folder(data: MoveItemsRequest, request: Request, user=Depends(get_current_user)):
    item_ids = [str(i).strip() for i in (data.item_ids or []) if str(i).strip()]
    if not item_ids:
        raise HTTPException(status_code=400, detail='item_ids is required')

    destination_parent_id = data.destination_parent_id
    if destination_parent_id in ['null', 'undefined', '']:
        destination_parent_id = None

    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail='Failed to authenticate with DMS')

    moved_ids = []
    failed = []

    try:
        for item_id in item_ids:
            display_name = None
            if data.item_names and isinstance(data.item_names, dict):
                display_name = data.item_names.get(item_id)

            if not display_name:
                try:
                    filename, _, _ = await db_connector.get_media_info_from_dms(dst, int(item_id))
                    display_name = filename
                except Exception:
                    display_name = None

            ok, message = await run_in_threadpool(
                wsdl_client.move_item_to_parent,
                dst,
                item_id,
                destination_parent_id,
                display_name
            )

            if ok:
                moved_ids.append(item_id)
            else:
                failed.append({'id': item_id, 'error': message or 'Failed to move'})

        return {
            'message': 'Move completed',
            'moved_count': len(moved_ids),
            'failed_count': len(failed),
            'moved_ids': moved_ids,
            'failed': failed,
        }
    except Exception as e:
        logging.error(f"Error in api_move_items_to_folder: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail='Failed to move items.')


@router.get('/api/folders/{folder_id}/download-zip')
async def api_download_folder_zip(
        folder_id: str,
        request: Request,
        part: Optional[int] = None,
        user=Depends(get_current_user)
):
    """
    Downloads the direct file contents of a folder as a ZIP archive.
    Subfolders are intentionally skipped — only files in the immediate folder are included.

    If the total size exceeds 500 MB, the response depends on the `part` parameter:
    - Without `part`: returns a JSON manifest listing how many parts are available.
    - With `part=N`: returns the Nth partition as a ZIP file.

    For folders ≤ 500 MB, always returns a single ZIP (ignores `part`).
    """
    dst = await run_in_threadpool(wsdl_client.dms_system_login)
    if not dst:
        raise HTTPException(status_code=500, detail='Failed to authenticate with DMS')

    try:
        root_info = await db_folders.get_folder_by_docnumber(folder_id)
        root_name = root_info['name'] if root_info else folder_id
    except Exception:
        root_name = folder_id

    # Only collect direct files — subfolders are skipped by design
    try:
        direct_files = await db_folders.get_folder_files(folder_id)
    except Exception as e:
        logging.error(f"Failed to list files in folder {folder_id}: {e}")
        raise HTTPException(status_code=500, detail='Failed to list folder contents')

    file_entries: list[tuple[str, str]] = [
        (f['id'], f['name']) for f in direct_files
    ]

    if not file_entries:
        raise HTTPException(status_code=404, detail='Folder has no files to download')

    # --- Download all file contents and measure sizes ---
    downloaded_files: list[tuple[str, bytes, str]] = []  # (archive_path, content, doc_id)
    total_bytes = 0

    for doc_id, archive_path in file_entries:
        try:
            content, dms_filename = await run_in_threadpool(wsdl_client.get_image_by_docnumber, dst, doc_id)
        except Exception as e:
            logging.warning(f"Skipping doc {doc_id} in ZIP (download error): {e}")
            continue

        if not content:
            continue

        # Ensure the archive path has the correct file extension from DMS
        _, dms_ext = os.path.splitext(dms_filename or '')
        if dms_ext:
            base, existing_ext = os.path.splitext(archive_path)
            if existing_ext.lower() != dms_ext.lower():
                archive_path = base + dms_ext

        total_bytes += len(content)
        downloaded_files.append((archive_path, bytes(content), doc_id))

    if not downloaded_files:
        raise HTTPException(status_code=404, detail='No files could be downloaded from this folder')

    # --- 1 GB Total Limit Check ---
    allow_large = user.get('LARGE_DOWNLOADS', False)
    if total_bytes > ZIP_TOTAL_SIZE_LIMIT_BYTES and not allow_large:
        raise HTTPException(
            status_code=413,
            detail='Not allowed to download the folder because it is bigger than 1 GB. If you want to enable this feature, contact the system administrator.'
        )

    safe_name = root_name.replace('"', '').replace('/', '_').replace('\\', '_') or folder_id

    # --- Single ZIP (total ≤ 500 MB) ---
    if total_bytes <= ZIP_PART_SIZE_LIMIT_BYTES:
        zip_buffer = io.BytesIO()
        try:
            with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                for archive_path, content, _ in downloaded_files:
                    zf.writestr(archive_path, content)
        except Exception as e:
            logging.error(f"Error building ZIP for folder {folder_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail='Failed to build ZIP archive')

        zip_buffer.seek(0)
        content_disposition = build_content_disposition(f'{safe_name}.zip', 'attachment')

        try:
            username = request.session['user'].get('username')
            system_id = await db_connector.get_user_system_id(username)
            if system_id:
                from database.activity import log_activity
                await log_activity(int(folder_id), 'Download', int(system_id))
        except Exception as e:
            logging.error(f"Failed to log folder download activity: {e}")

        return StreamingResponse(
            iter([zip_buffer.read()]),
            media_type='application/zip',
            headers={'Content-Disposition': content_disposition}
        )

    # --- Multipart: partition files into ~500 MB chunks ---
    parts: list[list[tuple[str, bytes]]] = []
    current_part: list[tuple[str, bytes]] = []
    current_part_size = 0

    for archive_path, content, _ in downloaded_files:
        file_size = len(content)

        # If adding this file would exceed the limit and current part is non-empty, start a new part
        if current_part and (current_part_size + file_size) > ZIP_PART_SIZE_LIMIT_BYTES:
            parts.append(current_part)
            current_part = []
            current_part_size = 0

        current_part.append((archive_path, content))
        current_part_size += file_size

    # Don't forget the last part
    if current_part:
        parts.append(current_part)

    total_parts = len(parts)

    # --- No part requested: return manifest ---
    if part is None:
        manifest_parts = []
        for i, p in enumerate(parts):
            part_size = sum(len(c) for _, c in p)
            manifest_parts.append({
                'part': i + 1,
                'file_count': len(p),
                'size_bytes': part_size
            })
        return JSONResponse({
            'multipart': True,
            'total_parts': total_parts,
            'folder_name': safe_name,
            'total_size_bytes': total_bytes,
            'parts': manifest_parts
        })

    # --- Part requested: validate and return that partition ---
    if part < 1 or part > total_parts:
        raise HTTPException(status_code=400, detail=f'Invalid part number. Must be between 1 and {total_parts}.')

    selected_files = parts[part - 1]
    zip_buffer = io.BytesIO()
    try:
        with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for archive_path, content in selected_files:
                zf.writestr(archive_path, content)
    except Exception as e:
        logging.error(f"Error building ZIP part {part} for folder {folder_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail='Failed to build ZIP archive')

    zip_buffer.seek(0)
    part_filename = f'{safe_name}_Part{part}of{total_parts}.zip'
    content_disposition = build_content_disposition(part_filename, 'attachment')

    try:
        username = request.session['user'].get('username')
        system_id = await db_connector.get_user_system_id(username)
        if system_id:
            from database.activity import log_activity
            await log_activity(int(folder_id), 'Download', int(system_id))
    except Exception as e:
        logging.error(f"Failed to log folder part download activity: {e}")

    return StreamingResponse(
        iter([zip_buffer.read()]),
        media_type='application/zip',
        headers={'Content-Disposition': content_disposition}
    )

from pydantic import BaseModel
class ReviewStatusRequest(BaseModel):
    status: str

@router.put('/api/folders/{folder_id}/review-status')
async def api_set_folder_review_status(folder_id: str, data: ReviewStatusRequest, user=Depends(get_current_user)):
    """Sets the review status for all direct child files of a folder. Only Editors/Admins can do this."""
    from utils.common import is_editor
    
    if not is_editor(user):
        raise HTTPException(status_code=403, detail="Only Editors can change review status.")
        
    if data.status not in ['verified', 'pending']:
        raise HTTPException(status_code=400, detail="Invalid status value.")
        
    from database.documents import set_folder_review_status
    updated_count = await set_folder_review_status(folder_id, data.status)
    
    return {"status": "success", "message": f"Review status updated for {updated_count} items"}