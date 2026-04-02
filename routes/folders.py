import io
import logging
import os
import zipfile

from fastapi import APIRouter, Request, HTTPException, Header, Depends
from fastapi.responses import StreamingResponse
from typing import Optional
import wsdl_client
from schemas.folders import CreateFolderRequest, RenameFolderRequest, MoveItemsRequest
from utils.common import get_current_user
from database import user_data, folders as db_folders
import db_connector

ZIP_SIZE_LIMIT_BYTES = 100 * 1024 * 1024  # 100 MB

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

def _get_file_size_from_dms(dst, doc_number):
    """
    Gets the file size of a document from the DMS by reading its content.
    Returns the size in bytes, or 0 if the size cannot be determined.
    """
    try:
        content_bytes = db_connector.get_media_content_from_dms(dst, doc_number)
        if content_bytes:
            return len(content_bytes)
    except Exception as e:
        logging.warning(f"Could not determine file size for doc {doc_number}: {e}")
    return 0

@router.get('/api/folders')
async def api_list_folders(
        request: Request,
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        scope: Optional[str] = None,
        media_type: Optional[str] = None,
        search: Optional[str] = None,
        parent_id: Optional[str] = None,
        user=Depends(get_current_user)
):
    if parent_id in ['null', 'undefined', '']:
        parent_id = None

    # Get the current logged-in user's username for permission filtering
    username = user.get('username')

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS")

    try:
        contents = await wsdl_client.list_folder_contents(
            dst, parent_id, x_app_source, scope=scope, media_type=media_type, search_term=search, username=username
        )
        return {"contents": contents}
    except Exception as e:
        logging.error(f"Error listing folders: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list folder contents.")

@router.post('/api/folders')
async def api_create_folder(request: Request, data: CreateFolderRequest, user=Depends(get_current_user)):
    username = user.get('username')

    parent_id = data.parent_id
    if not parent_id or str(parent_id).strip() == "":
        parent_id = None

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    try:
        # wsdl_client.create_dms_folder is synchronous (SOAP only), so no await needed
        new_folder_id = wsdl_client.create_dms_folder(
            dst=dst,
            folder_name=data.name,
            description=data.description,
            parent_id=parent_id,
            user_id=username
        )
        if new_folder_id:
            # Build trustee list: start from parent folder's trustees then add/ensure creator
            trustees = []
            if parent_id:
                try:
                    parent_trustees = wsdl_client.get_object_trustees(dst, str(parent_id))
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

            success, message = wsdl_client.set_trustees(
                dst=dst,
                doc_id=str(new_folder_id),
                library='RTA_MAIN',
                trustees=trustees,
                security_enabled='1'
            )
            if not success:
                logging.warning(f"Failed to set security on folder {new_folder_id}: {message}")

            return {"message": "Folder created", "folder_id": new_folder_id, "name": data.name}
        else:
            raise HTTPException(status_code=500, detail="Failed to create folder")
    except Exception as e:
        logging.error(f"Error creating folder: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create folder.")

@router.put('/api/folders/{folder_id}')
async def api_rename_folder(folder_id: str, request: Request, data: RenameFolderRequest, user=Depends(get_current_user)):

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    try:
        if not hasattr(data, 'system_id') or not data.system_id:
            raise HTTPException(status_code=400, detail="system_id is required for renaming")

        success = wsdl_client.rename_folder_display(dst, data.system_id, data.name)

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
    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    # --- Resolve user for quota restoration ---
    username = user.get('username')
    edms_user_id = await _resolve_edms_user_id(username)

    # --- Get file size before deletion (for single file items) ---
    file_size = _get_file_size_from_dms(dst, folder_id)

    try:
        # Try standard delete first
        success, message = wsdl_client.delete_document(dst, folder_id)

        if success:
            # --- Restore Quota for single item ---
            if edms_user_id and file_size > 0:
                restore_ok, restore_msg = await user_data.restore_user_quota(edms_user_id, file_size)
            return {"message": "Folder deleted", "id": folder_id}

        # Check for specific "Referenced by" error to prompt user
        if "referenced by one or more folders" in message.lower() or "referenced" in message.lower():
            # Automatically proceed to force delete without confirmation
            total_bytes_deleted = await wsdl_client.delete_folder_contents(dst, folder_id, delete_root=True)

            if total_bytes_deleted is False:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to clear folder contents. Some items may still be referenced elsewhere. Check logs for details."
                )

            # --- Restore Quota for all recursively deleted files ---
            # total_bytes_deleted only includes children sizes from recursive traversal.
            # file_size (measured above) covers the root item itself (0 for actual folders).
            bytes_freed = total_bytes_deleted if isinstance(total_bytes_deleted, int) else 0
            bytes_freed += file_size
            if edms_user_id and bytes_freed > 0:
                restore_ok, restore_msg = await user_data.restore_user_quota(edms_user_id, bytes_freed)

            return {"message": "Folder and contents deleted", "id": folder_id}

        # Other errors
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

    dst = wsdl_client.dms_system_login()
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

            ok, message = wsdl_client.move_item_to_parent(
                dst=dst,
                doc_number=item_id,
                new_parent_id=destination_parent_id,
                display_name=display_name,
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
async def api_download_folder_zip(folder_id: str, request: Request, user=Depends(get_current_user)):
    """
    Downloads the direct file contents of a folder as a ZIP archive.
    Subfolders are intentionally skipped — only files in the immediate folder are included.
    Rejects the request if the total uncompressed size exceeds 100 MB.
    """
    dst = wsdl_client.dms_system_login()
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

    # Download all files and build the ZIP in memory, guarding the 100 MB limit
    zip_buffer = io.BytesIO()
    total_bytes = 0

    try:
        with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for doc_id, archive_path in file_entries:
                try:
                    content, dms_filename = wsdl_client.get_image_by_docnumber(dst, doc_id)
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
                if total_bytes > ZIP_SIZE_LIMIT_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail='Folder exceeds the 100 MB download limit. Please download individual files instead.'
                    )

                zf.writestr(archive_path, bytes(content))
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error building ZIP for folder {folder_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail='Failed to build ZIP archive')

    zip_buffer.seek(0)
    safe_name = root_name.replace('"', '').replace('/', '_').replace('\\', '_') or folder_id
    content_disposition = f'attachment; filename="{safe_name}.zip"'

    return StreamingResponse(
        iter([zip_buffer.read()]),
        media_type='application/zip',
        headers={'Content-Disposition': content_disposition}
    )