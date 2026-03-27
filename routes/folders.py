import logging
from fastapi import APIRouter, Request, HTTPException, Header, Depends
from typing import Optional
import wsdl_client
from schemas.folders import CreateFolderRequest, RenameFolderRequest
from utils.common import get_current_user
from database import user_data
import db_connector

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
            # Set security so only the creator can see the folder
            trustees = [
                {
                    'username': username,
                    'rights': 255,  # Full control
                    'flag': 2  # User (not group)
                }
            ]
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