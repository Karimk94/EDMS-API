import logging
from fastapi import APIRouter, Request, HTTPException, Header
from typing import Optional
import wsdl_client
from schemas.folders import CreateFolderRequest, RenameFolderRequest

router = APIRouter()

@router.get('/api/folders')
async def api_list_folders(
        request: Request,
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        scope: Optional[str] = None,
        media_type: Optional[str] = None,
        search: Optional[str] = None,
        parent_id: Optional[str] = None
):
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if parent_id in ['null', 'undefined', '']:
        parent_id = None

    # Get the current logged-in user's username for permission filtering
    username = request.session['user'].get('username')

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS")

    try:
        contents = await wsdl_client.list_folder_contents(
            dst, parent_id, x_app_source, scope=scope, media_type=media_type, search_term=search, username=username
        )
        return {"contents": contents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/api/folders')
async def api_create_folder(request: Request, data: CreateFolderRequest):
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    username = request.session['user'].get('username')

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
        raise HTTPException(status_code=500, detail=str(e))

@router.put('/api/folders/{folder_id}')
async def api_rename_folder(folder_id: str, request: Request, data: RenameFolderRequest):
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

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
        raise HTTPException(status_code=500, detail=str(e))

@router.delete('/api/folders/{folder_id}')
async def api_delete_folder(folder_id: str, request: Request, force: bool = False):
    if 'user' not in request.session:
        raise HTTPException(status_code=401, detail="Unauthorized")

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate")

    try:
        # Try standard delete first
        success, message = wsdl_client.delete_document(dst, folder_id)

        if success:
            return {"message": "Folder deleted", "id": folder_id}

        # Check for specific "Referenced by" error to prompt user
        if "referenced by one or more folders" in message.lower() or "referenced" in message.lower():
            if not force:
                raise HTTPException(
                    status_code=409,
                    detail=f"Folder is not empty or contains referenced items. Use force delete to remove all contents. Error: {message}"
                )
            else:
                # User confirmed Force Delete

                contents_deleted = await wsdl_client.delete_folder_contents(dst, folder_id, delete_root=True)

                if not contents_deleted:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to clear folder contents. Some items may still be referenced elsewhere. Check logs for details."
                    )

                return {"message": "Folder and contents deleted", "id": folder_id}

        # Other errors
        raise HTTPException(status_code=500, detail=f"Delete failed: {message}")

    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"Error in api_delete_folder: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))