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

    dst = wsdl_client.dms_system_login()
    if not dst:
        raise HTTPException(status_code=500, detail="Failed to authenticate with DMS")

    try:
        # Await the async WSDL/DB call
        contents = await wsdl_client.list_folder_contents(
            dst, parent_id, x_app_source, scope=scope, media_type=media_type, search_term=search
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
        # Assuming sync SOAP call
        success = wsdl_client.rename_document(dst, folder_id, data.name)
        if success:
            return {"message": "Renamed", "id": folder_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to rename")
    except AttributeError:
         raise HTTPException(status_code=500, detail="Rename function not implemented in WSDL client")