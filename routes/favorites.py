from fastapi import APIRouter, Request, HTTPException, Header
import db_connector

router = APIRouter()

@router.post('/api/favorites/{doc_id}')
async def add_favorite_route(doc_id: int, request: Request):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_id = user.get('username')
    success, message = await db_connector.add_favorite(user_id, doc_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.delete('/api/favorites/{doc_id}')
async def remove_favorite_route(doc_id: int, request: Request):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_id = user.get('username')
    success, message = await db_connector.remove_favorite(user_id, doc_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.get('/api/favorites')
async def get_favorites_route(
        request: Request,
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        page: int = 1,
        pageSize: int = 20
):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_id = user.get('username')
    documents, total_rows = await db_connector.get_favorites(
        user_id, page, pageSize, app_source=x_app_source
    )

    total_pages = (total_rows + pageSize - 1) // pageSize if total_rows > 0 else 1

    return {
        "documents": documents,
        "page": page,
        "total_pages": total_pages,
        "total_documents": total_rows
    }