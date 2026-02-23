from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional
from database import researcher
import logging

router = APIRouter()

@router.get('/api/researcher/types')
async def get_search_types(request: Request):
    """
    Returns the available search types for the current user.
    """
    try:
        user = request.session.get('user')
        if not user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        user_id = user.get('username')
        types = await researcher.fetch_search_types(user_id)
        return {"types": types}
    except Exception as e:
        logging.error(f"Error fetching search types: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch search types")

@router.get('/api/researcher/search')
async def search_documents(
    request: Request,
    form_name: str,
    field_name: str,
    keyword: Optional[str] = None,
    search_form: Optional[str] = None, # Metadata about the search form
    search_field: Optional[str] = None, 
    display_field: Optional[str] = None,
    match_type: Optional[str] = 'like',
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    pageSize: int = 20
):
    """
    Performs a dynamic search based on the Researcher criteria.
    """
    try:
        user = request.session.get('user')
        if not user:
             raise HTTPException(status_code=401, detail="Unauthorized")

        user_id = user.get('username')
        
        documents, total_rows = await researcher.search_documents(
            user_id=user_id,
            form_name=form_name,
            field_name=field_name,
            keyword=keyword,
            search_form=search_form,
            search_field=search_field,
            match_type=match_type,
            date_from=date_from,
            date_to=date_to,
            display_field=display_field,
            page=page,
            page_size=pageSize
        )
        
        total_pages = (total_rows + pageSize - 1) // pageSize if total_rows > 0 else 1
        
        return {
            "documents": documents,
            "page": page,
            "total_pages": total_pages,
            "total_documents": total_rows
        }

    except Exception as e:
        logging.error(f"Error in researcher search: {e}")
        raise HTTPException(status_code=500, detail="Search failed")
