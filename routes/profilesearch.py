from fastapi import APIRouter, Request, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from database import profilesearch
import logging

router = APIRouter()


class SearchCriterion(BaseModel):
    field_name: str
    keyword: str = ""
    match_type: str = "like"
    search_form: str = ""
    search_field: str = ""
    display_field: str = ""


class MultiSearchRequest(BaseModel):
    scope: str = "0"
    criteria: List[SearchCriterion]
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: int = 1
    page_size: int = 20


@router.get('/api/profilesearch/scopes')
async def get_search_scopes(request: Request):
    """Returns the available search scopes for the current user."""
    try:
        user = request.session.get('user')
        if not user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        user_id = user.get('username')
        scopes = await profilesearch.fetch_search_scopes(user_id)
        return {"scopes": scopes}
    except Exception as e:
        logging.error(f"Error fetching search scopes: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch search scopes")


@router.get('/api/profilesearch/types')
async def get_search_types(request: Request, scope: Optional[str] = None):
    """
    Returns the available search types for the current user.
    Optionally filtered by scope.
    """
    try:
        user = request.session.get('user')
        if not user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        user_id = user.get('username')
        types = await profilesearch.fetch_search_types(user_id, scope=scope)
        return {"types": types}
    except Exception as e:
        logging.error(f"Error fetching search types: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch search types")


@router.post('/api/profilesearch/search')
async def search_documents_multi(request: Request, body: MultiSearchRequest):
    """
    Multi-criteria search. Accepts scope, an array of criteria (AND logic),
    a global date range, and pagination.
    """
    try:
        user = request.session.get('user')
        if not user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        user_id = user.get('username')

        criteria_dicts = [c.dict() for c in body.criteria]

        documents, total_rows = await profilesearch.search_documents_multi(
            user_id=user_id,
            scope=body.scope,
            criteria=criteria_dicts,
            date_from=body.date_from,
            date_to=body.date_to,
            page=body.page,
            page_size=body.page_size
        )

        total_pages = (total_rows + body.page_size - 1) // body.page_size if total_rows > 0 else 1

        return {
            "documents": documents,
            "page": body.page,
            "total_pages": total_pages,
            "total_documents": total_rows
        }

    except Exception as e:
        logging.error(f"Error in profilesearch search: {e}")
        raise HTTPException(status_code=500, detail="Search failed")


# Keep legacy GET endpoint for backwards compatibility
@router.get('/api/profilesearch/search')
async def search_documents_legacy(
    request: Request,
    form_name: str,
    field_name: str,
    keyword: Optional[str] = None,
    search_form: Optional[str] = None,
    search_field: Optional[str] = None,
    display_field: Optional[str] = None,
    match_type: Optional[str] = 'like',
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    pageSize: int = 20
):
    """Legacy single-criterion GET search (kept for backwards compatibility)."""
    try:
        user = request.session.get('user')
        if not user:
             raise HTTPException(status_code=401, detail="Unauthorized")

        user_id = user.get('username')

        documents, total_rows = await profilesearch.search_documents(
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
        logging.error(f"Error in profilesearch search: {e}")
        raise HTTPException(status_code=500, detail="Search failed")
