from fastapi import APIRouter, Request, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from database import profilesearch
import logging
from utils.common import get_current_user
from utils.ttl_cache import TTLCache

router = APIRouter()
metadata_cache = TTLCache(default_ttl_seconds=300, max_items=512)


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
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=50)


@router.get('/api/profilesearch/scopes')
async def get_search_scopes(request: Request, user=Depends(get_current_user)):
    """Returns the available search scopes for the current user."""
    try:
        user_id = user.get('username')
        cache_key = f"profilesearch:scopes:{str(user_id).strip().lower()}"
        cached_value = metadata_cache.get(cache_key)
        if cached_value is not None:
            return {"scopes": cached_value}

        scopes = await profilesearch.fetch_search_scopes(user_id)
        metadata_cache.set(cache_key, scopes)
        return {"scopes": scopes}
    except Exception as e:
        logging.error(f"Error fetching search scopes: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch search scopes")


@router.get('/api/profilesearch/types')
async def get_search_types(request: Request, scope: Optional[str] = None, user=Depends(get_current_user)):
    """
    Returns the available search types for the current user.
    Optionally filtered by scope.
    """
    try:
        user_id = user.get('username')
        normalized_scope = (scope or 'all').strip().lower()
        cache_key = f"profilesearch:types:{str(user_id).strip().lower()}:{normalized_scope}"
        cached_value = metadata_cache.get(cache_key)
        if cached_value is not None:
            return {"types": cached_value}

        types = await profilesearch.fetch_search_types(user_id, scope=scope)
        metadata_cache.set(cache_key, types)
        return {"types": types}
    except Exception as e:
        logging.error(f"Error fetching search types: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch search types")


@router.post('/api/profilesearch/search')
async def search_documents_multi(request: Request, body: MultiSearchRequest, user=Depends(get_current_user)):
    """
    Multi-criteria search. Accepts scope, an array of criteria (AND logic),
    a global date range, and pagination.
    """
    try:
        user_id = user.get('username')

        # Normalize criteria and protect backend from broad accidental scans.
        normalized_criteria = []
        for c in body.criteria:
            trimmed_keyword = (c.keyword or "").strip()
            trimmed_match = (c.match_type or "like").strip()
            if not c.field_name:
                continue

            # Skip empty criteria rows produced by UI placeholders.
            if not trimmed_keyword:
                continue

            if len(trimmed_keyword) < 2 and trimmed_match in ["like", "startsWith"]:
                raise HTTPException(status_code=400, detail="Keyword must be at least 2 characters.")

            normalized_criteria.append({
                "field_name": c.field_name,
                "keyword": trimmed_keyword,
                "match_type": trimmed_match,
                "search_form": c.search_form,
                "search_field": c.search_field,
                "display_field": c.display_field,
            })

        if len(normalized_criteria) > 6:
            normalized_criteria = normalized_criteria[:6]

        has_date_filter = bool((body.date_from or "").strip() or (body.date_to or "").strip())
        if not normalized_criteria and not has_date_filter:
            raise HTTPException(status_code=400, detail="At least one search criterion or date filter is required.")

        documents, total_rows = await profilesearch.search_documents_multi(
            user_id=user_id,
            scope=body.scope,
            criteria=normalized_criteria,
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
    pageSize: int = 20,
    user=Depends(get_current_user)
):
    """Legacy single-criterion GET search (kept for backwards compatibility)."""
    try:
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
