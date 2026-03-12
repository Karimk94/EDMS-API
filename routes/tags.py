from fastapi import APIRouter, Request, HTTPException, Depends, Header
import db_connector
import api_client
from utils.common import verify_editor, get_current_user
from schemas.tags import AddPersonRequest, ToggleShortlistRequest, ProcessingStatusRequest, AddTagRequest

router = APIRouter()

from pydantic import BaseModel
from typing import List

class BatchTagsRequest(BaseModel):
    doc_ids: List[int]

@router.post('/api/tags/batch')
async def api_get_tags_batch(request: Request, data: BatchTagsRequest, lang: str = 'en', user=Depends(get_current_user)):
    """Fetch tags for multiple documents in a single request (eliminates N+1 calls)."""
    security_level = user.get('security_level', 'Viewer')

    if not data.doc_ids:
        return {"tags": {}}

    # Limit batch size to prevent abuse
    doc_ids = data.doc_ids[:50]
    tags_map = await db_connector.fetch_tags_for_documents_batch(doc_ids, lang=lang, security_level=security_level)
    # Convert int keys to string keys for JSON compatibility
    return {"tags": {str(k): v for k, v in tags_map.items()}}

@router.post('/api/add_person')
async def api_add_person(data: AddPersonRequest, user=Depends(get_current_user)):
    if not data.name or len(data.name.strip()) < 2:
        raise HTTPException(status_code=400, detail='Invalid data.')
    try:
        is_arabic = (data.lang == 'ar') or (not data.name.strip().isascii())
        if is_arabic:
            name_arabic = data.name.strip()
            name_english = api_client.translate_text(name_arabic)
            if not name_english:
                raise HTTPException(status_code=500, detail='Failed to translate.')
        else:
            name_english = data.name.strip()
            name_arabic = api_client.translate_text(name_english) or None
    except Exception as e:
        raise HTTPException(status_code=500, detail='Translation failed. Please try again.')

    success, message = await db_connector.add_person_to_lkp(name_english, name_arabic)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)

@router.get('/api/persons')
async def api_get_persons(page: int = 1, search: str = '', lang: str = 'en', user=Depends(get_current_user)):
    persons, total_rows = await db_connector.fetch_lkp_persons(page=page, search=search, lang=lang)
    return {'options': persons, 'hasMore': (page * 20) < total_rows}

@router.get('/api/tags')
async def api_get_tags(
        request: Request,
        lang: str = 'en',
        x_app_source: str = Header("unknown", alias="X-App-Source"),
        user=Depends(get_current_user)
):
    security_level = user.get('security_level', 'Viewer')

    tags = await db_connector.fetch_all_tags(
        lang=lang, security_level=security_level, app_source=x_app_source
    )
    return tags

@router.get('/api/tags/{doc_id}')
async def api_get_tags_for_document(doc_id: int, request: Request, lang: str = 'en', user=Depends(get_current_user)):
    security_level = user.get('security_level', 'Viewer')

    tags = await db_connector.fetch_tags_for_document(
        doc_id, lang=lang, security_level=security_level
    )
    return {"tags": tags}

@router.post('/api/tags/shortlist', dependencies=[Depends(verify_editor)])
async def api_toggle_shortlist(data: ToggleShortlistRequest):
    if not data.tag:
        raise HTTPException(status_code=400, detail='Tag is required')
    success, result = await db_connector.toggle_tag_shortlist(data.tag)
    if success:
        return result
    else:
        raise HTTPException(status_code=400, detail=result)

@router.post('/api/processing_status')
async def api_processing_status(data: ProcessingStatusRequest, user=Depends(get_current_user)):
    if not data.docnumbers:
        raise HTTPException(status_code=400, detail="Invalid data.")
    still_processing = await db_connector.check_processing_status(data.docnumbers)
    return {"processing": still_processing}

@router.post('/api/tags/{doc_id}')
async def api_add_tag(doc_id: int, data: AddTagRequest, user=Depends(get_current_user)):
    if not data.tag or len(data.tag.strip()) < 2:
        raise HTTPException(status_code=400, detail='Invalid tag.')
    try:
        is_arabic = not data.tag.isascii()
        if is_arabic:
            arabic_keyword = data.tag
            english_keyword = api_client.translate_text(data.tag)
        else:
            english_keyword = data.tag
            arabic_keyword = api_client.translate_text(data.tag)

        if not english_keyword or not arabic_keyword:
            raise HTTPException(status_code=500, detail='Translation failed.')

        await db_connector.insert_keywords_and_tags(
            doc_id, [{'english': english_keyword, 'arabic': arabic_keyword}]
        )
        return {'message': 'Tag added successfully.'}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail='Failed to add tag.')

@router.delete('/api/tags/{doc_id}/{tag}')
async def api_delete_tag(doc_id: int, tag: str, user=Depends(get_current_user)):
    success, message = await db_connector.delete_tag_from_document(doc_id, tag)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=404, detail=message)