from fastapi import APIRouter, Request, HTTPException, Depends, Header
from typing import Optional, List
from pydantic import BaseModel
import db_connector
import api_client
from utils.common import verify_editor

router = APIRouter()


# --- Pydantic Models ---
class AddPersonRequest(BaseModel):
    name: str
    lang: str = 'en'


class ToggleShortlistRequest(BaseModel):
    tag: str


class ProcessingStatusRequest(BaseModel):
    docnumbers: List[int]


class AddTagRequest(BaseModel):
    tag: str


# --- Routes ---

@router.post('/api/add_person')
def api_add_person(data: AddPersonRequest):
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
        raise HTTPException(status_code=500, detail=f'Translation error: {e}')

    success, message = db_connector.add_person_to_lkp(name_english, name_arabic)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=500, detail=message)


@router.get('/api/persons')
def api_get_persons(page: int = 1, search: str = '', lang: str = 'en'):
    persons, total_rows = db_connector.fetch_lkp_persons(page=page, search=search, lang=lang)
    return {'options': persons, 'hasMore': (page * 20) < total_rows}


@router.get('/api/tags')
def api_get_tags(
        request: Request,
        lang: str = 'en',
        x_app_source: str = Header("unknown", alias="X-App-Source")
):
    user = request.session.get('user')
    security_level = user.get('security_level', 'Viewer') if user else 'Viewer'

    tags = db_connector.fetch_all_tags(
        lang=lang, security_level=security_level, app_source=x_app_source
    )
    return tags


@router.get('/api/tags/{doc_id}')
def api_get_tags_for_document(doc_id: int, request: Request, lang: str = 'en'):
    user = request.session.get('user')
    security_level = user.get('security_level', 'Viewer') if user else 'Viewer'

    tags = db_connector.fetch_tags_for_document(
        doc_id, lang=lang, security_level=security_level
    )
    return {"tags": tags}


@router.post('/api/tags/shortlist', dependencies=[Depends(verify_editor)])
def api_toggle_shortlist(data: ToggleShortlistRequest):
    if not data.tag:
        raise HTTPException(status_code=400, detail='Tag is required')
    success, result = db_connector.toggle_tag_shortlist(data.tag)
    if success:
        return result
    else:
        raise HTTPException(status_code=400, detail=result)


@router.post('/api/processing_status')
def api_processing_status(data: ProcessingStatusRequest):
    if not data.docnumbers:
        raise HTTPException(status_code=400, detail="Invalid data.")
    still_processing = db_connector.check_processing_status(data.docnumbers)
    return {"processing": still_processing}


@router.post('/api/tags/{doc_id}')
def api_add_tag(doc_id: int, data: AddTagRequest):
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

        db_connector.insert_keywords_and_tags(
            doc_id, [{'english': english_keyword, 'arabic': arabic_keyword}]
        )
        return {'message': 'Tag added successfully.'}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Server error: {e}')


@router.delete('/api/tags/{doc_id}/{tag}')
def api_delete_tag(doc_id: int, tag: str):
    success, message = db_connector.delete_tag_from_document(doc_id, tag)
    if success:
        return {'message': message}
    else:
        raise HTTPException(status_code=404, detail=message)