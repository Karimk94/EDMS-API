from fastapi import APIRouter, HTTPException
from typing import Optional
import logging
import db_connector
from schemas.events import CreateEventRequest

router = APIRouter()

@router.get('/api/events')
async def get_events_route(
        page: int = 1,
        pageSize: int = 20,
        search: Optional[str] = None,
        fetch_all: bool = False
):
    if page < 1: page = 1
    if pageSize > 100: pageSize = 100

    events_list, total_rows = await db_connector.get_events(
        page=page,
        page_size=pageSize,
        search=search,
        fetch_all=fetch_all
    )

    total_pages = (total_rows + pageSize - 1) // pageSize if total_rows > 0 else 1
    has_more = (page * pageSize) < total_rows

    return {
        "events": events_list,
        "page": page,
        "total_pages": total_pages,
        "hasMore": has_more
    }

@router.post('/api/events')
async def create_event_route(data: CreateEventRequest):
    if not data.name:
        raise HTTPException(status_code=400, detail="Event name is required.")

    event_id, message = await db_connector.create_event(data.name)
    if event_id:
        return {"id": event_id, "message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@router.get('/api/events/{event_id}/documents')
async def get_event_documents_route(event_id: int, page: int = 1):
    page_size = 1
    if page < 1: page = 1

    documents, total_pages, error_message = await db_connector.get_documents_for_event(
        event_id=event_id,
        page=page,
        page_size=page_size
    )

    if error_message:
        code = 404 if "not found" in error_message.lower() else 500
        raise HTTPException(status_code=code, detail=error_message)

    current_doc = documents[0] if documents else None
    return {"document": current_doc, "page": page, "total_pages": total_pages}

@router.get('/api/journey')
async def get_journey_data():
    try:
        journey_data = await db_connector.fetch_journey_data()
        return journey_data
    except Exception as e:
        logging.error(f"Error in /api/journey: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch journey data.")