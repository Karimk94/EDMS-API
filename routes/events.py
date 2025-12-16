from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional
from pydantic import BaseModel
import logging
import db_connector

router = APIRouter()


class CreateEventRequest(BaseModel):
    name: str


@router.get('/api/events')
def get_events_route(
        page: int = 1,
        pageSize: int = 20,
        search: Optional[str] = None,
        fetch_all: bool = False
):
    if page < 1: page = 1
    if pageSize > 100: pageSize = 100

    events_list, total_rows = db_connector.get_events(
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
def create_event_route(data: CreateEventRequest):
    if not data.name:
        raise HTTPException(status_code=400, detail="Event name is required.")

    event_id, message = db_connector.create_event(data.name)
    if event_id:
        return {"id": event_id, "message": message}
    else:
        raise HTTPException(status_code=400, detail=message)


@router.get('/api/events/{event_id}/documents')
def get_event_documents_route(event_id: int, page: int = 1):
    page_size = 1
    if page < 1: page = 1

    documents, total_pages, error_message = db_connector.get_documents_for_event(
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
def get_journey_data():
    try:
        journey_data = db_connector.fetch_journey_data()
        return journey_data
    except Exception as e:
        logging.error(f"Error in /api/journey: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch journey data.")