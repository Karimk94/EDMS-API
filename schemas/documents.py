from pydantic import BaseModel
from typing import List, Optional

class ProcessUploadRequest(BaseModel):
    docnumbers: List[int]

class UpdateMetadataRequest(BaseModel):
    doc_id: int
    abstract: Optional[str] = None
    date_taken: Optional[str] = None

class UpdateAbstractRequest(BaseModel):
    doc_id: int
    names: List[str]

class LinkEventRequest(BaseModel):
    event_id: Optional[int]