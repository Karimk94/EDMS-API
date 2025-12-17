from pydantic import BaseModel
from typing import List, Optional, Any

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

class DocumentSchema(BaseModel):
    doc_id: str
    doc_name: str
    author_id: str
    creation_date: str
    last_edit_date: Optional[str] = None
    application_id: str

class DocumentSearchRequest(BaseModel):
    library: str
    criteria: dict
    operator: Optional[str] = "AND"

class Trustee(BaseModel):
    username: str
    rights: int  # e.g., 255 for full control, 63 for standard
    flag: Optional[int] = 2

class SetTrusteesRequest(BaseModel):
    library: str
    trustees: List[Trustee]
    security_enabled: Optional[str] = "1"