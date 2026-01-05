from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ShareLinkCreateRequest(BaseModel):
    document_id: int
    expiry_date: Optional[datetime] = None
    target_email: Optional[str] = None

class ShareLinkResponse(BaseModel):
    token: str
    link: str
    expiry_date: Optional[datetime]

class ShareAccessRequest(BaseModel):
    viewer_email: str

class ShareAccessResponse(BaseModel):
    document: dict
    access_stats: dict

class ShareVerifyRequest(BaseModel):
    viewer_email: str
    otp: str