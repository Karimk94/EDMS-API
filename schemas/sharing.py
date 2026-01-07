from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime

ALLOWED_DOMAIN = "@rta.ae"

class ShareLinkCreateRequest(BaseModel):
    """
    Request model for creating a share link.

    Supports two sharing modes:
    1. Open mode (target_email=None): Any user with @rta.ae email can access
    2. Restricted mode (target_email set): Only the specified email can access
    """
    document_id: int
    expiry_date: Optional[datetime] = None
    target_email: Optional[str] = None  # Must be @rta.ae domain if provided

    @field_validator('target_email')
    @classmethod
    def validate_target_email(cls, v):
        if v is not None:
            v = v.strip().lower()
            if not v.endswith(ALLOWED_DOMAIN.lower()):
                raise ValueError(f"Target email must be from {ALLOWED_DOMAIN} domain")
            if '@' not in v or len(v) < 8:  # Minimum: a@rta.ae
                raise ValueError("Invalid email format")
        return v

class ShareLinkResponse(BaseModel):
    """Response model for a created share link."""
    token: str
    link: str
    expiry_date: Optional[datetime]
    target_email: Optional[str] = None
    share_mode: str = "open"  # "open" or "restricted"

class ShareInfoResponse(BaseModel):
    """Response model for share link info (public endpoint)."""
    is_restricted: bool
    target_email_hint: Optional[str] = None  # Masked email like "abc***@rta.ae"
    expiry_date: Optional[datetime] = None

class ShareAccessRequest(BaseModel):
    """Request model for requesting OTP access to a shared document."""
    viewer_email: str

    @field_validator('viewer_email')
    @classmethod
    def validate_viewer_email(cls, v):
        v = v.strip().lower()
        if not v.endswith(ALLOWED_DOMAIN.lower()):
            raise ValueError(f"Email must be from {ALLOWED_DOMAIN} domain")
        if '@' not in v or len(v) < 8:
            raise ValueError("Invalid email format")
        return v

class ShareAccessResponse(BaseModel):
    """Response model for successful document access."""
    document: dict
    access_stats: dict

class ShareVerifyRequest(BaseModel):
    """Request model for verifying OTP access."""
    viewer_email: str
    otp: str

    @field_validator('viewer_email')
    @classmethod
    def validate_viewer_email(cls, v):
        v = v.strip().lower()
        if not v.endswith(ALLOWED_DOMAIN.lower()):
            raise ValueError(f"Email must be from {ALLOWED_DOMAIN} domain")
        return v

    @field_validator('otp')
    @classmethod
    def validate_otp(cls, v):
        v = v.strip()
        if not v.isdigit() or len(v) != 6:
            raise ValueError("OTP must be a 6-digit number")
        return v