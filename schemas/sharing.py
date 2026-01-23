from pydantic import BaseModel, field_validator, model_validator, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

ALLOWED_DOMAIN = "@rta.ae"

class ShareLinkCreateRequest(BaseModel):
    """
    Request model for creating a share link.
    """
    document_id: Optional[int] = None
    folder_id: Optional[int] = None
    share_type: Optional[str] = 'file'  # 'file' or 'folder'
    item_name: Optional[str] = None
    expiry_date: Optional[datetime] = None
    target_email: Optional[str] = None

    @field_validator('target_email')
    @classmethod
    def validate_target_email(cls, v):
        if v is not None:
            v = v.strip().lower()
            if not v.endswith(ALLOWED_DOMAIN.lower()):
                raise ValueError(f"Target email must be from {ALLOWED_DOMAIN} domain")
            if '@' not in v or len(v) < 8:
                raise ValueError("Invalid email format")
        return v

    @model_validator(mode='after')
    def validate_ids_based_on_type(self):
        share_type = self.share_type or 'file'
        if share_type == 'folder':
            if not self.folder_id:
                raise ValueError("folder_id is required for folder shares")
        else:
            if not self.document_id:
                raise ValueError("document_id is required for file shares")
        return self

class ShareLinkResponse(BaseModel):
    """Response model for a created share link."""
    token: str
    link: str
    expiry_date: Optional[datetime]
    target_email: Optional[str] = None
    share_mode: str = "open"
    share_type: str = "file"

class ShareInfoResponse(BaseModel):
    """Response model for share link info (public endpoint)."""
    is_restricted: bool
    target_email: Optional[str] = None  # Full email for auto-OTP
    target_email_hint: Optional[str] = None  # Masked for display
    expiry_date: Optional[datetime] = None
    share_type: str = "file"
    skip_otp: bool = False  # True for restricted shares, False for open shares

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
    """Response model for successful document/folder access."""
    share_type: str
    shared_by: Optional[str] = None
    document: Optional[Dict[str, Any]] = None
    folder_id: Optional[int] = None
    access_stats: Optional[Dict[str, Any]] = None

class ShareVerifyRequest(BaseModel):
    """Request model for verifying OTP access."""
    viewer_email: str
    otp: Optional[str] = None  # Optional when skip_otp is True
    skip_otp: bool = False  # True to skip OTP verification for restricted shares

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
        if v is not None:
            v = v.strip()
            if not v.isdigit() or len(v) != 6:
                raise ValueError("OTP must be a 6-digit number")
        return v

    @model_validator(mode='after')
    def validate_otp_required(self):
        """OTP is required when skip_otp is False."""
        if not self.skip_otp and not self.otp:
            raise ValueError("OTP is required when skip_otp is False")
        return self

class SharedFolderContentsRequest(BaseModel):
    """Request model for getting shared folder contents."""
    viewer_email: str
    parent_id: Optional[str] = Field(default=None, description="Parent folder ID for navigation")

    @field_validator('viewer_email')
    @classmethod
    def validate_viewer_email(cls, v):
        v = v.strip().lower()
        if not v.endswith(ALLOWED_DOMAIN.lower()):
            raise ValueError(f"Email must be from {ALLOWED_DOMAIN} domain")
        return v

class SharedDocumentDownloadRequest(BaseModel):
    """Request model for downloading a shared document."""
    viewer_email: str
    doc_id: Optional[str] = Field(default=None, description="Document ID (required for folder shares)")

    @field_validator('viewer_email')
    @classmethod
    def validate_viewer_email(cls, v):
        v = v.strip().lower()
        if not v.endswith(ALLOWED_DOMAIN.lower()):
            raise ValueError(f"Email must be from {ALLOWED_DOMAIN} domain")
        return v

class FolderItem(BaseModel):
    """Model for a folder or file item."""
    id: str
    name: str
    type: str  # 'folder' or 'file'
    media_type: str
    system_id: Optional[str] = None

class BreadcrumbItem(BaseModel):
    """Model for a breadcrumb navigation item."""
    id: str
    name: str

class SharedFolderContentsResponse(BaseModel):
    """Response model for shared folder contents."""
    folder_id: str
    folder_name: str
    root_folder_id: str
    is_root: bool
    breadcrumbs: List[BreadcrumbItem]
    contents: List[FolderItem]