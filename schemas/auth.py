from pydantic import BaseModel
from typing import Optional

class LoginRequest(BaseModel):
    username: str
    password: str

class LangUpdateRequest(BaseModel):
    lang: str

class ThemeUpdateRequest(BaseModel):
    theme: str

class User(BaseModel):
    username: str
    token: str
    security_level: Optional[str] = "Viewer"

class GroupMember(BaseModel):
    user_id: str
    full_name: str
    system_id: Optional[str] = None

class TrusteeResponse(BaseModel):
    username: str
    flag: int
    rights: int

class Group(BaseModel):
    group_id: str
    group_name: str
    description: Optional[str] = None