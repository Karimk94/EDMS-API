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


# Admin Schemas
class EdmsUserResponse(BaseModel):
    username: str
    people_system_id: int
    edms_user_id: int
    user_ref_id: int
    security_level: str
    security_level_id: int
    lang: str
    theme: str


class AddEdmsUserRequest(BaseModel):
    user_system_id: int
    security_level_id: int
    lang: str = 'en'
    theme: str = 'light'


class SecurityLevelResponse(BaseModel):
    id: int
    name: str


class PeopleSearchResult(BaseModel):
    system_id: int
    user_id: str
    name: str


class UpdateEdmsUserRequest(BaseModel):
    security_level_id: int
    lang: str = 'en'
    theme: str = 'light'