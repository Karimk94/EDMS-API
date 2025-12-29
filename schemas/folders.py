from pydantic import BaseModel
from typing import Optional

class CreateFolderRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    parent_id: Optional[str] = None

class RenameFolderRequest(BaseModel):
    name: str
    system_id: Optional[int] = None