from pydantic import BaseModel
from typing import Dict, List, Optional

class CreateFolderRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    parent_id: Optional[str] = None

class RenameFolderRequest(BaseModel):
    name: str
    system_id: Optional[int] = None


class MoveItemsRequest(BaseModel):
    item_ids: List[str]
    destination_parent_id: Optional[str] = None
    item_names: Optional[Dict[str, str]] = None