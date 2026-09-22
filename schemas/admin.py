from pydantic import BaseModel
from typing import List

class BulkCheckinRequest(BaseModel):
    docnumbers: List[int]
