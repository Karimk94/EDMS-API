from pydantic import BaseModel
from typing import List

class AddPersonRequest(BaseModel):
    name: str
    lang: str = 'en'

class ToggleShortlistRequest(BaseModel):
    tag: str

class ProcessingStatusRequest(BaseModel):
    docnumbers: List[int]

class AddTagRequest(BaseModel):
    tag: str