from pydantic import BaseModel

class CreateEventRequest(BaseModel):
    name: str