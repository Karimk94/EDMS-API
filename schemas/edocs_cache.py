from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class EdocsClearCacheRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    user_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


class EdocsClearCacheAcceptedResponse(BaseModel):
    status: Literal["accepted"]
    message: str
    server_cache_root: str
