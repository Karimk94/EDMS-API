from pydantic import BaseModel

class LoginRequest(BaseModel):
    username: str
    password: str

class LangUpdateRequest(BaseModel):
    lang: str

class ThemeUpdateRequest(BaseModel):
    theme: str