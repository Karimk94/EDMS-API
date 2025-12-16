from fastapi import APIRouter, HTTPException, Request
import db_connector
import wsdl_client
from schemas.auth import LoginRequest, LangUpdateRequest, ThemeUpdateRequest

router = APIRouter()

@router.post('/api/auth/login')
async def login(request: Request, creds: LoginRequest):
    dst = wsdl_client.dms_user_login(creds.username, creds.password)
    if dst:
        user_details = await db_connector.get_user_details(creds.username)
        if user_details is None or 'security_level' not in user_details:
            raise HTTPException(status_code=401, detail="User not authorized for this application")

        request.session['user'] = user_details
        return {"message": "Login successful", "user": user_details}
    else:
        raise HTTPException(status_code=401, detail="Invalid DMS credentials")

@router.post('/api/auth/logout')
async def logout(request: Request):
    request.session.pop('user', None)
    return {"message": "Logout successful"}

@router.get('/api/auth/user')
async def get_user(request: Request):
    user_session = request.session.get('user')
    if user_session and 'username' in user_session:
        user_details = await db_connector.get_user_details(user_session['username'])
        if user_details:
            request.session['user'] = user_details
            return {'user': user_details}
        else:
            request.session.pop('user', None)
            raise HTTPException(status_code=401, detail='User not found')
    else:
        raise HTTPException(status_code=401, detail='Not authenticated')

@router.put('/api/user/language')
async def update_user_language(request: Request, data: LangUpdateRequest):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if data.lang not in ['en', 'ar']:
        raise HTTPException(status_code=400, detail="Invalid language")

    success = await db_connector.update_user_language(user['username'], data.lang)
    if success:
        user['lang'] = data.lang
        request.session['user'] = user
        return {"message": "Language updated"}
    else:
        raise HTTPException(status_code=500, detail="Failed to update language")

@router.put('/api/user/theme')
async def api_update_user_theme(request: Request, data: ThemeUpdateRequest):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if data.theme not in ['light', 'dark']:
        raise HTTPException(status_code=400, detail="Invalid theme")

    username = user['username']
    success = await db_connector.update_user_theme(username, data.theme)
    if success:
        user['theme'] = data.theme
        request.session['user'] = user
        return {"message": "Theme updated"}
    else:
        raise HTTPException(status_code=500, detail="Failed to update theme")