from fastapi import APIRouter, HTTPException, Request, Header, Query
import db_connector
import wsdl_client
from schemas.auth import LoginRequest, LangUpdateRequest, ThemeUpdateRequest, GroupMember, TrusteeResponse, Group
from typing import List

router = APIRouter()

@router.post('/api/auth/login')
async def login(request: Request, creds: LoginRequest):
    dst = wsdl_client.dms_user_login(creds.username, creds.password)
    if dst:
        user_details = await db_connector.get_user_details(creds.username)
        if user_details is None or 'security_level' not in user_details:
            raise HTTPException(status_code=401, detail="User not authorized for this application")

        # Store the DMS token in the user object
        user_details['token'] = dst
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
            # Preserve token if it exists in session but not in db details
            if 'token' in user_session:
                user_details['token'] = user_session['token']
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

# --- Groups & Security Endpoints (Updated to use Session) ---

def get_session_token(request: Request):
    user = request.session.get('user')
    if not user or 'token' not in user:
        token = db_connector.dms_system_login()
        if token: return token
        raise HTTPException(status_code=401, detail="Session required")
    return user['token']

def get_current_username(request: Request):
    user = request.session.get('user')
    return user['username'] if user else None

@router.get("/api/groups", response_model=List[Group])
def get_groups(request: Request):
    token = get_session_token(request)
    groups = wsdl_client.get_all_groups(token)
    return groups

@router.get("/api/groups/{group_id}/members", response_model=List[GroupMember])
def get_group_members_route(group_id: str, request: Request):
    token = get_session_token(request)
    members = wsdl_client.get_group_members(token, group_id)
    return members

@router.get("/api/document/{doc_id}/trustees", response_model=List[TrusteeResponse])
def get_doc_trustees(doc_id: str, request: Request):
    token = get_session_token(request)
    trustees = wsdl_client.get_object_trustees(token, doc_id)
    return trustees

@router.get("/api/groups/search_members", response_model=dict)
def search_group_members(
        request: Request,
        search: str = Query(""),
        page: int = 1
):
    token = get_session_token(request)
    username = get_current_username(request)

    members = []

    # If we have a username, try to get their group members
    if username:
        members = wsdl_client.get_current_user_group_members(token, username)
    else:
        # Fallback
        target_group = "EDMS_TEST_GRP_2"
        members = wsdl_client.search_users_in_group(token, target_group, search)

    # Filter by search term
    if search:
        members = [m for m in members if
                   search.lower() in m['full_name'].lower() or search.lower() in m['user_id'].lower()]

    # Pagination
    start = (page - 1) * 20
    end = start + 20
    paged_members = members[start:end]
    has_more = len(members) > end

    return {
        "options": [
            {"name_english": m['full_name'], "name_arabic": "", "user_id": m['user_id']}
            for m in paged_members
        ],
        "hasMore": has_more
    }