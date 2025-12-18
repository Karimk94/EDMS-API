from fastapi import APIRouter, HTTPException, Request, Query
import db_connector
import wsdl_client
from schemas.auth import LoginRequest, LangUpdateRequest, ThemeUpdateRequest, GroupMember, TrusteeResponse, Group
from typing import List
from services import processor

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

@router.get("/api/groups/{group_id}/members", response_model=List[GroupMember])
def get_group_members_route(group_id: str, request: Request):
    token = processor.get_session_token(request)
    members = wsdl_client.get_group_members(token, group_id)
    return members

@router.get("/api/document/{doc_id}/trustees", response_model=List[TrusteeResponse])
def get_doc_trustees(doc_id: str, request: Request):
    token = processor.get_session_token(request)
    trustees = wsdl_client.get_object_trustees(token, doc_id)
    return trustees

@router.get("/api/groups", response_model=List[Group])
def get_groups(request: Request):
    token = processor.get_session_token(request)
    user = request.session.get('user')

    # Get raw security level from user session
    raw_level = user.get('security_level', 0) if user else 0

    # Convert to integer using helper
    try:
        security_level = int(raw_level)
    except ValueError:
        security_level = processor.get_security_level_int(raw_level)

    username = user.get('username')

    # If admin (level 9), show all groups. Otherwise, show only user's groups.
    if security_level >= 9:
        groups = wsdl_client.get_all_groups(token)
    else:
        # Falls back to get_all_groups inside wsdl_client if user-search fails
        groups = wsdl_client.get_groups_for_user(token, username)

    return groups

@router.get("/api/groups/search_members", response_model=dict)
def search_group_members(
        request: Request,
        search: str = Query(""),
        page: int = 1,
        group_id: str = Query(None)
):
    token = processor.get_session_token(request)

    # Use provided group_id or fallback
    target_group = group_id if group_id else "EDMS_TEST_GRP_2"

    members = wsdl_client.search_users_in_group(token, target_group, search)

    # Manual Pagination since SOAP returns all
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