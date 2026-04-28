from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
import db_connector
import wsdl_client
from wsdl_client import users as wsdl_users
from schemas.auth import LoginRequest, LangUpdateRequest, ThemeUpdateRequest, GroupMember, TrusteeResponse, Group
from typing import List
from services import processor
from utils.common import get_current_user, get_session_token
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

router = APIRouter()

# Rate limiter for login endpoint — keyed by client IP
limiter = Limiter(key_func=get_remote_address)
EMS_ADMIN_GROUP_ID = 'EMS_ADMIN'
DOCS_SUPERVISORS_GROUP_ID = 'DOCS_SUPERVISORS'


def _extract_group_membership_flags(user_groups: list) -> dict:
    """Derive cached group membership flags from DMS groups payload."""
    is_ems_admin_group_member = False
    is_docs_supervisor = False

    for group in user_groups or []:
        group_id = str(group.get('group_id', '')).strip().upper()
        group_name = str(group.get('group_name', '')).strip().upper()
        if group_id == EMS_ADMIN_GROUP_ID.upper() or group_name == EMS_ADMIN_GROUP_ID.upper():
            is_ems_admin_group_member = True
        if group_id == DOCS_SUPERVISORS_GROUP_ID.upper() or group_name == DOCS_SUPERVISORS_GROUP_ID.upper():
            is_docs_supervisor = True

    return {
        'is_ems_admin_group_member': is_ems_admin_group_member,
        'is_docs_supervisor': is_docs_supervisor,
    }


def _is_member_of_group(user_groups: list, target_group_id: str) -> bool:
    """Case-insensitive group membership check against both id and name."""
    target_group = str(target_group_id or '').strip().upper()
    if not target_group:
        return False

    for group in user_groups or []:
        group_id = str(group.get('group_id', '')).strip().upper()
        group_name = str(group.get('group_name', '')).strip().upper()
        if group_id == target_group or group_name == target_group:
            return True

    return False


def _is_group_active(group: dict) -> bool:
    """Treat groups as active unless an explicit inactive flag/value is present."""
    for key in ('is_active', 'active', 'status'):
        if key in group:
            value = group.get(key)
            if value is None:
                return True
            if isinstance(value, bool):
                return value
            normalized = str(value).strip().lower()
            return normalized not in {'0', 'false', 'inactive', 'disabled', 'n'}

    return True


def _normalize_group_identifier(value: str) -> str:
    return str(value or '').strip().upper()


async def _get_user_group_access_context(token: str, username: str) -> tuple[list, bool, set[str]]:
    user_groups = await run_in_threadpool(wsdl_client.get_groups_for_user, token, username)
    is_docs_supervisor = _is_member_of_group(user_groups, DOCS_SUPERVISORS_GROUP_ID)

    allowed_group_ids = set()
    for group in user_groups or []:
        allowed_group_ids.add(_normalize_group_identifier(group.get('group_id')))
        allowed_group_ids.add(_normalize_group_identifier(group.get('group_name')))

    allowed_group_ids.discard('')
    return user_groups, is_docs_supervisor, allowed_group_ids


def _ensure_ems_admin_tab_permission(tab_permissions: list, is_ems_admin_group_member: bool) -> list:
    """Ensure EMS admin group members can always see/access EMS admin tab."""
    permissions = list(tab_permissions or [])
    if not is_ems_admin_group_member:
        return permissions

    has_existing = False
    for perm in permissions:
        if str(perm.get('tab_key', '')).lower() == 'ems_admin':
            perm['can_read'] = True
            has_existing = True
            break

    if not has_existing:
        permissions.append({'tab_key': 'ems_admin', 'can_read': True, 'can_write': False})

    return permissions

@router.post('/api/auth/login')
@limiter.limit("5/minute")
async def login(request: Request, creds: LoginRequest):
    dst, error_msg = wsdl_client.dms_user_login(creds.username, creds.password)
    if dst:
        # Mandatory: Check if user exists in Smart EDMS local DB
        db_user = await db_connector.get_user_details(creds.username)
        if not db_user:
            raise HTTPException(status_code=403, detail="User doesn't have smart EDMS account")

        # Get user's groups from DMS (proven working query)
        group_flags = {'is_ems_admin_group_member': False}
        try:
            user_groups = wsdl_client.get_groups_for_user(dst, creds.username)
            # logging.info(f"User {creds.username} belongs to groups: {[g.get('group_id') for g in user_groups]}")

            group_flags = _extract_group_membership_flags(user_groups)

            security_level = processor.determine_security_from_groups(user_groups)
            security_str = {9: 'Admin', 5: 'Editor', 0: 'Viewer'}.get(security_level, 'Viewer')
        except Exception as e:
            logging.error(f"Could not get DMS groups for {creds.username}: {e}")
            # Fallback to DB security level since DMS failed
            # db_user is already guaranteed to exist here
            if 'security_level' not in db_user:
                raise HTTPException(status_code=401, detail="User not authorized")
            security_str = db_user.get('security_level', 'Viewer')
            security_level = processor.get_security_level_int(security_str)

        # Fetch tab permissions — admin override bypasses DB
        if security_str == 'Admin':
            tab_permissions = db_connector.get_admin_full_permissions()
        else:
            # Get the people_system_id from the DB user record for per-user lookup
            people_system_id = db_user.get('people_system_id')
            if people_system_id:
                tab_permissions = await db_connector.get_tab_permissions_for_user(people_system_id)
            else:
                tab_permissions = []

        tab_permissions = _ensure_ems_admin_tab_permission(
            tab_permissions,
            group_flags.get('is_ems_admin_group_member', False)
        )

        # Use already fetched db_user for preferences
        db_prefs = db_user

        user_details = {
            'username': creds.username,
            'token': dst,
            'security_level': security_str,
            'dms_security_level': security_level,  # Numeric for logic
            'lang': db_prefs.get('lang', 'en') if db_prefs else 'en',
            'theme': db_prefs.get('theme', 'light') if db_prefs else 'light',
            'tab_permissions': tab_permissions,
            **group_flags,
        }

        request.session['user'] = user_details

        # Strip internal token before sending to client
        safe_user = {k: v for k, v in user_details.items() if k != 'token'}
        return {"message": "Login successful", "user": safe_user}
    else:
        raise HTTPException(status_code=401, detail=error_msg or "Invalid DMS credentials")

@router.post('/api/auth/logout')
async def logout(request: Request):
    request.session.clear()
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

            token = user_session.get('token')
            if token:
                try:
                    user_groups = wsdl_client.get_groups_for_user(token, user_session['username'])
                    group_flags = _extract_group_membership_flags(user_groups)
                    user_details.update(group_flags)
                except Exception as _wsdl_err:
                    logging.warning(f"Could not refresh group membership flags for {user_session.get('username')}: {_wsdl_err}")
                    user_details['is_ems_admin_group_member'] = user_session.get('is_ems_admin_group_member', False)
                    user_details['is_docs_supervisor'] = user_session.get('is_docs_supervisor', False)
            else:
                user_details['is_ems_admin_group_member'] = user_session.get('is_ems_admin_group_member', False)
                user_details['is_docs_supervisor'] = user_session.get('is_docs_supervisor', False)

            # Fetch tab permissions — per-user
            security_level = user_details.get('security_level', 'Viewer')
            if str(security_level).lower() == 'admin':
                user_details['tab_permissions'] = db_connector.get_admin_full_permissions()
            else:
                people_system_id = user_details.get('people_system_id')
                if people_system_id:
                    user_details['tab_permissions'] = await db_connector.get_tab_permissions_for_user(people_system_id)
                else:
                    user_details['tab_permissions'] = []

            user_details['tab_permissions'] = _ensure_ems_admin_tab_permission(
                user_details.get('tab_permissions', []),
                user_details.get('is_ems_admin_group_member', False)
            )

            request.session['user'] = user_details

            # Strip internal token before sending to client
            safe_user = {k: v for k, v in user_details.items() if k != 'token'}
            return {'user': safe_user}
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
async def get_group_members_route(group_id: str, request: Request, user=Depends(get_current_user)):
    token = get_session_token(request)
    username = user.get('username') if user else None

    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")

    _, is_docs_supervisor, allowed_group_ids = await _get_user_group_access_context(token, username)
    if not is_docs_supervisor and _normalize_group_identifier(group_id) not in allowed_group_ids:
        raise HTTPException(status_code=403, detail="You can only view members of your own groups")

    members = await run_in_threadpool(wsdl_client.get_group_members, token, group_id)
    return members

@router.get("/api/document/{doc_id}/trustees", response_model=List[TrusteeResponse])
async def get_doc_trustees(doc_id: str, request: Request, user=Depends(get_current_user)):
    token = get_session_token(request)
    trustees = await run_in_threadpool(wsdl_client.get_object_trustees, token, doc_id)
    return trustees

@router.get("/api/groups", response_model=dict)
async def get_groups(
    request: Request,
    search: str = Query(""),
    page: int = 1,
    limit: int = Query(0, ge=0),
    user=Depends(get_current_user)
):
    token = get_session_token(request)
    username = user.get('username') if user else None

    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # Get user's groups from DMS
    user_groups, is_docs_supervisor, _ = await _get_user_group_access_context(token, username)
    # logging.info(f"[/api/groups] User {username} has {len(user_groups)} direct groups: {[g.get('group_id') for g in user_groups]}")

    # Determine security level from their groups (for session bookkeeping only)
    security_level = processor.determine_security_from_groups(user_groups)
    # logging.info(f"[/api/groups] User {username} security_level={security_level}")

    # Update session with DMS-based security
    if user:
        user['dms_security_level'] = security_level
        request.session['user'] = user

    # Only explicitly privileged groups may see all groups.
    # EMS_ADMIN  → application-level administrators (hardcoded constant)
    # DOCS_SUPERVISORS → document supervisors (hardcoded constant)
    # Any other membership — including groups that happen to carry a name like
    # 'ADMIN' or 'ADMINISTRATOR' in the DMS — must NOT grant this access.
    if is_docs_supervisor:
        all_groups = await run_in_threadpool(wsdl_users.get_all_groups, token)
        # logging.info(f"[/api/groups] Admin mode: got {len(all_groups)} groups from get_all_groups")
        # Merge user's groups with all groups to ensure none are missed
        # (some groups might not be in v_groups view but user is still a member)
        existing_group_ids = {g.get('group_id') for g in all_groups}
        for ug in user_groups:
            if ug.get('group_id') not in existing_group_ids:
                all_groups.append(ug)
                # logging.info(f"[/api/groups] Added missing user group: {ug.get('group_id')}")
        # logging.info(f"[/api/groups] Final merged count: {len(all_groups)}")
    else:
        all_groups = user_groups
        # logging.info(f"[/api/groups] Non-admin mode: returning {len(all_groups)} user groups")

    # Keep only active groups when the source provides an activity marker.
    active_groups = [g for g in all_groups if _is_group_active(g)]

    # Filter by search term
    if search:
        search_lower = search.lower()
        filtered_groups = [
            g for g in active_groups
            if search_lower in (g.get('group_name') or g.get('name') or '').lower()
        ]
    else:
        filtered_groups = active_groups

    # Default behavior is to return all matching groups for dropdown clients.
    # Optional pagination remains available when limit > 0 is explicitly provided.
    if limit > 0:
        start = max(page - 1, 0) * limit
        end = start + limit
        paged_groups = filtered_groups[start:end]
        has_more = len(filtered_groups) > end
    else:
        paged_groups = filtered_groups
        has_more = False

    return {
        "options": paged_groups,
        "hasMore": has_more
    }

@router.get("/api/groups/search_members", response_model=dict)
async def search_group_members(
        request: Request,
        search: str = Query(""),
        page: int = 1,
        group_id: str = Query(None),
        user=Depends(get_current_user)
):
    token = get_session_token(request)
    username = user.get('username') if user else None

    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")

    if not group_id:
        raise HTTPException(status_code=400, detail="group_id is required")

    _, is_docs_supervisor, allowed_group_ids = await _get_user_group_access_context(token, username)
    if not is_docs_supervisor and _normalize_group_identifier(group_id) not in allowed_group_ids:
        raise HTTPException(status_code=403, detail="You can only search members in your own groups")

    members = await run_in_threadpool(wsdl_client.search_users_in_group, token, group_id, search)

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