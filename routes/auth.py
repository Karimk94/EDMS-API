from fastapi import APIRouter, HTTPException, Request, Query, Depends, Header
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
import asyncio
import os
import uuid
import db_connector
import wsdl_client
from wsdl_client import users as wsdl_users
from schemas.auth import LoginRequest, LangUpdateRequest, ThemeUpdateRequest, ViewModeUpdateRequest, ProfileViewModeUpdateRequest, GroupMember, TrusteeResponse, Group
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
DMS_GROUP_REFRESH_TIMEOUT_SECONDS = float(os.getenv('DMS_GROUP_REFRESH_TIMEOUT_SECONDS', '5'))
AUTH_DB_TIMEOUT_SECONDS = float(os.getenv('AUTH_DB_TIMEOUT_SECONDS', '5'))


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


async def _refresh_group_flags_for_session(token: str, username: str, session_user: dict) -> dict:
    """
    Best-effort group refresh for page/session restore.
    Never let DMS/WSDL slowness block /api/auth/user indefinitely.
    """
    try:
        user_groups = await asyncio.wait_for(
            run_in_threadpool(wsdl_client.get_groups_for_user, token, username),
            timeout=DMS_GROUP_REFRESH_TIMEOUT_SECONDS,
        )
        return _extract_group_membership_flags(user_groups)
    except asyncio.TimeoutError:
        logging.warning(
            "Timed out refreshing DMS group flags for %s after %.1fs; using cached session flags",
            username,
            DMS_GROUP_REFRESH_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logging.warning(
            "Could not refresh DMS group flags for %s: %s; using cached session flags",
            username,
            exc,
        )

    return {
        'is_ems_admin_group_member': session_user.get('is_ems_admin_group_member', False),
        'is_docs_supervisor': session_user.get('is_docs_supervisor', False),
    }


async def _get_user_details_for_session(username: str, session_user: dict) -> dict | None:
    """Fetch current DB user details, falling back to signed session data on DB stalls."""
    try:
        return await asyncio.wait_for(
            db_connector.get_user_details(username),
            timeout=AUTH_DB_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logging.warning(
            "Timed out fetching DB user details for %s after %.1fs; using cached session user",
            username,
            AUTH_DB_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logging.warning(
            "Failed fetching DB user details for %s: %s; using cached session user",
            username,
            exc,
        )

    if session_user:
        return dict(session_user)
    return None


async def _get_tab_permissions_for_session(user_details: dict, session_user: dict) -> list:
    """Fetch tab permissions without allowing a slow DB lookup to block session restore."""
    security_level = user_details.get('security_level', 'Viewer')
    if str(security_level).lower() == 'admin':
        return db_connector.get_admin_full_permissions()

    people_system_id = user_details.get('people_system_id')
    if not people_system_id:
        return session_user.get('tab_permissions', [])

    try:
        return await asyncio.wait_for(
            db_connector.get_tab_permissions_for_user(people_system_id),
            timeout=AUTH_DB_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logging.warning(
            "Timed out fetching tab permissions for user_id %s after %.1fs; using cached session permissions",
            people_system_id,
            AUTH_DB_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logging.warning(
            "Failed fetching tab permissions for user_id %s: %s; using cached session permissions",
            people_system_id,
            exc,
        )

    return session_user.get('tab_permissions', [])


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

def _extract_sso_username(request: Request, body_username: str | None = None) -> str | None:
    """
    Extracts username from incoming SSO headers or payload.
    Supports Windows Negotiate headers injected by IIS (X-Remote-User / Remote-User / x-iisnode-auth_user),
    standard reverse proxy headers, and direct payload/param inputs.
    Strips domain prefixes like RTADOM\\username or user@domain.
    """
    candidate = (
        body_username
        or request.headers.get("x-remote-user")
        or request.headers.get("remote-user")
        or request.headers.get("x-iisnode-auth_user")
        or request.headers.get("x-iisnode-logon_user")
        or request.headers.get("x-iisnode-remote_user")
        or request.headers.get("x-iisnode-unencrypted_user")
        or request.headers.get("x-forwarded-user")
        or request.headers.get("x-authenticated-user")
        or request.headers.get("x-auth-user")
        or request.headers.get("x-iis-user")
        or request.headers.get("logon_user")
        or request.headers.get("auth_user")
        or request.query_params.get("username")
        or request.query_params.get("desktop_user")
    )

    # Local development / loopback fallback only (never in production)
    if not candidate:
        allow_dev = os.getenv("ALLOW_DEV_SSO_FALLBACK", "false").lower() in ("true", "1")
        client_host = request.client.host if request.client else ""
        if allow_dev and client_host in ("127.0.0.1", "localhost", "::1"):
            import getpass
            candidate = os.environ.get("USERNAME") or getpass.getuser()

    if not candidate:
        return None

    raw = str(candidate).strip()
    if '\\' in raw:
        raw = raw.split('\\')[-1]
    elif '/' in raw:
        raw = raw.split('/')[-1]
    elif '@' in raw:
        raw = raw.split('@')[0]
    return raw.strip().lower()


async def _build_and_store_user_session(
    request: Request,
    username: str,
    db_user: dict,
    dst: str | None,
    x_app_source: str,
    login_type_msg: str = "Login successful",
) -> dict:
    """
    Common helper for both manual DMS and SSO logins to populate user groups,
    tab permissions, server session, and login logs.
    """
    session_id = str(uuid.uuid4())
    try:
        from database.login_logs import log_login
        client_ip = request.client.host if request.client else None
        await log_login(
            people_system_id=db_user.get('people_system_id'),
            session_id=session_id,
            app_source=x_app_source,
            ip_address=client_ip,
        )
    except Exception as login_log_err:
        logging.warning(f"Could not log login for {username}: {login_log_err}")

    group_flags = {'is_ems_admin_group_member': False, 'is_docs_supervisor': False}
    security_str = db_user.get('security_level', 'Viewer')
    security_level = processor.get_security_level_int(security_str)

    if dst:
        try:
            user_groups = await run_in_threadpool(wsdl_client.get_groups_for_user, dst, username)
            group_flags = _extract_group_membership_flags(user_groups)
            security_level = processor.determine_security_from_groups(user_groups)
            security_str = {9: 'Admin', 5: 'Editor', 0: 'Viewer'}.get(security_level, 'Viewer')
        except Exception as e:
            logging.error(f"Could not get DMS groups for {username}: {e}")
            security_str = db_user.get('security_level', 'Viewer')
            security_level = processor.get_security_level_int(security_str)

    # Tab permissions — admin override bypasses DB
    if security_str == 'Admin':
        tab_permissions = db_connector.get_admin_full_permissions()
    else:
        people_system_id = db_user.get('people_system_id')
        if people_system_id:
            tab_permissions = await db_connector.get_tab_permissions_for_user(people_system_id)
        else:
            tab_permissions = []

    tab_permissions = _ensure_ems_admin_tab_permission(
        tab_permissions,
        group_flags.get('is_ems_admin_group_member', False)
    )

    db_prefs = db_user

    user_details = {
        'username': username,
        'full_name': db_prefs.get('full_name') if db_prefs else username,
        'token': dst,
        'security_level': security_str,
        'dms_security_level': security_level,
        'lang': db_prefs.get('lang', 'en') if db_prefs else 'en',
        'theme': db_prefs.get('theme', 'light') if db_prefs else 'light',
        'view_mode': db_prefs.get('view_mode', 'grid') if db_prefs else 'grid',
        'profile_view_mode': db_prefs.get('profile_view_mode', 'grid') if db_prefs else 'grid',
        'tab_permissions': tab_permissions,
        **group_flags,
    }

    request.session['session_id'] = session_id
    request.session['user'] = user_details

    # Strip internal token before sending to client
    safe_user = {k: v for k, v in user_details.items() if k != 'token'}
    safe_user['session_id'] = session_id
    return {"message": login_type_msg, "user": safe_user}


@router.post('/api/auth/login')
@limiter.limit("255/minute")
async def login(
    request: Request,
    creds: LoginRequest,
    x_app_source: str = Header("unknown", alias="X-App-Source"),
):
    dst, error_msg = await run_in_threadpool(wsdl_client.dms_user_login, creds.username, creds.password)
    if not dst:
        raise HTTPException(status_code=401, detail=error_msg or "Invalid DMS credentials")

    # Check if user exists in Smart EDMS local DB (auto-provision if they exist in PEOPLE table)
    db_user, prov_err = await db_connector.ensure_edms_user(creds.username)
    if not db_user:
        raise HTTPException(status_code=403, detail=prov_err or "User doesn't have smart EDMS account")

    return await _build_and_store_user_session(
        request=request,
        username=creds.username,
        db_user=db_user,
        dst=dst,
        x_app_source=x_app_source,
        login_type_msg="Login successful"
    )


@router.api_route('/api/auth/sso', methods=['GET', 'POST'])
@limiter.limit("255/minute")
async def sso_login(
    request: Request,
    x_app_source: str = Header("SSO", alias="X-App-Source"),
):
    """
    Single Sign-On (SSO) login endpoint.
    Extracts the authenticated domain user identity (e.g. from IIS Windows Authentication
    headers like X-Remote-User / Remote-User, or request payload), validates against the
    PEOPLE table, auto-provisions a minimal EDMS profile if new, and initiates a session.
    """
    body_username = None
    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, dict):
                body_username = body.get("username") or body.get("desktop_user")
        except Exception:
            pass

    username = _extract_sso_username(request, body_username=body_username)
    if not username:
        raise HTTPException(
            status_code=401,
            detail="No SSO identity detected in request headers (e.g. X-Remote-User) or payload",
            headers={"WWW-Authenticate": "Negotiate, NTLM"}
        )

    # Check if user exists in Smart EDMS local DB (auto-provision if they exist in PEOPLE table)
    db_user, prov_err = await db_connector.ensure_edms_user(username)
    if not db_user:
        raise HTTPException(
            status_code=403,
            detail=prov_err or f"User '{username}' is not registered in the organization PEOPLE directory"
        )

    # Acquire system DMS token for SOAP operations
    dst = await run_in_threadpool(wsdl_client.dms_system_login)

    return await _build_and_store_user_session(
        request=request,
        username=username,
        db_user=db_user,
        dst=dst,
        x_app_source=x_app_source or "SSO",
        login_type_msg="SSO login successful"
    )


@router.post('/api/auth/logout')
async def logout(request: Request):
    session_id = request.session.get('session_id')

    # Best-effort: mark this server-side session inactive before clearing cookie.
    if session_id:
        try:
            from database.login_logs import mark_session_logout
            await mark_session_logout(session_id)
        except Exception as logout_log_err:
            logging.warning(f"Could not mark session logout for {session_id}: {logout_log_err}")

    request.session.clear()
    return {"message": "Logout successful"}

@router.get('/api/auth/user')
async def get_user(request: Request):
    user_session = request.session.get('user')
    if user_session and 'username' in user_session:
        user_details = await _get_user_details_for_session(user_session['username'], user_session)
        if user_details:
            # Preserve token if it exists in session but not in db details
            if 'token' in user_session:
                user_details['token'] = user_session['token']

            token = user_session.get('token')
            if token:
                group_flags = await _refresh_group_flags_for_session(
                    token,
                    user_session['username'],
                    user_session,
                )
                user_details.update(group_flags)
            else:
                user_details['is_ems_admin_group_member'] = user_session.get('is_ems_admin_group_member', False)
                user_details['is_docs_supervisor'] = user_session.get('is_docs_supervisor', False)

            # Fetch tab permissions — per-user
            user_details['tab_permissions'] = await _get_tab_permissions_for_session(
                user_details,
                user_session,
            )

            user_details['tab_permissions'] = _ensure_ems_admin_tab_permission(
                user_details.get('tab_permissions', []),
                user_details.get('is_ems_admin_group_member', False)
            )

            request.session['user'] = user_details

            # Strip internal token before sending to client
            safe_user = {k: v for k, v in user_details.items() if k != 'token'}
            safe_user['session_id'] = request.session.get('session_id')
            return {'user': safe_user}
        else:
            request.session.pop('user', None)
            return {'user': None}
    else:
        return {'user': None}

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

@router.put('/api/user/view-mode')
async def api_update_user_view_mode(request: Request, data: ViewModeUpdateRequest):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if data.view_mode not in ['grid', 'list']:
        raise HTTPException(status_code=400, detail="Invalid view mode")

    username = user['username']
    success = await db_connector.update_user_view_mode(username, data.view_mode)
    if success:
        user['view_mode'] = data.view_mode
        request.session['user'] = user
        return {"message": "View mode updated"}
    else:
        raise HTTPException(status_code=500, detail="Failed to update view mode")

@router.put('/api/user/profile-view-mode')
async def api_update_user_profile_view_mode(request: Request, data: ProfileViewModeUpdateRequest):
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if data.profile_view_mode not in ['grid', 'list']:
        raise HTTPException(status_code=400, detail="Invalid view mode")

    username = user['username']
    success = await db_connector.update_user_profile_view_mode(username, data.profile_view_mode)
    if success:
        user['profile_view_mode'] = data.profile_view_mode
        request.session['user'] = user
        return {"message": "Profile view mode updated"}
    else:
        raise HTTPException(status_code=500, detail="Failed to update profile view mode in database. Make sure PROFILE_VIEW_MODE column exists.")

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
