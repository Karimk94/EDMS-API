from fastapi import APIRouter, HTTPException, Request, Query
from typing import List
import logging
from database import admin as admin_db
from schemas.auth import EdmsUserResponse, AddEdmsUserRequest, SecurityLevelResponse, PeopleSearchResult, UpdateEdmsUserRequest

router = APIRouter()

# Allowlist of usernames that can access admin panel regardless of security level
# Add usernames in lowercase
ADMIN_ALLOWLIST = ['test_user1', 'okool_kaabdulwahed', 'okool_arfelous']


def check_admin_access(request: Request) -> bool:
    """Check if current user has admin access (Editor+, Admin, or in allowlist)."""
    user = request.session.get('user')
    if not user:
        return False
    
    username = user.get('username', '').lower()
    security_level = user.get('security_level', '')
    
    # Check allowlist first
    if username in ADMIN_ALLOWLIST:
        return True
    
    # Check security level (Editor or Admin)
    if security_level in ['Editor', 'Admin']:
        return True
    
    return False


@router.get("/api/admin/users")
async def get_all_users(
    request: Request,
    search: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100)
):
    """Get paginated EDMS users. Requires Editor+ access or allowlist."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    result = await admin_db.get_all_edms_users(search=search, page=page, limit=limit)
    return result


@router.get("/api/admin/security-levels", response_model=List[SecurityLevelResponse])
async def get_security_levels(request: Request):
    """Get all available security levels. Requires Editor+ access or allowlist."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    levels = await admin_db.get_security_levels()
    return levels


@router.get("/api/admin/search-people", response_model=List[PeopleSearchResult])
async def search_people(request: Request, search: str = Query("")):
    """Search for users in PEOPLE table not yet in EDMS. Requires Editor+ access or allowlist."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    users = await admin_db.search_people(search)
    return users


@router.post("/api/admin/users")
async def add_user(request: Request, user_data: AddEdmsUserRequest):
    """Add a new user to EDMS security table. Requires Editor+ access or allowlist."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    success, message = await admin_db.add_edms_user(
        user_system_id=user_data.user_system_id,
        security_level_id=user_data.security_level_id,
        lang=user_data.lang,
        theme=user_data.theme,
        quota=user_data.quota
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}


@router.delete("/api/admin/users/{edms_user_id}")
async def delete_user(request: Request, edms_user_id: int):
    """Delete a user from EDMS security table. Requires Editor+ access or allowlist."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    success, message = await admin_db.delete_edms_user(edms_user_id)
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}


@router.put("/api/admin/users/{edms_user_id}")
async def update_user(request: Request, edms_user_id: int, user_data: UpdateEdmsUserRequest):
    """Update an existing user in EDMS security table. Requires Editor+ access or allowlist."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    success, message = await admin_db.update_edms_user(
        edms_user_id=edms_user_id,
        security_level_id=user_data.security_level_id,
        lang=user_data.lang,
        theme=user_data.theme,
        remaining_quota=user_data.remaining_quota,
        quota=user_data.quota
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}


@router.get("/api/admin/check-access")
async def check_access(request: Request):
    """Check if current user has admin access. Used by frontend for route protection."""
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    has_access = check_admin_access(request)
    return {"has_access": has_access, "username": user.get('username')}
