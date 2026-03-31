from fastapi import APIRouter, HTTPException, Request, Query
from typing import List
from pydantic import BaseModel
import logging
import wsdl_client
from database import admin as admin_db
from database import tab_permissions as tab_perms_db
from schemas.auth import EdmsUserResponse, AddEdmsUserRequest, SecurityLevelResponse, PeopleSearchResult, UpdateEdmsUserRequest
from services.processing_queue import processing_queue, set_worker_paused, is_worker_paused, request_worker_drain, get_worker_mode, set_worker_mode
import db_connector

router = APIRouter()

# Allowlist of usernames that can access admin panel regardless of security level
# Add usernames in lowercase
ADMIN_ALLOWLIST = ['test_user1', 'okool_kaabdulwahed', 'okool_arfelous', 'dmedms']
EMS_ADMIN_GROUP_ID = 'EMS_ADMIN'


def check_admin_access(request: Request) -> bool:
    """Check if current user has admin access (Editor+, Admin, allowlist, or EMS_ADMIN group)."""
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

    # Fast path: cached group membership from login/session refresh
    if user.get('is_ems_admin_group_member') is True:
        return True

    # Check DMS group membership for EMS admin group
    token = user.get('token')
    username = user.get('username')
    if token and username:
        try:
            user_groups = wsdl_client.get_groups_for_user(token, username)
            target_group = EMS_ADMIN_GROUP_ID.upper()
            for group in user_groups or []:
                group_id = str(group.get('group_id', '')).strip().upper()
                group_name = str(group.get('group_name', '')).strip().upper()
                if group_id == target_group or group_name == target_group:
                    user['is_ems_admin_group_member'] = True
                    request.session['user'] = user
                    return True
        except Exception as exc:
            logging.warning(f"Failed to validate EMS admin group membership for user {username}: {exc}")
    
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


@router.get("/api/admin/processing-queue/status")
async def get_processing_queue_status(request: Request):
    """Returns local processing queue status for admin monitoring."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    summary = processing_queue.get_status_summary()
    failures = processing_queue.get_recent_failures(limit=8)
    in_progress = processing_queue.get_recent_jobs_by_status(status="in_progress", limit=8)
    last_mode_change = processing_queue.get_last_worker_mode_change()

    # Show a small preview of Oracle TAGGING_QUEUE candidates so admins can
    # verify pending upstream work even when local worker queue is empty/paused.
    oracle_pending = await db_connector.get_documents_to_process()
    oracle_pending_count = await db_connector.get_documents_to_process_count()
    queued_preview = [
        {
            "docnumber": int(row.get("docnumber")),
            "attempts": int(row.get("attempts") or 0),
            "error": "",
            "updated_at": "-",
            "status": "queued",
        }
        for row in oracle_pending
        if row.get("docnumber") is not None
    ]

    return {
        "summary": summary,
        "recent_failures": failures,
        "recent_in_progress": in_progress,
        "recent_queued": queued_preview,
        "oracle_queued_count": oracle_pending_count,
        "source_counts": {
            "local_failed": int(summary.get("failed", 0)),
            "local_in_progress": int(summary.get("in_progress", 0)),
            "oracle_queued": int(oracle_pending_count),
        },
        "worker_paused": is_worker_paused(),
        "worker_mode": get_worker_mode(),
        "last_mode_change": last_mode_change,
    }


@router.post("/api/admin/processing-queue/worker/pause")
async def pause_processing_worker(request: Request):
    """Pause local queue worker without stopping the API process."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    actor = ((request.session.get('user') or {}).get('username') or 'unknown')
    set_worker_mode("paused", actor=actor, reason="manual pause")
    paused = set_worker_paused(True)
    return {"message": "Processing worker paused.", "worker_paused": paused}


@router.post("/api/admin/processing-queue/worker/resume")
async def resume_processing_worker(request: Request):
    """Resume local queue worker after a pause."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    actor = ((request.session.get('user') or {}).get('username') or 'unknown')
    set_worker_mode("running", actor=actor, reason="manual resume")
    paused = set_worker_paused(False)
    return {"message": "Processing worker resumed.", "worker_paused": paused}


@router.post("/api/admin/processing-queue/worker/drain")
async def drain_processing_worker(request: Request):
    """Gracefully drain current work, then transition worker to paused."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    actor = ((request.session.get('user') or {}).get('username') or 'unknown')
    mode = request_worker_drain()
    set_worker_mode(mode, actor=actor, reason="manual drain")
    return {"message": "Processing worker set to draining mode.", "worker_mode": mode}


@router.post("/api/admin/processing-queue/retry-failed")
async def retry_failed_queue_jobs(
    request: Request,
    limit: int = Query(100, ge=1, le=1000)
):
    """Retry failed local queue jobs and reset Oracle TAGGING_QUEUE attempts for those docs."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    failed_docnumbers = processing_queue.get_failed_docnumbers(limit=limit)
    if not failed_docnumbers:
        return {
            "message": "No failed jobs to retry.",
            "failed_found": 0,
            "db_reset_count": 0,
            "requeued": 0,
        }

    db_reset_count = await db_connector.reset_processing_attempts(failed_docnumbers)
    retry_result = processing_queue.retry_failed_jobs(failed_docnumbers)
    return {
        "message": "Failed jobs requeued.",
        "failed_found": len(failed_docnumbers),
        "db_reset_count": db_reset_count,
        "requeued": int(retry_result.get("requeued", 0)),
    }


@router.delete("/api/admin/processing-queue/completed")
async def clear_completed_queue_jobs(
    request: Request,
    older_than_hours: int = Query(24, ge=0, le=24 * 365)
):
    """Remove completed jobs from local queue table to keep it lean."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    result = processing_queue.purge_completed_jobs(older_than_hours=older_than_hours)
    return {
        "message": "Completed queue jobs cleared.",
        "deleted": int(result.get("deleted", 0)),
        "older_than_hours": int(result.get("older_than_hours", older_than_hours)),
    }


class RetryQueueDocsRequest(BaseModel):
    docnumbers: List[int]


@router.post("/api/admin/processing-queue/retry-selected")
async def retry_selected_queue_jobs(request: Request, body: RetryQueueDocsRequest):
    """Retry selected queue jobs by docnumber and reset Oracle attempts for those docs."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    selected = []
    for doc in body.docnumbers:
        try:
            selected.append(int(doc))
        except (TypeError, ValueError):
            continue

    if not selected:
        raise HTTPException(status_code=400, detail="No valid docnumbers provided.")

    db_reset_count = await db_connector.reset_processing_attempts(selected)
    retry_result = processing_queue.retry_failed_jobs(selected)
    return {
        "message": "Selected jobs requeued.",
        "selected_count": len(selected),
        "db_reset_count": db_reset_count,
        "requeued": int(retry_result.get("requeued", 0)),
    }


# --- Tab Permissions Endpoints (Per-User) ---

class UpsertTabPermissionRequest(BaseModel):
    user_id: int  # PEOPLE.SYSTEM_ID
    tab_key: str
    can_read: bool = True
    can_write: bool = False


@router.get("/api/admin/tab-permissions/{user_id}")
async def get_user_tab_permissions(request: Request, user_id: int):
    """Get tab permissions for a specific user. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    perms = await tab_perms_db.get_tab_permissions_for_user_admin(user_id)
    return {"permissions": perms}


@router.put("/api/admin/tab-permissions")
async def upsert_tab_permission(request: Request, data: UpsertTabPermissionRequest):
    """Insert or update a tab permission for a user. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    success, message = await tab_perms_db.upsert_tab_permission(
        user_id=data.user_id,
        tab_key=data.tab_key,
        can_read=data.can_read,
        can_write=data.can_write
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}


@router.post("/api/admin/tab-permissions/init/{user_id}")
async def init_user_tab_permissions(request: Request, user_id: int):
    """Create default tab permissions for a user. Called after adding a new user."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    success, message = await tab_perms_db.create_default_permissions_for_user(user_id)
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}


@router.delete("/api/admin/tab-permissions/{permission_id}")
async def delete_tab_permission(request: Request, permission_id: int):
    """Delete a specific tab permission. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    success, message = await tab_perms_db.delete_tab_permission(permission_id)
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}
