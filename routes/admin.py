from fastapi import APIRouter, HTTPException, Request, Query
from typing import List, Optional
from pydantic import BaseModel
import logging
import oracledb
import re
import wsdl_client
from wsdl_client.auth import dms_system_login
from wsdl_client.documents import force_checkin_document_soap
from wsdl_client.base import get_soap_client, find_client_with_operation
from zeep import xsd
from database import admin as admin_db
from schemas.admin import BulkCheckinRequest
from database import documents as documents_db
from database import tab_permissions as tab_perms_db
from database import login_logs as login_logs_db
from database.connection import get_async_connection
from schemas.auth import EdmsUserResponse, AddEdmsUserRequest, SecurityLevelResponse, PeopleSearchResult, UpdateEdmsUserRequest
from services.processing_queue import processing_queue, set_worker_paused, is_worker_paused, request_worker_drain, get_worker_mode, set_worker_mode
import db_connector

router = APIRouter()

# Allowlist of usernames that can access admin panel regardless of security level
# Add usernames in lowercase
ADMIN_ALLOWLIST = ['test_user1', 'okool_kaabdulwahed', 'okool_arfelous', 'dmedms']
EMS_ADMIN_GROUP_ID = 'EMS_ADMIN'


def check_admin_access(request: Request) -> bool:
    """Check if current user has admin access (allowlist only)."""
    user = request.session.get('user')
    if not user:
        return False
    
    username = user.get('username', '').lower()
    return username in ADMIN_ALLOWLIST


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


@router.get("/api/admin/history/{docnumber}")
async def get_document_history(request: Request, docnumber: int):
    """Get activity history for a specific document/folder. Requires admin or owner access."""
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
        
    is_admin = check_admin_access(request)
    
    if not is_admin:
        doc = await documents_db.get_document_by_id(docnumber)
        username = user.get('username')
        if not doc or doc.get('author') != username:
            raise HTTPException(status_code=403, detail="Access denied. Admin or owner privileges required.")
    
    history = await admin_db.get_document_history(docnumber)
    return {"history": history}


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


# --- Login History Endpoints ---

@router.get("/api/admin/login-history")
async def get_login_history(
    request: Request,
    search: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100)
):
    """Get Smart EDMS login history. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    result = await login_logs_db.get_login_history(search=search, page=page, limit=limit)
    return result


@router.get("/api/admin/active-sessions")
async def get_active_sessions(request: Request):
    """Get all currently active Smart EDMS sessions. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    sessions = await login_logs_db.get_active_sessions()
    return {"sessions": sessions}


class TerminateSessionRequest(BaseModel):
    session_id: str


@router.post("/api/admin/active-sessions/terminate")
async def terminate_session(request: Request, data: TerminateSessionRequest):
    """Terminate an active session server-side. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    success, message = await login_logs_db.terminate_session(data.session_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)

    return {"message": message}


# --- Profiles Tab Endpoints ---

@router.get("/api/admin/profiles")
async def get_profiles(request: Request, search: str = ""):
    """Get all EDMS profiles. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    profiles = await admin_db.get_admin_profiles(search=search)
    return {"profiles": profiles}


@router.get("/api/admin/profiles/{profile_system_id}/groups")
async def get_profile_groups(request: Request, profile_system_id: int, search: str = ""):
    """Get groups mapped to a specific profile. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    groups = await admin_db.get_profile_groups(profile_system_id, search=search)
    return {"groups": groups}


@router.get("/api/admin/groups/{group_system_id}/users")
async def get_group_users(request: Request, group_system_id: int, search: str = ""):
    """Get users mapped to a specific group. Requires admin access."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    users = await admin_db.get_group_users(group_system_id, search=search)
    return {"users": users}


# --- SQL Query Endpoints ---

# Allowed tables for SQL queries (whitelist)
ALLOWED_TABLES = {
    'PROFILE': 'Main document/profile table',
    'PEOPLE': 'User accounts and information',
    'FOLDER_ITEM': 'Folder structure and contents',
    'KEYWORD': 'Tags/keywords for documents',
    'LKP_PERSON': 'VIP/Person lookup table',
    'LKP_DOCUMENT_TAGS': 'Document-tag relationships',
    'LKP_EDMS_USR_SECUR': 'EDMS user security settings',
    'LKP_EDMS_SECURITY': 'Security levels (Admin, Editor, Viewer)',
    'LKP_EDMS_USR_DATA': 'User quota data',
    'LKP_FAVORITES_DOC': 'User favorites',
    'LKP_AS_SE': 'Login sessions and history',
    'TAGGING_QUEUE': 'AI processing queue',
    'APPS': 'Application/file type mappings',
    'FORMS': 'Form definitions',
    'GROUPS': 'User groups',
    'PEOPLEGROUPS': 'User-group relationships',
    'FORM_USER': 'Form permissions',
    'SIEM_EDMS_VIEW': 'Activity/audit log view',
    'LKP_TAB_PERMISSIONS': 'Tab permissions for users',
}

# Query templates for common operations
QUERY_TEMPLATES = [
    {
        'name': 'Recent Documents',
        'description': 'Get the 10 most recent documents',
        'query': 'SELECT DOCNUMBER, ABSTRACT, AUTHOR, RTADOCDATE, DOCNAME FROM PROFILE WHERE FORM = 2740 ORDER BY DOCNUMBER DESC FETCH FIRST 10 ROWS ONLY'
    },
    {
        'name': 'User List',
        'description': 'Get all EDMS users with their security levels',
        'query': "SELECT p.USER_ID, p.FULL_NAME, sl.NAME as SECURITY_LEVEL, us.LANG, us.THEME FROM LKP_EDMS_USR_SECUR us JOIN PEOPLE p ON us.USER_ID = p.SYSTEM_ID JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID ORDER BY p.USER_ID"
    },
    {
        'name': 'Folder Contents',
        'description': 'Get items in a specific folder (replace FOLDER_DOCNUMBER)',
        'query': "SELECT DOCNUMBER, DISPLAYNAME, NODE_TYPE FROM FOLDER_ITEM WHERE PARENT = 'FOLDER_DOCNUMBER' ORDER BY NODE_TYPE, DISPLAYNAME"
    },
    {
        'name': 'Document Tags',
        'description': 'Get all tags for a specific document (replace DOCNUMBER)',
        'query': "SELECT k.KEYWORD_ID, k.DESCRIPTION FROM LKP_DOCUMENT_TAGS ldt JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID WHERE ldt.DOCNUMBER = DOCNUMBER"
    },
    {
        'name': 'Active Sessions',
        'description': 'Get currently active user sessions',
        'query': "SELECT l.SESSION_ID, p.USER_ID, p.FULL_NAME, l.LOGIN_DATE, l.LAST_ACTIVITY, l.APP_SOURCE, l.IP_ADDRESS FROM LKP_AS_SE l JOIN PEOPLE p ON l.PEOPLE_SYSTEM_ID = p.SYSTEM_ID WHERE l.IS_ACTIVE = 'Y' ORDER BY l.LAST_ACTIVITY DESC"
    },
    {
        'name': 'Processing Queue Status',
        'description': 'Get documents pending AI processing',
        'query': 'SELECT DOCNUMBER, ATTEMPTS, STATUS, ERROR, LAST_UPDATE FROM TAGGING_QUEUE WHERE STATUS != 3 ORDER BY LAST_UPDATE DESC FETCH FIRST 20 ROWS ONLY'
    },
    {
        'name': 'Tag Usage Count',
        'description': 'Get most used tags/keywords',
        'query': "SELECT k.KEYWORD_ID, k.DESCRIPTION, COUNT(ldt.DOCNUMBER) as DOC_COUNT FROM KEYWORD k JOIN LKP_DOCUMENT_TAGS ldt ON k.SYSTEM_ID = ldt.TAG_ID GROUP BY k.KEYWORD_ID, k.DESCRIPTION ORDER BY DOC_COUNT DESC FETCH FIRST 20 ROWS ONLY"
    },
    {
        'name': 'VIP Person Documents',
        'description': 'Find documents containing a specific VIP name (replace NAME)',
        'query': "SELECT DOCNUMBER, ABSTRACT, AUTHOR FROM PROFILE WHERE FORM = 2740 AND UPPER(ABSTRACT) LIKE '%NAME%' FETCH FIRST 20 ROWS ONLY"
    },
]


class SqlQueryRequest(BaseModel):
    query: str


def validate_sql_query(sql: str) -> tuple[bool, str]:
    """
    Validates that a SQL query is safe to execute.
    Returns (is_valid, error_message).
    """
    if not sql or not sql.strip():
        return False, "Query cannot be empty"
    
    # Remove comments
    sql_cleaned = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql_cleaned = re.sub(r'/\*.*?\*/', '', sql_cleaned, flags=re.DOTALL)
    sql_cleaned = sql_cleaned.strip()
    
    # Strip trailing semicolons
    sql_cleaned = sql_cleaned.rstrip(';').strip()
    
    if not sql_cleaned:
        return False, "Query cannot be empty after removing comments"
    
    # Check for multiple statements (semicolons remaining)
    if ';' in sql_cleaned:
        return False, "Multiple SQL statements are not permitted"
    
    # Check for SELECT statement
    sql_upper = sql_cleaned.upper()
    if not sql_upper.startswith('SELECT'):
        return False, "Only SELECT statements are allowed"
    
    # Block dangerous keywords
    blocked_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE',
        'MERGE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL', 'BEGIN', 'DECLARE',
        'INTO', 'BULK', 'COLLECT'
    ]
    
    for keyword in blocked_keywords:
        # Use word boundary to avoid false positives
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"Keyword '{keyword}' is not allowed. Only SELECT statements are permitted."
    
    # Check for allowed tables
    # Extract table names from the query (simplified approach)
    # Look for FROM and JOIN keywords followed by table names
    table_pattern = r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)'
    found_tables = re.findall(table_pattern, sql_upper)
    
    if not found_tables:
        return False, "Query must specify a valid table (e.g. FROM <table_name>). Click 'Show Schema' to view allowed tables."
    
    for table in found_tables:
        if table not in ALLOWED_TABLES:
            allowed_list = ', '.join(sorted(ALLOWED_TABLES.keys()))
            return False, f"Table '{table}' does not exist or is not permitted. Allowed tables are: {allowed_list}"
    
    return True, ""


@router.get("/api/admin/sql-query/tables")
async def get_allowed_tables(request: Request):
    """Get list of allowed tables and their schemas for SQL queries."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    conn = await get_async_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    tables_info = []
    try:
        async with conn.cursor() as cursor:
            for table_name in sorted(ALLOWED_TABLES.keys()):
                # Get column information for each table
                try:
                    await cursor.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
                        FROM USER_TAB_COLUMNS
                        WHERE TABLE_NAME = :table_name
                        ORDER BY COLUMN_ID
                    """, table_name=table_name)
                    
                    columns = []
                    async for row in cursor:
                        columns.append({
                            'name': row[0],
                            'type': row[1],
                            'length': row[2],
                            'nullable': row[3] == 'Y'
                        })
                    
                    tables_info.append({
                        'name': table_name,
                        'description': ALLOWED_TABLES[table_name],
                        'columns': columns
                    })
                except oracledb.Error:
                    # Table might not exist or no access
                    tables_info.append({
                        'name': table_name,
                        'description': ALLOWED_TABLES[table_name],
                        'columns': [],
                        'error': 'Could not fetch column information'
                    })
    finally:
        if conn:
            await conn.close()
    
    return {"tables": tables_info}


@router.get("/api/admin/sql-query/templates")
async def get_query_templates(request: Request):
    """Get predefined query templates."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    return {"templates": QUERY_TEMPLATES}


@router.post("/api/admin/sql-query")
async def execute_sql_query(request: Request, body: SqlQueryRequest):
    """Execute a read-only SQL query and return results."""
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")
    
    # Strip trailing semicolon and whitespace
    clean_query = body.query.strip().rstrip(';').strip()
    
    # Validate the query
    is_valid, error_message = validate_sql_query(clean_query)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error_message)
    
    conn = await get_async_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    MAX_ROWS = 1000
    QUERY_TIMEOUT_SECONDS = 30
    
    try:
        async with conn.cursor() as cursor:
            # Set array size for better performance
            cursor.arraysize = 100
            
            # Execute the query
            await cursor.execute(clean_query)
            
            # Get column names
            if cursor.description:
                columns = [col[0] for col in cursor.description]
            else:
                columns = []
            
            # Fetch rows with limit
            rows = []
            row_count = 0
            truncated = False
            
            async for row in cursor:
                if row_count >= MAX_ROWS:
                    truncated = True
                    break
                
                # Convert row to list and handle Oracle-specific types
                processed_row = []
                for value in row:
                    if isinstance(value, oracledb.LOB):
                        processed_row.append(value.read())
                    elif hasattr(value, 'isoformat'):
                        # Handle datetime objects
                        processed_row.append(value.isoformat())
                    else:
                        processed_row.append(value)
                
                rows.append(processed_row)
                row_count += 1
            
            return {
                "columns": columns,
                "rows": rows,
                "total_rows": len(rows),
                "truncated": truncated,
                "max_rows": MAX_ROWS
            }
    
    except oracledb.Error as e:
        error_msg = str(e)
        # Extract more readable error message
        if hasattr(e, 'args') and e.args:
            error_obj = e.args[0]
            if hasattr(error_obj, 'message'):
                error_msg = error_obj.message
        
        logging.error(f"SQL Query error: {error_msg}")
        
        # Provide user-friendly messages for common Oracle errors
        if "ORA-00942" in error_msg:
            friendly_msg = "Table or view does not exist in the database (ORA-00942). Please check the table name."
        elif "ORA-00904" in error_msg:
            friendly_msg = f"Invalid column name (ORA-00904): {error_msg}. Check the table schema."
        elif "ORA-00911" in error_msg:
            friendly_msg = "Invalid character in query (ORA-00911). Ensure there are no special or trailing characters."
        else:
            friendly_msg = f"Query error: {error_msg}"
            
        raise HTTPException(status_code=400, detail=friendly_msg)
    
    except Exception as e:
        logging.error(f"Unexpected SQL query error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
    finally:
        if conn:
            await conn.close()


# ---------------------------------------------------------------------------
# Checked-Out Documents Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/admin/checked-out")
async def get_checked_out_documents(
    request: Request,
    days: int = Query(1, ge=1, le=365, description="How many days back to look"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """
    Return all documents currently checked out (CHECKIN_DATE IS NULL).
    Filters by `date_from` and `date_to` if provided, otherwise by the last `days`.
    """
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    try:
        docs = await admin_db.fetch_checked_out_docs(days=days, date_from=date_from, date_to=date_to)
        return {"documents": docs, "total": len(docs), "days": days, "date_from": date_from, "date_to": date_to}
    except Exception as exc:
        logging.error(f"Error fetching checked-out documents: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching checked-out documents: {str(exc)}")


@router.post("/api/admin/checked-out/checkin")
async def force_checkin_documents(
    request: Request,
    body: BulkCheckinRequest,
):
    """
    Force-check-in the provided list of document numbers via DMS SOAP (UpdateObject %UNLOCK).
    Returns per-document success/failure results.
    """
    if not check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    if not body.docnumbers:
        raise HTTPException(status_code=400, detail="No document numbers provided.")

    # Login to DMS using cached system token
    dst = dms_system_login(force_refresh=True)
    if not dst:
        raise HTTPException(status_code=503, detail="Could not authenticate with DMS service.")

    results = []
    for doc_number in body.docnumbers:
        success, result_code = force_checkin_document_soap(dst, doc_number)
        results.append({
            "docnumber": doc_number,
            "success": success,
            "result_code": result_code,
        })
        if success:
            logging.info(f"[Admin CheckIn] Force checked-in doc {doc_number}")
        else:
            logging.warning(f"[Admin CheckIn] Failed for doc {doc_number}, resultCode={result_code}")

    checked_in = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"])

    return {
        "results": results,
        "checked_in": checked_in,
        "failed": failed,
        "total": len(results),
    }
