from fastapi import APIRouter, HTTPException, Request
import logging
import wsdl_client
from database.ems_admin import (
    get_agencies,
    get_sections,
    add_section,
    update_section,
    get_departments,
    add_department,
    update_department,
    get_departments_by_agency,
    get_ems_sections,
    add_ems_section,
    update_ems_section
)

router = APIRouter()
EMS_ADMIN_GROUP_ID = 'EMS_ADMIN'

# --- Authorization Helper ---
def _has_ems_admin_tab_access(user: dict) -> bool:
    tab_permissions = user.get("tab_permissions") or []
    for perm in tab_permissions:
        if str(perm.get("tab_key", "")).strip().lower() == "ems_admin" and bool(perm.get("can_read")):
            return True
    return False


async def check_admin_access(request: Request) -> bool:
    """Check if user has admin access (EMS_ADMIN group or hardcoded admins)."""
    if "user" not in request.session:
        return False
    
    user = request.session.get("user", {})
    username = str(user.get("username", "")).strip().lower()
    
    # HARDCODED LIST OF USERNAMES
    HARDCODED_EMS_ADMINS = [
        "admin",
        # Add other hardcoded usernames here
    ]
    
    if username in [u.lower() for u in HARDCODED_EMS_ADMINS]:
        return True

    # Fast path: cached group membership from login/session refresh
    if user.get("is_ems_admin_group_member") is True:
        return True

    token = user.get("token")
    original_username = user.get("username")
    if token and original_username:
        try:
            user_groups = wsdl_client.get_groups_for_user(token, original_username)
            target_group = EMS_ADMIN_GROUP_ID.upper()
            for group in user_groups or []:
                group_id = str(group.get("group_id", "")).strip().upper()
                group_name = str(group.get("group_name", "")).strip().upper()
                if group_id == target_group or group_name == target_group:
                    user["is_ems_admin_group_member"] = True
                    request.session["user"] = user
                    return True
        except Exception as exc:
            logging.warning(f"Failed to validate EMS admin group membership for user {original_username}: {exc}")

    return False


from fastapi import Response

@router.get("/api/ems-admin/check-access")
async def check_ems_admin_access(request: Request, response: Response):
    """Check if current user can access EMS admin area."""
    # Ensure no caching for this endpoint
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    
    if "user" not in request.session:
        raise HTTPException(status_code=401, detail="Not authenticated")

    has_access = await check_admin_access(request)
    user = request.session.get("user", {})
    return {"has_access": has_access, "username": user.get("username")}


# --- AGENCIES ---

@router.get("/api/departments/agencies")
async def get_agencies_endpoint(request: Request):
    """Get all active agencies."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        agencies = await get_agencies()
        return {
            "agencies": agencies,
            "success": True
        }
    except Exception as e:
        logging.error(f"Error fetching agencies: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# --- SECTIONS (Companies) ---

@router.get("/api/sections")
async def get_sections_endpoint(
    request: Request,
    name: str = "",
    disabled: str = "N",
    page: int = 1,
    per_page: int = 10
):
    """Get sections (companies) with pagination and search."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        result = await get_sections(name=name, disabled=disabled, page=page, per_page=per_page)
        return {
            **result,
            "success": True
        }
    except Exception as e:
        logging.error(f"Error fetching sections: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/sections/add")
async def add_section_endpoint(request: Request, data: dict):
    """Add a new section (company)."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        name = data.get("name")
        translation = data.get("translation", "")
        
        if not name:
            raise HTTPException(status_code=400, detail="Section name is required")
        
        success, message = await add_section(name, translation)
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {
            "success": True,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error adding section: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/sections/update")
async def update_section_endpoint(request: Request, data: dict):
    """Update an existing section (company)."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        secid = data.get("secid")
        name = data.get("name")
        translation = data.get("translation", "")
        disabled = data.get("disabled", "N")
        
        if not secid or not name:
            raise HTTPException(status_code=400, detail="Section ID and name are required")
        
        success, message = await update_section(secid, name, translation, disabled)
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {
            "success": True,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating section: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# --- DEPARTMENTS ---

@router.get("/api/departments")
async def get_departments_endpoint(
    request: Request,
    name: str = "",
    agency_id: int = None,
    page: int = 1,
    per_page: int = 10
):
    """Get departments with pagination, search, and agency filter."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        result = await get_departments(name=name, agency_id=agency_id, page=page, per_page=per_page)
        return {
            **result,
            "success": True
        }
    except Exception as e:
        logging.error(f"Error fetching departments: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/departments/add")
async def add_department_endpoint(request: Request, data: dict):
    """Add a new department."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        name = data.get("name")
        translation = data.get("translation", "")
        short = data.get("short", "")
        agency_system_id = data.get("agency_system_id")
        
        if not name or not agency_system_id:
            raise HTTPException(status_code=400, detail="Department name and agency are required")
        
        success, message = await add_department(name, translation, short, agency_system_id)
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {
            "success": True,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error adding department: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/departments/update")
async def update_department_endpoint(request: Request, data: dict):
    """Update an existing department."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        deptid = data.get("deptid")
        name = data.get("name")
        translation = data.get("translation", "")
        
        if not deptid or not name:
            raise HTTPException(status_code=400, detail="Department ID and name are required")
        
        success, message = await update_department(deptid, name, translation)
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {
            "success": True,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating department: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# --- EMS SECTIONS (Hierarchical Sections under Departments) ---

@router.get("/api/ems_sections/departments_by_agency/{agency_system_id}")
async def get_departments_by_agency_endpoint(
    request: Request,
    agency_system_id: int
):
    """Get departments filtered by agency."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        departments = await get_departments_by_agency(agency_system_id)
        return {
            "departments": departments,
            "success": True
        }
    except Exception as e:
        logging.error(f"Error fetching departments by agency: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/ems_sections")
async def get_ems_sections_endpoint(
    request: Request,
    dept_system_id: int = None,
    name: str = "",
    page: int = 1,
    per_page: int = 10
):
    """Get EMS sections with pagination, search, and department filter."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        result = await get_ems_sections(dept_system_id=dept_system_id, name=name, page=page, per_page=per_page)
        return {
            **result,
            "success": True
        }
    except Exception as e:
        logging.error(f"Error fetching EMS sections: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/ems_sections/add")
async def add_ems_section_endpoint(request: Request, data: dict):
    """Add a new EMS section within a department."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        ems_code = data.get("ems_code")
        name = data.get("name")
        translation = data.get("translation", "")
        dept_system_id = data.get("dept_system_id")
        
        if not ems_code or not name or not dept_system_id:
            raise HTTPException(status_code=400, detail="EMS Code, name, and department are required")
        
        success, message = await add_ems_section(ems_code, name, translation, dept_system_id)
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {
            "success": True,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error adding EMS section: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/ems_sections/update")
async def update_ems_section_endpoint(request: Request, data: dict):
    """Update an existing EMS section."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        secid = data.get("secid")
        name = data.get("name")
        translation = data.get("translation", "")
        disabled = data.get("disabled", "N")
        parent_dept_system_id = data.get("parent_dept_system_id")
        
        if not secid or not name or not parent_dept_system_id:
            raise HTTPException(status_code=400, detail="EMS Section ID, name, and department are required")
        
        success, message = await update_ems_section(secid, name, translation, disabled, parent_dept_system_id)
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {
            "success": True,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating EMS section: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
