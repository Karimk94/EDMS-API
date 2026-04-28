from fastapi import APIRouter, HTTPException, Request, Query, Depends
from typing import List, Optional, Any, Dict
from pydantic import BaseModel
from routes.ems_admin import check_admin_access
from database import edms_people

router = APIRouter()

# --- Pydantic Models ---

class EdmsUserCreate(BaseModel):
    user_id: str
    full_name: str
    email: str
    password_plain: str
    primary_group: int
    allow_login: str
    disabled: str
    secid: int
    additional_groups: List[int]
    network_aliases: List[str]
    hr_login: Optional[str] = None
    hr_empno: Optional[str] = None


class EdmsUserUpdate(BaseModel):
    full_name: str
    email: str
    password_plain: Optional[str] = None
    primary_group: int
    allow_login: str
    disabled: str
    secid: int
    additional_groups: List[int]
    network_aliases: List[str]


# --- Endpoints ---

@router.get("/api/edms-people")
async def get_edms_people(
    request: Request,
    search: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100)
):
    """Get paginated EDMS users (PEOPLE table). Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    
    result = await edms_people.get_edms_people(search=search, page=page, limit=limit)
    return result


@router.get("/api/edms-people/hr-employees")
async def search_hr_employees(
    request: Request,
    search: str = Query("")
):
    """Search HR employees not yet in EDMS. Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    
    employees = await edms_people.search_hr_employees(search)
    return employees


@router.get("/api/edms-people/hr/agencies")
async def get_hr_agencies(request: Request):
    """Get all agencies for HR selection. Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    return await edms_people.get_hr_agencies()

@router.get("/api/edms-people/hr/departments")
async def get_hr_departments(request: Request, agency_id: int = Query(...)):
    """Get departments for a specific agency. Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    return await edms_people.get_hr_departments(agency_id)

@router.get("/api/edms-people/hr/sections")
async def get_hr_sections(request: Request, dept_id: int = Query(...)):
    """Get sections for a specific department. Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    return await edms_people.get_hr_sections(dept_id)

@router.get("/api/edms-people/groups")
async def get_all_groups(request: Request):
    """Get all EDMS groups for dropdown selection. Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    
    groups = await edms_people.get_all_groups()
    return groups


@router.get("/api/edms-people/{system_id}/details")
async def get_person_details(request: Request, system_id: int):
    """Get group memberships and aliases for a user. Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
    
    details = await edms_people.get_person_details(system_id)
    return details


@router.post("/api/edms-people")
async def add_edms_person(request: Request, data: EdmsUserCreate):
    """Create a new EDMS user (PEOPLE table). Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
        
    if not data.user_id:
        raise HTTPException(status_code=400, detail="Username is required")
        
    success, message = await edms_people.add_edms_person(
        user_id=data.user_id,
        full_name=data.full_name,
        email=data.email,
        password_plain=data.password_plain,
        primary_group=data.primary_group,
        allow_login=data.allow_login,
        disabled=data.disabled,
        secid=data.secid,
        additional_groups=data.additional_groups,
        network_aliases=data.network_aliases,
        hr_login=data.hr_login,
        hr_empno=data.hr_empno
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
        
    return {"message": message}


@router.put("/api/edms-people/{system_id}")
async def update_edms_person(request: Request, system_id: int, data: EdmsUserUpdate):
    """Update an existing EDMS user (PEOPLE table). Requires EMS Admin."""
    if not await check_admin_access(request):
        raise HTTPException(status_code=403, detail="Access denied. EMS Admin privileges required.")
        
    success, message = await edms_people.update_edms_person(
        system_id=system_id,
        full_name=data.full_name,
        email=data.email,
        password_plain=data.password_plain,
        primary_group=data.primary_group,
        allow_login=data.allow_login,
        disabled=data.disabled,
        secid=data.secid,
        additional_groups=data.additional_groups,
        network_aliases=data.network_aliases
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
        
    return {"message": message}
