from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel

from routes.admin import ADMIN_ALLOWLIST
from schemas.edocs_cache import (
    EdocsClearCacheAcceptedResponse,
    EdocsClearCacheRequest,
)
from services.edocs_cache import get_server_edocs_cache_root, run_server_edocs_cache_clear


router = APIRouter()


class AdminPrincipal(BaseModel):
    username: str


def get_current_admin(request: Request) -> AdminPrincipal:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    username = str(user.get("username") or "").strip().lower()
    if username not in ADMIN_ALLOWLIST:
        raise HTTPException(status_code=403, detail="Access denied. Admin privileges required.")

    return AdminPrincipal(username=username)


@router.post(
    "/api/admin/edocs/clear-cache",
    response_model=EdocsClearCacheAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def clear_edocs_cache(
    background_tasks: BackgroundTasks,
    request_data: EdocsClearCacheRequest | None = None,
    admin: AdminPrincipal = Depends(get_current_admin),
) -> EdocsClearCacheAcceptedResponse:
    requested_user_id = request_data.user_id if request_data else None
    background_tasks.add_task(
        run_server_edocs_cache_clear,
        requested_by=admin.username,
        user_id=requested_user_id,
    )

    return EdocsClearCacheAcceptedResponse(
        status="accepted",
        message="Server eDOCS cache clear started in the background.",
        server_cache_root=str(get_server_edocs_cache_root()),
    )
