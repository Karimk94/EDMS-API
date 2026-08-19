import logging
from starlette.responses import JSONResponse
from database import login_logs


class SessionValidationMiddleware:
    """
    Pure ASGI middleware that enforces server-side session termination.

    It must run AFTER Starlette's SessionMiddleware has populated
    request.scope["session"]. Register it BEFORE SessionMiddleware in app.py so
    SessionMiddleware becomes the outer layer.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        session = scope.get("session") or {}
        session_id = session.get("session_id")

        if session_id:
            try:
                active = await login_logs.is_session_active(session_id)
                if not active:
                    # Terminated: clear the session so SessionMiddleware removes
                    # the cookie on the way out, then reject the request.
                    session.clear()
                    scope["session"] = session
                    response = JSONResponse(
                        status_code=401,
                        content={"detail": "Session has been terminated"},
                    )
                    await response(scope, receive, send)
                    return
            except Exception as exc:
                # Fail open — never block a request due to a session-check failure.
                logging.warning(
                    f"Session validation skipped for session {session_id}: {exc}"
                )

        await self.app(scope, receive, send)