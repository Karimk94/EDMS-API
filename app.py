import logging
import os
import asyncio
import time
import uuid
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv
from database.connection import ensure_performance_indexes

# Import Routers
from routes import auth, documents, media, tags, events, folders, favorites, memories, sharing, admin, ems_admin, profilesearch
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

# Load environment variables
load_dotenv()

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Log Filtering ---
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# --- Background Cache Eviction ---
from utils.cache_eviction import cleanup_video_cache
from services.processing_queue import processing_worker_loop

async def periodic_cache_cleanup():
    """Runs video cache cleanup every 6 hours."""
    video_cache_path = os.path.join(os.getcwd(), 'video_cache')
    while True:
        try:
            result = cleanup_video_cache(video_cache_path)
            if result.get('deleted_count', 0) > 0:
                logging.info(f"Cache cleanup: {result}")
        except Exception as e:
            logging.error(f"Cache cleanup error: {e}")
        await asyncio.sleep(6 * 3600)  # Every 6 hours

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Modern lifespan handler replacing deprecated @app.on_event."""
    index_result = ensure_performance_indexes()
    if index_result.get('created'):
        logging.info(f"Performance indexes created: {index_result['created']}")

    cache_cleanup_task = asyncio.create_task(periodic_cache_cleanup())
    queue_stop_event = asyncio.Event()
    queue_worker_task = asyncio.create_task(processing_worker_loop(queue_stop_event))

    try:
        yield
    finally:
        queue_stop_event.set()
        cache_cleanup_task.cancel()
        queue_worker_task.cancel()
        await asyncio.gather(cache_cleanup_task, queue_worker_task, return_exceptions=True)

app = FastAPI(title="EDMS Middleware API", lifespan=lifespan)

# --- Rate Limiting ---
app.state.limiter = auth.limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- Static Files Setup ---
# Create static directory if it doesn't exist
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'static')
STATIC_IMAGES_DIR = os.path.join(STATIC_DIR, 'images')

if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)
if not os.path.exists(STATIC_IMAGES_DIR):
    os.makedirs(STATIC_IMAGES_DIR)

# Mount static files directory
# Files in /static will be accessible at /static/...
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# --- Security & Middleware ---

secret_key = os.getenv('FLASK_SECRET_KEY')
if not secret_key:
    raise RuntimeError("FLASK_SECRET_KEY environment variable is not set. Cannot start without it.")

app.add_middleware(
    SessionMiddleware,
    secret_key=secret_key,
    max_age=5184000,
    session_cookie="session",
    https_only=False,
    same_site="lax"
)

# 2. CORS Middleware — restrict to configured frontend URL
frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
allowed_origins = [origin.strip() for origin in frontend_url.split(',')]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-App-Source", "Cookie"],
)

# 3. Security Headers Middleware (CSP, X-Frame-Options, etc.)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    # Paths that are intentionally embedded in iframes — skip X-Frame-Options
    EMBEDDABLE_PATHS = ("/api/pdf/",)

    async def dispatch(self, request: StarletteRequest, call_next):
        response: StarletteResponse = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' blob: data:; "
            "font-src 'self' data:; "
            "connect-src 'self'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        # Allow embedding only for routes explicitly designed for iframe use
        if not any(request.url.path.startswith(p) for p in self.EMBEDDABLE_PATHS):
            response.headers["X-Frame-Options"] = "DENY"
        return response

app.add_middleware(SecurityHeadersMiddleware)


class RequestMetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        started_at = time.perf_counter()
        response: StarletteResponse | None = None

        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            session_obj = request.scope.get("session") or {}
            user = (session_obj.get("user") or {}).get("username", "anonymous")

            payload = {
                "event": "http_request",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": duration_ms,
                "user": user,
            }
            # logging.info(json.dumps(payload, ensure_ascii=True))

# --- Register Routes ---
app.include_router(auth.router)
app.include_router(documents.router)
app.include_router(media.router)
app.include_router(tags.router)
app.include_router(events.router)
app.include_router(folders.router)
app.include_router(favorites.router)
app.include_router(memories.router)
app.include_router(sharing.router)
app.include_router(admin.router)
app.include_router(ems_admin.router)
app.include_router(profilesearch.router)

@app.get("/")
def health_check():
    return {"status": "EDMS API is running"}



if __name__ == '__main__':
    import uvicorn
    port = int(os.environ.get('PORT', 5000))
    uvicorn.run(
        "app:app",
        host='0.0.0.0',
        port=port,
        reload=False
    )