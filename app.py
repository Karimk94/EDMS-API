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
from routes import auth, documents, media, tags, events, folders, favorites, memories, sharing, admin, edocs_admin, ems_admin, profilesearch, edms_people
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

# Load environment variables
load_dotenv()

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Log Filtering ---
# Enable uvicorn access logs to see all requests (including 500 errors)
logging.getLogger("uvicorn.access").setLevel(logging.INFO)

# --- Background Cache Eviction ---
from utils.cache_eviction import cleanup_video_cache
from utils.log_cleanup import cleanup_old_logs

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

async def periodic_log_cleanup():
    """Runs log cleanup daily to delete IIS/app logs older than 14 days."""
    logs_path = os.path.join(os.getcwd(), 'logs')
    while True:
        try:
            result = cleanup_old_logs(logs_path, max_age_days=14)
            if result.get('deleted_count', 0) > 0:
                logging.info(f"Log cleanup: {result}")
        except Exception as e:
            logging.error(f"Log cleanup error: {e}")
        await asyncio.sleep(24 * 3600)  # Every 24 hours

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Modern lifespan handler replacing deprecated @app.on_event."""
    # --- Best-effort DB index creation (non-blocking, with timeout) ---
    try:
        index_result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, ensure_performance_indexes),
            timeout=30
        )
        if index_result.get('created'):
            logging.info(f"Performance indexes created: {index_result['created']}")
    except asyncio.TimeoutError:
        logging.warning("Startup: ensure_performance_indexes timed out after 30s — skipping.")
    except Exception as e:
        logging.warning(f"Startup: ensure_performance_indexes failed — skipping: {e}")

    cache_cleanup_task = asyncio.create_task(periodic_cache_cleanup())
    log_cleanup_task = asyncio.create_task(periodic_log_cleanup())
    
    # Import and run the background processing worker
    from services.processing_queue import processing_worker_loop
    worker_stop_event = asyncio.Event()
    worker_task = asyncio.create_task(processing_worker_loop(worker_stop_event))
    logging.info("Started background processing worker loop.")

    try:
        yield
    finally:
        worker_stop_event.set()
        cache_cleanup_task.cancel()
        log_cleanup_task.cancel()
        # Give worker a moment to shut down gracefully, then gather all
        await asyncio.gather(worker_task, cache_cleanup_task, log_cleanup_task, return_exceptions=True)

app = FastAPI(title="EDMS Middleware API", lifespan=lifespan)

# --- Rate Limiting ---
app.state.limiter = auth.limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- Global Exception Handler (catches all unhandled errors) ---
from fastapi.responses import JSONResponse
import traceback

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Log all unhandled exceptions with full traceback."""
    error_id = str(uuid.uuid4())[:8]
    tb = traceback.format_exc()
    
    # Log the full error with traceback
    logging.error(
        f"[ERROR-{error_id}] Unhandled exception on {request.method} {request.url.path}: "
        f"{type(exc).__name__}: {exc}\n{tb}"
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "detail": str(exc),
            "type": type(exc).__name__
        }
    )

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
            logging.info(json.dumps(payload, ensure_ascii=True))

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
app.include_router(edocs_admin.router)
app.include_router(ems_admin.router)
app.include_router(profilesearch.router)
app.include_router(edms_people.router)

@app.get("/")
def health_check():
    return {"status": "EDMS API is running"}


@app.get("/api/health")
async def diagnostic_health_check():
    """
    Diagnostic endpoint to test DB and DMS connectivity.
    Helps identify which service is causing 500 errors.
    """
    results = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {}
    }
    
    # Test 1: Database connectivity
    try:
        from database.connection import get_connection
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        cursor.fetchone()
        cursor.close()
        conn.close()
        results["checks"]["database"] = {"status": "ok", "message": "Connected successfully"}
    except Exception as e:
        results["checks"]["database"] = {"status": "error", "message": str(e)}
        results["status"] = "unhealthy"
    
    # Test 2: DMS/WSDL connectivity
    try:
        import requests
        wsdl_url = os.getenv('WSDL_URL')
        if wsdl_url:
            response = requests.get(wsdl_url, timeout=10)
            if response.status_code == 200:
                results["checks"]["dms_wsdl"] = {"status": "ok", "message": f"WSDL reachable at {wsdl_url}"}
            else:
                results["checks"]["dms_wsdl"] = {"status": "error", "message": f"WSDL returned status {response.status_code}"}
                results["status"] = "unhealthy"
        else:
            results["checks"]["dms_wsdl"] = {"status": "error", "message": "WSDL_URL not configured"}
            results["status"] = "unhealthy"
    except Exception as e:
        results["checks"]["dms_wsdl"] = {"status": "error", "message": str(e)}
        results["status"] = "unhealthy"
    
    # Test 3: Environment variables
    env_checks = {}
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_SERVICE_NAME', 'DB_USERNAME', 'DB_PASSWORD', 
                     'WSDL_URL', 'DMS_USER', 'DMS_PASSWORD', 'FLASK_SECRET_KEY']
    for var in required_vars:
        value = os.getenv(var)
        env_checks[var] = "set" if value else "MISSING"
        if not value:
            results["status"] = "unhealthy"
    results["checks"]["environment"] = env_checks
    
    return results



if __name__ == '__main__':
    import uvicorn
    port = int(os.environ.get('HTTP_PLATFORM_PORT') or os.environ.get('PORT', 5000))
    uvicorn.run(
        "app:app",
        host='0.0.0.0',
        port=port,
        reload=False
    )
