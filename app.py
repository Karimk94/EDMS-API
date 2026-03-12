import logging
import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv

# Import Routers
from routes import auth, documents, media, tags, events, folders, favorites, memories, sharing, admin, profilesearch
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
    asyncio.create_task(periodic_cache_cleanup())
    yield

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
    allow_methods=["*"],
    allow_headers=["*"],
)

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