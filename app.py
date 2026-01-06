import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv

# Import Routers
from routes import auth, documents, media, tags, events, folders, favorites, memories, sharing

# Load environment variables
load_dotenv()

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Log Filtering ---
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

app = FastAPI(title="EDMS Middleware API")

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

secret_key = os.getenv('FLASK_SECRET_KEY', 'super-secret-key-fallback')
app.add_middleware(
    SessionMiddleware,
    secret_key=secret_key,
    max_age=5184000,
    session_cookie="session"
)

# 2. CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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