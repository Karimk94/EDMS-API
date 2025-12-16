import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv

# Import Routers
from routes import auth, documents, media, tags, events, folders, favorites, memories

# Load environment variables
load_dotenv()

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(title="EDMS Middleware API")

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
        reload=True,
        reload_excludes=[
            "*.log",
            "*.pyc",
            "__pycache__",
            "venv",
            ".idea",
            ".vscode",
            ".git",
            "thumbnail_cache",
            "video_cache",
            "chroma_db",
            "*.tmp"
        ]
    )