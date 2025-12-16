@echo off
echo Starting EDMS Middleware API (FastAPI)...
call venv/Scripts/activate

REM --host 0.0.0.0 makes it accessible externally
REM --workers 4 enables multiprocessing (similar to production WSGI servers)
uvicorn app:app --host 0.0.0.0 --port 5000 --workers 4

pause