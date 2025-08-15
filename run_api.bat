@echo off
echo Starting EDMS Middleware API...
call venv/Scripts/activate
flask run --host=0.0.0.0 --port=5000