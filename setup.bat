@echo off
REM =================================================================
REM  Setup Script for EDMS Middleware API
REM  - Creates a virtual environment
REM  - Installs required Python packages from requirements.txt
REM =================================================================

echo [1/3] Changing directory to the script's location...
cd /d "%~dp0"

echo [2/3] Creating Python virtual environment...
if not exist venv (
    python -m venv venv
)

echo [3/3] Installing required packages...
call venv\Scripts\activate.bat
pip install -r requirements.txt

echo.
echo Middleware setup complete.
echo You can now run the API using the 'run_api.bat' script.
pause
