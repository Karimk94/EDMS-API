@echo off
cd /d "%~dp0"

echo [1/3] Creating virtual environment (if not exists)...
if not exist venv (
    python -m venv venv
)

echo.
echo [2/3] Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo [3/3] Installing packages into virtual environment (offline)...
pip install --no-index --find-links=./packages setuptools wheel
pip install --no-index --find-links=./packages -r requirements.txt

echo.
echo Installation Complete! Run the API using run_api.bat
pause