@echo off
REM =================================================================
REM  Package Downloader for Offline Deployment
REM  - Downloads all packages from requirements.txt into a 'wheels' folder.
REM =================================================================

echo [1/2] Changing directory to the script's location...
cd /d "%~dp0"

echo [2/2] Downloading packages to the 'wheels' folder...
call venv\Scripts\activate.bat
if not exist wheels mkdir wheels
pip download -r requirements.txt -d wheels

echo.
echo Package download complete. The 'wheels' folder is now ready.
pause