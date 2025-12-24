@echo off
cd /d "%~dp0"

echo Installing Build Tools...
pip install --no-index --find-links=./packages setuptools wheel

echo.
echo Installing Requirements...
pip install --no-index --find-links=./packages -r requirements.txt

echo.
echo Installation Complete!
pause