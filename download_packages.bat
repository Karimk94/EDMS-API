@echo off
echo Cleaning old packages...
rmdir /s /q packages
mkdir packages

echo Downloading packages for Windows Python 3.12...

pip wheel -r requirements.txt --wheel-dir ./packages

echo.
echo Download complete. Copy the 'packages' folder and 'requirements.txt' to your server.
pause