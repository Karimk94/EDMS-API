@echo off
echo Cleaning old packages...
rmdir /s /q packages
mkdir packages

echo Downloading packages for Windows Python 3.12 (cp312, win_amd64)...

pip download ^
  --python-version 3.12 ^
  --platform win_amd64 ^
  --implementation cp ^
  --abi cp312 ^
  --only-binary :all: ^
  --dest ./packages ^
  -r requirements.txt

echo.
echo Download complete. Copy the 'packages' folder and 'requirements.txt' to your server.
pause