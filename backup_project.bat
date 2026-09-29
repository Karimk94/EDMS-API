@echo off
REM ============================================================================
REM  backup_project.bat
REM  ----------------------------------------------------------------------------
REM  Creates a backup ZIP of the EDMS API project containing only the functional
REM  source files and folders. Logs, packages, virtual environments, caches,
REM  thumbnails, vector DB data, IDE/VCS metadata and compiled Python files are
REM  excluded so the archive stays small and portable.
REM
REM  The resulting zip is written to the PARENT folder of the project so it is
REM  never included in its own backup.
REM
REM  Usage:  double-click  OR  run from a command prompt:
REM          backup_project.bat
REM ============================================================================

setlocal enabledelayedexpansion

REM --- Resolve the project folder (the folder this script lives in) -----------
set "PROJECT_DIR=%~dp0"
if "%PROJECT_DIR:~-1%"=="\" set "PROJECT_DIR=%PROJECT_DIR:~0,-1%"

REM --- Destination folder is the parent of the project ------------------------
for %%I in ("%PROJECT_DIR%") do set "PARENT_DIR=%%~dpI"
if "%PARENT_DIR:~-1%"=="\" set "PARENT_DIR=%PARENT_DIR:~0,-1%"

REM --- Build a timestamp (YYYY-MM-DD_HH-MM-SS) that is locale independent -----
REM  Uses PowerShell (always present on Windows 10/11) instead of the now
REM  deprecated/removed wmic utility.
for /f "usebackq delims=" %%T in (`powershell -NoProfile -Command "Get-Date -Format 'yyyy-MM-dd_HH-mm-ss'"`) do set "TS=%%T"
if not defined TS goto :timestamp_fail

REM --- Compose output zip name ------------------------------------------------
set "ZIP_NAME=EDMS_API_backup_%TS%.zip"
set "ZIP_PATH=%PARENT_DIR%\%ZIP_NAME%"

echo.
echo ============================================================
echo  EDMS API project backup
echo ============================================================
echo  Project  : %PROJECT_DIR%
echo  Output   : %ZIP_PATH%
echo ------------------------------------------------------------
echo  Excluding: venv, packages, chroma_db, *_cache folders,
echo             __pycache__, graphify-out, .git, .idea, .vscode,
echo             .env, *.pyc/*.pyo, *.log, runtime sqlite queue DB,
echo             previous backup zips.
echo ============================================================
echo.

REM --- Delegate the heavy lifting to PowerShell -------------------------------
REM  PowerShell is used because it offers reliable recursive enumeration,
REM  glob-style filtering and Compress-Archive, all of which are built into
REM  Windows 10/11 with no extra dependencies.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$src='%PROJECT_DIR%';" ^
  "$dst='%ZIP_PATH%';" ^
  "$srcN=[System.IO.Path]::GetFileName($src);" ^
  "$excludeDirs=@('venv','packages','chroma_db','download_cache','video_cache','thumbnail_cache','temp_thumbnail_cache','graphify-out','.git','.idea','.vscode','__pycache__');" ^
  "$excludeFiles=@('.env');" ^
  "$excludeExt=@('.pyc','.pyo','.log');" ^
  "$excludeName=@('processing_queue.sqlite3','processing_queue.sqlite3-wal','processing_queue.sqlite3-shm');" ^
  "function Test-Excluded($full){ $rel=$full.Substring($src.Length).TrimStart('\','/'); $parts=$rel -split '[\\/]'; foreach($d in $excludeDirs){ if($parts -contains $d){ return $true } } $fn=[System.IO.Path]::GetFileName($full); if($excludeFiles -contains $fn){ return $true } $ext=[System.IO.Path]::GetExtension($full); if($excludeExt -contains $ext.ToLower()){ return $true } if($excludeName -contains $fn){ return $true } return $false }" ^
  "$files=Get-ChildItem -Path $src -Recurse -File -Force | Where-Object { -not (Test-Excluded $_.FullName) };" ^
  "if(-not $files){ Write-Host 'No files found to back up.'; exit 2 }" ^
  "Write-Host ('Including ' + $files.Count + ' files...');" ^
  "$tempDir=Join-Path $env:TEMP ('edms_backup_' + [System.IO.Path]::GetRandomFileName());" ^
  "$null=New-Item -ItemType Directory -Path $tempDir -Force;" ^
  "$copied=0;" ^
  "foreach($f in $files){" ^
  "  $rel=$f.FullName.Substring($src.Length).TrimStart('\','/');" ^
  "  $rel=$rel -replace '/','\';" ^
  "  $target=Join-Path $tempDir (Join-Path $srcN $rel);" ^
  "  $parent=Split-Path $target -Parent;" ^
  "  if(-not (Test-Path $parent)){ $null=New-Item -ItemType Directory -Path $parent -Force };" ^
  "  Copy-Item -Path $f.FullName -Destination $target -Force;" ^
  "  $copied++;" ^
  "};" ^
  "if(Test-Path $dst){ Remove-Item $dst -Force };" ^
  "Compress-Archive -Path (Join-Path $tempDir '*') -DestinationPath $dst -CompressionLevel Optimal -Force;" ^
  "Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue;" ^
  "Write-Host ('Backup complete: ' + $copied + ' files archived.');"

set "EXITCODE=%ERRORLEVEL%"

echo.
if %EXITCODE% EQU 0 (
  echo ------------------------------------------------------------
  echo  SUCCESS - Backup created:
  echo  %ZIP_PATH%
  echo ------------------------------------------------------------
) else (
  echo ------------------------------------------------------------
  echo  FAILED  - Backup could not be created ^(exit code %EXITCODE%^).
  echo ------------------------------------------------------------
)
echo.
pause
exit /b %EXITCODE%

:timestamp_fail
echo.
echo ERROR: Could not determine current timestamp. Aborting.
echo.
pause
exit /b 1

endlocal
