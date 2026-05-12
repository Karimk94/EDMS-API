@echo off
setlocal enabledelayedexpansion

echo Creating archive of project files (excluding .gitignore patterns)...
echo.

REM Generate a timestamp for the zip filename
set "foldername=EDMS API"
for /f "tokens=1-6 delims=.:/ " %%a in ("%DATE% %TIME%") do (
    set "ts=%%c-%%a-%%b_%%d-%%e-%%f"
)
set "ts=%ts: =0%"
set "zipname=%foldername%_%ts%.zip"

REM Use git to get the list of tracked + untracked non-ignored files
REM This handles all .gitignore patterns correctly with their relative paths
REM And we also exclude .git, create_archive.bat, and venv files
set "psfile=%temp%\create_archive_temp.ps1"
> "%psfile%" echo $src = (Get-Location).Path
>> "%psfile%" echo $dest = Join-Path (Join-Path $src '..') '%zipname%'
>> "%psfile%" echo $excludes = @('.git', '.gitignore', 'create_archive.bat', 'venv')
>> "%psfile%" echo function Test-Excluded($relPath) {
>> "%psfile%" echo     foreach ($p in $excludes) {
>> "%psfile%" echo         if ($relPath -eq $p -or $relPath.StartsWith("$p/") -or $relPath.StartsWith("$p\")) { return $true }
>> "%psfile%" echo     }
>> "%psfile%" echo     return $false
>> "%psfile%" echo }
>> "%psfile%" echo $files = @()
>> "%psfile%" echo git ls-files -c --others --exclude-standard ^| ForEach-Object { if (-not (Test-Excluded $_)) { $files += $_ } }
>> "%psfile%" echo Write-Host ("Including " + $files.Count + " files...")
>> "%psfile%" echo if ($files.Count -eq 0) { Write-Host "No files found."; exit 0 }
>> "%psfile%" echo $tempDir = Join-Path $env:TEMP ([System.IO.Path]::GetRandomFileName())
>> "%psfile%" echo $null = New-Item -ItemType Directory -Path $tempDir -Force
>> "%psfile%" echo foreach ($rel in $files) {
>> "%psfile%" echo     $target = Join-Path $tempDir ($rel -replace '/', '\')
>> "%psfile%" echo     $parent = Split-Path $target -Parent
>> "%psfile%" echo     if (-not (Test-Path $parent)) { $null = New-Item -ItemType Directory -Path $parent -Force }
>> "%psfile%" echo     Copy-Item -Path (Join-Path $src $rel) -Destination $target
>> "%psfile%" echo }
>> "%psfile%" echo Compress-Archive -Path "$tempDir\*" -DestinationPath $dest -CompressionLevel Optimal
>> "%psfile%" echo Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
>> "%psfile%" echo Write-Host ("Created: " + $dest)

powershell -NoProfile -ExecutionPolicy Bypass -File "%psfile%"
set "exitcode=%ERRORLEVEL%"

del "%psfile%" 2>nul

if %exitcode% EQU 0 (
    echo.
    echo Archive created successfully!
    echo Location: ..\%zipname%
) else (
    echo.
    echo Failed to create archive.
    pause
    exit /b 1
)

echo.
pause
endlocal