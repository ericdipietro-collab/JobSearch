@echo off
setlocal enabledelayedexpansion
title Build Job Search Installer

cd /d "%~dp0"

echo.
echo ============================================================
echo  Job Search Dashboard -- Installer Build Script
echo ============================================================
echo.

REM ── 1. Check for Inno Setup ───────────────────────────────────────────────────
set "ISCC="
for %%P in (
    "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
    "C:\Program Files\Inno Setup 6\ISCC.exe"
) do (
    if not defined ISCC (
        if exist %%P set "ISCC=%%~P"
    )
)

if not defined ISCC (
    echo  Inno Setup 6 not found.
    echo.
    echo  Download and install it from:
    echo    https://jrsoftware.org/isdl.php
    echo.
    echo  Then re-run this script.
    echo.
    pause
    start https://jrsoftware.org/isdl.php
    exit /b 1
)
echo  [OK] Inno Setup: %ISCC%

REM ── 2. Download Python 3.11.9 installer if missing ───────────────────────────
set "PY_EXE=downloads\python-3.11.9-amd64.exe"
set "PY_URL=https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"

if not exist "downloads" mkdir downloads

if exist "%PY_EXE%" (
    echo  [OK] Python installer already downloaded.
) else (
    echo  Downloading Python 3.11.9 installer (~25 MB^) ...
    curl -L --progress-bar -o "%PY_EXE%" "%PY_URL%"
    if errorlevel 1 (
        echo.
        echo  ERROR: Download failed. Check your internet connection and try again.
        pause
        exit /b 1
    )
    echo  [OK] Python installer downloaded.
)

REM ── 3. Create output directory ────────────────────────────────────────────────
if not exist "..\dist" mkdir "..\dist"

REM ── 4. Compile the installer ──────────────────────────────────────────────────
echo.
echo  Compiling installer ...
echo.
"%ISCC%" jobsearch_setup.iss
if errorlevel 1 (
    echo.
    echo  ERROR: Inno Setup compilation failed.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Build complete!
echo  Output: dist\JobSearchSetup.exe
echo ============================================================
echo.
explorer "..\dist"
pause
