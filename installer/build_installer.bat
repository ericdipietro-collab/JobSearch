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

set "BUILD_PY="
for /f "delims=" %%V in ('py -3.11 --version 2^>^&1') do (
    if not defined BUILD_PY (
        echo %%V | findstr /r "Python 3\.11\." >nul 2>&1
        if !errorlevel!==0 set "BUILD_PY=py -3.11"
    )
)

if not defined BUILD_PY (
    for %%D in (
        "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
        "C:\Python311\python.exe"
        "%ProgramFiles%\Python311\python.exe"
    ) do (
        if not defined BUILD_PY (
            if exist %%D (
                for /f "delims=" %%V in ('%%~D --version 2^>^&1') do (
                    echo %%V | findstr /r "Python 3\.11\." >nul 2>&1
                    if !errorlevel!==0 set "BUILD_PY=%%~D"
                )
            )
        )
    )
)

if not defined BUILD_PY (
    echo  Python 3.11 is required to prepare the offline wheel bundle.
    pause
    exit /b 1
)
echo  [OK] Build Python: %BUILD_PY%

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
if not exist "wheels" mkdir "wheels"

echo.
echo  Preparing offline wheel bundle ...
del /q "wheels\*" >nul 2>&1
%BUILD_PY% -m pip download --only-binary=:all: -r "..\requirements.txt" -d "wheels"
if errorlevel 1 (
    echo.
    echo  ERROR: Failed to download runtime wheels.
    pause
    exit /b 1
)
REM pyaml is a transitive dep of streamlit not listed in requirements.txt directly
%BUILD_PY% -m pip download --only-binary=:all: pyaml -d "wheels"
if errorlevel 1 (
    echo  WARNING: Could not download pyaml wheel ^(non-fatal, PyPI fallback available^).
)
REM build backend is now setuptools (always bundled in any venv — no download needed)

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
