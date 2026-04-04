@echo off
setlocal enabledelayedexpansion
title Job Search Dashboard

REM ── Detect setup-only mode early (installer runs us hidden — no pause allowed) ─
set "SETUP_ONLY=0"
if /i "%~1"=="--setup-only" set "SETUP_ONLY=1"

REM ── Locate Python 3.9+ ───────────────────────────────────────────────────────
set "PYTHON="

REM -- 1. Try PATH-based commands (py launcher is most reliable on Windows)
for %%P in (py python python3) do (
    if not defined PYTHON (
        for /f "delims=" %%V in ('%%P --version 2^>^&1') do (
            echo %%V | findstr /r "Python 3\.[9-9]\. Python 3\.[1-9][0-9]\." >nul 2>&1
            if !errorlevel!==0 set "PYTHON=%%P"
        )
    )
)

REM -- 2. Fallback: search common per-user and system install locations directly.
REM    Handles cases where Python was installed but PATH wasn't updated (e.g.
REM    fresh install by the bundled installer before a reboot, or user skipped
REM    "Add Python to PATH" during setup).
if not defined PYTHON (
    for %%D in (
        "%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python39\python.exe"
        "C:\Python313\python.exe"
        "C:\Python312\python.exe"
        "C:\Python311\python.exe"
        "C:\Python310\python.exe"
        "C:\Python39\python.exe"
        "%ProgramFiles%\Python313\python.exe"
        "%ProgramFiles%\Python312\python.exe"
        "%ProgramFiles%\Python311\python.exe"
    ) do (
        if not defined PYTHON (
            if exist %%D (
                for /f "delims=" %%V in ('%%~D --version 2^>^&1') do (
                    echo %%V | findstr /r "Python 3\.[9-9]\. Python 3\.[1-9][0-9]\." >nul 2>&1
                    if !errorlevel!==0 set "PYTHON=%%~D"
                )
            )
        )
    )
)

if not defined PYTHON (
    echo.
    echo  ============================================================
    echo   Python 3.9 or newer is required but was not found.
    echo.
    echo   If you installed using JobSearchSetup.exe, try rebooting
    echo   once and launching again -- the PATH update takes effect
    echo   after a restart.
    echo.
    echo   Or download Python free from:  https://www.python.org/downloads/
    echo   During install, check "Add Python to PATH", then re-launch.
    echo  ============================================================
    echo.
    if "%SETUP_ONLY%"=="0" pause
    if "%SETUP_ONLY%"=="0" start https://www.python.org/downloads/
    exit /b 1
)

REM ── Set working directory to folder containing this script ────────────────────
cd /d "%~dp0"

REM ── First-run: copy example config if preferences.yaml is missing ─────────────
if not exist "config\job_search_preferences.yaml" (
    if exist "config\job_search_preferences.example.yaml" (
        echo  Copying example preferences to config\job_search_preferences.yaml ...
        copy /y "config\job_search_preferences.example.yaml" "config\job_search_preferences.yaml" >nul
        echo  Done.  Open Search Settings in the app to customise salary and location.
    )
)
if not exist "config\job_search_companies_contract.yaml" (
    if exist "config\job_search_companies_contract_test.yaml" (
        echo  Copying contractor registry to config\job_search_companies_contract.yaml ...
        copy /y "config\job_search_companies_contract_test.yaml" "config\job_search_companies_contract.yaml" >nul
    )
)

REM ── Create virtual environment on first run ───────────────────────────────────
if not exist ".venv\Scripts\activate.bat" (
    echo.
    echo  Setting up for the first time (this takes about a minute^) ...
    echo.
    if exist ".venv" (
        echo  Removing incomplete virtual environment...
        rmdir /s /q ".venv"
    )
    %PYTHON% -m venv .venv
    if errorlevel 1 (
        echo  ERROR: Could not create virtual environment.
        if "%SETUP_ONLY%"=="0" pause
        exit /b 1
    )
)

REM ── Activate venv ─────────────────────────────────────────────────────────────
call .venv\Scripts\activate.bat

REM ── Install / update dependencies (fast no-op after first run) ───────────────
set "STAMP_FILE=.venv\.deps_installed"
set "NEEDS_DEPS=0"
if not exist "%STAMP_FILE%" set "NEEDS_DEPS=1"
if "%NEEDS_DEPS%"=="0" (
    python -c "from pathlib import Path; import sys; stamp=Path(r'.venv/.deps_installed'); deps=[Path('requirements.txt'), Path('pyproject.toml')]; sys.exit(0 if stamp.exists() and all((not p.exists()) or p.stat().st_mtime <= stamp.stat().st_mtime for p in deps) else 1)"
    if errorlevel 1 set "NEEDS_DEPS=1"
)

if "%NEEDS_DEPS%"=="1" (
    echo  Installing pinned runtime dependencies...
    python -m pip install -q --upgrade pip
    if exist "installer\wheels" (
        REM --find-links prefers the bundled wheels for speed; no --no-index so pip
        REM can fall back to PyPI for any transitive deps missing from the bundle.
        python -m pip install -q --find-links installer\wheels -r requirements.txt
    ) else (
        python -m pip install -q -r requirements.txt
    )
    REM setuptools is always present in any venv — no extra build backend needed.
    python -m pip install -q -e .
    if errorlevel 1 (
        echo.
        echo  ERROR: Failed to install dependencies.
        echo  Check your internet connection and try again.
        if "%SETUP_ONLY%"=="0" pause
        exit /b 1
    )
    > "%STAMP_FILE%" echo dependencies installed
) else (
    echo  Runtime environment already prepared.
)

REM ── Ensure results and config dirs exist ─────────────────────────────────────
if not exist "results" mkdir results
if not exist "config"  mkdir config

REM ── Setup-only mode (called by installer to pre-warm the venv) ───────────────
if "%SETUP_ONLY%"=="1" (
    echo  Setup complete. Launch Job Search Dashboard to start the app.
    exit /b 0
)

REM ── Launch dashboard ──────────────────────────────────────────────────────────
echo.
echo  Starting Job Search Dashboard ...
echo  It will open in your browser automatically.
echo  Close this window to stop the app.
echo.
python -m streamlit run app.py --server.headless false --browser.gatherUsageStats false
