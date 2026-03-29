@echo off
setlocal enabledelayedexpansion
title Job Search Dashboard

REM ── Locate Python 3.9+ ───────────────────────────────────────────────────────
set "PYTHON="
for %%P in (python python3 py) do (
    if not defined PYTHON (
        for /f "delims=" %%V in ('%%P --version 2^>^&1') do (
            echo %%V | findstr /r "Python 3\.[9-9]\. Python 3\.[1-9][0-9]\." >nul 2>&1
            if !errorlevel!==0 set "PYTHON=%%P"
        )
    )
)

if not defined PYTHON (
    echo.
    echo  ============================================================
    echo   Python 3.9 or newer is required but was not found.
    echo.
    echo   Download it free from:  https://www.python.org/downloads/
    echo.
    echo   During install, check the box:
    echo     "Add Python to PATH"
    echo.
    echo   Then close and re-open this file.
    echo  ============================================================
    echo.
    pause
    start https://www.python.org/downloads/
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

REM ── Create virtual environment on first run ───────────────────────────────────
if not exist ".venv\Scripts\activate.bat" (
    echo.
    echo  Setting up for the first time (this takes about a minute^) ...
    echo.
    %PYTHON% -m venv .venv
    if errorlevel 1 (
        echo  ERROR: Could not create virtual environment.
        pause
        exit /b 1
    )
)

REM ── Activate venv ─────────────────────────────────────────────────────────────
call .venv\Scripts\activate.bat

REM ── Install / update dependencies (fast no-op after first run^) ───────────────
echo  Checking dependencies...
python -m pip install -q --upgrade pip
python -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo.
    echo  ERROR: Failed to install dependencies.
    echo  Check your internet connection and try again.
    pause
    exit /b 1
)

REM ── Ensure results and config dirs exist ─────────────────────────────────────
if not exist "results" mkdir results
if not exist "config"  mkdir config

REM ── Launch dashboard ──────────────────────────────────────────────────────────
echo.
echo  Starting Job Search Dashboard ...
echo  It will open in your browser automatically.
echo  Close this window to stop the app.
echo.
python -m streamlit run app.py --server.headless false --browser.gatherUsageStats false
