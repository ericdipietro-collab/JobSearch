@echo off
setlocal enabledelayedexpansion
title Job Search Dashboard

REM Detect setup-only mode early (installer runs us hidden - no pause allowed)
set "SETUP_ONLY=0"
if /i "%~1"=="--setup-only" set "SETUP_ONLY=1"

REM Locate supported Python runtime.
REM Release artifacts bundle wheels for Python 3.11 only, so prefer and require 3.11.
set "PYTHON="
set "FOUND_OTHER_PYTHON=0"

REM 1. Prefer the Python launcher targeting 3.11 explicitly.
if not defined PYTHON (
    for /f "delims=" %%V in ('py -3.11 --version 2^>^&1') do (
        echo %%V | findstr /r "Python 3\.11\." >nul 2>&1
        if !errorlevel!==0 set "PYTHON=py -3.11"
    )
)

REM 2. Detect unsupported PATH-based Python so we can explain the issue clearly.
for %%P in (py python python3) do (
    if not defined PYTHON (
        for /f "delims=" %%V in ('%%P --version 2^>^&1') do (
            echo %%V | findstr /r "Python 3\." >nul 2>&1
            if !errorlevel!==0 set "FOUND_OTHER_PYTHON=1"
        )
    )
)

REM 3. Fallback: search common Python 3.11 install locations directly.
if not defined PYTHON (
    for %%D in (
        "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
        "C:\Python311\python.exe"
        "%ProgramFiles%\Python311\python.exe"
    ) do (
        if not defined PYTHON (
            if exist %%D (
                for /f "delims=" %%V in ('%%~D --version 2^>^&1') do (
                    echo %%V | findstr /r "Python 3\.11\." >nul 2>&1
                    if !errorlevel!==0 set "PYTHON=%%~D"
                )
            )
        )
    )
)

if not defined PYTHON (
    echo.
    echo  ============================================================
    echo   Python 3.11 is required by this packaged release but was not found.
    echo.
    if "%FOUND_OTHER_PYTHON%"=="1" (
        echo   Another Python version was detected, but the packaged dependency
        echo   bundle is built for Python 3.11 only. Using 3.12/3.13 can force
        echo   source builds that look for Visual Studio tools such as vswhere.exe.
        echo.
    )
    if exist "installer\downloads\python-3.11.9-amd64.exe" (
        echo   A bundled Python 3.11 installer is available and can be started now.
        echo   After Python finishes installing, relaunch Job Search Dashboard.
    ) else (
        echo   Install Python 3.11 from:
        echo   https://www.python.org/downloads/release/python-3119/
        echo   During install, check "Add Python to PATH", then re-launch.
    )
    echo  ============================================================
    echo.
    if "%SETUP_ONLY%"=="0" pause
    if "%SETUP_ONLY%"=="0" (
        if exist "installer\downloads\python-3.11.9-amd64.exe" (
            start "" "installer\downloads\python-3.11.9-amd64.exe"
        ) else (
            start https://www.python.org/downloads/
        )
    )
    exit /b 1
)

REM Set working directory to folder containing this script
cd /d "%~dp0"

REM Keep runtime state out of the install/package directory.
if not defined JOBSEARCH_HOME (
    set "JOBSEARCH_HOME=%LOCALAPPDATA%\JobSearchDashboardData"
)
set "APP_HOME=%JOBSEARCH_HOME%"
set "APP_CONFIG=%APP_HOME%\config"
set "APP_RESULTS=%APP_HOME%\results"
set "APP_DATA=%APP_HOME%\data"
set "APP_VENV=%APP_HOME%\.venv"
set "STAMP_FILE=%APP_VENV%\.deps_installed"

if not exist "%APP_HOME%" mkdir "%APP_HOME%"
if not exist "%APP_CONFIG%" mkdir "%APP_CONFIG%"
if not exist "%APP_RESULTS%" mkdir "%APP_RESULTS%"
if not exist "%APP_DATA%" mkdir "%APP_DATA%"

REM Migrate legacy install-local state forward on first run.
if exist "config" (
    if not exist "%APP_CONFIG%\job_search_preferences.yaml" if exist "config\job_search_preferences.yaml" copy /y "config\job_search_preferences.yaml" "%APP_CONFIG%\job_search_preferences.yaml" >nul
    if not exist "%APP_CONFIG%\job_search_companies.yaml" if exist "config\job_search_companies.yaml" copy /y "config\job_search_companies.yaml" "%APP_CONFIG%\job_search_companies.yaml" >nul
    if not exist "%APP_CONFIG%\job_search_companies_contract.yaml" if exist "config\job_search_companies_contract.yaml" copy /y "config\job_search_companies_contract.yaml" "%APP_CONFIG%\job_search_companies_contract.yaml" >nul
)
if exist "results" (
    if not exist "%APP_RESULTS%\jobsearch.db" if exist "results\jobsearch.db" copy /y "results\jobsearch.db" "%APP_RESULTS%\jobsearch.db" >nul
)

REM Seed runtime config from packaged defaults when missing.
if not exist "%APP_CONFIG%\job_search_preferences.yaml" (
    if exist "config\job_search_preferences.example.yaml" (
        echo  Copying example preferences to %APP_CONFIG%\job_search_preferences.yaml ...
        copy /y "config\job_search_preferences.example.yaml" "%APP_CONFIG%\job_search_preferences.yaml" >nul
        echo  Done. Open Search Settings in the app to customize salary and location.
    )
)
if not exist "%APP_CONFIG%\job_search_companies.yaml" (
    if exist "config\job_search_companies.yaml" (
        echo  Copying primary registry to %APP_CONFIG%\job_search_companies.yaml ...
        copy /y "config\job_search_companies.yaml" "%APP_CONFIG%\job_search_companies.yaml" >nul
    )
)
if not exist "%APP_CONFIG%\job_search_companies_contract.yaml" (
    if exist "config\job_search_companies_contract.yaml" (
        echo  Copying contractor registry to %APP_CONFIG%\job_search_companies_contract.yaml ...
        copy /y "config\job_search_companies_contract.yaml" "%APP_CONFIG%\job_search_companies_contract.yaml" >nul
    ) else if exist "config\job_search_companies_contract_test.yaml" (
        echo  Copying contractor registry to %APP_CONFIG%\job_search_companies_contract.yaml ...
        copy /y "config\job_search_companies_contract_test.yaml" "%APP_CONFIG%\job_search_companies_contract.yaml" >nul
    )
)

REM Create virtual environment on first run
if not exist "%APP_VENV%\Scripts\activate.bat" (
    echo.
    echo  Setting up for the first time (this takes about a minute^) ...
    echo.
    if exist "%APP_VENV%" (
        echo  Removing incomplete virtual environment...
        rmdir /s /q "%APP_VENV%"
    )
    %PYTHON% -m venv "%APP_VENV%"
    if errorlevel 1 (
        echo  ERROR: Could not create virtual environment.
        if "%SETUP_ONLY%"=="0" pause
        exit /b 1
    )
)

REM Activate venv
call "%APP_VENV%\Scripts\activate.bat"

REM Install / update dependencies (fast no-op after first run)
set "NEEDS_DEPS=0"
if not exist "%STAMP_FILE%" set "NEEDS_DEPS=1"
if "%NEEDS_DEPS%"=="0" (
    python -c "from pathlib import Path; import sys; stamp=Path(r'%STAMP_FILE%'); deps=[Path('requirements.txt'), Path('pyproject.toml')]; sys.exit(0 if stamp.exists() and all((not p.exists()) or p.stat().st_mtime <= stamp.stat().st_mtime for p in deps) else 1)"
    if errorlevel 1 set "NEEDS_DEPS=1"
)

if "%NEEDS_DEPS%"=="1" (
    echo  Installing pinned runtime dependencies...
    python -m pip install -q --upgrade pip
    if exist "installer\wheels" (
        REM --find-links prefers the bundled wheels for speed; no --no-index so pip
        REM can fall back to PyPI for any transitive deps missing from the bundle.
        python -m pip install -q --only-binary=:all: --find-links installer\wheels -r requirements.txt
    ) else (
        python -m pip install -q --only-binary=:all: -r requirements.txt
    )
    REM compat mode adds a .pth file - no C compiler or vswhere.exe needed.
    set SETUPTOOLS_EDITABLE_MODE=compat
    python -m pip install -q -e .
    if errorlevel 1 (
        echo.
        echo  ERROR: Failed to install dependencies.
        echo  This release requires prebuilt wheels and a supported Python 3.11 runtime.
        echo  Check your internet connection and verify Python 3.11 is installed, then try again.
        if "%SETUP_ONLY%"=="0" pause
        exit /b 1
    )
    > "%STAMP_FILE%" echo dependencies installed
) else (
    echo  Runtime environment already prepared.
)

REM Setup-only mode (called by installer to pre-warm the venv)
if "%SETUP_ONLY%"=="1" (
    echo  Setup complete. Launch Job Search Dashboard to start the app.
    exit /b 0
)

REM Launch dashboard
echo.
echo  Starting Job Search Dashboard ...
echo  It will open in your browser automatically.
echo  Close this window to stop the app.
echo.
python -m streamlit run app.py --server.headless false --browser.gatherUsageStats false
