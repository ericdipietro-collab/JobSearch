@echo off
setlocal

echo ============================================================
echo  Job Search Deep Search Add-on Installer (Windows)
echo  Installs Playwright + Chromium for JavaScript-heavy sites
echo ============================================================
echo.

:: Check for virtual environment first (preferred)
set VENV_PYTHON=%~dp0..\venv\Scripts\python.exe
set VENV_PYTHON2=%~dp0..\.venv\Scripts\python.exe

if exist "%VENV_PYTHON%" (
    set PYTHON=%VENV_PYTHON%
    goto :found_python
)
if exist "%VENV_PYTHON2%" (
    set PYTHON=%VENV_PYTHON2%
    goto :found_python
)

:: Fall back to system Python
where python >nul 2>&1
if %ERRORLEVEL% == 0 (
    set PYTHON=python
    goto :found_python
)

where python3 >nul 2>&1
if %ERRORLEVEL% == 0 (
    set PYTHON=python3
    goto :found_python
)

echo ERROR: Python not found. Please run the main launcher first to set up the environment.
pause
exit /b 1

:found_python
echo Using Python: %PYTHON%
echo.

echo Step 1 of 2: Installing playwright Python package...
"%PYTHON%" -m pip install "playwright>=1.40.0"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install playwright package.
    pause
    exit /b 1
)

echo.
echo Step 2 of 2: Installing Chromium browser (~170MB download)...
"%PYTHON%" -m playwright install chromium
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install Chromium. Check your internet connection and try again.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Deep Search installation complete!
echo.
echo  To use it: open the dashboard, go to "Run Job Search",
echo  and enable the "Deep Search" toggle before running.
echo.
echo  Or via CLI: python run_job_search_v6.py --deep-search
echo ============================================================
echo.
pause
