@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

for /f "tokens=2 delims== " %%V in ('findstr /b /c:"__version__ =" "..\..\src\jobsearch\__init__.py"') do set "RAW_VERSION=%%V"
set "VERSION=%RAW_VERSION:"=%"

set "ROOT=%~dp0..\.."
set "DIST=%ROOT%\dist"
set "STAGE=%DIST%\JobSearchDashboard-portable"
set "ZIP=%DIST%\JobSearchDashboard-portable-%VERSION%.zip"

if exist "%STAGE%" rmdir /s /q "%STAGE%"
if exist "%ZIP%" del /f /q "%ZIP%"
if not exist "%DIST%" mkdir "%DIST%"

mkdir "%STAGE%"
mkdir "%STAGE%\src"
mkdir "%STAGE%\config"
mkdir "%STAGE%\docs"
mkdir "%STAGE%\installer"
mkdir "%STAGE%\installer\assets"
mkdir "%STAGE%\installer\wheels"
mkdir "%STAGE%\installer\downloads"

copy /y "%ROOT%\app.py" "%STAGE%\app.py" >nul
copy /y "%ROOT%\launch.bat" "%STAGE%\launch.bat" >nul
copy /y "%ROOT%\launch.vbs" "%STAGE%\launch.vbs" >nul
copy /y "%ROOT%\requirements.txt" "%STAGE%\requirements.txt" >nul
copy /y "%ROOT%\pyproject.toml" "%STAGE%\pyproject.toml" >nul

xcopy /e /i /y "%ROOT%\src" "%STAGE%\src" >nul
xcopy /e /i /y "%ROOT%\installer\wheels" "%STAGE%\installer\wheels" >nul
copy /y "%ROOT%\installer\assets\app.ico" "%STAGE%\installer\assets\app.ico" >nul

if exist "%ROOT%\installer\downloads\python-3.11.9-amd64.exe" copy /y "%ROOT%\installer\downloads\python-3.11.9-amd64.exe" "%STAGE%\installer\downloads\python-3.11.9-amd64.exe" >nul
if exist "%ROOT%\config\job_search_preferences.example.yaml" copy /y "%ROOT%\config\job_search_preferences.example.yaml" "%STAGE%\config\job_search_preferences.example.yaml" >nul
if exist "%ROOT%\config\job_search_companies.yaml" copy /y "%ROOT%\config\job_search_companies.yaml" "%STAGE%\config\job_search_companies.yaml" >nul
if exist "%ROOT%\config\job_search_companies_contract.yaml" copy /y "%ROOT%\config\job_search_companies_contract.yaml" "%STAGE%\config\job_search_companies_contract.yaml" >nul
if exist "%ROOT%\README.md" copy /y "%ROOT%\README.md" "%STAGE%\docs\README.md" >nul
if exist "%ROOT%\GETTING_STARTED.md" copy /y "%ROOT%\GETTING_STARTED.md" "%STAGE%\docs\GETTING_STARTED.md" >nul
if exist "%~dp0README.md" copy /y "%~dp0README.md" "%STAGE%\docs\PORTABLE_RELEASE.md" >nul

powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path '%STAGE%\*' -DestinationPath '%ZIP%' -Force"
if errorlevel 1 exit /b 1

echo Built %ZIP%
