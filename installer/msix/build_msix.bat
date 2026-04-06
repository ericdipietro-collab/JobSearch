@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

set "CSC=C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
set "MAKEAPPX=C:\Progra~2\Windows Kits\10\bin\10.0.26100.0\x64\makeappx.exe"
set "SIGNTOOL=C:\Progra~2\Windows Kits\10\bin\10.0.26100.0\x64\signtool.exe"

if not exist "%CSC%" (
  echo Missing C# compiler: %CSC%
  exit /b 1
)
if not exist "%MAKEAPPX%" (
  echo Missing makeappx.exe: %MAKEAPPX%
  exit /b 1
)

call build_launcher.bat
if errorlevel 1 exit /b 1

set "PACKAGE_NAME=JobSearchDashboard"
set "PUBLISHER=CN=JobSearchDashboardTest"
set "VERSION=2.0.0.0"
set "ROOT=%~dp0..\.."
set "STAGE=%~dp0build\AppFiles"
set "DIST=%ROOT%\dist"
set "MSIX_NAME=JobSearchDashboard_%VERSION%_x64.msix"
set "MSIX_PATH=%DIST%\%MSIX_NAME%"

if exist "%~dp0build" rmdir /s /q "%~dp0build"
mkdir "%STAGE%"
mkdir "%STAGE%\Assets"
mkdir "%STAGE%\src"
mkdir "%STAGE%\config"
mkdir "%STAGE%\docs"
mkdir "%STAGE%\installer"

copy /y "%~dp0bin\JobSearchLauncher.exe" "%STAGE%\JobSearchLauncher.exe" >nul
copy /y "%ROOT%\launch.bat" "%STAGE%\launch.bat" >nul
copy /y "%ROOT%\launch.vbs" "%STAGE%\launch.vbs" >nul
copy /y "%ROOT%\app.py" "%STAGE%\app.py" >nul
copy /y "%ROOT%\requirements.txt" "%STAGE%\requirements.txt" >nul
copy /y "%ROOT%\pyproject.toml" "%STAGE%\pyproject.toml" >nul

xcopy /e /i /y "%ROOT%\src" "%STAGE%\src" >nul
xcopy /e /i /y "%ROOT%\installer\wheels" "%STAGE%\installer\wheels" >nul
copy /y "%ROOT%\installer\assets\app.ico" "%STAGE%\installer\app.ico" >nul

if exist "%ROOT%\config\job_search_preferences.example.yaml" copy /y "%ROOT%\config\job_search_preferences.example.yaml" "%STAGE%\config\job_search_preferences.example.yaml" >nul
if exist "%ROOT%\config\job_search_companies.yaml" copy /y "%ROOT%\config\job_search_companies.yaml" "%STAGE%\config\job_search_companies.yaml" >nul
if exist "%ROOT%\config\job_search_companies_contract.yaml" copy /y "%ROOT%\config\job_search_companies_contract.yaml" "%STAGE%\config\job_search_companies_contract.yaml" >nul
if exist "%ROOT%\config\job_search_companies_aggregators.yaml" copy /y "%ROOT%\config\job_search_companies_aggregators.yaml" "%STAGE%\config\job_search_companies_aggregators.yaml" >nul
if exist "%ROOT%\config\job_search_companies_jobspy.yaml" copy /y "%ROOT%\config\job_search_companies_jobspy.yaml" "%STAGE%\config\job_search_companies_jobspy.yaml" >nul
if exist "%ROOT%\README.md" copy /y "%ROOT%\README.md" "%STAGE%\docs\README.md" >nul
if exist "%ROOT%\GETTING_STARTED.md" copy /y "%ROOT%\GETTING_STARTED.md" "%STAGE%\docs\GETTING_STARTED.md" >nul

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Add-Type -AssemblyName System.Drawing; " ^
  "$assets = @(" ^
  "  @{N='Square44x44Logo.png';W=44;H=44}," ^
  "  @{N='Square71x71Logo.png';W=71;H=71}," ^
  "  @{N='Square150x150Logo.png';W=150;H=150}," ^
  "  @{N='Square310x310Logo.png';W=310;H=310}," ^
  "  @{N='Wide310x150Logo.png';W=310;H=150}," ^
  "  @{N='StoreLogo.png';W=50;H=50}" ^
  "); " ^
  "$dir = '%STAGE%\Assets'; " ^
  "foreach($a in $assets){" ^
  "  $bmp = New-Object System.Drawing.Bitmap($a.W,$a.H);" ^
  "  $g = [System.Drawing.Graphics]::FromImage($bmp);" ^
  "  $g.Clear([System.Drawing.Color]::FromArgb(34,67,120));" ^
  "  $brush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::White);" ^
  "  $fontSize = [Math]::Max([int]($a.H / 3), 10);" ^
  "  $font = New-Object System.Drawing.Font('Segoe UI', $fontSize, [System.Drawing.FontStyle]::Bold);" ^
  "  $sf = New-Object System.Drawing.StringFormat;" ^
  "  $sf.Alignment = [System.Drawing.StringAlignment]::Center;" ^
  "  $sf.LineAlignment = [System.Drawing.StringAlignment]::Center;" ^
  "  $g.DrawString('JS', $font, $brush, (New-Object System.Drawing.RectangleF(0,0,$a.W,$a.H)), $sf);" ^
  "  $bmp.Save((Join-Path $dir $a.N), [System.Drawing.Imaging.ImageFormat]::Png);" ^
  "  $font.Dispose(); $brush.Dispose(); $g.Dispose(); $bmp.Dispose();" ^
  "}"
if errorlevel 1 exit /b 1

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$template = Get-Content '%~dp0Package.appxmanifest.template.xml' -Raw; " ^
  "$template = $template.Replace('__PACKAGE_NAME__','%PACKAGE_NAME%').Replace('__PUBLISHER__','%PUBLISHER%').Replace('__VERSION__','%VERSION%'); " ^
  "[IO.File]::WriteAllText('%STAGE%\AppxManifest.xml', $template, [Text.UTF8Encoding]::new($false))"
if errorlevel 1 exit /b 1

if not exist "%DIST%" mkdir "%DIST%"
if exist "%MSIX_PATH%" del /f /q "%MSIX_PATH%"

"%MAKEAPPX%" pack /d "%STAGE%" /p "%MSIX_PATH%" /o
if errorlevel 1 exit /b 1

if defined JOBSEARCH_MSIX_PFX_PATH (
  if not exist "%SIGNTOOL%" (
    echo signtool.exe not found, skipping signing.
    goto :appinstaller
  )
  if not defined JOBSEARCH_MSIX_PFX_PASSWORD (
    echo JOBSEARCH_MSIX_PFX_PATH is set but JOBSEARCH_MSIX_PFX_PASSWORD is missing.
    exit /b 1
  )
  "%SIGNTOOL%" sign /fd SHA256 /f "%JOBSEARCH_MSIX_PFX_PATH%" /p "%JOBSEARCH_MSIX_PFX_PASSWORD%" "%MSIX_PATH%"
  if errorlevel 1 exit /b 1
)

:appinstaller
if defined JOBSEARCH_APPINSTALLER_BASE_URI (
  set "BASE_URI=%JOBSEARCH_APPINSTALLER_BASE_URI%"
  if "!BASE_URI:~-1!"=="/" set "BASE_URI=!BASE_URI:~0,-1!"
  set "PACKAGE_URI=!BASE_URI!/%MSIX_NAME%"
  set "APPINSTALLER_URI=!BASE_URI!/JobSearchDashboard.appinstaller"
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$template = Get-Content '%~dp0JobSearchDashboard.appinstaller.template' -Raw; " ^
    "$template = $template.Replace('__PACKAGE_NAME__','%PACKAGE_NAME%').Replace('__PUBLISHER__','%PUBLISHER%').Replace('__VERSION__','%VERSION%').Replace('__PACKAGE_URI__','!PACKAGE_URI!').Replace('__APPINSTALLER_URI__','!APPINSTALLER_URI!'); " ^
    "[IO.File]::WriteAllText('%DIST%\JobSearchDashboard.appinstaller', $template, [Text.UTF8Encoding]::new($false))"
  if errorlevel 1 exit /b 1
)

echo Built %MSIX_PATH%
if defined JOBSEARCH_APPINSTALLER_BASE_URI echo Built %DIST%\JobSearchDashboard.appinstaller
