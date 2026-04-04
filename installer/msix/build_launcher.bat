@echo off
setlocal
cd /d "%~dp0"

set "CSC=C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
if not exist "%CSC%" (
    echo C# compiler not found at %CSC%
    exit /b 1
)

if not exist "bin" mkdir bin

"%CSC%" ^
  /nologo ^
  /target:winexe ^
  /optimize+ ^
  /out:bin\JobSearchLauncher.exe ^
  /reference:System.dll ^
  /reference:System.Core.dll ^
  /reference:System.Windows.Forms.dll ^
  JobSearchLauncher.cs

if errorlevel 1 exit /b 1

echo Built installer\msix\bin\JobSearchLauncher.exe
