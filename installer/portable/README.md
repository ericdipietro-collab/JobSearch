# Portable Release

This folder contains the build script for a no-installer portable release.

The portable release produces:
- a staged portable app folder under `dist\JobSearchDashboard-portable`
- a zip file `dist\JobSearchDashboard-portable-<version>.zip`

Characteristics:
- no Inno installer required
- no MSIX required
- runtime state is stored under `%LOCALAPPDATA%\JobSearchDashboardData`
- the extracted app folder stays read-only / replaceable
- ships the current default registries for:
  - main ATS companies
  - contractor companies
  - aggregator companies
  - JobSpy experimental companies

If Python is missing on the target machine:
- `launch.bat` will offer the bundled signed Python installer from `installer\downloads\python-3.11.9-amd64.exe`

How to build:
```powershell
installer\portable\build_portable.bat
```

What to upload to GitHub Releases:
- `dist\JobSearchDashboard-portable-2.0.0.zip`

Recommended build flow:
1. Rebuild `installer\wheels` first via `installer\build_installer.bat` if dependencies changed
2. Then run `installer\portable\build_portable.bat`

What end users do:
1. Download the zip
2. Extract it anywhere
3. Run `launch.vbs` or `launch.bat`
4. If Python is missing, allow the bundled Python installer to run
5. Relaunch the app
