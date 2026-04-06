# MSIX Release

This folder contains the build script and templates for the Windows MSIX package.

The MSIX build produces:
- `dist\JobSearchDashboard_2.0.0.0_x64.msix`
- optionally `dist\JobSearchDashboard.appinstaller` when a base URI is provided

Characteristics:
- packaged Windows install path
- writable runtime state stays under `%LOCALAPPDATA%\JobSearchDashboardData`
- ships the current default registries for:
  - main ATS companies
  - contractor companies
  - aggregator companies
  - JobSpy experimental companies

How to build:
```powershell
installer\msix\build_msix.bat
```

Optional App Installer output:
```powershell
$env:JOBSEARCH_APPINSTALLER_BASE_URI="https://github.com/<owner>/<repo>/releases/download/v2.0.0"
installer\msix\build_msix.bat
```

Optional signing:
```powershell
$env:JOBSEARCH_MSIX_PFX_PATH="C:\path\to\cert.pfx"
$env:JOBSEARCH_MSIX_PFX_PASSWORD="your-password"
installer\msix\build_msix.bat
```

Upgrade note:
- if you are upgrading from the older split-database v1 release, run the legacy migration script once before launching the new build
- current builds upgrade the unified SQLite schema in place, but they do not auto-merge the old split DB pair

Recommended build flow:
1. Rebuild `installer\wheels` first via `installer\build_installer.bat` if dependencies changed
2. Then run `installer\msix\build_msix.bat`

Notes:
- unsigned MSIX packages can still trigger Windows trust / Smart App Control friction
- rebuild the MSIX after runtime-visible code or config changes
- if you publish an `.appinstaller`, use the final public release URL for the matching version
