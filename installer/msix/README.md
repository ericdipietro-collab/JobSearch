# MSIX / App Installer Notes

This repo is partially prepared for an MSIX/App Installer distribution path.

What is ready:
- Runtime state can live outside the install directory via `JOBSEARCH_HOME`.
- `launch.bat` now defaults writable state to:
  - `%LOCALAPPDATA%\JobSearchDashboardData\config`
  - `%LOCALAPPDATA%\JobSearchDashboardData\results`
  - `%LOCALAPPDATA%\JobSearchDashboardData\data`
  - `%LOCALAPPDATA%\JobSearchDashboardData\.venv`
- Existing install-local `config` and `results\jobsearch.db` are migrated forward on first launch.

Current launcher path:
- `JobSearchLauncher.cs` is a small Win32 launcher source.
- `build_launcher.bat` builds `bin\JobSearchLauncher.exe`.
- The launcher expects `launch.bat` to sit beside it and forwards args like `--setup-only`.

What still blocks a real MSIX package:
- A package manifest is still needed.
- App Installer metadata still needs to be added.
- Signing is still required for real-world distribution.

Recommended next steps:
1. Build `bin\JobSearchLauncher.exe`.
2. Create `Package.appxmanifest`.
3. Add `.appinstaller` generation.
4. Sign the package with a trusted RSA code-signing certificate.

Notes:
- Unsigned MSIX/App Installer packages will still run into Windows trust / Smart App Control issues.
- GitHub Releases hosting is fine for `.appinstaller` and `.msix`, but it does not replace signing.
