; ─────────────────────────────────────────────────────────────────────────────
; Job Search Dashboard — Inno Setup 6 installer script
;
; Run build_installer.bat to build JobSearchSetup.exe.
; Requires: Inno Setup 6  https://jrsoftware.org/isinfo.php
; ─────────────────────────────────────────────────────────────────────────────

#define AppName      "Job Search Dashboard"
#define AppVersion   "2.0.0"
#define AppPublisher "Job Search Tools"
#define AppExeName   "launch.vbs"
#define PythonVer    "3.11.9"
#define PythonExe    "python-3.11.9-amd64.exe"

[Setup]
AppId={{8F3A2B1C-4D5E-6F7A-8B9C-0D1E2F3A4B5C}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL=https://github.com/ericdipietro-collab/JobSearch
AppSupportURL=https://github.com/ericdipietro-collab/JobSearch/issues
DefaultDirName={localappdata}\JobSearchDashboard
DefaultGroupName={#AppName}
AllowNoIcons=yes
; No admin rights needed — installs per-user
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
OutputDir=..\dist
OutputBaseFilename=JobSearchSetup
SetupIconFile=assets\app.ico
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
WizardSizePercent=110
UninstallDisplayIcon={app}\installer\assets\app.ico
DisableWelcomePage=no
DisableDirPage=no
DisableReadyPage=no
; Show finish page with "Launch now" option
ShowComponentSizes=no
VersionInfoVersion={#AppVersion}.0.0
VersionInfoCompany={#AppPublisher}
VersionInfoDescription={#AppName} Setup

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon";  Description: "Create a &Desktop shortcut";     GroupDescription: "Shortcuts:"
Name: "startmenuicon"; Description: "Create a &Start Menu shortcut"; GroupDescription: "Shortcuts:"

[Files]
; ── App source (exclude dev/runtime artifacts) ────────────────────────────────
Source: "..\app.py";               DestDir: "{app}"; Flags: ignoreversion
Source: "..\launch.bat";           DestDir: "{app}"; Flags: ignoreversion
Source: "..\launch.vbs";           DestDir: "{app}"; Flags: ignoreversion
Source: "..\requirements.txt";     DestDir: "{app}"; Flags: ignoreversion
Source: "..\pyproject.toml";       DestDir: "{app}"; Flags: ignoreversion
Source: "..\src\*";                DestDir: "{app}\src";      Flags: ignoreversion recursesubdirs
Source: "..\config\job_search_companies.yaml";         DestDir: "{app}\config"; Flags: ignoreversion onlyifdoesntexist
Source: "..\config\job_search_companies_contract.yaml"; DestDir: "{app}\config"; Flags: ignoreversion onlyifdoesntexist skipifsourcedoesntexist
Source: "..\config\job_search_companies_aggregators.yaml"; DestDir: "{app}\config"; Flags: ignoreversion onlyifdoesntexist skipifsourcedoesntexist
; Always ship the latest curated primary registry alongside user-preserved config for upgrade review/import.
Source: "..\config\job_search_companies.yaml"; DestDir: "{app}\config"; DestName: "job_search_companies.upgrade_defaults.yaml"; Flags: ignoreversion
Source: "..\config\job_search_preferences.example.yaml"; DestDir: "{app}\config"; Flags: ignoreversion
; Preferences only copied if not already present (preserves user edits on reinstall)
Source: "..\config\job_search_preferences.example.yaml"; DestDir: "{app}\config"; DestName: "job_search_preferences.yaml"; Flags: onlyifdoesntexist
Source: "..\README.md";            DestDir: "{app}\docs";     Flags: ignoreversion skipifsourcedoesntexist
Source: "..\GETTING_STARTED.md";   DestDir: "{app}\docs";     Flags: ignoreversion skipifsourcedoesntexist
Source: "..\deep_search\*";        DestDir: "{app}\deep_search"; Flags: ignoreversion recursesubdirs skipifsourcedoesntexist
Source: "wheels\*";                DestDir: "{app}\installer\wheels"; Flags: ignoreversion recursesubdirs skipifsourcedoesntexist
; Installer icon (for uninstaller display)
Source: "assets\app.ico";          DestDir: "{app}\installer\assets"; Flags: ignoreversion
; Additional tools
Source: "..\rescore_pipeline.py";     DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "..\config\manual_jobs_template.csv"; DestDir: "{app}\config"; Flags: ignoreversion
Source: "..\docs\*";               DestDir: "{app}\docs";     Flags: ignoreversion recursesubdirs skipifsourcedoesntexist
; ── Bundled Python installer ──────────────────────────────────────────────────
Source: "downloads\{#PythonExe}";  DestDir: "{tmp}"; Flags: deleteafterinstall; Check: NeedsPython

[InstallDelete]
; ── Remove v1.6 flat-layout artifacts before v2.0 files land ─────────────────
; v1.6 kept all Python modules at the app root and in top-level db/, services/,
; views/ folders.  v2.0 consolidates everything under src/jobsearch/.  Leaving
; the old files in place can cause import shadowing and general confusion.
;
; User data (results/, config/) is never touched — those dirs carry
; uninsneveruninstall in [Dirs] and are not listed here.

; Root-level Python scripts replaced by the src package + CLI
Type: files; Name: "{app}\ats_db.py"
Type: files; Name: "{app}\heal_ats_yaml.py"
Type: files; Name: "{app}\job_search_v6.py"
Type: files; Name: "{app}\run_job_search_v6.py"
Type: files; Name: "{app}\scoring.py"
Type: files; Name: "{app}\scraper.py"
Type: files; Name: "{app}\models.py"

; Old top-level package directories (now live under src/jobsearch/)
Type: filesandordirs; Name: "{app}\db"
Type: filesandordirs; Name: "{app}\services"
Type: filesandordirs; Name: "{app}\views"
Type: filesandordirs; Name: "{app}\scraper"

; Stale bytecode caches from the old layout
Type: filesandordirs; Name: "{app}\__pycache__"
Type: filesandordirs; Name: "{app}\db\__pycache__"
Type: filesandordirs; Name: "{app}\services\__pycache__"
Type: filesandordirs; Name: "{app}\views\__pycache__"
Type: filesandordirs; Name: "{app}\scraper\__pycache__"

; Old venv — must be rebuilt against the new src/ editable install.
; launch.bat will recreate it during the post-install --setup-only run.
Type: filesandordirs; Name: "{app}\.venv"

[Dirs]
Name: "{app}\results"; Flags: uninsneveruninstall
Name: "{app}\config";  Flags: uninsneveruninstall

[Icons]
Name: "{group}\{#AppName}";            Filename: "wscript.exe"; Parameters: """{app}\launch.vbs"""; WorkingDir: "{app}"; IconFilename: "{app}\installer\assets\app.ico"; Tasks: startmenuicon
Name: "{group}\Uninstall {#AppName}";  Filename: "{uninstallexe}"; Tasks: startmenuicon
Name: "{userdesktop}\{#AppName}";      Filename: "wscript.exe"; Parameters: """{app}\launch.vbs"""; WorkingDir: "{app}"; IconFilename: "{app}\installer\assets\app.ico"; Tasks: desktopicon

[Run]
; ── Install Python if missing ─────────────────────────────────────────────────
Filename: "{tmp}\{#PythonExe}"; \
  Parameters: "/quiet InstallAllUsers=0 PrependPath=1 Include_test=0 Include_doc=0 Include_tcltk=0"; \
  StatusMsg: "Installing Python {#PythonVer} (one-time, ~30 seconds)..."; \
  Check: NeedsPython; \
  Flags: waituntilterminated

; ── First-run dependency install via launch.bat ───────────────────────────────
; launch.bat already handles venv creation + pip install on first run.
; We trigger it hidden here so the user's first real launch is instant.
Filename: "cmd.exe"; \
  Parameters: "/c ""{app}\launch.bat"" --setup-only"; \
  WorkingDir: "{app}"; \
  StatusMsg: "Installing dependencies (one-time, ~1 minute)..."; \
  Flags: waituntilterminated runhidden

; ── Offer to launch immediately after install ─────────────────────────────────
Filename: "wscript.exe"; \
  Parameters: """{app}\launch.vbs"""; \
  WorkingDir: "{app}"; \
  Description: "Launch {#AppName} now"; \
  Flags: nowait postinstall skipifsilent unchecked

[UninstallDelete]
; Remove venv on uninstall (user data in results/ and config/ is preserved)
Type: filesandordirs; Name: "{app}\.venv"
Type: filesandordirs; Name: "{app}\__pycache__"

[Code]
// ── Python detection ──────────────────────────────────────────────────────────
//
// Strategy (most-to-least reliable):
//  1. Registry — HKCU/HKLM Software\Python\PythonCore\3.11  (official installer)
//  2. py --version — Python Launcher, immune to App Execution Aliases
//  3. python --version — last resort, can be fooled by Win11 Store stub
//
// Windows 11 ships a "python.exe" App Execution Alias that may return exit
// code 0 with empty output, or open the Store.  The registry and py.exe checks
// are not affected by this.

function _VersionOk(Major, Minor: Integer): Boolean;
begin
  Result := (Major = 3) and (Minor = 11);
end;

// Check HKCU then HKLM for PythonCore\3.11 written by the official installer.
function _PythonInRegistry(var Major, Minor: Integer): Boolean;
var
  i: Integer;
  KeyBase, VerStr: String;
  Roots: array[0..1] of Integer;
begin
  Result := False;
  Major := 0; Minor := 0;
  Roots[0] := HKEY_CURRENT_USER;
  Roots[1] := HKEY_LOCAL_MACHINE;
  for i := 0 to 1 do begin
    KeyBase := 'Software\Python\PythonCore';
    VerStr := '3.11';
    if RegKeyExists(Roots[i], KeyBase + '\' + VerStr) then begin
      Major := 3;
      Minor := 11;
      Result := True;
      Exit;
    end;
  end;
end;

// Try "py -3.11 --version" (Python Launcher — never an App Execution Alias).
function _PythonViaLauncher(var Major, Minor: Integer): Boolean;
var
  Output: AnsiString;
  TmpFile: String;
  ResultCode: Integer;
begin
  Result := False;
  Major := 0; Minor := 0;
  TmpFile := ExpandConstant('{tmp}\pyver_launcher.txt');
  Exec('cmd.exe',
       '/c py -3.11 --version > "' + TmpFile + '" 2>&1',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if ResultCode = 0 then
    if LoadStringFromFile(TmpFile, Output) then
      if Pos('Python 3.11.', Output) > 0 then begin
        Major := 3;
        Minor := 11;
        Result := True;
      end;
end;

// Last resort: "python --version", but only accept Python 3.11.
function _PythonViaCli(var Major, Minor: Integer): Boolean;
var
  Output: AnsiString;
  TmpFile: String;
  ResultCode: Integer;
begin
  Result := False;
  Major := 0; Minor := 0;
  TmpFile := ExpandConstant('{tmp}\pyver_cli.txt');
  Exec('cmd.exe',
       '/c python --version > "' + TmpFile + '" 2>&1',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if ResultCode = 0 then
    if LoadStringFromFile(TmpFile, Output) then
      if Pos('Python 3.11.', Output) > 0 then begin
        Major := 3;
        Minor := 11;
        Result := True;
      end;
end;

function NeedsPython: Boolean;
var
  Major, Minor: Integer;
begin
  // Registry check first — most reliable
  if _PythonInRegistry(Major, Minor) and _VersionOk(Major, Minor) then begin
    Result := False; Exit;
  end;
  // Python Launcher second
  if _PythonViaLauncher(Major, Minor) and _VersionOk(Major, Minor) then begin
    Result := False; Exit;
  end;
  // CLI fallback — skip if output looks like the Win11 Store stub (empty/short)
  if _PythonViaCli(Major, Minor) and _VersionOk(Major, Minor) then begin
    Result := False; Exit;
  end;
  Result := True;
end;

// ── setup-only flag: skip launch.bat startup when called from installer ───────
// launch.bat checks for this flag and exits after venv/pip setup without
// starting streamlit.  See launch.bat for the matching check.
