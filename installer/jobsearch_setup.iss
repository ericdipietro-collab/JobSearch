; ─────────────────────────────────────────────────────────────────────────────
; Job Search Dashboard — Inno Setup 6 installer script
;
; Run build_installer.bat to build JobSearchSetup.exe.
; Requires: Inno Setup 6  https://jrsoftware.org/isinfo.php
; ─────────────────────────────────────────────────────────────────────────────

#define AppName      "Job Search Dashboard"
#define AppVersion   "1.1"
#define AppPublisher "Job Search Tools"
#define AppExeName   "launch.bat"
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
Source: "..\ats_db.py";            DestDir: "{app}"; Flags: ignoreversion
Source: "..\launch.bat";           DestDir: "{app}"; Flags: ignoreversion
Source: "..\requirements.txt";     DestDir: "{app}"; Flags: ignoreversion
Source: "..\views\*";              DestDir: "{app}\views"; Flags: ignoreversion recursesubdirs
Source: "..\config\job_search_companies.yaml";         DestDir: "{app}\config"; Flags: ignoreversion
Source: "..\config\job_search_preferences.example.yaml"; DestDir: "{app}\config"; Flags: ignoreversion
; Preferences only copied if not already present (preserves user edits on reinstall)
Source: "..\config\job_search_preferences.example.yaml"; DestDir: "{app}\config"; DestName: "job_search_preferences.yaml"; Flags: onlyifdoesntexist
; Installer icon (for uninstaller display)
Source: "assets\app.ico";          DestDir: "{app}\installer\assets"; Flags: ignoreversion
; Optional modules present in the repo
Source: "..\heal_ats_yaml.py";        DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "..\run_job_search_v6.py";    DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "..\job_search_v6.py";        DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "..\rescore_pipeline.py";     DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "..\config\manual_jobs_template.csv"; DestDir: "{app}\config"; Flags: ignoreversion
Source: "..\db\*";                 DestDir: "{app}\db";       Flags: ignoreversion recursesubdirs skipifsourcedoesntexist
Source: "..\services\*";           DestDir: "{app}\services"; Flags: ignoreversion recursesubdirs skipifsourcedoesntexist
Source: "..\docs\*";               DestDir: "{app}\docs";     Flags: ignoreversion recursesubdirs skipifsourcedoesntexist
; ── Bundled Python installer ──────────────────────────────────────────────────
Source: "downloads\{#PythonExe}";  DestDir: "{tmp}"; Flags: deleteafterinstall; Check: NeedsPython

[Dirs]
Name: "{app}\results"; Flags: uninsneveruninstall
Name: "{app}\config";  Flags: uninsneveruninstall

[Icons]
Name: "{group}\{#AppName}";            Filename: "{app}\launch.bat"; WorkingDir: "{app}"; IconFilename: "{app}\installer\assets\app.ico"; Tasks: startmenuicon
Name: "{group}\Uninstall {#AppName}";  Filename: "{uninstallexe}"; Tasks: startmenuicon
Name: "{commondesktop}\{#AppName}";    Filename: "{app}\launch.bat"; WorkingDir: "{app}"; IconFilename: "{app}\installer\assets\app.ico"; Tasks: desktopicon

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
Filename: "{app}\launch.bat"; \
  Parameters: ""; \
  WorkingDir: "{app}"; \
  Description: "Launch {#AppName} now"; \
  Flags: nowait postinstall skipifsilent unchecked

[UninstallDelete]
; Remove venv on uninstall (user data in results/ and config/ is preserved)
Type: filesandordirs; Name: "{app}\.venv"
Type: filesandordirs; Name: "{app}\__pycache__"

[Code]
// ── Python detection ──────────────────────────────────────────────────────────

function GetPythonVersion(var Major, Minor: Integer): Boolean;
var
  Output: AnsiString;
  TmpFile: String;
  ResultCode: Integer;
begin
  TmpFile := ExpandConstant('{tmp}\pyver.txt');
  Exec('cmd.exe',
       '/c python --version > "' + TmpFile + '" 2>&1',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  Result := False;
  Major := 0; Minor := 0;
  if ResultCode = 0 then begin
    if LoadStringFromFile(TmpFile, Output) then begin
      // Output is e.g. "Python 3.11.9"
      if Pos('Python 3.', Output) > 0 then begin
        Major := 3;
        Minor := StrToIntDef(Copy(Output, Pos('Python 3.', Output) + 9, 2), 0);
        Result := True;
      end;
    end;
  end;
end;

function NeedsPython: Boolean;
var
  Major, Minor: Integer;
begin
  Result := not GetPythonVersion(Major, Minor) or (Major < 3) or ((Major = 3) and (Minor < 9));
end;

// ── Pre-install check: warn if Python installer is missing ───────────────────

function InitializeSetup: Boolean;
var
  PyExe: String;
begin
  Result := True;
  if NeedsPython then begin
    PyExe := ExpandConstant('{src}\downloads\{#PythonExe}');
    // The file check is done by Inno's Check: directive; this is belt-and-suspenders.
    if not FileExists(PyExe) then begin
      MsgBox(
        'Python is not installed, and the bundled Python installer was not found.' + #13#10
        + #13#10
        + 'Please either:' + #13#10
        + '  1. Install Python 3.9+ from https://python.org/downloads' + #13#10
        + '     (check "Add Python to PATH"), then re-run this installer' + #13#10
        + '  2. Re-build the installer using build_installer.bat' + #13#10
        + '     (it downloads the Python installer automatically)',
        mbError, MB_OK);
      Result := False;
    end;
  end;
end;

// ── setup-only flag: skip launch.bat startup when called from installer ───────
// launch.bat checks for this flag and exits after venv/pip setup without
// starting streamlit.  See launch.bat for the matching check.
