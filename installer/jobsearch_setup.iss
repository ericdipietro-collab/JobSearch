; ─────────────────────────────────────────────────────────────────────────────
; Job Search Dashboard — Inno Setup 6 installer script
;
; Run build_installer.bat to build JobSearchSetup.exe.
; Requires: Inno Setup 6  https://jrsoftware.org/isinfo.php
; ─────────────────────────────────────────────────────────────────────────────

#define AppName      "Job Search Dashboard"
#define AppVersion   "1.3"
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
Name: "{userdesktop}\{#AppName}";      Filename: "{app}\launch.bat"; WorkingDir: "{app}"; IconFilename: "{app}\installer\assets\app.ico"; Tasks: desktopicon

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
//
// Strategy (most-to-least reliable):
//  1. Registry — HKCU/HKLM Software\Python\PythonCore\3.x  (official installer)
//  2. py --version — Python Launcher, immune to App Execution Aliases
//  3. python --version — last resort, can be fooled by Win11 Store stub
//
// Windows 11 ships a "python.exe" App Execution Alias that may return exit
// code 0 with empty output, or open the Store.  The registry and py.exe checks
// are not affected by this.

function _VersionOk(Major, Minor: Integer): Boolean;
begin
  Result := (Major > 3) or ((Major = 3) and (Minor >= 9));
end;

// Check HKCU then HKLM for PythonCore\3.x keys written by the official installer.
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
    // Enumerate subkeys named like "3.11", "3.12", etc.
    // Inno Setup doesn't have EnumRegKeys so we probe the 9 most likely minors.
    // Minor 9..13 covers Python 3.9 through 3.13.
    Minor := 13;
    while Minor >= 9 do begin
      VerStr := '3.' + IntToStr(Minor);
      if RegKeyExists(Roots[i], KeyBase + '\' + VerStr) then begin
        Major := 3;
        Result := True;
        Exit;
      end;
      Minor := Minor - 1;
    end;
  end;
end;

// Try "py --version" (Python Launcher — never an App Execution Alias).
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
       '/c py --version > "' + TmpFile + '" 2>&1',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if ResultCode = 0 then
    if LoadStringFromFile(TmpFile, Output) then
      if Pos('Python 3.', Output) > 0 then begin
        Major := 3;
        Minor := StrToIntDef(Copy(Output, Pos('Python 3.', Output) + 9, 2), 0);
        Result := True;
      end;
end;

// Last resort: "python --version".
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
      if Pos('Python 3.', Output) > 0 then begin
        Major := 3;
        Minor := StrToIntDef(Copy(Output, Pos('Python 3.', Output) + 9, 2), 0);
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
