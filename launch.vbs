' launch.vbs — starts Job Search Dashboard without showing a terminal window.
' The shortcut and installer both point here instead of launch.bat directly.
' If something goes wrong, run launch.bat directly to see the error output.

Dim oShell, scriptDir
Set oShell = CreateObject("WScript.Shell")
scriptDir = Left(WScript.ScriptFullName, InStrRev(WScript.ScriptFullName, "\"))
' Window style 0 = hidden. False = don't wait (VBScript exits, app keeps running).
oShell.Run """" & scriptDir & "launch.bat""", 0, False
