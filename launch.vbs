' launch.vbs — starts Job Search Dashboard without showing a terminal window.
' The shortcut and installer both point here instead of launch.bat directly.
' If something goes wrong, run launch.bat directly to see the error output.

Dim oShell, scriptDir
Set oShell = CreateObject("WScript.Shell")
scriptDir = Left(WScript.ScriptFullName, InStrRev(WScript.ScriptFullName, "\"))
' Window style 1 = normal visible window. False = don't wait (VBScript exits, app keeps running).
' The terminal shows startup progress and stays open while the app runs;
' closing it stops the app.  If an error occurs the user can read the message.
oShell.Run """" & scriptDir & "launch.bat""", 1, False
