using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Windows.Forms;

namespace JobSearchLauncher
{
    internal static class Program
    {
        [STAThread]
        private static int Main(string[] args)
        {
            string exeDir = AppDomain.CurrentDomain.BaseDirectory;
            string launchBat = Path.Combine(exeDir, "launch.bat");
            if (!File.Exists(launchBat))
            {
                MessageBox.Show(
                    "launch.bat was not found next to JobSearchLauncher.exe.\n\n" +
                    "This launcher expects to live beside the app bootstrap files.",
                    "Job Search Dashboard",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
                return 1;
            }

            bool setupOnly = args.Any(a => string.Equals(a, "--setup-only", StringComparison.OrdinalIgnoreCase));
            string forwardedArgs = string.Join(" ", args.Select(QuoteArg));
            string cmdArgs = "/c \"" + launchBat + (forwardedArgs.Length > 0 ? " " + forwardedArgs : "") + "\"";

            var psi = new ProcessStartInfo
            {
                FileName = Environment.GetEnvironmentVariable("ComSpec") ?? "cmd.exe",
                Arguments = cmdArgs,
                WorkingDirectory = exeDir,
                UseShellExecute = false,
                CreateNoWindow = false,
            };

            try
            {
                using (Process process = Process.Start(psi))
                {
                    if (process == null)
                    {
                        MessageBox.Show(
                            "Failed to launch the Job Search Dashboard bootstrap process.",
                            "Job Search Dashboard",
                            MessageBoxButtons.OK,
                            MessageBoxIcon.Error);
                        return 1;
                    }

                    if (setupOnly)
                    {
                        process.WaitForExit();
                        return process.ExitCode;
                    }

                    return 0;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    "Failed to start Job Search Dashboard.\n\n" + ex.Message,
                    "Job Search Dashboard",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
                return 1;
            }
        }

        private static string QuoteArg(string arg)
        {
            if (string.IsNullOrEmpty(arg))
            {
                return "\"\"";
            }

            if (!arg.Any(char.IsWhiteSpace) && !arg.Contains('"'))
            {
                return arg;
            }

            return "\"" + arg.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
        }
    }
}
