# windesktop-launch_matrix.py
#
# ExecuteScript (Script Engine: python) Script Body for the WindowsDesktopCpp agent class's
# !matrix flow — issue #4 companion to windesktop-launch_stream.py. Adapted from
# agent-NvidiaNano-launch_matrix.py (kill-relaunch + local file:// HTML + forced fullscreen),
# ported to Windows chrome.exe + windesktop-reposition_chrome.ps1 instead of chromium/wmctrl.
#
# Fixed action, no payload needed (matches agent-NvidiaNano-launch_matrix.py / gaming-pc's
# matrix flow — "on" is the only mode). Loads windesktop-matrix-screensaver.html via a
# file:// URL. Same profile dir and target monitor as windesktop-launch_stream.py so the two
# share window-kill scoping and never collide with each other or with screen2.
#
# Paste this whole file's onTrigger function as the ExecuteScript processor's Script Body.

import subprocess
import os
import time

PROFILE_DIR = r"C:\minifi-windesktop\chrome-profile-v2"
REPOSITION_SCRIPT = r"C:\minifi-windesktop\reposition_chrome.ps1"
MATRIX_HTML = r"C:\minifi-windesktop\matrix-screensaver.html"
TARGET_X, TARGET_Y, TARGET_W, TARGET_H = -1920, 0, 1280, 720
CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]


def find_chrome():
    for p in CHROME_PATHS:
        if os.path.exists(p):
            return p
    return "chrome.exe"


# This is the exact entrypoint MiNiFi C++ calls on every loop execution
def onTrigger(context, session):
    flow_file = session.get()
    if not flow_file:
        return

    try:
        url = "file:///" + MATRIX_HTML.replace("\\", "/")

        subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
             f"Where-Object {{ $_.CommandLine -and $_.CommandLine -like '*{PROFILE_DIR}*' }} | "
             "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"],
            capture_output=True,
        )
        time.sleep(1.0)

        chrome = find_chrome()
        subprocess.Popen(
            [chrome, "--new-window", f"--user-data-dir={PROFILE_DIR}",
             f"--window-position={TARGET_X},{TARGET_Y}",
             f"--window-size={TARGET_W},{TARGET_H}", url],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

        # No -SiteFullscreenKey here: a local canvas page has no site chrome to hide beyond
        # Chrome's own toolbar, so F11 alone (reposition_chrome.ps1's default) is enough —
        # unlike Twitch, there's no in-page Fullscreen API hotkey to also simulate.
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", REPOSITION_SCRIPT,
             "-X", str(TARGET_X), "-Y", str(TARGET_Y),
             "-W", str(TARGET_W), "-H", str(TARGET_H),
             "-ProfileDir", PROFILE_DIR,
             "-TimeoutSeconds", "10"],
            capture_output=True, text=True,
        )
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()

        if out.startswith("OK"):
            session.putAttribute(flow_file, "python.matrix.status", "Success")
            session.putAttribute(flow_file, "python.matrix.reposition", out)
        else:
            session.putAttribute(flow_file, "python.matrix.status", "RepositionFailed")
            session.putAttribute(flow_file, "python.matrix.reposition", out or err)
        session.transfer(flow_file, REL_SUCCESS)

    except Exception as e:
        session.putAttribute(flow_file, "python.error", str(e))
        session.transfer(flow_file, REL_FAILURE)
