# windesktop-launch_stream.py
#
# ExecuteScript (Script Engine: python) Script Body for the WindowsDesktopCpp agent class's
# !load flow — issue #4 (Test a Windows Python Processor for TwitchChatListener). Adapted from
# agent-NvidiaNano-launch_stream.py (Chrome/chromium kill-relaunch pattern) and
# browser_launcher.py (Windows chrome.exe path + reposition_chrome.ps1 handoff), ported to run
# *inside* the MiNiFi agent process itself instead of behind a separate always-on listener
# service like browser_launcher.py's :5901 — that's the whole point of this test.
#
# Own port (:18081 /loadWindows), own Chrome profile dir (C:\minifi-windesktop\chrome-profile-v2),
# own reposition script (windesktop-reposition_chrome.ps1) — does not touch screen2's
# C:\minifi-manual\chrome-kiosk-profile or the :5901/:18080 listeners already in production.
#
# Target monitor: DISPLAY1, the gaming PC's left/non-primary monitor (-1920,0,1280x720),
# confirmed via [System.Windows.Forms.Screen]::AllScreens to be the one NOT already claimed by
# the production screen2 flow (which owns DISPLAY2, the primary 1920x1080 monitor at 0,0).
#
# Paste this whole file's onTrigger function as the ExecuteScript processor's Script Body.

import subprocess
import os
import time
import json


class ReadContentCallback:
    def __init__(self):
        self.content = ""
    def process(self, input_stream):
        self.content = input_stream.read().decode('utf-8')
        return len(self.content)


PROFILE_DIR = r"C:\minifi-windesktop\chrome-profile-v2"
REPOSITION_SCRIPT = r"C:\minifi-windesktop\reposition_chrome.ps1"
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
        reader = ReadContentCallback()
        session.read(flow_file, reader)
        payload = json.loads(reader.content) if reader.content.strip() else {}
        streamer = payload.get("streamer")
        if not streamer:
            raise ValueError("payload missing 'streamer' field")

        url = f"https://www.twitch.tv/{streamer}"

        # Kill only chrome.exe processes running with OUR profile dir — scoped via
        # Get-CimInstance's CommandLine match so screen2's browser_launcher.py chrome
        # (a different --user-data-dir) is never touched, unlike a blanket
        # `taskkill /IM chrome.exe /F` which would hit every chrome window on the box.
        subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
             f"Where-Object {{ $_.CommandLine -and $_.CommandLine -like '*{PROFILE_DIR}*' }} | "
             "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"],
            capture_output=True,
        )
        time.sleep(1.0)

        chrome = find_chrome()
        proc = subprocess.Popen(
            [chrome, "--new-window", f"--user-data-dir={PROFILE_DIR}",
             f"--window-position={TARGET_X},{TARGET_Y}",
             f"--window-size={TARGET_W},{TARGET_H}", url],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

        # No --kiosk on launch, same reasoning as browser_launcher.py: kiosk fullscreen
        # locks to whichever monitor the cursor is on at launch and MoveWindow afterward
        # only relocates the frame, not the composited pixels. Launch windowed, reposition,
        # THEN fullscreen — reposition_chrome.ps1 does the F11 + Twitch 'f' hotkey.
        # -ExecutionPolicy Bypass is required here: when the agent runs as the
        # "Apache NiFi MiNiFi" Windows service (LocalSystem), that account's effective
        # policy is Restricted (no CurrentUser RemoteSigned override the way an
        # interactive user profile has), so a bare `-File script.ps1` is refused with
        # "running scripts is disabled on this system" — confirmed live 2026-07-28.
        # Bypass sidesteps that regardless of caller identity. It does NOT, by itself,
        # fix the deeper Session-0 issue below.
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", REPOSITION_SCRIPT,
             "-X", str(TARGET_X), "-Y", str(TARGET_Y),
             "-W", str(TARGET_W), "-H", str(TARGET_H),
             "-ProfileDir", PROFILE_DIR,
             "-SiteFullscreenKey", "f",
             "-TimeoutSeconds", "10"],
            capture_output=True, text=True,
        )
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()

        session.putAttribute(flow_file, "python.load.streamer", streamer)
        if out.startswith("OK"):
            session.putAttribute(flow_file, "python.load.status", "Success")
            session.putAttribute(flow_file, "python.load.reposition", out)
            session.transfer(flow_file, REL_SUCCESS)
        else:
            session.putAttribute(flow_file, "python.load.status", "RepositionFailed")
            session.putAttribute(flow_file, "python.load.reposition", out or err)
            session.transfer(flow_file, REL_SUCCESS)  # chrome did launch; reposition is best-effort

    except Exception as e:
        session.putAttribute(flow_file, "python.error", str(e))
        session.transfer(flow_file, REL_FAILURE)
