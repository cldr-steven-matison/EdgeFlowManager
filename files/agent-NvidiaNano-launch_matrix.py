import subprocess
import os
import time
import urllib.request

# The stream launcher's /stop endpoint (mpv_stream_launcher_linux.py).
# screen1's stream player is mpv now, not Chromium, so the pkill below no
# longer tears a live stream down on its own — without this call the mpv
# window would stay fullscreen on top of the matrix page.
STREAM_LAUNCHER_STOP = "http://127.0.0.1:5902/stop/screen1"


def stop_stream_best_effort():
    """Nothing playing, or launcher not up? Either is fine — still show matrix."""
    try:
        req = urllib.request.Request(STREAM_LAUNCHER_STOP, data=b"{}", method="POST",
                                      headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


# This is the exact entrypoint MiNiFi C++ calls on every loop execution
def onTrigger(context, session):

    flow_file = session.get()

    if flow_file:
        try:
            # Same fixed launch — no payload needed, "on" is the only mode.
            # File URL, not a bare path: Chromium needs the file:// scheme to
            # load a local page in --kiosk mode the same way it loads a real URL.
            url = "file:///home/tunastreet/matrix-screensaver.html"

            env = os.environ.copy()
            env["DISPLAY"] = ":0"
            env["XAUTHORITY"] = "/run/user/1000/gdm/Xauthority"  # confirmed live value, see agent-NvidiaNano-launch_stream.py
            env["XDG_RUNTIME_DIR"] = "/run/user/1000"
            env["DBUS_SESSION_BUS_ADDRESS"] = "unix:path=/run/user/1000/bus"

            # Get a live stream out of the way first — mpv, via the launcher's
            # /stop (which stops playback and minimizes the window rather than
            # killing the process, so the next !load is still a warm start).
            stop_stream_best_effort()

            # SIGKILL + a real wait, because a surviving process holding its
            # profile lock makes the new launch silently proxy into it and
            # ignore --kiosk. This now only clears a *previous matrix* Chromium
            # — the stream is mpv, handled above.
            #
            # Scoped to the profile dir rather than a bare "chromium": every
            # process in Chromium's tree carries --user-data-dir in its own
            # argv, so this still catches the whole tree (the same reasoning
            # lofi-idle-watcher.sh documents), while a bare "chromium" match
            # also hits unrelated processes that merely mention the word.
            # Confirmed live 2026-08-02: the broad pattern SIGKILLed an
            # unrelated shell whose command line happened to contain it.
            #
            # The leading "--" of the flag is deliberately omitted: pkill parses
            # a pattern starting with dashes as an option and silently kills
            # nothing (also confirmed live — matrix windows piled up, one per
            # !matrix, because every kill was a no-op).
            subprocess.run(["pkill", "-9", "-f", "user-data-dir=/tmp/chromium-matrix-display"],
                           check=False)
            time.sleep(1.5)

            # Separate profile dir from the stream loader's
            # (/tmp/chromium-stream-display) — never used at the same time
            # since the pkill above always clears the field first, but keeps
            # the two launch paths independent rather than sharing a lock.
            proc = subprocess.Popen(
                ["chromium-browser", "--new-window", "--kiosk", "--start-fullscreen",
                 "--window-position=0,0", "--user-data-dir=/tmp/chromium-matrix-display", url],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            )

            time.sleep(1.5)
            exit_code = proc.poll()
            if exit_code is not None:
                stderr_output = proc.stderr.read().decode('utf-8', errors='ignore')[:500]
                raise RuntimeError(f"chromium exited immediately (code {exit_code}): {stderr_output}")

            # Chromium's own --kiosk/--start-fullscreen don't reliably get
            # Mutter to grant real X11 fullscreen state on this device (see
            # agent-NvidiaNano-launch_stream.py) — force it after the fact
            # with wmctrl, backgrounded since ExecuteScript runs on a single
            # shared thread. First attempt matched on WM_CLASS via `wmctrl -lx`
            # ("chromium.chromium") — wrong, real WM_CLASS strings like
            # "chromium-browser.Chromium-browser" don't contain that literal
            # substring, so the poll silently never found the window and
            # fullscreen never fired (confirmed live: page loaded, stayed
            # windowed). Real fix: match on window *title* instead, same as
            # agent-NvidiaNano-launch_stream.py — Chromium always appends
            # " - Chromium" to the title bar regardless of page content, so
            # this doesn't depend on knowing the matrix HTML's own <title>.
            fullscreen_poll = (
                "for i in $(seq 1 240); do "
                "  if wmctrl -l | grep -qi -- ' - Chromium'; then "
                "    wmctrl -r 'Chromium' -b add,fullscreen; "
                "    break; "
                "  fi; "
                "  sleep 0.25; "
                "done"
            )
            subprocess.Popen(
                ["bash", "-c", fullscreen_poll],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )

            session.putAttribute(flow_file, "python.matrix.status", "Success")
            session.transfer(flow_file, REL_SUCCESS)

        except Exception as e:
            session.putAttribute(flow_file, "python.error", str(e))
            session.transfer(flow_file, REL_FAILURE)
