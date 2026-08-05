import json
import urllib.request

# Callback class for reading the session stream
class ReadContentCallback:
    def __init__(self):
        self.content = ""
    def process(self, input_stream):
        self.content = input_stream.read().decode('utf-8')
        return len(self.content)  # Good practice to return bytes read


# Native host listener that owns the actual playback — mpv_stream_launcher_linux.py
# (persistent mpv + IPC), replacing this script's former Chromium kill/relaunch.
# Same migration already proven on StarlinkAI and WindowsDesktop; screen1 was
# the last screen still building its own URL, which is why `!load kick:<slug>
# screen1` resolved to a Twitch 404 while every other screen handled it.
#
# Unlike the WindowsDesktop equivalent (host.docker.internal — that agent runs in
# a pod with no display of its own), MiNiFi runs natively on this Jetson, so the
# listener is plain loopback.
LISTENER_URL = "http://127.0.0.1:5902/load/screen1"


# This is the exact entrypoint MiNiFi C++ calls on every loop execution
def onTrigger(context, session):

    flow_file = session.get()

    if flow_file:
        try:
            # 1. Read upstream payload — expects JSON like {"streamer": "xqc", ...}
            reader = ReadContentCallback()
            session.read(flow_file, reader)

            payload = json.loads(reader.content) if reader.content.strip() else {}
            streamer = payload.get("streamer")
            if not streamer:
                raise ValueError("payload missing 'streamer' field")

            # 2. Hand off to the launcher — it builds the actual Twitch/Kick URL
            # itself from the raw streamer value (build_url(), gaining
            # kick:<slug> support), so this script no longer constructs a URL.
            #
            # Timeout is generous because a cold start has to spawn mpv and let
            # yt-dlp resolve the playlist; a warm mpv answers in milliseconds.
            # ExecuteScript runs on MiNiFi's single shared thread, so this must
            # not be unbounded.
            body = json.dumps({"streamer": streamer}).encode('utf-8')
            req = urllib.request.Request(LISTENER_URL, data=body, method="POST",
                                          headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode('utf-8'))

            if not result.get("ok"):
                raise RuntimeError(f"listener reported failure: {result.get('error')}")

            session.putAttribute(flow_file, "python.load.status", "Success")
            session.putAttribute(flow_file, "python.load.streamer", streamer)
            session.putAttribute(flow_file, "python.load.url", result.get("url", ""))

            # 3. Route to success relationship
            session.transfer(flow_file, REL_SUCCESS)

        except Exception as e:
            # If it breaks, append the error message to an attribute and fail it
            session.putAttribute(flow_file, "python.error", str(e))
            session.transfer(flow_file, REL_FAILURE)
