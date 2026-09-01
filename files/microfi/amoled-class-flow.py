#!/usr/bin/env python3
"""Rebuild the AMOLED agent-class flow through EFM's Flow Designer API (#191).

    python3 amoled-class-flow.py show
    python3 amoled-class-flow.py clear                 # delete every connection + processor
    python3 amoled-class-flow.py build <spec>          # touch-audio | imu-display
    python3 amoled-class-flow.py publish "<comment>"

Contract (reverse-engineered, see reference_efm_flow_designer_api):
  GET  /efm/api/designer/client-identifier               -> revision.clientId
  GET  /efm/api/designer/flows/summaries                  -> flow id per class
  POST /efm/api/designer/flows/{f}/process-groups/{pg}/processors
  POST /efm/api/designer/flows/{f}/process-groups/{pg}/connections
  DELETE .../connections/{id}?version=&clientId=   (use the entity's own revision)
  DELETE .../processors/{id}?version=&clientId=
  GET  /efm/api/designer/flows/{f}/validate
  POST /efm/api/designer/flows/{f}/publish {"comments": ...}

Create Designer nodes only AFTER the class manifest re-pin has landed
(DELETE + POST /efm/api/agent-class-manifest-config) or the node stays
"not an available Processor type". Every MicroFi processor carries a
`success` relationship; sinks must auto-terminate it or validation fails.
MicroFi kMaxFlowNodes=4: never push more than 4 processors to this class.
"""
import json
import sys
import urllib.request
import uuid

EFM = "http://localhost:10090/efm/api"
CLASS = "AMOLED"
BUNDLE = {"group": "org.apache.nifi", "artifact": "microfi-system", "version": "0.1.0"}

BROKER = "mqtt://192.168.1.121:1883"

SPECS = {
    # #191 rungs 4+5 test flow: touch out over MQTT, audio in over HTTP.
    "touch-audio": {
        "processors": [
            {"key": "src", "type": "GetTouch", "pos": (100, 100),
             "props": {"Events": "Release", "Output Format": "JSON"}, "auto": []},
            {"key": "mqtt", "type": "PublishMQTT", "pos": (100, 400),
             "props": {"Broker URI": BROKER, "Client ID": "amoled-touch",
                       "Topic": "microfi/amoled/touch", "Quality of Service": "0"},
             "auto": ["success"]},
            {"key": "http", "type": "ListenHTTP", "pos": (800, 100),
             "props": {"Base Path": "/play", "Listening Port": "8095"}, "auto": []},
            {"key": "play", "type": "PlayAudio", "pos": (800, 400),
             "props": {"Interrupt": "true", "Volume": "100"}, "auto": ["success"]},
        ],
        "connections": [("src", "mqtt"), ("http", "play")],
    },
    # #191 rung 6 verify flow: POST /record on the board -> 3 s mic clip
    # goes broker-direct as WAV on microfi/amoled/audio, the capture event
    # rides the flow to microfi/amoled/audio/meta.
    "record": {
        "processors": [
            {"key": "http", "type": "ListenHTTP", "pos": (100, 100),
             "props": {"Base Path": "/record", "Listening Port": "8095"}, "auto": []},
            {"key": "mic", "type": "CaptureAudio", "pos": (100, 400),
             "props": {"Broker URI": BROKER, "Audio Topic": "microfi/amoled/audio",
                       "Client ID": "amoled-mic", "Clip Seconds": "3",
                       "Capture Every N Ticks": "0"}, "auto": []},
            {"key": "mqtt", "type": "PublishMQTT", "pos": (100, 700),
             "props": {"Broker URI": BROKER, "Client ID": "amoled-audio-meta",
                       "Topic": "microfi/amoled/audio/meta", "Quality of Service": "0"},
             "auto": ["success"]},
        ],
        "connections": [("http", "mic"), ("mic", "mqtt")],
    },
    # Same, but a tap/swipe on the glass is the record button.
    "touch-record": {
        "processors": [
            {"key": "src", "type": "GetTouch", "pos": (100, 100),
             "props": {"Events": "Release", "Output Format": "JSON"}, "auto": []},
            {"key": "mic", "type": "CaptureAudio", "pos": (100, 400),
             "props": {"Broker URI": BROKER, "Audio Topic": "microfi/amoled/audio",
                       "Client ID": "amoled-mic", "Clip Seconds": "3",
                       "Capture Every N Ticks": "0"}, "auto": []},
            {"key": "mqtt", "type": "PublishMQTT", "pos": (100, 700),
             "props": {"Broker URI": BROKER, "Client ID": "amoled-audio-meta",
                       "Topic": "microfi/amoled/audio/meta", "Quality of Service": "0"},
             "auto": ["success"]},
        ],
        "connections": [("src", "mic"), ("mic", "mqtt")],
    },
    # Both triggers on one CaptureAudio: a tap on the glass OR POST /record
    # (4 nodes, 3 connections -- the last shape that fits kMaxFlowNodes=4).
    "record-both": {
        "processors": [
            {"key": "src", "type": "GetTouch", "pos": (100, 100),
             "props": {"Events": "Release", "Output Format": "JSON"}, "auto": []},
            {"key": "http", "type": "ListenHTTP", "pos": (500, 100),
             "props": {"Base Path": "/record", "Listening Port": "8095"}, "auto": []},
            {"key": "mic", "type": "CaptureAudio", "pos": (300, 400),
             "props": {"Broker URI": BROKER, "Audio Topic": "microfi/amoled/audio",
                       "Client ID": "amoled-mic", "Clip Seconds": "3",
                       "Capture Every N Ticks": "0"}, "auto": []},
            {"key": "mqtt", "type": "PublishMQTT", "pos": (300, 700),
             "props": {"Broker URI": BROKER, "Client ID": "amoled-audio-meta",
                       "Topic": "microfi/amoled/audio/meta", "Quality of Service": "0"},
             "auto": ["success"]},
        ],
        "connections": [("src", "mic"), ("http", "mic"), ("mic", "mqtt")],
    },
    # #227 as-built, for restoring the shake/DisplayMessage demo.
    "imu-display": {
        "processors": [
            {"key": "src", "type": "GetIMU", "pos": (100, 100),
             "props": {"Read Interval": "1 s", "Output Format": "JSON",
                       "Accel Full Scale": "4g", "Gyro Full Scale": "512dps",
                       "Motion Threshold (g)": "0.3"}, "auto": []},
            {"key": "mqtt", "type": "PublishMQTT", "pos": (100, 400),
             "props": {"Broker URI": BROKER, "Client ID": "amoled-imu",
                       "Topic": "microfi/amoled/imu", "Quality of Service": "0"},
             "auto": ["success"]},
            {"key": "http", "type": "ListenHTTP", "pos": (800, 100),
             "props": {"Base Path": "/message", "Listening Port": "8095"}, "auto": []},
            {"key": "disp", "type": "DisplayMessage", "pos": (800, 400),
             "props": {}, "auto": ["success"]},
        ],
        "connections": [("src", "mqtt"), ("http", "disp")],
    },
}


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(EFM + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def flow_ids():
    _, summaries = call("GET", "/designer/flows/summaries")
    for e in (summaries["elements"] if isinstance(summaries, dict) else summaries):
        if e.get("agentClass") == CLASS:
            return e["identifier"], e["rootProcessGroupIdentifier"]
    raise SystemExit(f"no designer flow for class {CLASS}")


def client_id():
    _, r = call("GET", "/designer/client-identifier")
    return r["clientId"]


def show(flow):
    _, d = call("GET", f"/designer/flows/{flow}")
    fc = d["flowContent"]
    for p in fc.get("processors", []):
        print("PROC", p["identifier"], p["type"], json.dumps({k: v for k, v in p["properties"].items() if v}),
              p.get("autoTerminatedRelationships"))
    for c in fc.get("connections", []):
        print("CONN", c["identifier"], c["source"]["id"], "->", c["destination"]["id"], c["selectedRelationships"])
    return fc


def clear(flow, cid):
    fc = show(flow)
    for c in fc.get("connections", []):
        _, ent = call("GET", f"/designer/flows/{flow}/connections/{c['identifier']}")
        ver = ent["revision"]["version"]
        st, _ = call("DELETE", f"/designer/flows/{flow}/connections/{c['identifier']}?version={ver}&clientId={cid}")
        print("del conn", c["identifier"], st)
    for p in fc.get("processors", []):
        _, ent = call("GET", f"/designer/flows/{flow}/processors/{p['identifier']}")
        ver = ent["revision"]["version"]
        st, _ = call("DELETE", f"/designer/flows/{flow}/processors/{p['identifier']}?version={ver}&clientId={cid}")
        print("del proc", p["type"], st)


def build(flow, pg, cid, spec):
    ids = {}
    for p in spec["processors"]:
        body = {
            "revision": {"version": 0, "clientId": cid},
            "componentConfiguration": {
                "componentType": "PROCESSOR", "type": p["type"], "bundle": BUNDLE,
                "name": p["type"], "position": {"x": float(p["pos"][0]), "y": float(p["pos"][1])},
                "properties": p["props"], "autoTerminatedRelationships": p["auto"],
            },
            "requestId": str(uuid.uuid4()),
        }
        st, r = call("POST", f"/designer/flows/{flow}/process-groups/{pg}/processors", body)
        if st != 201:
            raise SystemExit(f"create {p['type']}: {st} {r}")
        ids[p["key"]] = r["componentConfiguration"]["identifier"]
        print("created", p["type"], ids[p["key"]])
    for a, b in spec["connections"]:
        body = {
            "revision": {"version": 0, "clientId": cid},
            "componentConfiguration": {
                "componentType": "CONNECTION",
                "source": {"id": ids[a], "type": "PROCESSOR", "groupId": pg},
                "destination": {"id": ids[b], "type": "PROCESSOR", "groupId": pg},
                "selectedRelationships": ["success"], "bends": [],
            },
            "requestId": str(uuid.uuid4()),
        }
        st, r = call("POST", f"/designer/flows/{flow}/process-groups/{pg}/connections", body)
        print("connect", a, "->", b, st if st == 201 else r)
    st, v = call("GET", f"/designer/flows/{flow}/validate")
    print("validate", st, v)


def publish(flow, comment):
    st, r = call("POST", f"/designer/flows/{flow}/publish", {"comments": comment})
    print("publish", st, json.dumps(r)[:300] if r else r)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "show"
    flow, pg = flow_ids()
    if cmd == "show":
        show(flow)
    elif cmd == "clear":
        clear(flow, client_id())
    elif cmd == "build":
        build(flow, pg, client_id(), SPECS[sys.argv[2]])
    elif cmd == "publish":
        publish(flow, sys.argv[2] if len(sys.argv) > 2 else "#191")
    else:
        raise SystemExit(__doc__)
