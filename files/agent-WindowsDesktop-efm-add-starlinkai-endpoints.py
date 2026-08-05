#!/usr/bin/env python3
"""
Adds 4 new ListenHTTP -> [EvaluateJsonPath ->] InvokeHTTP pairs to the live
StarlinkAI EFM flow, exposing Lemonade's embeddings/reranking/TTS/transcription
endpoints the same way chat/completions is already exposed. Run this FROM
MINI-Gaming-G1 (WindowsDesktop), where the EFM server is local -- see
beelink-starlink-efm-ai.md "Handoff to WindowsDesktop" / "Real run, 2026-07-22"
sections for the full story.

Usage:
    python3 agent-WindowsDesktop-efm-add-starlinkai-endpoints.py [--dry-run] [--efm-host HOST]

Defaults to --efm-host 127.0.0.1 (EFM is local on this box).

HISTORY -- v1 of this script guessed a whole-flow-document PUT
(PUT /efm/api/designer/flows/{flowId}) for the write step. That endpoint
does not exist on this EFM build at all -- confirmed via EFM's own pod log
on the first real run (2026-07-22): `HttpRequestMethodNotSupportedException:
Request method 'PUT' is not supported`, a routing-layer 500 before any
business logic even runs. Nothing was written by that failed attempt.

v2 (this version) uses the ACTUAL confirmed EFM Flow Designer API contract,
reverse-engineered from EFM's own Angular bundle back on 2026-07-18/19 for
the Twitch chat-bot's KubernetesPod/NvidiaNano flow edits (see the
`reference-efm-flow-designer-api` memory / `how-to-nifi-and-ai.md` sec5h):
per-component POST create (processors, then connections), GET validate,
POST publish. Confirmed working end-to-end against this exact flow on
2026-07-22 -- flow published to version 12, 16 processors, 19 connections,
zero validation errors.

WHAT THIS STILL DOES NOT KNOW FOR CERTAIN (verify before trusting the result):
- Whether StarlinkAI's actual running MiNiFi agent on the Beelink has picked
  up the published flow yet. A successful publish means the *server* has the
  new version; the agent applies it on its next heartbeat. This script does
  not check live agent heartbeat/version state (EFM 2.3.1 has no clean REST
  endpoint for that -- checked and confirmed absent on 2026-07-22, see the
  doc). Check the EFM UI's Agents view, or just try hitting the new ports.
- Whether the Beelink's Windows Firewall permits 8081-8084 on the Tailscale
  interface -- never checked as of 2026-07-22.
- Whether InvokeHTTP's "Content-type" property forwards correctly for the
  transcription pair. That processor's incoming request is multipart/
  form-data with a boundary in the Content-Type header; hardcoding
  "application/json" (like the existing chat pair does) would corrupt it.
  This script sets it to "${mime.type}" instead, betting that MiNiFi's
  ListenHTTP sets that attribute from the incoming Content-Type header the
  same way NiFi's does -- UNCONFIRMED. Test with a real audio file POST
  before trusting this pair; if it's wrong, the fix is that one property.
- Whether Lemonade's non-chat endpoints respond correctly THROUGH this
  MiNiFi routing layer -- confirmed reachable directly (curl straight to
  Lemonade on 2026-07-21) but never yet through these new processor pairs.

Safe to re-run: checks for existing processor names before adding, so a
partial or repeat run won't duplicate processors.
"""
import argparse
import json
import urllib.request
import urllib.error
import uuid

AGENT_CLASS = "StarlinkAI"
LEMONADE_BASE = "http://localhost:13305/api/v1"

# (name_suffix, port, base_path, remote_path, needs_json_path_extraction, listen_http_header_capture)
NEW_ENDPOINTS = [
    ("Embeddings", 8081, "embeddings", "/embeddings", True, None),
    ("Reranking", 8082, "reranking", "/reranking", True, None),
    ("Speech", 8083, "speech", "/audio/speech", True, None),
    # Transcription: multipart body, no top-level JSON to path into -- caller
    # sends `request_id` as an HTTP header instead, captured directly as a
    # FlowFile attribute (no EvaluateJsonPath needed for this one).
    ("Transcription", 8084, "transcriptions", "/audio/transcriptions", False, "request_id"),
]


def http_json(method, url, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw.decode("utf-8", errors="replace")


def find_flow_id(base):
    status, flows = http_json("GET", f"{base}/efm/api/designer/flows?agentClass={AGENT_CLASS}")
    if status != 200:
        raise SystemExit(f"Could not list flows for {AGENT_CLASS}: HTTP {status} {flows}")
    for f in flows["elements"]:
        if f["agentClass"] == AGENT_CLASS:
            return f["identifier"]
    raise SystemExit(f"No flow found for agent class {AGENT_CLASS}")


def client_id(base):
    status, body = http_json("GET", f"{base}/efm/api/designer/client-identifier")
    if status != 200:
        raise SystemExit(f"Could not get client identifier: HTTP {status} {body}")
    return body["clientId"]


def create_processor(base, flow_id, group_id, cid, name, ptype, bundle, properties, position, auto_term=None):
    body = {
        "revision": {"version": 0, "clientId": cid},
        "componentConfiguration": {
            "componentType": "PROCESSOR",
            "type": ptype,
            "bundle": bundle,
            "name": name,
            "position": position,
            "properties": properties,
            "autoTerminatedRelationships": auto_term or [],
        },
        "requestId": str(uuid.uuid4()),
    }
    status, resp = http_json(
        "POST", f"{base}/efm/api/designer/flows/{flow_id}/process-groups/{group_id}/processors", body
    )
    if status != 201:
        raise SystemExit(f"Create processor {name} failed: HTTP {status} {resp}")
    real_id = resp["componentConfiguration"]["identifier"]
    print(f"[ok] created {name} -> {real_id}")
    return real_id


def create_connection(base, flow_id, group_id, cid, source_id, dest_id, relationships):
    body = {
        "revision": {"version": 0, "clientId": cid},
        "componentConfiguration": {
            "componentType": "CONNECTION",
            "name": "",
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": group_id},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": group_id},
            "selectedRelationships": relationships,
            "bends": [],
        },
        "requestId": str(uuid.uuid4()),
    }
    status, resp = http_json(
        "POST", f"{base}/efm/api/designer/flows/{flow_id}/process-groups/{group_id}/connections", body
    )
    if status != 201:
        raise SystemExit(f"Create connection {source_id}->{dest_id} failed: HTTP {status} {resp}")
    return resp["componentConfiguration"]["identifier"]


def listen_http_spec(name, port, base_path, header_capture):
    return (
        f"ListenHTTP-{name}",
        "org.apache.nifi.minifi.processors.ListenHTTP",
        {"group": "org.apache.nifi.minifi", "artifact": "minifi-civet-extensions", "version": "1.26.02"},
        {
            "Base Path": base_path,
            "SSL Verify Peer": "no",
            "Batch Size": "1",
            "SSL Minimum Version": "TLS1.2",
            "Buffer Size": "1",
            "Listening Port": str(port),
            "HTTP Headers to receive as Attributes (Regex)": header_capture,
            "Authorized DN Pattern": ".*",
        },
        None,  # ListenHTTP's only relationship (success) is always wired, never auto-terminated
    )


def invoke_http_spec(name, remote_url, content_type):
    return (
        f"InvokeHTTP-{name}",
        "org.apache.nifi.minifi.processors.InvokeHTTP",
        {"group": "org.apache.nifi.minifi", "artifact": "minifi-standard-processors", "version": "1.26.02"},
        {
            "Attributes to Send": "request_id",
            "Invalid HTTP Header Field Handling Strategy": "transform",
            "Read Timeout": "10 min",
            "Send Message Body": "true",
            "Connection Timeout": "5 min",
            "send-message-body": "true",
            "Content-type": content_type,
            "Always Output Response": "false",
            "HTTP Method": "POST",
            "Include Date Header": "true",
            "Use Chunked Encoding": "false",
            "Disable Peer Verification": "false",
            "Penalize on \"No Retry\"": "false",
            "Follow Redirects": "true",
            "Remote URL": remote_url,
        },
        # Deliberately auto-terminate failure/retry/no-retry here rather than
        # wiring into the existing debug funnel -- that funnel is already
        # flagged for removal (see Next Steps item 3 in the doc), no sense
        # growing it by 4x right before it's torn out.
        ["failure", "retry", "no retry"],
    )


def evaluate_json_path_spec(name):
    return (
        f"EvaluateJsonPath-{name}",
        "org.apache.nifi.minifi.processors.EvaluateJsonPath",
        {"group": "org.apache.nifi.minifi", "artifact": "minifi-standard-processors", "version": "1.26.02"},
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Null Value Representation": "empty string",
            "Path Not Found Behavior": "ignore",
            "request_id": "$.request_id",
        },
        # Must match the original chat pipeline's EvaluateJsonPath
        # (confirmed live 2026-07-22) or EFM's validate step rejects the
        # flow before publish: unhandled failure/unmatched relationships.
        ["failure", "unmatched"],
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--efm-host", default="127.0.0.1")
    ap.add_argument("--efm-port", default="10090")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = f"http://{args.efm_host}:{args.efm_port}"

    flow_id = find_flow_id(base)
    status, flow = http_json("GET", f"{base}/efm/api/designer/flows/{flow_id}")
    if status != 200:
        raise SystemExit(f"Could not fetch flow {flow_id}: HTTP {status}")

    fc = flow["flowContent"]
    group_id = fc["identifier"]
    existing_names = {p["name"] for p in fc["processors"]}

    # PublishKafka is the shared sink every new InvokeHTTP's success/response
    # routes into, same as the existing chat pair -- request_id already
    # disambiguates responses on the consumer side, no need for new topics.
    publish_kafka_id = next(p["identifier"] for p in fc["processors"] if p["type"].endswith("PublishKafka"))

    to_add = [e for e in NEW_ENDPOINTS if f"ListenHTTP-{e[0]}" not in existing_names]
    for name, *_ in [e for e in NEW_ENDPOINTS if e not in to_add]:
        print(f"[skip] ListenHTTP-{name} already present, not re-adding")

    if not to_add:
        print("Nothing to add -- all 4 pairs already present.")
        return

    if args.dry_run:
        print("\n--dry-run: not writing. Would add:")
        for name, port, base_path, remote_path, needs_json_path, _ in to_add:
            print(f" - {name}: ListenHTTP :{port}/{base_path} -> "
                  f"{'EvaluateJsonPath -> ' if needs_json_path else ''}InvokeHTTP -> {remote_path} -> PublishKafka")
        return

    cid = client_id(base)
    y_offset = -700  # existing processors cluster around y ~ -466 to -200; stack new ones below

    for idx, (name, port, base_path, remote_path, needs_json_path, header_capture) in enumerate(to_add):
        y = y_offset + idx * 250

        lh_name, lh_type, lh_bundle, lh_props, lh_auto = listen_http_spec(name, port, base_path, header_capture)
        listen_id = create_processor(base, flow_id, group_id, cid, lh_name, lh_type, lh_bundle, lh_props,
                                      {"x": -600, "y": y}, lh_auto)

        ih_name, ih_type, ih_bundle, ih_props, ih_auto = invoke_http_spec(
            name, f"{LEMONADE_BASE}{remote_path}", "${mime.type}" if name == "Transcription" else "application/json"
        )
        invoke_id = create_processor(base, flow_id, group_id, cid, ih_name, ih_type, ih_bundle, ih_props,
                                      {"x": 0, "y": y}, ih_auto)

        if needs_json_path:
            ej_name, ej_type, ej_bundle, ej_props, ej_auto = evaluate_json_path_spec(name)
            eval_id = create_processor(base, flow_id, group_id, cid, ej_name, ej_type, ej_bundle, ej_props,
                                        {"x": -300, "y": y}, ej_auto)
            create_connection(base, flow_id, group_id, cid, listen_id, eval_id, ["success"])
            create_connection(base, flow_id, group_id, cid, eval_id, invoke_id, ["matched"])
        else:
            create_connection(base, flow_id, group_id, cid, listen_id, invoke_id, ["success"])

        create_connection(base, flow_id, group_id, cid, invoke_id, publish_kafka_id, ["success", "response"])

        print(f"[done] {name} pair wired")

    status, val = http_json("GET", f"{base}/efm/api/designer/flows/{flow_id}/validate")
    errs = (val or {}).get("validationErrors", [])
    print(f"\nvalidate: HTTP {status}, {len(errs)} error(s)")
    if errs:
        for e in errs:
            print(" -", e.get("versionedComponent", {}).get("name"), ":", e.get("validationErrors"))
        print("Validation errors present -- NOT publishing. Fix the flow (EFM UI or a follow-up API call) "
              "before trying to publish; do not force a publish over unresolved validation errors.")
        return

    pub_status, pub_result = http_json("POST", f"{base}/efm/api/designer/flows/{flow_id}/publish",
                                        {"comments": "Add embeddings/reranking/speech/transcription endpoints"})
    print(f"POST publish: HTTP {pub_status} {pub_result}")
    if pub_status not in (200, 201, 204):
        print("Publish call failed -- flow content was created but not pushed live. Check the EFM UI.")
    else:
        print("Published. Server has the new version; the StarlinkAI agent applies it on its next heartbeat "
              "-- verify via the EFM UI's Agents view or by testing the new ports directly, don't assume "
              "the agent has it yet just because publish returned 200.")


if __name__ == "__main__":
    main()
