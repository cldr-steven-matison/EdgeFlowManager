# Chapter 20: SparkPlug B — MQTT/IIoT edge demo

This is the second real-world finale demo: an edge device publishing MQTT telemetry through Mosquitto into a NiFi process group and out to Kafka, with Sparkplug B's binary IIoT payload as the second, heavier-weight leg alongside plain JSON. It took three real course changes to get here — the sensor device changed, the NiFi process group got wiped and had to be restored, and a leftover debug rig turned out to be polluting the exact topic the real device publishes on. All three are part of the story, not cleaned out of it.

**Protocol and processor mechanics live in [Chapter 13 — EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md).** What Sparkplug B is, the Mosquitto broker manifests, MiNiFi C++'s stock relay-only MQTT processors versus NiFi's `ConsumeMQTTIIoT` decoder, the two-leg process-group pattern, and both test-publisher scripts are covered there in depth — this chapter assumes that background and doesn't re-derive it. This chapter is the field story of one real device shipping real telemetry through that pipeline, plus the two incidents that came with it.

## Prerequisites

- The CSO stack (NiFi, Kafka/Strimzi) running in minikube under `cld-streaming`/`cfm-streaming` on `MINI-Gaming-G1` (WindowsDesktop) — the host where the live NiFi flow in this chapter actually runs.
- Mosquitto deployed and reachable from both the edge device and NiFi — manifests in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md). **This doc assumed Mosquitto was already live from an earlier pass — it wasn't.** When the edge-device plan for this chapter was first written, the live cluster had no `mqtt` namespace at all; the only Mosquitto in the fleet was on a different host entirely. It was actually deployed here for real on 2026-07-31.

## NiFi Ingestion — The `SparkPlug` Process Group

A NiFi process group named `SparkPlug` (exported at [`files/SparkPlug.json`](files/SparkPlug.json)) holds the two-leg pattern Chapter 13 documents in full — `ConsumeMQTT` on the plain-JSON topic, `ConsumeMQTTIIoT` on `spBv1.0/#`. Both terminated at a dead-end `EOL` output port for a long time — the PG was built far enough to prove MQTT ingestion worked, but never wired to Kafka. **As of 2026-08-14 the live instance is wired and delivering (#164):** both legs publish to Kafka with real MicroFi device traffic confirmed end-to-end, and the committed export matches the live flow.

### Incident — The PG Had Been Silently Deleted, Not Just Stale

Coming back to close that gap, the `SparkPlug` PG wasn't in the live flow *at all*. Dumping `mynifi-0`'s raw `flow.json.gz` and walking every process group recursively found no trace of it — the only copy left was the 2026-06-16 committed export. The pod's age lined up with a known 2026-07-10 pod-recreate incident (`data`/`flowfile`/`content` repos are `emptyDir`, not PVC-backed at the time), which is the likely cause: the PG was lost then and nobody had rebuilt it since. Live state is authoritative over docs and memory for exactly this reason — the checked-in export was right, but only because it happened to predate the loss.

Fix: re-import the export directly.

```bash
# from the committed export, files/SparkPlug.json
curl -k -u "$NIFI_USER:$NIFI_PASS" \
  -F "file=@files/SparkPlug.json" \
  "https://<nifi-host>/nifi-api/process-groups/<root-pg-id>/process-groups/upload"
```

### Wiring Both Legs

`ConsumeMQTT`'s output was originally scoped to be the only leg wired — an earlier plan for this chapter said "leave `ConsumeMQTTIIoT`/Sparkplug B alone" and ship only the simpler plain-JSON path. That was wrong for this specific demo: this chapter *is* the SparkPlug demo, so the real Sparkplug B path needs a real downstream too, not just the JSON shortcut.

Both legs got their own `PublishKafka`:

- `ConsumeMQTT` → **`ExtractDeviceId`** (`EvaluateJsonPath`, `device_id` from `$.device_id`) → **`PublishKafka-XiaoTelemetry`** — topic `xiao_telemetry`, key `${device_id}`. Replaces the `EOL` dead-end for the `Message` relationship. The MicroFi telemetry publisher's payload is JSON carrying its own agent-class name (`{"device_id":"MicroFi-1"}`), so the Kafka key is the device's class identity — verified live: consumed records key on `MicroFi-1`.
- `ConsumeMQTTIIoT` → **`PublishKafka-SparkplugTelemetry`** — topic `sparkplug_telemetry`, key `${device_id}`. The binary Sparkplug B path is field-validated end to end on the live instance (2026-08-14): a real XIAO ESP32-S3 (`MicroFi-3`, running the unified MicroFi firmware's `PublishSparkplug` processor) producing genuine Sparkplug B `NBIRTH`/`NDATA` on `spBv1.0/MicroFi/…/MicroFi-3`, consumed and delivered to `sparkplug_telemetry` — the Sparkplug leg's device identity travels in its topic segments rather than a `device_id` attribute, so records on this leg currently carry a null key. The full protocol story is in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md).
- `parse.failure` on both legs still routes to `EOL`, unchanged.
- Kafka connection settings (`my-cluster-kafka-bootstrap.cld-streaming.svc:9092`, `PLAINTEXT`, no SASL) were copied from other live processors in the same cluster, not guessed.

> **⚠️ Never GET-then-PUT a processor with sensitive properties.** `ConsumeMQTT`/`ConsumeMQTTIIoT`'s `Password` field reads back `null` on this pair (not the usual masked `********`), but the rule is the same regardless: check `descriptors[...].sensitive` before any full-entity PUT, and re-verify on every live pull — never assume a checked-in export still matches what's live.

PG validated clean and started. Exported and committed: [`755a4d9`](https://github.com/cldr-steven-matison/DesktopShare/commit/755a4d9).

## The Edge Publisher — Three Generations

### Session 1 — Simulated Publishers (Field-Run, Mac)

The first confirmed end-to-end run used two plain Python scripts against a port-forwarded Mosquitto, proving both consumer legs before any real device existed. Terminal history and full scripts are in the Appendix below; the shape that matters:

Plain JSON, matching `ConsumeMQTT`'s filter exactly:

```json
{"device_id": "MacMockSensor-01", "temperature": 22.43, "humidity": 53.29, "timestamp": 1781614422}
```

Real Sparkplug B binary (`NBIRTH` + `NDATA`, via `pysparkplug`), matching `ConsumeMQTTIIoT`'s filter:

```
Sent Sparkplug NDATA (Seq: 1) -> Temp: 28.87 | Humid: 49.59
```

### The BME280-on-Jetson Path — Parked, Not Shipped

The original hardware plan called for a real BME280 environment sensor wired to the Jetson Orin Nano (`NvidiaNano`) over I2C. A full bus scan (`i2cdetect -y` across every adapter on the board) found nothing physically wired at any address — no sensor was ever attached. On top of that, the two competing drafts for this step disagreed on library: a Waveshare HAT recipe using `adafruit-circuitpython-bme280`/`board`/`busio` (Blinka), versus a leftover `~/bme280_test.py` on the box already using the different `RPi.bme280` package — and `board` wasn't even importable without installing Blinka first. Two open questions bundled into "wire a BME280," neither resolved. Parked rather than chased further; nothing here blocks the rest of the chapter.

### Real Hardware — Seeed XIAO ESP32-S3

The device that actually shipped real telemetry is a Seeed XIAO ESP32-S3 plugged into a third array host (`StarlinkAI`) over USB — not the Jetson, and not a wait on a not-yet-arrived sensor device. Confirmed via `esptool --port /dev/ttyACM0 chip-id`: ESP32-S3 (QFN56 rev v0.2), 8MB embedded PSRAM, MAC `e0:72:a1:fb:fd:04`, FQBN `esp32:esp32:XIAO_ESP32S3`.

No `sudo`/apt available in the WSL2 session that did this — `esptool` and `arduino-cli` went in as user-local binaries in `~/.local/bin` from GitHub release tarballs instead of the apt path a first draft of this plan assumed.

```bash
arduino-cli core install esp32:esp32 --additional-urls https://raw.githubusercontent.com/espressif/arduino-esp32/gh-pages/package_esp32_index.json
arduino-cli lib install PubSubClient ArduinoJson
```

Firmware (`xiao-telemetry.ino`, device-local, not committed — same pattern as its gitignored `secrets.h`) publishes every ~5s to `test/sensor/data`, matching the existing shape exactly:

```json
{"device_id": "XiaoESP32-01", "temperature": 47.8, "humidity": null, "timestamp": 1785853894}
```

Two fixes were needed against the plan as originally written:

- **`temprature_sens_read()` doesn't link on S3.** That ROM function is classic-ESP32-only. Switched to the Arduino core's cross-variant `temperatureRead()`.
- **Real NTP sync, not a placeholder epoch.** The original plan used `millis()/1000` as a stand-in `timestamp`, which would have published a fake epoch. Added `configTime()` before the first publish so the field is real.

Broker address is the literal LAN dotted-quad, `192.168.1.121:1883` — the XIAO has no Tailscale client and joins the WindowsDesktop/EFM WiFi AP directly, confirmed via a real `MQTT: connected` CONNACK on first boot.

```bash
arduino-cli compile --fqbn esp32:esp32:XIAO_ESP32S3 xiao-telemetry
arduino-cli upload -p /dev/ttyACM0 --fqbn esp32:esp32:XIAO_ESP32S3 xiao-telemetry
arduino-cli monitor -p /dev/ttyACM0 -c baudrate=115200
```

### Independent Verification — Don't Trust the Firmware's Own Serial Log

The serial monitor showing "connected" and "published" isn't proof anything reached the broker. A real topology snag showed up doing this the right way: the host holding the XIAO isn't actually on the same LAN as WindowsDesktop/EFM despite both landing in `192.168.1.0/24` — that's a coincidental private-IP overlap across two different physical networks, not a routing bug, and `Test-NetConnection`/ARP to the broker fail from that host even though the XIAO itself (same WiFi AP as WindowsDesktop) connects fine. Verification had to go over Tailscale instead — the one path the two hosts actually share:

```python
# paho-mqtt subscribe, run from the StarlinkAI host against WindowsDesktop's Tailscale IP
import paho.mqtt.client as mqtt
client = mqtt.Client()
client.connect("100.68.113.126", 1883, 60)
client.subscribe("test/sensor/data")
client.loop_forever()
```

5 real messages received, matching the firmware's serial log exactly.

## End-to-End Test

With the PG restored, both legs wired, and the XIAO flashed:

```bash
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic xiao_telemetry --from-beginning
```

## Incident — The Exact Topic the Real Device Uses Had a Second, Unrelated Publisher

Checking `xiao_telemetry` directly after the wiring landed: real messages were arriving (Kafka offset climbing steadily), but every message sampled — 91 out of 91 — was a JSON blob from an unrelated `GenerateFlowFile` payload, not XIAO telemetry.

Root cause: a separate EFM agent class, `MicroFi`, had a leftover `GenerateFlowFile → PublishMQTT` pair (`Client ID: xiao-microfi-1`) actively publishing to Mosquitto on `test/sensor/data` at roughly 1/sec — the *exact same topic* the real XIAO publishes to and `ConsumeMQTT` filters on. It looked like a debug-repro rig from an earlier issue that never got stopped. `ConsumeMQTT` has no way to distinguish the two — everything on that topic flows straight into `xiao_telemetry`.

Fix, via the EFM Designer API (`MicroFi` is a MiNiFi C++ agent class, not a NiFi process group, so this isn't a NiFi-side change):

```bash
# delete the connection, then both processors
curl -X DELETE ".../efm/api/flows/<flow-id>/connections/<connection-id>"
curl -X DELETE ".../efm/api/flows/<flow-id>/processors/<generateflowfile-id>"
curl -X DELETE ".../efm/api/flows/<flow-id>/processors/<publishmqtt-id>"
```

Flow validated clean (`validationErrors: []`) and published — `MicroFi` flow version bumped to 16.

Verified two ways:

- `xiao_telemetry`, 20s live consume at the tip after the fix: 0 messages, versus a steady ~1/sec before.
- Raw MQTT `test/sensor/data` via `mosquitto_sub`, 20s: also silent.

That silence cuts both ways — at the time this chapter was first folded, it also meant the real
XIAO device wasn't powered on and publishing. That gap has since closed: see the 2026-09-01
re-validation below.

### Re-Validation After the Prod Cutover — 2026-09-01

The 2026-08-26 cluster cutover (the prod stack moving to a new minikube profile) left this demo's
substrate partially gone: the `mqtt` namespace was **empty** — no Mosquitto pod, no service — and
every processor in the `SparkPlug` PG (and its sibling bridge PGs) was stopped. A committed
manifest set (`files/mosquitto.yaml` + `files/mosquitto-configmap.yaml`) made the broker a
one-command redeploy into the new cluster; the PGs restarted clean (17 processors across four PGs,
zero bulletins), and the re-powered devices reconnected on their own — the XIAO firmware targets
the stable LAN address `192.168.1.121:1883`, which the host-side port-forward re-bound as soon as
the service existed again.

Fresh end-to-end proof, all three units at once (evidence:
[`files/issue-138/`](https://github.com/cldr-steven-matison/DesktopShare/tree/main/files/issue-138)):
`{"device_id":"MicroFi-1"}` records landing in `xiao_telemetry` keyed `MicroFi-1`; MicroFi-3's
`PublishSparkplug` NDATA (seq advancing) landing in `sparkplug_telemetry` with `parse.failure = 0`;
MicroFi-2's camera JPEGs landing in `microfi2.camera.jpg`. The live `ConsumeMQTTIIoT` configuration
diffed clean against the committed `files/SparkPlug.json` — the export held.

> **⚠️ A shared MQTT topic has no per-publisher isolation.** Anything else that publishes to the same topic a `ConsumeMQTT` filters on ends up indistinguishable downstream unless the payload shape itself carries something to key on (`device_id`, in this case — the noise publisher had none, which is what made it possible to tell the two apart at all). Don't assume a topic is single-producer just because one flow was built assuming that.

## Edge Intelligence (Stretch) — Designed, Not Run

The original plan for this chapter included a further phase: a MiNiFi flow running directly on the edge device, consuming its own Sparkplug B locally, running a small TensorRT/ONNX anomaly-detection model, and triggering a GPIO buzzer on an extreme reading — real edge AI, not just relay. The specific BME280-on-Jetson design for that phase remains blocked: no sensor was ever physically wired to the board, and the two competing library recipes (`adafruit-circuitpython-bme280`/Blinka versus `RPi.bme280`, which are incompatible) were never reconciled. That leg is still parked.

What was built and field-verified instead is the conceptual equivalent for the XIAO platform: `NvidiaNanoSparkPlug` consuming MQTT data, `ExecuteScript` running a threshold condition on each reading, and on a match, `InvokeHTTP` posting back into the XIAO's `ListenHTTP` endpoint to trigger an actuation — a real sensor-to-edge-decision-to-actuation round-trip, confirmed live via `ESTABLISHED` connection on the Jetson. That architecture (`ConsumeMQTT → ExecuteScript → InvokeHTTP actuation`, forwarding unconditionally to central Kafka) is documented in the live-assembly section below; the GPIO buzzer and TensorRT/ONNX layers remain future work once a real sensor value is on the wire to threshold against.

## Live Assembly Toward the Full Architecture (#109) — How It Landed

#106 asked for the real end-to-end chain: **XIAO publishes to Mosquitto → NvidiaNano runs inference on the reading → NvidiaNano Site-to-Site to NiFi K8s.** Two of the three legs were built and confirmed live; the Site-to-Site leg was **descoped by direction change rather than finished** — the 2026-08-05 pivot (detailed below) replaced it with the actuation round-trip, and that descope stands as the final shape for this guide: the edge concepts the S2S leg existed to prove (edge decision → central delivery) are proven by the Kafka legs and the round-trip instead. The step-by-step S2S enablement recipe below is kept as the documented path for whenever a production reason to turn it on actually arrives.

**Context that changed the plan.** The same day this work started, the live `NvidiaNano` EFM class was cut over (#28) from the Ch19 TensorRT/`PublishKafka` pipeline to an unrelated Java relay (`classify`/`streamChat`/`matrix` — screen and matrix-screensaver control, ports 8080/8081/8082). That flow is live production and was left untouched. The XIAO-sensor-inference leg needed a **new, separate EFM agent class** instead of reusing `NvidiaNano`.

**MicroFi (XIAO) — rebuilt, transport confirmed live.** The `MicroFi` class's live flow had drifted to a `GetGPIO`/`ListenHTTP` test rig (leftover from the Ch12 engine-bug work) — no publisher at all. Replaced with the same `GenerateFlowFile → PublishMQTT` shape Ch12 already proved (`Broker URI: mqtt://192.168.1.121:1883`, `Topic: test/sensor/data`, flowVersion 17). An initial 20s broker-log watch showed no connects — the same transport-layer failure shape Ch12 hit once before. That concern was resolved in the next build pass: the transport issue cleared after a real power-cycle, and the ESTABLISHED connection described below (real `ESTABLISHED` to `192.168.1.198:8095` confirmed live via `ss -tn`) is direct evidence the device's networking to the broker subnet is sound.

**NvidiaNanoSparkPlug — new class, built, confirmed live.** A second C++ MiNiFi agent was enrolled under a brand-new class, `NvidiaNanoSparkPlug`, on the same physical Jetson (`tunastreet@192.168.1.197`) — the original `NvidiaNano`-class systemd service (`minifi.service`) is left exactly as the #28 cutover left it (inactive, superseded by the Java process), untouched. The new agent runs from `~/nifi-minifi-cpp-sparkplug` as a plain background process (`bin/minifi.sh run`, not systemd — EFM's agent-deployer hard-codes the systemd unit name `minifi`, so a second systemd-managed agent isn't possible on one host via that path). Flow: `ConsumeMQTT` (`tcp://192.168.1.121:1883`, `Topic: test/sensor/data`) → `ExecuteScript` (reuses [`gpu_nifi_tensorRT-3.py`](files/gpu_nifi_tensorRT-3.py), already staged in the asset directory from the pre-cutover `NvidiaNano` install, `chmod +x`'d). Published as flowVersion 1, and the agent's own log confirms it for real: `Successfully connected to MQTT broker tcp://192.168.1.121:1883` / `Successfully subscribed to MQTT topic test/sensor/data`. Exported: [`files/efm/NvidiaNanoSparkPlug.json`](files/efm/NvidiaNanoSparkPlug.json).

`ExecuteScript`'s `success` relationship is **temporarily auto-terminated** rather than wired to a `RemoteProcessGroup` — there is nowhere real to send it yet (see next).

**NiFi K8s Site-to-Site — blocked, needs a human decision.** The production NiFi (`mynifi-0`, `cfm-streaming`) has **no Site-to-Site configuration at all** — not disabled, never set up. Ch10/11's proven S2S recipe ran on a separate `s2s-lab` profile, not this instance. Turning this leg on for real means, on `mynifi-0` specifically:

1. Add to the `Nifi` CR's `spec.configOverride.nifiProperties.upsert` (triggers an operator-managed pod restart of `mynifi-0` — confirmed idle first, 0 active threads cluster-wide, before this was even attempted):
   ```yaml
   nifi.remote.input.host: mynifi-web.mynifi.cfm-streaming.svc.cluster.local
   nifi.remote.input.secure: "true"
   nifi.remote.input.http.enabled: "true"
   ```
2. Create an Input Port (e.g. `from-nvidianano`) inside the `SparkPlug` PG with a downstream connection (an input port with no outgoing connection won't start) — a `PublishKafka-NvidiaNanoInference` processor is the natural target, matching the existing two-leg pattern.
3. Declare a `User` CR for the peer identity (SAN-matched, not DN — see Ch10's "What NOT to do"), granting `write` on `/data-transfer/input-ports/<from-nvidianano-uuid>` and `read` on `/site-to-site`.
4. Issue a client cert for the Jetson's new agent (SAN matching the `User.spec.identity`), mount it, and set `nifi.remote.input.secure=true` + `nifi.security.client.*` in the new agent's `minifi.properties` (Ch10's exact recipe — MiNiFi C++ has no SSL-context-service field on the RPG, client identity is global).
5. Build the `RemoteProcessGroup` in the Designer, wire `ExecuteScript`'s `success` to it, validate, publish.

Step 1 is what's actually blocking this — a production NiFi config change with an implicit pod restart. It needs a person to run it (or approve it), not an agent proceeding unattended on a shared production service. Steps 2-5 follow directly from Ch10's already-proven recipe once step 1 lands.

**Direction changed mid-build (2026-08-05): Site-to-Site isn't needed to prove the edge concepts out.** Steven's call, live in the issue thread — the architecture pivoted to a real round-trip instead: XIAO ships sensor data to Mosquitto (as already built), and separately runs `ListenHTTP` as an actuation-trigger endpoint (take a photo, record on mic, write to SD card — the exact action still open, scoped by how much fits on the device). `NvidiaNanoSparkPlug` consumes the MQTT data, and on a condition, calls back into the XIAO's `ListenHTTP` to fire that action. Steven has 3 physical XIAO units on hand, so the eventual shape is one flow type per device, not everything crammed onto one.

**A v1 of that round-trip is built and field-verified live**, on the same single connected XIAO used throughout this chapter:

- **MicroFi (XIAO) — back to 3 nodes**, after a false start at 4. `GenerateFlowFile → PublishMQTT` (unchanged) plus a re-added `ListenHTTP-Trigger` (port `8095`, base path `/test`, `success` auto-terminated — no downstream `LogAttribute` this time, see the node-count note below). Published flowVersion 19.
- **NvidiaNanoSparkPlug — the full round trip.** `ExecuteScript` now fans out two ways: `PublishKafka-NvidiaNanoInference` (unconditional, every reading) and a new `RouteOnAttribute-Trigger → InvokeHTTP-TriggerXiao` leg that only fires when `ExecuteScript` sets `trigger.actuation=true` (a placeholder condition — deterministic on even UTC seconds, since there's no real sensor value on the wire yet to threshold on; swap for a real signal once one exists). `InvokeHTTP-TriggerXiao` POSTs to `http://192.168.1.198:8095/test`. Published flowVersion 4. Verified live via `ss -tn` on the Jetson: a real `ESTABLISHED` connection to `192.168.1.198:8095`, confirming the trigger genuinely fires and reaches the XIAO — not just validated-and-published.

**How the device roles settled — one flow type per unit.** The single `MicroFi` class this
section's history describes was later split into per-device classes, exactly along Steven's
"one flow type per device" line: **`MicroFi-1`** carries the plain-JSON telemetry emit
(`GenerateFlowFile → PublishMQTT`, the `ConsumeMQTT` leg's producer), **`MicroFi-2`** the camera
(`CaptureImage` broker-direct, bridged by the `MicroFi2CameraBridge` NiFi PG), and **`MicroFi-3`**
the real Sparkplug B emit via the unified firmware's native `PublishSparkplug` — the producer
behind Chapter 18's Entry 10. Anywhere this chapter or that card says "MicroFi-3" or "MicroFi",
they are the same physical XIAO family; the class names encode the per-unit flow role.

**Actuation re-fielded 2026-09-01 — a visible LED, driven from central NiFi.** With the flows kept
separate per that split, the actuation leg was re-proven on **MicroFi-1**: its class flow swapped
(via the EFM Designer API, original flow backed up) to `ListenHTTP(/led :8095) → SetGPIO(pin 21,
level from-content)`, and a new central-NiFi process group **`MicroFiLedActuation`**
(`GenerateFlowFile → InvokeHTTP POST http://192.168.1.198:8095/led`, failure/retry legs to a log
processor per this guide's Retry-is-not-Failure rule) drove the board's user LED off and on from
the canvas — two FlowFiles through, zero failures. That is the round-trip in its simplest
teachable form: a flow decision anywhere in the array becomes a physical state change on the
glass-less-est device in the fleet. Evidence and the Designer-API swap script:
[`files/issue-138/`](https://github.com/cldr-steven-matison/DesktopShare/tree/main/files/issue-138).

**Three real bugs found and fixed getting here, worth recording:**

1. **`gpu_nifi_tensorRT-3.py` assumed JSON input; the XIAO's payload is plain text.** `json.loads()` was throwing on every real message and routing to `failure` — silently, since `failure` was auto-terminated. Fixed by wrapping non-JSON content as `{"raw": ...}` instead of assuming a shape that was never there.
2. **`PublishKafka-NvidiaNanoInference` pointed at the in-cluster Kafka DNS name** (`my-cluster-kafka-bootstrap.cld-streaming.svc:9092`), unreachable from a physical device outside the cluster — the exact external-listener trap `efm-nvidia-jetson-nano.md` already documented for this exact Jetson. Fixed to the LAN NodePort address (`192.168.1.121:31623`); a full agent restart (not just a property push) was needed before the corrected broker address actually took effect.
3. **A stale `httpd_start failed (port=8095)` on MicroFi after a live flow hot-swap.** The 4-node version of the MicroFi flow (`GenerateFlowFile`, `PublishMQTT`, `ListenHTTP`, `LogAttribute`) silently dropped `ListenHTTP`/`LogAttribute` on apply — not the previously-documented `kMaxFlowNodes` bug this time, but the *previous* `ListenHTTP` instance's socket never released on a hot flow-swap, so the new one's `httpd_start` failed silently. A real power-cycle (not just a republish) cleared it; the 3-node version bound clean on first boot. **Don't trust a live flow-swap alone to release a MicroFi processor's held OS resources (sockets, GPIO) — verify with a fresh power-cycle if a processor that binds a system resource doesn't come up.**
4. **`RouteOnAttribute`'s dynamic-property Expression Language got mangled through the C2 push when it contained nested single quotes** (`${trigger.actuation:equals('true')}` arrived on-device as the literal expression `false`, not the real predicate — confirmed by reading the regenerated `config.yml` directly, not trusting the Designer's own echo of what it sent). Worked around by using a bare attribute reference (`${trigger.actuation}`, which evaluates to the attribute's own `"true"`/`"false"` string — NiFi/MiNiFi's route-matching contract) instead of a function call with quoted arguments. Whether this is a genuine EFM/C2 escaping bug or a config.yml-generation issue wasn't root-caused further; flagged as a real trap for the next EL expression with nested quotes pushed through this same path.

## What NOT to Do

**Assume a checked-in flow export matches what's live.** The `SparkPlug` PG existed only in a 2026-06-16 export by the time this chapter's NiFi work started — the live copy had been silently lost in an unrelated pod-recreate incident weeks earlier. Dump the live `flow.json.gz` before trusting any doc or export.

**GET-then-PUT a processor with sensitive properties, even when the field reads back `null`.** `ConsumeMQTT`/`ConsumeMQTTIIoT`'s `Password` isn't masked as `********` on this pair, but treat every sensitive field the same regardless of what it happens to read back as.

**Trust a topic is single-producer because one flow assumes it.** The exact topic `ConsumeMQTT` filters on had a second, unrelated publisher running for an unknown length of time before it was caught — only visible by sampling actual message content, not by watching the offset climb.

**Trust a firmware's own serial log as proof of delivery.** "WiFi connected" and "publish successful" on the device side don't confirm the broker received anything. Subscribe independently, from a different process, before calling a publish path verified.

**Chase the BME280 hardware path further without a decision.** Two unresolved questions (which physical module, which Python library) were bundled into one blocked task. Both got flagged and parked rather than guessed at.

**Trust the EFM Designer's own echo of a property as proof of what the device actually received.** A `RouteOnAttribute` Expression-Language value with nested single quotes (`equals('true')`) round-tripped correctly through every Designer API response but landed on the real device as a mangled literal (`false`). The regenerated `config.yml` on the agent itself was the only place that showed the real, corrupted value.

**Assume EFM's agent-deployer's `serviceName` parameter controls the systemd unit name.** It doesn't — the deployer script hard-codes `SERVICE_NAME="minifi"` regardless of what's POSTed. A second systemd-managed C++ agent on the same host isn't possible via the deployer; run it with `bin/minifi.sh run` (foreground/backgroundable, no systemd) instead, and don't reuse an existing `nifi-minifi-cpp-*` install directory for a new class without clearing its persisted `conf/config.yml` first — a copied install boots straight into its old flow, including binding the same ports a live agent may already hold.

## Appendix — Reusable Command Forms

**Both test-publisher scripts (plain-JSON and real Sparkplug B binary via `pysparkplug`) and the raw terminal history of the first field run are in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md#test-publishers)** — reproduced there in full rather than duplicated here, since Chapter 13 is now the canonical protocol/processor reference this chapter points to.

### Consume the Live Topics

```bash
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic xiao_telemetry --from-beginning

kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sparkplug_telemetry --from-beginning
```

## Related Chapters

- Ch12 — [EFM and MicroFi](ch12-efm-and-microfi.md): the ESP32 C2-agent/EFM-enrollment side of this same XIAO device family, including the `PublishMQTT` work this demo's MQTT egress leans on and the leftover debug rig behind this chapter's topic-contamination incident.
- Ch13 — [EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md): the protocol and processor mechanics behind this chapter — what Sparkplug B is, the Mosquitto deploy, the two-leg process-group pattern, both test-publisher scripts.
- Ch18 — [Sample gallery](ch18-sample-gallery.md): [`SparkPlug.json`](files/SparkPlug.json) belongs here alongside the other runnable flows.
- Ch19 — [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md): the `ExecuteScript`/TensorRT pattern this chapter's stretch phase reuses.
- Ch21 — [Metrics & Observability](ch21-metrics-and-observability.md): the Prometheus/Grafana layer that watches this same NiFi/Kafka stack.
