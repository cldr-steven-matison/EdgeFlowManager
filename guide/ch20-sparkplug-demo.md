# Chapter 20: SparkPlug B — MQTT/IIoT edge demo

This is the second real-world finale demo: an edge device publishing MQTT telemetry through Mosquitto into a NiFi process group and out to Kafka, with Sparkplug B's binary IIoT payload as the second, heavier-weight leg alongside plain JSON. It took three real course changes to get here — the sensor device changed, the NiFi process group got wiped and had to be restored, and a leftover debug rig turned out to be polluting the exact topic the real device publishes on. All three are part of the story, not cleaned out of it.

**Protocol and processor mechanics live in [Chapter 13 — EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md).** What Sparkplug B is, the Mosquitto broker manifests, MiNiFi C++'s stock relay-only MQTT processors versus NiFi's `ConsumeMQTTIIoT` decoder, the two-leg process-group pattern, and both test-publisher scripts are covered there in depth — this chapter assumes that background and doesn't re-derive it. This chapter is the field story of one real device shipping real telemetry through that pipeline, plus the two incidents that came with it.

## Prerequisites

- The CSO stack (NiFi, Kafka/Strimzi) running in minikube under `cld-streaming`/`cfm-streaming` on `MINI-Gaming-G1` (WindowsDesktop) — the host where the live NiFi flow in this chapter actually runs.
- Mosquitto deployed and reachable from both the edge device and NiFi — manifests in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md). **This doc assumed Mosquitto was already live from an earlier pass — it wasn't.** When the edge-device plan for this chapter was first written, the live cluster had no `mqtt` namespace at all; the only Mosquitto in the fleet was on a different host entirely. It was actually deployed here for real on 2026-07-31.

## NiFi ingestion — the `SparkPlug` process group

A NiFi process group named `SparkPlug` (exported at [`files/SparkPlug.json`](../files/SparkPlug.json)) holds the two-leg pattern Chapter 13 documents in full — `ConsumeMQTT` on the plain-JSON topic, `ConsumeMQTTIIoT` on `spBv1.0/#`. Both terminated at a dead-end `EOL` output port for a long time — the PG was built far enough to prove MQTT ingestion worked, but never wired to Kafka.

### Incident — the PG had been silently deleted, not just stale

Coming back to close that gap, the `SparkPlug` PG wasn't in the live flow *at all*. Dumping `mynifi-0`'s raw `flow.json.gz` and walking every process group recursively found no trace of it — the only copy left was the 2026-06-16 committed export. The pod's age lined up with a known 2026-07-10 pod-recreate incident (`data`/`flowfile`/`content` repos are `emptyDir`, not PVC-backed at the time), which is the likely cause: the PG was lost then and nobody had rebuilt it since. Live state is authoritative over docs and memory for exactly this reason — the checked-in export was right, but only because it happened to predate the loss.

Fix: re-import the export directly.

```bash
# from the committed export, files/SparkPlug.json
curl -k -u "$NIFI_USER:$NIFI_PASS" \
  -F "file=@files/SparkPlug.json" \
  "https://<nifi-host>/nifi-api/process-groups/<root-pg-id>/process-groups/upload"
```

### Wiring both legs

`ConsumeMQTT`'s output was originally scoped to be the only leg wired — an earlier plan for this chapter said "leave `ConsumeMQTTIIoT`/Sparkplug B alone" and ship only the simpler plain-JSON path. That was wrong for this specific demo: this chapter *is* the SparkPlug demo, so the real Sparkplug B path needs a real downstream too, not just the JSON shortcut.

Both legs got their own `PublishKafka`:

- `ConsumeMQTT` → **`PublishKafka-XiaoTelemetry`** — topic `xiao_telemetry`, key `${device_id}`. Replaces the `EOL` dead-end for the `Message` relationship.
- `ConsumeMQTTIIoT` → **`PublishKafka-SparkplugTelemetry`** — topic `sparkplug_telemetry`, key `${device_id}`. Wired ahead of a real Sparkplug B producer (none exists yet — this leg is proven-consuming but not yet field-run end to end with a live binary publisher).
- `parse.failure` on both legs still routes to `EOL`, unchanged.
- Kafka connection settings (`my-cluster-kafka-bootstrap.cld-streaming.svc:9092`, `PLAINTEXT`, no SASL) were copied from other live processors in the same cluster, not guessed.

> **⚠️ Never GET-then-PUT a processor with sensitive properties.** `ConsumeMQTT`/`ConsumeMQTTIIoT`'s `Password` field reads back `null` on this pair (not the usual masked `********`), but the rule is the same regardless: check `descriptors[...].sensitive` before any full-entity PUT, and re-verify on every live pull — never assume a checked-in export still matches what's live.

PG validated clean and started. Exported and committed: [`755a4d9`](https://github.com/cldr-steven-matison/DesktopShare/commit/755a4d9).

## The edge publisher — three generations

### Session 1 — simulated publishers (field-run, Mac)

The first confirmed end-to-end run used two plain Python scripts against a port-forwarded Mosquitto, proving both consumer legs before any real device existed. Terminal history and full scripts are in the Appendix below; the shape that matters:

Plain JSON, matching `ConsumeMQTT`'s filter exactly:

```json
{"device_id": "MacMockSensor-01", "temperature": 22.43, "humidity": 53.29, "timestamp": 1781614422}
```

Real Sparkplug B binary (`NBIRTH` + `NDATA`, via `pysparkplug`), matching `ConsumeMQTTIIoT`'s filter:

```
Sent Sparkplug NDATA (Seq: 1) -> Temp: 28.87 | Humid: 49.59
```

### The BME280-on-Jetson path — parked, not shipped

The original hardware plan called for a real BME280 environment sensor wired to the Jetson Orin Nano (`NvidiaNano`) over I2C. A full bus scan (`i2cdetect -y` across every adapter on the board) found nothing physically wired at any address — no sensor was ever attached. On top of that, the two competing drafts for this step disagreed on library: a Waveshare HAT recipe using `adafruit-circuitpython-bme280`/`board`/`busio` (Blinka), versus a leftover `~/bme280_test.py` on the box already using the different `RPi.bme280` package — and `board` wasn't even importable without installing Blinka first. Two open questions bundled into "wire a BME280," neither resolved. Parked rather than chased further; nothing here blocks the rest of the chapter.

### Real hardware — Seeed XIAO ESP32-S3

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

### Independent verification — don't trust the firmware's own serial log

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

## End-to-end test

With the PG restored, both legs wired, and the XIAO flashed:

```bash
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic xiao_telemetry --from-beginning
```

## Incident — the exact topic the real device uses had a second, unrelated publisher

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

That silence cuts both ways — as of this chapter, it also means the real XIAO device isn't currently powered on and publishing (confirmed the same way: a live `mosquitto_sub -t test/sensor/data` and a live consume of `xiao_telemetry` both come back empty). The wiring and the fix are real and field-verified against real XIAO messages captured earlier; the device itself just isn't live at the moment this chapter was folded. Re-flashing or re-powering the XIAO is the only remaining step to see fresh messages land.

> **⚠️ A shared MQTT topic has no per-publisher isolation.** Anything else that publishes to the same topic a `ConsumeMQTT` filters on ends up indistinguishable downstream unless the payload shape itself carries something to key on (`device_id`, in this case — the noise publisher had none, which is what made it possible to tell the two apart at all). Don't assume a topic is single-producer just because one flow was built assuming that.

## Edge intelligence (stretch) — designed, not run

The original plan for this chapter included a further phase: a MiNiFi flow running directly on the edge device, consuming its own Sparkplug B locally, running a small TensorRT/ONNX anomaly-detection model, and triggering a GPIO buzzer on an extreme reading — real edge AI, not just relay. That phase depends on the BME280-real-sensor leg above, which stayed blocked, so it was never field-run. Left here as future work rather than removed, since the architecture (`ConsumeMQTTIIoT → ExecuteScript (TensorRT/ONNX) → GPIO buzzer`, still forwarding upstream to central Kafka) is sound and matches the pattern already field-proven for the Jetson in [Chapter 19](ch19-efm-and-nvidia-jetson.md).

## What NOT to do

**Assume a checked-in flow export matches what's live.** The `SparkPlug` PG existed only in a 2026-06-16 export by the time this chapter's NiFi work started — the live copy had been silently lost in an unrelated pod-recreate incident weeks earlier. Dump the live `flow.json.gz` before trusting any doc or export.

**GET-then-PUT a processor with sensitive properties, even when the field reads back `null`.** `ConsumeMQTT`/`ConsumeMQTTIIoT`'s `Password` isn't masked as `********` on this pair, but treat every sensitive field the same regardless of what it happens to read back as.

**Trust a topic is single-producer because one flow assumes it.** The exact topic `ConsumeMQTT` filters on had a second, unrelated publisher running for an unknown length of time before it was caught — only visible by sampling actual message content, not by watching the offset climb.

**Trust a firmware's own serial log as proof of delivery.** "WiFi connected" and "publish successful" on the device side don't confirm the broker received anything. Subscribe independently, from a different process, before calling a publish path verified.

**Chase the BME280 hardware path further without a decision.** Two unresolved questions (which physical module, which Python library) were bundled into one blocked task. Both got flagged and parked rather than guessed at.

## Appendix — reusable command forms

**Both test-publisher scripts (plain-JSON and real Sparkplug B binary via `pysparkplug`) and the raw terminal history of the first field run are in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md#test-publishers)** — reproduced there in full rather than duplicated here, since Chapter 13 is now the canonical protocol/processor reference this chapter points to.

### Consume the live topics

```bash
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic xiao_telemetry --from-beginning

kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sparkplug_telemetry --from-beginning
```

## Related chapters

- Ch12 — [EFM and MicroFi](ch12-efm-and-microfi.md): the ESP32 C2-agent/EFM-enrollment side of this same XIAO device family, including the `PublishMQTT` work this demo's MQTT egress leans on and the leftover debug rig behind this chapter's topic-contamination incident.
- Ch13 — [EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md): the protocol and processor mechanics behind this chapter — what Sparkplug B is, the Mosquitto deploy, the two-leg process-group pattern, both test-publisher scripts.
- Ch18 — [Sample gallery](ch18-sample-gallery.md): [`SparkPlug.json`](../files/SparkPlug.json) belongs here alongside the other runnable flows.
- Ch19 — [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md): the `ExecuteScript`/TensorRT pattern this chapter's stretch phase reuses.
- Ch21 — [Metrics & Observability](ch21-metrics-and-observability.md): the Prometheus/Grafana layer that watches this same NiFi/Kafka stack.
