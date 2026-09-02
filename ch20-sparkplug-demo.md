# Chapter 20: SparkPlug B — MQTT/IIoT Edge Demo

This is the guide's industrial-IoT finale. Three XIAO ESP32-S3 units, the MicroFi fleet from [Chapter 12](ch12-efm-and-microfi.md), publish over one Mosquitto broker into one NiFi process group and out to Kafka on two legs: plain JSON on one, spec-compliant Sparkplug B on the other. A third flow closes the loop the other way, turning a FlowFile on the central NiFi canvas into a physical LED on a board across the room.

[Chapter 13](ch13-efm-and-sparkplug-mqtt.md) explains what Sparkplug B is, how `ConsumeMQTTIIoT` decodes it, and ships the two test publishers. This chapter assumes that and assembles the demo: what runs where, how to import it, and how to prove each hop independently.

## The Demo at a Glance

```
MicroFi-1  GenerateFlowFile ─→ PublishMQTT ──(test/sensor/data, JSON)──┐
MicroFi-3  GenerateFlowFile ─→ PublishSparkplug ──(spBv1.0/MicroFi/…)──┤
MicroFi-2  CaptureImage ─→ PublishMQTT ──(microfi2/camera/*, JPEG)─────┤
                                                                       ▼
                                                        Mosquitto  (mqtt namespace)
                                                                       │
        ┌──────────────────────────────────────────────────────────────┤
        ▼                                                              ▼
  SparkPlug PG (NiFi)                                    MicroFi2CameraBridge PG (NiFi)
   ConsumeMQTT ─→ ExtractDeviceId ─→ PublishKafka ─→ xiao_telemetry     ConsumeMQTT ─→ PublishKafka ─→ microfi2.camera.*
   ConsumeMQTTIIoT ─────────────────→ PublishKafka ─→ sparkplug_telemetry

  MicroFiLedActuation PG (NiFi)                          MicroFi-1 (device)
   GenerateLedLevel ─→ InvokeLedOnMicroFi1 ── POST /led ──→ ListenHTTP ─→ SetGPIO (pin 21)
```

| Unit | EFM class | Role | Class flow export |
|---|---|---|---|
| XIAO #1 | `MicroFi-1` | JSON telemetry publisher; LED actuation target | [`files/microfi/microfi-1-telemetry.json`](files/microfi/microfi-1-telemetry.json) · [`files/microfi/microfi-3-led-flow-backup.json`](files/microfi/microfi-3-led-flow-backup.json) (the LED flow) |
| XIAO #2 | `MicroFi-2` | Camera, JPEG broker-direct | [`files/microfi/microfi-2-camera.json`](files/microfi/microfi-2-camera.json) |
| XIAO #3 | `MicroFi-3` | Sparkplug B publisher (`NBIRTH`/`NDATA`) | [`files/microfi/microfi-3-sparkplug.json`](files/microfi/microfi-3-sparkplug.json) |

The NiFi side is three process groups on the central canvas: [`files/SparkPlug.json`](files/SparkPlug.json), [`files/microfi/MicroFi2CameraBridge.json`](files/microfi/MicroFi2CameraBridge.json), and `MicroFiLedActuation` (three processors, built in the next sections). Every unit runs one flow type. A unit can be repurposed by publishing a different class flow to it; the exports above are the shapes to swap between.

## Prerequisites

- The CSO stack on minikube. NiFi is the CFM-operator `mynifi` in `cfm-streaming`; Kafka is the Strimzi cluster in `cld-streaming` (`my-cluster-kafka-bootstrap.cld-streaming.svc:9092` in-cluster, NodePort `31623` on the host's LAN address from outside it).
- Mosquitto in its own `mqtt` namespace, one command from the committed manifests:

  ```bash
  kubectl apply -f files/mosquitto-configmap.yaml -f files/mosquitto.yaml
  kubectl get deploy,svc -n mqtt        # want mosquitto 1/1 and the NodePort service
  ```

  NiFi reaches it in-cluster at `tcp://mosquitto.mqtt.svc.cluster.local:1883`. The devices reach it through the host at `192.168.1.121:1883`, which needs **two** things on a WSL2/Windows host: a `kubectl port-forward --address <lan-ip> svc/mosquitto 1883:1883 -n mqtt` and an inbound Windows Firewall allow rule for TCP 1883. The port-forward alone looks up from the host and is invisible from the LAN.
- The three MicroFi units enrolled in EFM with their class flows published ([Chapter 12](ch12-efm-and-microfi.md)). Each unit's firmware carries the broker address as a literal LAN dotted-quad; the boards have no Tailscale client and join the same WiFi AP as the host.
- No hardware yet? Chapter 13's two Python publishers exercise both legs of the `SparkPlug` PG from a laptop against a port-forwarded broker.

## NiFi Ingestion — the `SparkPlug` Process Group

Import the committed export rather than rebuilding the two legs by hand:

```bash
curl -k -u "$NIFI_USER:$NIFI_PASS" \
  -F "file=@files/SparkPlug.json" \
  "https://<nifi-host>/nifi-api/process-groups/<root-pg-id>/process-groups/upload"
```

Two independent consumer legs share the broker, because two kinds of publisher exist at once:

- **JSON leg.** `ConsumeMQTT` (topic filter `test/sensor/data`) → `ExtractDeviceId` (`EvaluateJsonPath`, `device_id` from `$.device_id`) → `PublishKafka-XiaoTelemetry` (topic `xiao_telemetry`, key `${device_id}`). The payload carries the publisher's own agent-class name, so every Kafka record is keyed by the device's class identity: `MicroFi-1`.
- **Sparkplug leg.** `ConsumeMQTTIIoT` (topic filter `spBv1.0/#`) → `PublishKafka-SparkplugTelemetry` (topic `sparkplug_telemetry`). `ConsumeMQTTIIoT` decodes the protobuf itself and routes every well-formed message to `Message`; anything it cannot parse goes to `parse.failure`. On this leg the device identity travels in the topic segments (`spBv1.0/<group>/<type>/<edge-node>`), not in a `device_id` attribute, so the records carry a null key.
- `parse.failure` on both legs routes to an `EOL` output port. Nothing is auto-terminated, so a bad payload is visible in a queue instead of vanishing.

Both `PublishKafka` processors use the in-cluster bootstrap address, `PLAINTEXT`, no SASL, copied from the other live processors on the same cluster.

> **⚠️ Never GET-then-PUT `ConsumeMQTT` or `ConsumeMQTTIIoT`.** Both carry a sensitive `Password` property. On this pair it reads back as a literal `null` rather than the usual `********`, and the rule is the same: check `descriptors[...].sensitive` before any full-entity PUT, or bind the broker password to a Parameter Context and never touch the entity at all.

## The Publishers — One Flow Type per Unit

**MicroFi-1, plain JSON.** `GenerateFlowFile → PublishMQTT` (`Broker URI: mqtt://192.168.1.121:1883`, `Topic: test/sensor/data`, QoS 0). The generated content is the class name as JSON:

```json
{"device_id":"MicroFi-1"}
```

That single field is what keys the Kafka records. A `ListenHTTP` on port `8095` sits in the same class flow unconnected, ready for the actuation section below.

**MicroFi-3, Sparkplug B.** `GenerateFlowFile-SpbTick → PublishSparkplug-Telemetry`. The processor owns the session state machine: it publishes `NBIRTH` first (declaring `bdSeq`, `Node Control/Rebirth`, and the metric set), then one `NDATA` per tick with an advancing `seq`, on:

```
spBv1.0/MicroFi/NBIRTH/MicroFi-3
spBv1.0/MicroFi/NDATA/MicroFi-3
```

Group `MicroFi`, edge node `MicroFi-3`. The encoder is the vendored `EmbeddedSparkplugNode`/nanopb stack compiled into the MicroFi firmware (Chapter 12's registry). The same publisher shape on a MiNiFi **Java** agent, via the native `PublishSparkplug` NAR, is Chapter 13's "Publishing Sparkplug B from MiNiFi" and Chapter 18's Entry 11.

**MicroFi-2, camera.** Not Sparkplug, but it rides the same broker and shows the pattern every MicroFi media processor uses: a FlowFile holds at most 256 bytes, so `CaptureImage` publishes the JPEG itself, broker-direct, on `microfi2/camera/jpg` and emits only a metadata FlowFile for `PublishMQTT-Meta` on `microfi2/camera/meta`. The `MicroFi2CameraBridge` PG (`ConsumeMQTT → PublishKafka`, failure → log) lands both topics in Kafka as `microfi2.camera.*`.

## Verify End to End

Prove each hop from a vantage point that is not the previous hop. A device's own serial log saying `published` proves the firmware ran the publish call, nothing more.

**1. The broker, independently of the device.** Subscribe from inside the cluster; both payload kinds should scroll:

```bash
kubectl exec -n mqtt deploy/mosquitto -- mosquitto_sub -v -t 'test/sensor/data' -t 'spBv1.0/#'
```

The JSON leg prints readable `{"device_id":"MicroFi-1"}` lines. The Sparkplug leg prints binary; the topic names are the readable part, and `NBIRTH` appears once per device boot before the `NDATA` stream.

**2. NiFi decoded it.** On the `SparkPlug` PG, `ConsumeMQTTIIoT`'s `Message` connection should carry the traffic and `parse.failure` should stay at zero. A queue building on `parse.failure` means something is publishing non-Sparkplug bytes under `spBv1.0/#`.

**3. Kafka has it, keyed.**

```bash
# JSON leg, keyed by device class
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic xiao_telemetry --property print.key=true --property key.separator=" | " --timeout-ms 15000
# expected: MicroFi-1 | {"device_id":"MicroFi-1"}

# Sparkplug leg, binary protobuf records: NBIRTH then NDATA
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sparkplug_telemetry --timeout-ms 15000
```

The Sparkplug records are not human-readable, but the metric names are literal strings inside the protobuf, so `Sensors/Temperature` is visible in the raw bytes. That, plus `Message`-not-`parse.failure` in NiFi, is the verification standard for "this device speaks Sparkplug B."

## Round Trip — a FlowFile Becomes an LED

Ingest proves the edge can talk to the center. The round trip proves the center can act on the edge, and it is the simplest teachable form of every actuation leg in this guide: a FlowFile on the central canvas becomes a physical state change on the smallest device in the fleet.

**Device side, two nodes** (class flow: [`files/microfi/microfi-3-led-flow-backup.json`](files/microfi/microfi-3-led-flow-backup.json)):

```
ListenHTTP (Listening Port 8095, Base Path /led) ─(success)─→ SetGPIO (Pin 21, Pin Level from-content, Invert)
```

`SetGPIO` reads the level from the request body: `1/0/on/off/high/low/toggle`. `Invert` is set because the XIAO's user LED is active-low. The FlowFile content *is* the pin level; no attributes survive the HTTP hop, because MiNiFi-style `ListenHTTP` is fire-and-forget (Chapter 16's trap list).

**NiFi side, the `MicroFiLedActuation` process group**, three processors:

```
GenerateLedLevel (GenerateFlowFile, content "1" or "0")
  ─(success)─→ InvokeLedOnMicroFi1 (InvokeHTTP, POST http://192.168.1.198:8095/led)
                 ─(Failure / Retry / No Retry)─→ LogLedFailure (LogAttribute)
```

Every non-success relationship of the `InvokeHTTP` lands on the log processor, so a device that is offline shows up as a queue and a log line, never as a silent drop. Run `GenerateLedLevel` once with content `1`, then once with `0`; two FlowFiles through, the LED on and then off, and the failure queue empty.

Test the device before the PG, directly:

```bash
curl -X POST http://192.168.1.198:8095/led -d 1     # LED on, HTTP 200
curl -X POST http://192.168.1.198:8095/led -d 0     # LED off
```

**Swapping a unit's role.** MicroFi-1 normally runs the JSON publisher. To run the LED flow on it, publish the LED class flow through the EFM Designer API (delete the current components, create the two processors and one connection, validate, publish) and publish the telemetry export back afterwards. [`files/microfi/amoled-class-flow.py`](files/microfi/amoled-class-flow.py) is the worked builder for exactly that swap on the AMOLED class; the same three calls apply to any MicroFi class. Publishing a new class flow re-applies the graph in place. On current firmware the old `ListenHTTP` releases its port before the new graph starts; on a firmware build without the teardown hook, power-cycle the unit if a port-binding processor does not come up.

## Edge Decision on the Jetson — Designed and Exported

The same threshold-to-actuation idea runs on a MiNiFi C++ agent when the decision should be made at the edge rather than on the central canvas. The `NvidiaNanoSparkPlug` class flow ([`files/efm/NvidiaNanoSparkPlug.json`](files/efm/NvidiaNanoSparkPlug.json)):

```
ConsumeMQTT-XiaoSensor (tcp://192.168.1.121:1883, test/sensor/data)
  → ExecuteScript-TensorRT (files/gpu_nifi_tensorRT-3.py)
      → PublishKafka-NvidiaNanoInference (192.168.1.121:31623, every reading)
      → RouteOnAttribute-Trigger (${trigger.actuation}) → InvokeHTTP-TriggerXiao (POST http://192.168.1.198:8095/…)
```

The script wraps non-JSON content as `{"raw": …}` before it does anything else, since the MicroFi payload is plain text. The class is registered in EFM with this flow published; the Jetson itself currently runs the Java `NvidiaNano` agent from [Chapter 19](ch19-efm-and-nvidia-jetson.md), so this flow runs when a C++ agent is started under the `NvidiaNanoSparkPlug` class with `bin/minifi.sh run` from its own install directory. The Kafka address is the host's NodePort, not the in-cluster DNS name, because a physical device outside the cluster cannot resolve the latter.

Site-to-Site is not part of this demo. Every hop here goes through the broker or Kafka; [Chapter 11](ch11-site-to-site.md) is the recipe if a deployment needs a MiNiFi agent to deliver into NiFi directly.

## What NOT to Do

**Trust a checked-in flow export to match what is live.** A process group can be lost to a pod recreate and survive only as an export. Dump the live `flow.json.gz` before trusting any doc or export, and re-export after every change so the export is worth trusting next time.

**Assume a topic has one publisher because one flow was built that way.** `ConsumeMQTT` has no per-publisher isolation; anything else on `test/sensor/data` lands in `xiao_telemetry` indistinguishably unless the payload carries a field to key on. Watching the Kafka offset climb does not tell you whose messages they are. Sample the content.

**Take a firmware's serial log as proof of delivery.** `MQTT: connected` and `published` on the device side prove nothing reached the broker. Subscribe from a different process before calling a publish path verified.

**GET-then-PUT a processor whose sensitive property reads back `null`.** The masked form varies; the destruction on PUT does not.

**Point a device's `PublishKafka` at the in-cluster bootstrap DNS name.** A physical agent outside the cluster needs the external listener on the host's LAN address, and a MiNiFi C++ agent needs a full restart, not just a property push, before a changed broker address takes effect.

**Push an Expression Language predicate with nested single quotes through EFM's C2 path to a MiNiFi C++ agent.** `${trigger.actuation:equals('true')}` arrives on the device as the literal `false`. Use a bare attribute reference (`${trigger.actuation}`) and read the agent's regenerated `config.yml` to see what it received; the Designer's echo of the property is not that.

**Hot-swap a flow that binds a port on a MicroFi build without the teardown hook.** The previous `ListenHTTP` socket is never released, the new one's `httpd_start` fails with only a log line, and a republish does not clear it. Power-cycle.

**Expect EFM's agent deployer to honor `serviceName`.** The deployer script hard-codes the systemd unit name `minifi`, so a second systemd-managed C++ agent on the same host is not possible through it. Run the second agent with `bin/minifi.sh run` from a clean install directory; a copied directory boots straight into its old `conf/config.yml`, ports and all.

## Related Chapters

- [Chapter 12 — EFM and MicroFi](ch12-efm-and-microfi.md): the fleet, the processor registry (`PublishMQTT`, `PublishSparkplug`, `ListenHTTP`, `SetGPIO`, `CaptureImage`), and how a class flow gets onto a unit.
- [Chapter 13 — EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md): the protocol, the broker manifests, `ConsumeMQTTIIoT`, the test publishers, and publishing Sparkplug B from MiNiFi Java.
- [Chapter 18 — Sample Gallery](ch18-sample-gallery.md): Entries 10 (two-leg ingest), 11 (`PublishSparkplug` on MiNiFi Java), and 12 (LED actuation round trip) are this chapter's flows as runnable cards.
- [Chapter 19 — EFM + NVIDIA Jetson](ch19-efm-and-nvidia-jetson.md): the `ExecuteScript`/TensorRT pattern the Jetson leg reuses, and the Java agent that runs on the board today.
- [Chapter 21 — Metrics & Observability](ch21-metrics-and-observability.md): the Prometheus/Grafana layer watching this NiFi/Kafka stack and the fleet's heartbeats.
