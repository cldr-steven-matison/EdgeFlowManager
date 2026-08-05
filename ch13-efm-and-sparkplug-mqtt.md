# Chapter 13: EFM and Sparkplug MQTT

This chapter is the protocol-and-processor reference for Sparkplug B: what the spec actually
defines, how it rides on MQTT, what MiNiFi C++ can and cannot do with it natively, and how NiFi's
`ConsumeMQTTIIoT` processor decodes the binary payload. It's written to be read *before* Chapter
20's demo narrative — that chapter tells the story of one specific edge device shipping real
telemetry through this pipeline (device swaps, an incident, live verification); this chapter is
the mechanics that story depends on. If you want the protocol explained once, correctly, with the
processors that touch it — this is that chapter.

## Prerequisites

- The CSO stack (NiFi, Kafka/Strimzi) running in minikube — see the earlier EFM-on-Kubernetes
  chapters for how that's deployed.
- A namespace to deploy Mosquitto into (this chapter uses `mqtt`, reachable from both NiFi and any
  MiNiFi/edge agent).
- Familiarity with EFM agent enrollment ([Chapter 19](ch19-efm-and-nvidia-jetson.md)) if you intend
  to run the MQTT leg on a MiNiFi C++ agent rather than only in NiFi.

## What Sparkplug B is

Sparkplug B is an Eclipse-specification, protobuf-encoded MQTT payload format built for industrial
IoT (IIoT). It layers two things on top of plain MQTT that plain MQTT does not give you on its own:

1. **A defined topic namespace.** Every Sparkplug B message publishes to:

   ```
   spBv1.0/<group_id>/<message_type>/<edge_node_id>[/<device_id>]
   ```

   - `spBv1.0` — the fixed namespace/version prefix.
   - `<group_id>` — a logical grouping of edge nodes (e.g. a factory line, a site).
   - `<message_type>` — one of the lifecycle message types below.
   - `<edge_node_id>` — the identifier of the publishing edge device/gateway.
   - `<device_id>` — present only for device-scoped messages (`DBIRTH`/`DDATA`/`DDEATH`); omitted
     for node-scoped messages (`NBIRTH`/`NDATA`/`NDEATH`).

2. **A defined message lifecycle**, so a subscriber always knows the current state of every
   publisher without polling:

   | Message type | Meaning |
   |---|---|
   | `NBIRTH` | Node birth certificate — an edge node announcing itself online, with its full initial metric set |
   | `NDATA` | Node data — incremental metric updates from an already-born node |
   | `NDEATH` | Node death certificate — the node going offline (published by the *broker*, via MQTT Last Will and Testament, if the node disconnects uncleanly) |
   | `DBIRTH` | Device birth — a sub-device under a node announcing itself, with its metric set |
   | `DDATA` | Device data — incremental updates from a device |
   | `DDEATH` | Device death |
   | `STATE` | Primary host application online/offline state (see "Primary Host Application" below) |

   The birth/death pattern is the entire point of the spec: a subscriber that comes online after an
   edge node has already been publishing for hours doesn't need to guess the node's current metric
   set — the most recent `NBIRTH` on that topic (retained by the broker) has the full state, and
   every `NDATA` since is a diff against it.

3. **A binary payload.** The message body itself is a Google Protobuf-encoded `Payload` message —
   not JSON. Each metric carries a name, a datatype enum, a value, and a timestamp, plus a
   monotonically increasing sequence number (`seq`, 0-255, wraps) that lets a subscriber detect a
   dropped message. This is why Sparkplug B needs a purpose-built decoder rather than a generic
   MQTT-to-JSON processor — the bytes on the wire are not human-readable and a `ConsumeMQTT` +
   `EvaluateJsonPath` pattern simply doesn't work against them.

**Where this sits in the stack:** a device (real sensor or edge agent) publishes Sparkplug B over
MQTT to a broker. Something downstream — a MiNiFi agent, or NiFi directly — subscribes, decodes the
protobuf, and forwards the result (typically as JSON) to Kafka for everything else in the CSO stack
to consume. The pattern is the same shape as every other edge-to-NiFi flow in this guide: an
untrusted/lightweight edge protocol in, a normalized record out.

## Broker — Mosquitto in minikube

Both the NiFi ingestion leg and any edge MQTT publisher need a broker they can both reach. This
chapter deploys Eclipse Mosquitto into its own namespace in the same cluster NiFi runs in — no new
infrastructure, reuses the existing minikube.

```bash
kubectl create namespace mqtt
```

```yaml
# mosquitto-configMap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mosquitto-config
  namespace: mqtt
data:
  mosquitto.conf: |
    listener 1883
    allow_anonymous true
    persistence true
    persistence_location /mosquitto/data/
    log_dest stdout
```

```yaml
# mosquitto.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mosquitto
  namespace: mqtt
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mosquitto
  template:
    metadata:
      labels:
        app: mosquitto
    spec:
      containers:
      - name: mosquitto
        image: eclipse-mosquitto:2.0.21
        ports:
        - containerPort: 1883
        volumeMounts:
        - name: config
          mountPath: /mosquitto/config
        - name: data
          mountPath: /mosquitto/data
      volumes:
      - name: config
        configMap:
          name: mosquitto-config
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: mosquitto
  namespace: mqtt
spec:
  selector:
    app: mosquitto
  ports:
  - port: 1883
    targetPort: 1883
  type: NodePort   # easy access from an off-cluster edge device / your workstation
```

```bash
kubectl apply -f mosquitto-configMap.yaml
kubectl apply -f mosquitto.yaml
kubectl get svc -n mqtt
```

Note the assigned NodePort (typically in the `30000+` range) — an off-cluster publisher connects to
`<minikube-ip>:<nodeport>`. From your own workstation, a straightforward port-forward is usually
easier than routing through the NodePort:

```bash
kubectl port-forward pod/mosquitto-<pod-suffix> 1883:1883 -n mqtt
```

`persistence true` matters here specifically because of the birth/death pattern above: Mosquitto
needs to retain the last-seen state for `NBIRTH` messages published with the MQTT retain flag so a
late subscriber gets current state immediately rather than waiting for the next `NDATA`. Bare
`allow_anonymous true` with no auth is a lab-only posture — fine for this cluster's threat model,
not something to carry into a production deployment without revisiting.

## The MiNiFi C++ side — stock MQTT, no dedicated Sparkplug processor

This is the detail that catches people coming from the NiFi side: **there is no
`ConsumeMQTTIIoT`-equivalent on MiNiFi C++.** The C++ agent's MQTT support ships stock, in the base
image, as `libminifi-mqtt-extensions.so`, and it exposes exactly two processors:

- **`ConsumeMQTT`** — subscribes to an MQTT topic (filter), emits one FlowFile per received
  message with the raw payload bytes as FlowFile content.
- **`PublishMQTT`** — publishes a FlowFile's content to an MQTT topic.

Both are generic MQTT processors. Neither one knows anything about Sparkplug B's protobuf schema —
`ConsumeMQTT` subscribed to `spBv1.0/#` will happily deliver FlowFiles whose content is raw
Sparkplug B protobuf bytes, but MiNiFi C++ has no stock processor that decodes those bytes into
usable fields. Decoding a Sparkplug payload on the MiNiFi C++ side would require a custom Python
processor (see [Chapter 6](ch06-minifi-custom-python-processors.md)) linking a protobuf library
against the compiled `.proto` schema, or an `ExecuteScript` doing the same — nothing like this
ships today.

**What this means practically:** if a MiNiFi C++ agent needs to *act* on Sparkplug B content at the
edge (the Phase 5 "edge intelligence" pattern referenced later in this chapter and detailed in
[Chapter 19](ch19-efm-and-nvidia-jetson.md)'s TensorRT flow), the decode step has to happen in
custom code on that agent. If the agent's job is simply to *relay* Sparkplug B onward, `ConsumeMQTT`
→ `PublishMQTT` (or `PublishKafka` for raw bytes) works fine as an opaque pass-through — MiNiFi
never needs to understand the payload to move it.

This asymmetry — full protocol support on the NiFi side, relay-only on the MiNiFi C++ side — is
also why the reference architecture in this chapter and in Chapter 20 puts the actual decode in
NiFi, not at the edge. The edge agent's job is getting bytes off the wire reliably; NiFi's job is
understanding what they mean.

## The NiFi side — `ConsumeMQTTIIoT`

NiFi ships a processor purpose-built for this: **`ConsumeMQTTIIoT`**. Unlike generic `ConsumeMQTT`,
it understands the Sparkplug B protobuf schema natively and decodes `NBIRTH`/`NDATA`/`NDEATH`/
`DBIRTH`/`DDATA`/`DDEATH` payloads into structured records — no separate schema registry or manual
protobuf-to-JSON step required.

Two behaviors worth knowing before wiring it into a flow:

- **It can act as a Sparkplug B "Primary Host Application."** The spec defines this role: a
  subscriber that publishes its own `STATE` messages (online/offline) so edge nodes know whether a
  primary consumer is currently listening, and that can issue a **Rebirth request** — asking an
  edge node to republish a fresh `NBIRTH` (its full current state) on demand, rather than waiting
  for the node's own reconnect cycle. `ConsumeMQTTIIoT` can be configured to take on this role.
- **Topic filter is the same Sparkplug namespace pattern**, typically `spBv1.0/#` to catch every
  group/node/device on the broker, or scoped narrower (`spBv1.0/FactoryLine1/#`) once you know
  which groups you actually care about.

### Two-leg process-group pattern

The field-validated NiFi process group for this chapter's material (exported at
[`files/SparkPlug.json`](../files/SparkPlug.json)) runs **two independent consumer legs off the
same broker**, because two different kinds of publishers exist in this lab at once — a plain-JSON
test/demo publisher and a real Sparkplug B binary publisher:

```
ConsumeMQTT        (Topic Filter: test/sensor/data)   → PublishKafka  (topic: xiao_telemetry)
ConsumeMQTTIIoT     (Topic Filter: spBv1.0/#)          → PublishKafka  (topic: sparkplug_telemetry)
```

- **`ConsumeMQTT`** — plain JSON payloads on `test/sensor/data`. This is the "any device that just
  wants to publish JSON without adopting the full Sparkplug spec" path — no protobuf, no
  birth/death lifecycle, just a flat JSON object per message. `parse.failure` routes off to a
  dead-end for anything malformed.
- **`ConsumeMQTTIIoT`** — real Sparkplug B binary on `spBv1.0/#`. This is the spec-compliant path:
  every message on this leg went through a real `NBIRTH`/`NDATA` lifecycle and protobuf encoding.

Both legs terminate in their own `PublishKafka` processor, each with its own topic, keyed on
`${device_id}`:

- `ConsumeMQTT` → **`PublishKafka-XiaoTelemetry`** — topic `xiao_telemetry`.
- `ConsumeMQTTIIoT` → **`PublishKafka-SparkplugTelemetry`** — topic `sparkplug_telemetry`.

Both point at the same broker: `my-cluster-kafka-bootstrap.cld-streaming.svc:9092`, `PLAINTEXT`, no
SASL — the same Kafka connection settings used by other live processors in this cluster, not
independently guessed.

Running both legs side by side in one process group is deliberate, not an artifact of indecision:
it lets a JSON-only device (no Sparkplug library, no protobuf dependency) and a fully
spec-compliant Sparkplug B device coexist on the same broker and land in Kafka as two clearly
separated topics, rather than forcing every edge publisher onto the heavier spec just to get data
in.

**Why two legs instead of one processor handling both:** `ConsumeMQTTIIoT` expects Sparkplug B's
protobuf wire format — pointing it at a topic carrying plain JSON would fail to decode every
message. Conversely, `ConsumeMQTT` has no protobuf decode at all, so pointing it at `spBv1.0/#`
would deliver undecoded binary garbage downstream. The topic filter is effectively the dispatch
key between "spec-compliant Sparkplug" and "anything simpler that just wants a broker."

### Sample flow — `files/SparkPlug.json`

The committed export, [`files/SparkPlug.json`](../files/SparkPlug.json), is the field-run version
of the process group above. Import it directly rather than rebuilding the two legs from scratch:

```bash
curl -k -u "$NIFI_USER:$NIFI_PASS" \
  -F "file=@files/SparkPlug.json" \
  "https://<nifi-host>/nifi-api/process-groups/<root-pg-id>/process-groups/upload"
```

> **⚠️ Never GET-then-PUT a processor with sensitive properties.** Both `ConsumeMQTT` and
> `ConsumeMQTTIIoT` have a `Password` property. If your broker has auth configured, check
> `descriptors[...].sensitive` before any full-entity PUT against a live processor — a masked
> value (`********`, or in this pair's case, a literal `null`) written straight back destroys the
> real credential. Use a Parameter Context for the broker password instead of hand-editing the
> processor entity. See [Chapter 20](ch20-sparkplug-demo.md) for a real occurrence of this exact
> gotcha against this exact processor pair.

## Test publishers

Two publisher scripts exercise the two legs independently. Both are plain Python against a
port-forwarded Mosquitto — no edge hardware required to validate the NiFi side of this pipeline.

### Plain JSON — matches the `ConsumeMQTT` leg

```python
# mqtt_test_publisher.py
import time
import json
import random
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "test/sensor/data"

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT, 60)
client.loop_start()

try:
    while True:
        payload = {
            "device_id": "MacMockSensor-01",
            "temperature": round(random.uniform(20.0, 30.0), 2),
            "humidity": round(random.uniform(40.0, 60.0), 2),
            "timestamp": int(time.time())
        }
        client.publish(TOPIC, json.dumps(payload))
        print(f"Published: {payload}")
        time.sleep(2)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
```

Field-run sample output:

```
Connecting to Mosquitto broker at localhost:1883...
Successfully connected! Publishing data to topic 'test/sensor/data' every 2 seconds. Press Ctrl+C to stop.
Published: {'device_id': 'MacMockSensor-01', 'temperature': 22.43, 'humidity': 53.29, 'timestamp': 1781614422}
Published: {'device_id': 'MacMockSensor-01', 'temperature': 24.88, 'humidity': 51.33, 'timestamp': 1781614424}
Published: {'device_id': 'MacMockSensor-01', 'temperature': 21.82, 'humidity': 41.39, 'timestamp': 1781614426}
```

### Real Sparkplug B binary — matches the `ConsumeMQTTIIoT` leg

This is the important one for validating the actual protobuf decode path — it constructs
spec-compliant `NBIRTH`/`NDATA` messages via `pysparkplug` and publishes them binary-encoded to the
correct namespace-prefixed topics.

```bash
pip install pysparkplug paho-mqtt
```

```python
# sparkplug_test_publisher.py
import time
import random
import paho.mqtt.client as mqtt
from pysparkplug import NBirth, NData, Metric, DataType, get_current_timestamp

BROKER = "localhost"
PORT = 1883
GROUP_ID = "MacLocalTest"
EDGE_NODE_ID = "Mac-Node-01"

TOPIC_NBIRTH = f"spBv1.0/{GROUP_ID}/NBIRTH/{EDGE_NODE_ID}"
TOPIC_NDATA = f"spBv1.0/{GROUP_ID}/NDATA/{EDGE_NODE_ID}"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)
client.loop_start()

# 1. Publish Node Birth Certificate (NBIRTH) — required before any NDATA
ts_birth = get_current_timestamp()
metrics_birth = (
    Metric(name="Temperature", datatype=DataType.FLOAT, value=22.0, timestamp=ts_birth),
    Metric(name="Humidity", datatype=DataType.FLOAT, value=50.0, timestamp=ts_birth),
)
client.publish(TOPIC_NBIRTH, NBirth(timestamp=ts_birth, seq=0, metrics=metrics_birth).encode(), qos=1)

seq = 1
try:
    while True:
        temp_val = round(random.uniform(20.0, 35.0), 2)
        humid_val = round(random.uniform(40.0, 60.0), 2)
        ts_data = get_current_timestamp()
        metrics_data = (
            Metric(name="Temperature", datatype=DataType.FLOAT, value=temp_val, timestamp=ts_data),
            Metric(name="Humidity", datatype=DataType.FLOAT, value=humid_val, timestamp=ts_data),
        )
        # 2. Publish Node Data (NDATA)
        client.publish(TOPIC_NDATA, NData(timestamp=ts_data, seq=seq, metrics=metrics_data).encode(), qos=1)
        print(f"Sent Sparkplug NDATA (Seq: {seq}) -> Temp: {temp_val} | Humid: {humid_val}")
        seq = (seq + 1) % 256   # Sparkplug sequence numbers wrap 0-255
        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
```

Field-run sample output:

```
Connecting to Mosquitto broker at localhost:1883...
Publishing binary Sparkplug B NBIRTH payload...
Node is ONLINE. Sending NDATA every 5 seconds. Press Ctrl+C to stop.
Sent Sparkplug NDATA (Seq: 1) -> Temp: 28.87 | Humid: 49.59
Sent Sparkplug NDATA (Seq: 2) -> Temp: 26.07 | Humid: 51.89
Sent Sparkplug NDATA (Seq: 3) -> Temp: 31.36 | Humid: 46.76
Sent Sparkplug NDATA (Seq: 4) -> Temp: 21.02 | Humid: 46.02
```

Notice the `seq` field in the printed output tracks 1, 2, 3, 4... — that's the sequence number
Sparkplug B's spec defines specifically so a subscriber can detect a dropped or out-of-order
message: a gap in that counter is a signal to request a rebirth rather than silently trust stale
state.

### Terminal history — a real field run

```terminal
source venv/bin/activate
pip install paho-mqtt
pip install pysparkplug
nano sparkplug_test_publisher.py
python sparkplug_test_publisher.py
```

```bash
kubectl apply -f mosquitto-configMap.yaml
kubectl apply -f mosquitto.yaml

# find the actual pod name first
kubectl get pods -n mqtt

kubectl port-forward pod/mosquitto-b7876bbf7-7kstt 1883:1883 -n mqtt

source venv/bin/activate
python sparkplug_test_publisher.py
```

## Field validation — what's confirmed and what isn't

Being precise about what has and hasn't actually run against real infrastructure, per this guide's
convention of not blurring designed-but-untested with field-proven:

**Confirmed, field-run:**
- Mosquitto deployment into minikube (`mqtt` namespace) — live and reachable.
- The plain-JSON publisher against `ConsumeMQTT` (`test/sensor/data`) — real messages received and
  visible in NiFi provenance.
- The `pysparkplug` binary publisher against `ConsumeMQTTIIoT` (`spBv1.0/#`) — real `NBIRTH`/`NDATA`
  messages published and consumed; `ConsumeMQTTIIoT` decoded the protobuf correctly.
- Both legs wired all the way to Kafka (`xiao_telemetry`, `sparkplug_telemetry`) — see
  [Chapter 20](ch20-sparkplug-demo.md) for the live re-wiring and the incident that came with it.
- A real hardware device (Seeed XIAO ESP32-S3) publishing plain JSON matching the `ConsumeMQTT`
  leg's shape — see Chapter 20.

**Designed, not yet field-run:**
- A real hardware device publishing genuine Sparkplug B binary (the `ConsumeMQTTIIoT` leg has only
  ever been field-tested against the `pysparkplug` Mac-side simulator, never against physical
  sensor hardware). Chapter 20 notes this explicitly: the `sparkplug_telemetry` Kafka topic is
  "proven-consuming but not yet field-run end to end with a live binary producer."
  <br><br>
  **This is the one genuinely open technical question this chapter can't resolve without live
  hardware and cluster access:** does a real embedded device (ESP32-class or similar) have a
  practical, low-footprint path to producing spec-compliant Sparkplug B protobuf — as opposed to
  the Python-side `pysparkplug` library used for every field run so far? Options exist (a
  C/C++ protobuf-lite Sparkplug encoder, or a gateway pattern where the embedded device speaks
  plain JSON/MQTT to a MiNiFi or small-script relay that re-encodes as Sparkplug B before handing
  it to Mosquitto) but none has been tried against this lab's actual hardware. Flagging as open
  rather than picking one and presenting it as validated.
- The Primary Host Application / Rebirth-request behavior of `ConsumeMQTTIIoT` — the processor
  supports it, but no field run in this lab has exercised a live rebirth request against a
  connected edge node.
- Edge-side decode of Sparkplug B on a MiNiFi C++ agent via custom Python (rather than relay-only) —
  not attempted; see the MiNiFi C++ section above for why this would require custom code no stock
  extension currently provides.

## Edge intelligence — pointer, not scope

A further design exists for running Sparkplug B decode *and* a small inference model directly on
an edge device — `ConsumeMQTTIIoT` (or a MiNiFi relay) feeding an `ExecuteScript` step doing
TensorRT/ONNX anomaly detection, triggering a local GPIO actuator (buzzer) on an extreme reading,
while still forwarding upstream to central Kafka. This is designed but not field-run — it's the
Jetson/TensorRT pattern already field-proven in [Chapter 19](ch19-efm-and-nvidia-jetson.md) for a
different (non-Sparkplug) flow, extended conceptually to a Sparkplug B input. It belongs to
Chapters 19/20, not here — this chapter stops at the protocol and the two NiFi legs; the edge-AI
actuation story is deliberately out of scope for a protocol-mechanics chapter. See
[Chapter 20](ch20-sparkplug-demo.md)'s "Edge intelligence (stretch)" section for the current status.

## All the ways to learn about EFM and Sparkplug

Given issue #108's ask for "all the ways to learn about EFM and Sparkplug" — the concrete paths
that exist in this lab and its source material, gathered in one place:

- **This chapter** — protocol mechanics, broker deploy, both NiFi processors, test publishers.
- **[Chapter 20](ch20-sparkplug-demo.md)** — the end-to-end demo narrative: a real device, a
  process-group-loss incident and recovery, a topic-contamination incident and fix, live
  verification technique (don't trust a device's own serial log).
- **[Chapter 19](ch19-efm-and-nvidia-jetson.md)** — the `ExecuteScript`/TensorRT edge-inference
  pattern the Sparkplug "edge intelligence" stretch phase reuses, field-proven on a different flow.
- **[Chapter 6](ch06-minifi-custom-python-processors.md)** — what it would take to add a real
  Sparkplug B decoder as a MiNiFi C++ custom processor, if the relay-only limitation above ever
  needs closing.
- **`files/SparkPlug.json`** — the actual importable process group; reading its processor
  configuration in the NiFi UI after import is often faster than re-deriving property values from
  prose.
- **The Sparkplug B specification itself** (Eclipse Tahu / Sparkplug specification, published by
  the Eclipse Foundation) — this chapter covers the subset relevant to this lab's flows
  (`NBIRTH`/`NDATA`/`NDEATH`, the topic namespace, the sequence number); the full spec also defines
  `DBIRTH`/`DDATA`/`DDEATH` device-scoped semantics and the Primary Host `STATE` mechanism in more
  depth than reproduced here.
- **`pysparkplug`'s own source/docs** — the library used for every binary-payload field test in
  this lab; its `Metric`/`DataType`/`NBirth`/`NData` API surface is the practical on-ramp for
  writing another Sparkplug B publisher without hand-rolling protobuf encoding.

## What NOT to do

**Point `ConsumeMQTTIIoT` at a topic carrying plain JSON, or `ConsumeMQTT` at `spBv1.0/#` expecting
decoded output.** The two processors are not interchangeable — one expects Sparkplug B protobuf,
the other has no protobuf decode at all. Match the processor to the actual wire format on that
topic, per-leg, not per-flow.

**Assume MiNiFi C++ has a Sparkplug-aware processor because NiFi does.** It doesn't.
`ConsumeMQTT`/`PublishMQTT` on the C++ agent are generic MQTT — fine for relay, not for decode.
Don't design an edge flow around edge-side Sparkplug decode without first confirming you're
prepared to write and maintain the custom processor that would require.

**GET-then-PUT `ConsumeMQTT`/`ConsumeMQTTIIoT` when a broker password is set.** Same rule as every
other sensitive NiFi property in this guide — check `sensitive` in the descriptor before any
full-entity PUT, regardless of whether the field happens to read back masked or literally `null`.

**Treat a Sparkplug B message without a preceding `NBIRTH` as trustworthy.** The spec's entire
state model depends on the birth certificate establishing the full metric set first; `NDATA` before
`NBIRTH` (or after a missed sequence number) means a subscriber's view of that node's state may
already be wrong. This is what the Primary Host / Rebirth-request mechanism exists to correct —
don't build downstream logic that ignores `seq` gaps.

**Present the ESP32/embedded Sparkplug B producer question as solved.** As of this chapter, every
field-validated binary-payload publisher in this lab is the Python `pysparkplug` script running on
a workstation, not embedded firmware. Say so plainly rather than implying the XIAO or any other
device has been proven to speak real Sparkplug B — it has only been proven to speak the simpler
plain-JSON leg.

## Related chapters

- Ch12 — [EFM + MicroFi](ch12-efm-and-microfi.md): the ESP32-class agent-enrollment side (device
  onboarding under EFM); this chapter assumes an already-enrolled or non-EFM edge publisher and
  focuses on the Sparkplug protocol/processor layer instead.
- Ch19 — [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md): the `ExecuteScript`/TensorRT
  edge-inference pattern referenced by the "edge intelligence" stretch design above.
- Ch20 — [SparkPlug B — MQTT/IIoT edge demo](ch20-sparkplug-demo.md): the end-to-end demo narrative
  this chapter is the technical reference for — real device, real incidents, live verification.
