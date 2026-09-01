# Chapter 13: EFM and Sparkplug MQTT

This chapter is the protocol-and-processor reference for Sparkplug B: what the spec actually
defines, how it rides on MQTT, what MiNiFi C++ and MiNiFi Java can and cannot do with it natively,
and how NiFi's `ConsumeMQTTIIoT` processor decodes the binary payload. It's written to be read
*before* Chapter 20's demo narrative — that chapter tells the story of one specific edge device
shipping real telemetry through this pipeline (device swaps, an incident, live verification); this
chapter is the mechanics that story depends on. If you want the protocol explained once, correctly,
with the processors that touch it — this is that chapter.

## Prerequisites

- The CSO stack (NiFi, Kafka/Strimzi) running in minikube — see the earlier EFM-on-Kubernetes
  chapters for how that's deployed.
- A namespace to deploy Mosquitto into (this chapter uses `mqtt`, reachable from both NiFi and any
  MiNiFi/edge agent).
- Familiarity with EFM agent enrollment ([Chapter 19](ch19-efm-and-nvidia-jetson.md)) if you intend
  to run the MQTT leg on a MiNiFi C++ or MiNiFi Java agent rather than only in NiFi.

## What Sparkplug B Is

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

## Broker — Mosquitto in Minikube

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

## The MiNiFi C++ Side — Stock MQTT, No Dedicated Sparkplug Processor

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

## The MiNiFi Java Side — Native Sparkplug Decode at the Edge

Unlike C++, MiNiFi Java can decode Sparkplug B natively at the edge — **field-confirmed**, not just
designed. The `ConsumeMQTTIIoT` processor is not in the stock CEM `2.24.08.0-19` tarball (the Java
processor catalog carries no MQTT/IIoT/Sparkplug component out of the box), but it loads on a Java
agent the same way the Kafka and scripting NARs do: drop the Cloudera CDF IIoT NAR into the agent's
`extensions/` autoload directory.

**The NAR and its dependency closure.** `ConsumeMQTTIIoT` ships in the Cloudera-proprietary
`nifi-cdf-iiot-mqtt-nar` — parcel-only, absent from the open-source `-extension` bundle (which
carries only the Apache `nifi-mqtt-nar`). Side-load the NAR **with its full dependency closure**, all
at the same `group:id:version` (on CFM 4.12.0, the `2.6.0.4.12.0.x` set):

```
nifi-cdf-iiot-mqtt-nar
  └ nifi-mqtt-nar
      └ nifi-standard-shared-nar
          └ nifi-standard-services-api-nar
```

Restart the agent (or let the autoloader pick them up) and `ConsumeMQTTIIoT` resolves as a real type
in the agent manifest and the EFM Designer palette. `NarUnpacker` fails the *entire* batch if any one
side-loaded NAR is malformed, so verify each is a clean archive — the same drop-in mechanics used for
PLC4X and IIoT on Java elsewhere in this guide.

**One thing that surprises people coming from full NiFi: there is no separate `MQTTIIoTReader`
controller service in the CDF IIoT NAR.** The NAR ships exactly one component — the `ConsumeMQTTIIoT`
processor — and the Sparkplug B protobuf decode is built into it. `Record Reader` and `Record Writer`
are *optional* properties (and must be set together if used at all); none is required to decode.
Point `ConsumeMQTTIIoT` at `spBv1.0/#` with just a Broker URI and it decodes on its own.

**What decode looks like on the agent.** With the NAR loaded and a `ConsumeMQTTIIoT` → `LogAttribute`
flow published to the agent, a `pysparkplug` publisher's `NBIRTH`/`NDATA` messages are decoded at the
edge exactly as they are in NiFi: every message routes via the `Message` relationship (**not**
`parse.failure` — the agent's own parser validated them as real Sparkplug B), the topic namespace is
parsed into `mqtt.topic.segment.*` attributes (`spBv1.0` / group / message-type / edge-node), and the
decoded output carries the correct metric names and float32 values — `Temperature`/`Humidity`
matching the publisher's `NBIRTH` exactly and its `NDATA` value ranges. This is the same verification
standard used for the NiFi side below.

**Relay is still an option.** If you don't want to carry the CDF NAR, the C++-style relay pattern
works on Java too: `ConsumeMQTT` (generic MQTT, no protobuf decode) subscribes to `spBv1.0/#` and
forwards raw bytes to Kafka or NiFi for downstream decode. But native edge decode via
`ConsumeMQTTIIoT` is a real, confirmed capability on MiNiFi Java — the design target of putting the
decode on a Java edge agent (rather than only in NiFi) is achievable today.

## Publishing Sparkplug B from MiNiFi — the Missing Half, Now Built

Everything above is the **consume/decode** side. The publish side has no stock answer on either
MiNiFi flavor: the CDF `nifi-cdf-iiot-mqtt-nar` ships exactly one component (`ConsumeMQTTIIoT`,
consume-only), and stock `PublishMQTT` moves raw bytes with no idea what a birth certificate or a
`seq` counter is. An edge agent that needs to *originate* Sparkplug B has to encode the protobuf
and run the session state machine itself.

The worked comparison of every route — MiNiFi Java via Eclipse Tahu (`ExecuteScript` prototype or
a custom NAR) versus MiNiFi C++ (embedded-CPython `pysparkplug` or a custom `.so` vendoring
Tahu-C/nanopb, the direct analog of MicroFi's own C++ `PublishSparkplug`) — lives in the how-to
[`minifi-sparkplug-publish.md`](https://github.com/cldr-steven-matison/DesktopShare/blob/main/minifi-sparkplug-publish.md),
with code skeletons, side-load mechanics, and a verify checklist.

**The recommended route is built and field-verified: a native Java `PublishSparkplug` processor**
([`nifi-sparkplug-bundle`](https://github.com/cldr-steven-matison/NiFi2-Processor-Playground/tree/main/nifi-sparkplug-bundle),
one-pager: [`sparkplug-publish-processor.md`](https://github.com/cldr-steven-matison/DesktopShare/blob/main/sparkplug-publish-processor.md)).
One FlowFile of flat JSON metrics in → spec-compliant Sparkplug B out: NBIRTH-first (declaring
`bdSeq` and `Node Control/Rebirth`), NDATA per FlowFile, NDEATH registered as the MQTT will,
`bdSeq`/`seq` (0–255 wrap) managed internally (Eclipse Tahu encode, Paho transport, self-contained
NAR — no parent NAR to line up).

**Field-verified end-to-end 2026-09-01** (evidence:
[`files/issue-138/`](https://github.com/cldr-steven-matison/DesktopShare/tree/main/files/issue-138)):
the NAR side-loaded onto a fresh EFM-managed MiNiFi Java agent (class `SparkplugJavaLab`, enrolled
via EFM `generateCommand`), a two-node Designer flow
(`GenerateFlowFile({"Sensors/Temperature": 22.5, …}) → PublishSparkplug`) published, and the wire
showed a correct NBIRTH (seq=0) then NDATA with advancing `seq` — which the live NiFi
`ConsumeMQTTIIoT` accepted via its `Message` relationship (zero `parse.failure`, the real
verification standard) all the way into Kafka.

Two side-load mechanics discovered in that run, worth knowing before repeating it:

- **A hot-loaded NAR does not refresh the agent's C2 manifest.** The Java agent's `NarAutoLoader`
  picks the NAR up from `extensions/` within seconds (`Loaded extensions for
  com.example:nifi-sparkplug-nar`), but the manifest it heartbeats to EFM is built at startup —
  the new processor won't appear in the Designer palette until the agent restarts.
- **Pinning the refreshed manifest to the class** uses
  `POST /efm/api/agent-class-manifest-config` with field name `agentClassName` (not `agentClass`),
  after which the Designer resolves the new type.

## The NiFi Side — `ConsumeMQTTIIoT`

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

### Two-Leg Process-Group Pattern

The field-validated NiFi process group for this chapter's material (exported at
[`files/SparkPlug.json`](files/SparkPlug.json)) runs **two independent consumer legs off the
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

- `ConsumeMQTT` → **`ExtractDeviceId`** (`EvaluateJsonPath`, `device_id` from `$.device_id`) →
  **`PublishKafka-XiaoTelemetry`** — topic `xiao_telemetry`. The JSON publisher carries its own
  agent-class name in the payload, so the Kafka key resolves to the device's class identity
  (verified live: records keyed `MicroFi-1`).
- `ConsumeMQTTIIoT` → **`PublishKafka-SparkplugTelemetry`** — topic `sparkplug_telemetry`. On
  this leg the device identity travels in the Sparkplug topic segments
  (`spBv1.0/<group>/<type>/<edge-node>`), not a `device_id` attribute, so records currently
  carry a null key.

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

### Sample Flow — `files/SparkPlug.json`

The committed export, [`files/SparkPlug.json`](files/SparkPlug.json), is the field-run version
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

## Test Publishers

Two publisher scripts exercise the two legs independently. Both are plain Python against a
port-forwarded Mosquitto — no edge hardware required to validate the NiFi side of this pipeline.

### Plain JSON — Matches the `ConsumeMQTT` Leg

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

### Real Sparkplug B Binary — Matches the `ConsumeMQTTIIoT` Leg

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

### Terminal History — A Real Field Run

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

## Field Validation — What's Confirmed and What Isn't

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
- **A real hardware device (the same Seeed XIAO ESP32-S3 Sense from Chapter 20) publishing genuine
  Sparkplug B binary.** A practical low-footprint path for embedded Sparkplug B is confirmed: real
  firmware on a production microcontroller can speak spec-compliant Sparkplug B.
  [`mkeras/EmbeddedSparkplugNode`](https://github.com/mkeras/EmbeddedSparkplugNode) (a `nanopb`-based
  Sparkplug B encoder, MQTT-library-agnostic) dropped into the existing `xiao-telemetry.ino` sketch
  as a second, additive publish leg — the plain-JSON leg stayed unmodified and kept working
  side-by-side. The device published real `NBIRTH`/`NDATA` to
  `spBv1.0/XiaoTelemetry/{NBIRTH,NDATA}/XiaoESP32-01`, with one real metric (`Sensors/Temperature`,
  the same internal-temp sensor value the JSON leg already reports). Verified independently via NiFi
  provenance (not the firmware's own serial log): `ConsumeMQTTIIoT` routed both messages via its
  `Message` relationship (not `parse.failure`, i.e. NiFi's own parser validated them as real
  Sparkplug B), the raw wire bytes sent to Kafka contain the literal metric name
  `Sensors/Temperature` and a real float32 value (`42.79999923706055`, matching the JSON leg's
  `42.8` from the same tick), and a `SEND` provenance event confirms delivery to
  `my-cluster-kafka-bootstrap.cld-streaming.svc:9092/sparkplug_telemetry` — the same topic the
  `pysparkplug` simulator already proved reachable.
- **Native Sparkplug B decode at the edge on MiNiFi Java.** With the Cloudera CDF
  `nifi-cdf-iiot-mqtt-nar` (plus its dependency closure) side-loaded into a Java agent's
  `extensions/` directory, a `ConsumeMQTTIIoT` → `LogAttribute` flow published to the agent decoded a
  `pysparkplug` publisher's `NBIRTH`/`NDATA` messages *on the agent itself* — every message routed via
  the `Message` relationship (zero `parse.failure`), the topic namespace parsed into
  `mqtt.topic.segment.*` attributes, and the decoded output carried the correct metric names and
  float32 values (`NBIRTH` `Temperature=22.0`/`Humidity=50.0` matching the publisher exactly, `NDATA`
  values in the publisher's ranges). The CDF IIoT NAR ships no separate `MQTTIIoTReader` controller
  service — the decode is built into `ConsumeMQTTIIoT`, with `Record Reader`/`Record Writer` optional.
- **Sparkplug B publish from a MiNiFi Java agent via the native `PublishSparkplug` NAR** — the
  full origination chain (FlowFile JSON → NBIRTH/NDATA on the wire → decoded by the live
  `ConsumeMQTTIIoT`, `Message`-not-`parse.failure` → Kafka) ran end-to-end 2026-09-01 on a fresh
  EFM-managed Java agent. See "Publishing Sparkplug B from MiNiFi" above; wire capture, agent log,
  and Kafka sample in
  [`files/issue-138/`](https://github.com/cldr-steven-matison/DesktopShare/tree/main/files/issue-138).
- **The Primary Host Application / Rebirth-request behavior of `ConsumeMQTTIIoT` — fielded live
  2026-09-01, with a split verdict.** With `Primary Host Application=true` and
  `Send Rebirth Requests=true` (validation then requires a *literal* group in the topic filter —
  `spBv1.0/MicroFi/#`, a wildcard group is rejected — plus explicit `Node IDs`), the processor
  published its own `STATE` birth (`{"online": true, …}`) on schedule-start and a real **NCMD**
  carrying `Node Control/Rebirth = true` to `spBv1.0/MicroFi/NCMD/MicroFi-3`. The consumer side of
  the mechanism is field-verified. The *device* side is not honored by the current MicroFi firmware:
  it declares `Node Control/Rebirth` in its NBIRTH but never subscribes to its NCMD topic, so the
  node kept publishing NDATA and never re-birthed (wire capture:
  [`files/issue-138/rebirth-field-run-capture.txt`](https://github.com/cldr-steven-matison/DesktopShare/blob/main/files/issue-138/rebirth-field-run-capture.txt)).

**Explicitly not pursued, with reason:**
- Edge-side decode of Sparkplug B on a MiNiFi **C++** agent via custom Python (rather than
  relay-only) — not attempted, and effectively **moot** since native decode was field-confirmed on
  MiNiFi **Java** (above): Java is the production decode runtime everywhere this lab decodes at the
  edge, and the C++ agents' relay-only role stands. The custom-code path remains documented in the
  MiNiFi C++ section if a C++-only deployment ever needs it.

## All the Ways to Learn About EFM and Sparkplug

All the concrete ways to learn about EFM and Sparkplug that exist in this lab and its source
material, gathered in one place:

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
- **[`minifi-sparkplug-publish.md`](https://github.com/cldr-steven-matison/DesktopShare/blob/main/minifi-sparkplug-publish.md)**
  — the publish-side how-to: every route to originating Sparkplug B from a MiNiFi agent (Java/Tahu
  vs C++/pysparkplug/`.so`), compared, with skeletons and the end-to-end verify checklist.
- **[`nifi-sparkplug-bundle`](https://github.com/cldr-steven-matison/NiFi2-Processor-Playground/tree/main/nifi-sparkplug-bundle)**
  — the field-verified native Java `PublishSparkplug` processor itself: readable, unit-tested
  reference code for the NBIRTH/NDATA/NDEATH session state machine
  (one-pager: [`sparkplug-publish-processor.md`](https://github.com/cldr-steven-matison/DesktopShare/blob/main/sparkplug-publish-processor.md)).

## What NOT to Do

**Point `ConsumeMQTTIIoT` at a topic carrying plain JSON, or `ConsumeMQTT` at `spBv1.0/#` expecting
decoded output.** The two processors are not interchangeable — one expects Sparkplug B protobuf,
the other has no protobuf decode at all. Match the processor to the actual wire format on that
topic, per-leg, not per-flow.

**Assume a MiNiFi agent has a Sparkplug-aware processor because NiFi does.** It doesn't — not
by default, and not on C++ at all. `ConsumeMQTT`/`PublishMQTT` on the C++ agent are generic MQTT:
fine for relay, not for decode. On MiNiFi **Java**, `ConsumeMQTTIIoT` *is* loadable via a CDF IIoT
NAR drop-in and decodes Sparkplug B natively at the edge (confirmed above) — but it is not present
in the stock CEM tarball, so it's there only if you side-load the `nifi-cdf-iiot-mqtt-nar` closure.
Don't design an edge flow around native edge-side Sparkplug decode without first confirming the NAR
is actually present and loaded in your specific agent build.

**GET-then-PUT `ConsumeMQTT`/`ConsumeMQTTIIoT` when a broker password is set.** Same rule as every
other sensitive NiFi property in this guide — check `sensitive` in the descriptor before any
full-entity PUT, regardless of whether the field happens to read back masked or literally `null`.

**Treat a Sparkplug B message without a preceding `NBIRTH` as trustworthy.** The spec's entire
state model depends on the birth certificate establishing the full metric set first; `NDATA` before
`NBIRTH` (or after a missed sequence number) means a subscriber's view of that node's state may
already be wrong. This is what the Primary Host / Rebirth-request mechanism exists to correct —
don't build downstream logic that ignores `seq` gaps.

**Declare `Node Control/Rebirth` in an NBIRTH without subscribing to your own NCMD topic.** The
birth certificate advertises the rebirth control metric to every Primary Host on the broker; a
publisher that declares it but never listens for the NCMD (the current MicroFi firmware, per the
2026-09-01 field run) silently breaks the spec's recovery mechanism — the host's rebirth request
goes nowhere and its view of the node stays stale. Either subscribe and honor the request, or
don't declare the metric.

**Treat embedded Sparkplug B publish as unverified when the field record says otherwise.** The
Seeed XIAO ESP32-S3 has been verified publishing genuine Sparkplug B (`NBIRTH`/`NDATA`), decoded
by `ConsumeMQTTIIoT` in NiFi provenance — not inferred from the device's own serial log, but
confirmed via NiFi's own parse routing and the raw wire bytes arriving in Kafka. If a claim about
an embedded publisher doesn't cite NiFi-side verification, it isn't verified.

## Related Chapters

- Ch12 — [EFM + MicroFi](ch12-efm-and-microfi.md): the ESP32-class agent-enrollment side (device
  onboarding under EFM); this chapter assumes an already-enrolled or non-EFM edge publisher and
  focuses on the Sparkplug protocol/processor layer instead.
- Ch19 — [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md): the `ExecuteScript`/TensorRT
  edge-inference pattern referenced by the "edge intelligence" stretch design above.
- Ch20 — [SparkPlug B — MQTT/IIoT edge demo](ch20-sparkplug-demo.md): the end-to-end demo narrative
  this chapter is the technical reference for — real device, real incidents, live verification.
