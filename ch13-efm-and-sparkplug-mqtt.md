# Chapter 13: EFM and Sparkplug MQTT

This chapter is the protocol and processor reference for Sparkplug B. What the spec defines, how it rides on MQTT, what MiNiFi C++ and MiNiFi Java can and cannot do with it, and how NiFi's `ConsumeMQTTIIoT` processor decodes the binary payload. Read it before [Chapter 20](ch20-sparkplug-demo.md). That chapter tells the story of one edge device shipping telemetry through this pipeline. This chapter is the mechanics that story depends on.

## Prerequisites

- The CSO stack (NiFi, Kafka/Strimzi) running in minikube. The earlier EFM-on-Kubernetes chapters cover how that is deployed.
- A namespace to deploy Mosquitto into. This chapter uses `mqtt`, reachable from both NiFi and any MiNiFi or edge agent.
- Familiarity with EFM agent enrollment ([Chapter 19](ch19-efm-and-nvidia-jetson.md)) if you intend to run the MQTT leg on a MiNiFi C++ or MiNiFi Java agent as well as in NiFi.

## What Sparkplug B Is

Sparkplug B is an Eclipse specification for a protobuf-encoded MQTT payload format built for industrial IoT. It layers three things on top of plain MQTT that plain MQTT does not give you on its own.

**A defined topic namespace.** Every Sparkplug B message publishes to

```
spBv1.0/<group_id>/<message_type>/<edge_node_id>[/<device_id>]
```

| Segment | Meaning |
|---|---|
| `spBv1.0` | The fixed namespace and version prefix |
| `<group_id>` | A logical grouping of edge nodes (a factory line, a site) |
| `<message_type>` | One of the lifecycle message types below |
| `<edge_node_id>` | The identifier of the publishing edge device or gateway |
| `<device_id>` | Present only for device-scoped messages (`DBIRTH`/`DDATA`/`DDEATH`). Omitted for node-scoped messages (`NBIRTH`/`NDATA`/`NDEATH`) |

**A defined message lifecycle**, so a subscriber always knows the current state of every publisher without polling.

| Message type | Meaning |
|---|---|
| `NBIRTH` | Node birth certificate. An edge node announcing itself online, with its full initial metric set |
| `NDATA` | Node data. Incremental metric updates from an already-born node |
| `NDEATH` | Node death certificate. The node going offline (published by the broker, via MQTT Last Will and Testament, if the node disconnects uncleanly) |
| `DBIRTH` | Device birth. A sub-device under a node announcing itself, with its metric set |
| `DDATA` | Device data. Incremental updates from a device |
| `DDEATH` | Device death |
| `STATE` | Primary host application online/offline state (see "Primary Host Application" below) |

The birth and death pattern is the point of the spec. A subscriber that comes online after an edge node has been publishing for hours does not need to guess the node's current metric set. The most recent `NBIRTH` on that topic (retained by the broker) has the full state, and every `NDATA` since is a diff against it.

**A binary payload.** The message body is a Google Protobuf-encoded `Payload` message. JSON never appears on the wire. Each metric carries a name, a datatype enum, a value, and a timestamp, plus a monotonically increasing sequence number (`seq`, 0 to 255, wraps) that lets a subscriber detect a dropped message. This is why Sparkplug B needs a purpose-built decoder. A generic MQTT-to-JSON processor cannot read it. The bytes on the wire are not human-readable, and a `ConsumeMQTT` plus `EvaluateJsonPath` pattern does not work against them.

Where this sits in the stack. A device (a sensor or an edge agent) publishes Sparkplug B over MQTT to a broker. Something downstream, a MiNiFi agent or NiFi directly, subscribes, decodes the protobuf, and forwards the result (typically as JSON) to Kafka for everything else in the CSO stack to consume. The pattern is the same shape as every other edge-to-NiFi flow in this guide. A lightweight edge protocol in, a normalized record out.

## Broker, Mosquitto in Minikube

Both the NiFi ingestion leg and any edge MQTT publisher need a broker they can both reach. This chapter deploys Eclipse Mosquitto into its own namespace in the same cluster NiFi runs in. No new infrastructure, it reuses the existing minikube.

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

Note the assigned NodePort (typically in the `30000+` range). An off-cluster publisher connects to `<minikube-ip>:<nodeport>`. From your own workstation, a port-forward is usually easier than routing through the NodePort.

```bash
# find the actual pod name first
kubectl get pods -n mqtt

kubectl port-forward pod/mosquitto-<pod-suffix> 1883:1883 -n mqtt
```

`persistence true` matters because of the birth and death pattern above. Mosquitto needs to retain the last-seen state for `NBIRTH` messages published with the MQTT retain flag, so a late subscriber gets current state immediately, without waiting for the next `NDATA`. Bare `allow_anonymous true` with no auth is a lab-only posture. Fine for this cluster. Revisit it before any production deployment.

## The MiNiFi C++ Side, Stock MQTT and No Sparkplug Processor

This is the detail that catches people coming from the NiFi side. There is no `ConsumeMQTTIIoT` equivalent on MiNiFi C++. The C++ agent's MQTT support ships stock, in the base image, as `libminifi-mqtt-extensions.so`, and it exposes exactly two processors.

| Processor | What it does |
|---|---|
| `ConsumeMQTT` | Subscribes to an MQTT topic (filter), emits one FlowFile per received message with the raw payload bytes as FlowFile content |
| `PublishMQTT` | Publishes a FlowFile's content to an MQTT topic |

Both are generic MQTT processors. Neither one knows anything about Sparkplug B's protobuf schema. `ConsumeMQTT` subscribed to `spBv1.0/#` will happily deliver FlowFiles whose content is raw Sparkplug B protobuf bytes, but MiNiFi C++ has no stock processor that decodes those bytes into usable fields. Decoding a Sparkplug payload on the MiNiFi C++ side would take a custom Python processor (see [Chapter 6](ch06-minifi-custom-python-processors.md)) linking a protobuf library against the compiled `.proto` schema, or an `ExecuteScript` doing the same. Nothing like this ships today.

What this means in practice. If a MiNiFi C++ agent needs to act on Sparkplug B content at the edge, the decode step has to happen in custom code on that agent. If the agent's job is to relay Sparkplug B onward, `ConsumeMQTT` to `PublishMQTT` (or `PublishKafka` for raw bytes) works as an opaque pass-through. MiNiFi never needs to understand the payload to move it.

This asymmetry, full protocol support on the NiFi side and relay-only on the MiNiFi C++ side, is why the reference architecture in this chapter and in Chapter 20 puts the decode in NiFi and not at the edge. The edge agent's job is getting bytes off the wire reliably. NiFi's job is understanding what they mean.

## The MiNiFi Java Side, Native Sparkplug Decode at the Edge

Unlike C++, MiNiFi Java can decode Sparkplug B at the edge. The `ConsumeMQTTIIoT` processor is not in the stock CEM `2.24.08.0-19` tarball (the Java processor catalog carries no MQTT, IIoT, or Sparkplug component out of the box), but it loads on a Java agent the same way the Kafka and scripting NARs do. Drop the Cloudera CDF IIoT NAR into the agent's `extensions/` autoload directory.

**The NAR and its dependency closure.** `ConsumeMQTTIIoT` ships in the Cloudera-proprietary `nifi-cdf-iiot-mqtt-nar`. It is parcel-only and absent from the open-source `-extension` bundle (which carries only the Apache `nifi-mqtt-nar`). Side-load the NAR with its full dependency closure, all at the same `group:id:version` (on CFM 4.12.0, the `2.6.0.4.12.0.x` set).

```
nifi-cdf-iiot-mqtt-nar
  └ nifi-mqtt-nar
      └ nifi-standard-shared-nar
          └ nifi-standard-services-api-nar
```

Restart the agent (or let the autoloader pick them up) and `ConsumeMQTTIIoT` resolves as a type in the agent manifest and the EFM Designer palette. `NarUnpacker` fails the entire batch if any one side-loaded NAR is malformed, so check that each is a clean archive. These are the same drop-in mechanics used for PLC4X and IIoT on Java elsewhere in this guide.

**There is no separate `MQTTIIoTReader` controller service in the CDF IIoT NAR.** This surprises people coming from full NiFi. The NAR ships exactly one component, the `ConsumeMQTTIIoT` processor, and the Sparkplug B protobuf decode is built into it. `Record Reader` and `Record Writer` are optional properties (and must be set together if used at all). None is required to decode. Point `ConsumeMQTTIIoT` at `spBv1.0/#` with just a Broker URI and it decodes on its own.

**What decode looks like on the agent.** With the NAR loaded and a `ConsumeMQTTIIoT` to `LogAttribute` flow published to the agent, a `pysparkplug` publisher's `NBIRTH`/`NDATA` messages decode at the edge exactly as they do in NiFi. Every message routes via the `Message` relationship (not `parse.failure`), the topic namespace is parsed into `mqtt.topic.segment.*` attributes (`spBv1.0`, group, message type, edge node), and the decoded output carries the metric names and float32 values from the publisher (`Temperature`/`Humidity` matching the `NBIRTH` and the `NDATA` value ranges).

**Relay is still an option.** If you do not want to carry the CDF NAR, the C++-style relay pattern works on Java too. `ConsumeMQTT` (generic MQTT, no protobuf decode) subscribes to `spBv1.0/#` and forwards raw bytes to Kafka or NiFi for downstream decode.

## Publishing Sparkplug B from MiNiFi

Everything above is the consume and decode side. The publish side has no stock answer on either MiNiFi flavor. The CDF `nifi-cdf-iiot-mqtt-nar` ships exactly one component (`ConsumeMQTTIIoT`, consume-only), and stock `PublishMQTT` moves raw bytes with no idea what a birth certificate or a `seq` counter is. An edge agent that needs to originate Sparkplug B has to encode the protobuf and run the session state machine itself.

There are two routes. MiNiFi Java via Eclipse Tahu, as an `ExecuteScript` prototype or a custom NAR. Or MiNiFi C++ via embedded-CPython `pysparkplug` or a custom `.so` vendoring Tahu-C/nanopb, the direct analog of MicroFi's own C++ `PublishSparkplug`.

**The route to use is a native Java `PublishSparkplug` processor**, the [`nifi-sparkplug-bundle`](https://github.com/cldr-steven-matison/NiFi2-Processor-Playground/tree/main/nifi-sparkplug-bundle). One FlowFile of flat JSON metrics in, spec-compliant Sparkplug B out. NBIRTH first (declaring `bdSeq` and `Node Control/Rebirth`), NDATA per FlowFile, NDEATH registered as the MQTT will, `bdSeq`/`seq` (0 to 255 wrap) managed internally. Eclipse Tahu does the encode, Paho the transport, and the NAR is self-contained with no parent NAR to line up.

Side-load the NAR onto an EFM-managed MiNiFi Java agent, publish a two-node Designer flow (`GenerateFlowFile({"Sensors/Temperature": 22.5, …})` to `PublishSparkplug`), and the wire shows an NBIRTH (seq=0) then NDATA with advancing `seq`, which the NiFi `ConsumeMQTTIIoT` accepts via its `Message` relationship all the way into Kafka.

Two side-load mechanics to know before doing it.

- A hot-loaded NAR does not refresh the agent's C2 manifest. The Java agent's `NarAutoLoader` picks the NAR up from `extensions/` within seconds (`Loaded extensions for com.example:nifi-sparkplug-nar`), but the manifest it heartbeats to EFM is built at startup. The new processor does not appear in the Designer palette until the agent restarts.
- Pinning the refreshed manifest to the class uses `POST /efm/api/agent-class-manifest-config` with field name `agentClassName` (not `agentClass`), after which the Designer resolves the new type.

## The NiFi Side, `ConsumeMQTTIIoT`

NiFi ships a processor purpose-built for this, `ConsumeMQTTIIoT`. Unlike generic `ConsumeMQTT`, it understands the Sparkplug B protobuf schema and decodes `NBIRTH`/`NDATA`/`NDEATH`/`DBIRTH`/`DDATA`/`DDEATH` payloads into structured records. No separate schema registry, no manual protobuf-to-JSON step.

Two behaviors to know before wiring it into a flow.

**It can act as a Sparkplug B Primary Host Application.** The spec defines this role. A subscriber that publishes its own `STATE` messages (online/offline) so edge nodes know whether a primary consumer is listening, and that can issue a Rebirth request, asking an edge node to republish a fresh `NBIRTH` (its full current state) on demand, without waiting for the node's own reconnect cycle. With `Primary Host Application=true` and `Send Rebirth Requests=true`, the processor publishes its own `STATE` birth (`{"online": true, …}`) on schedule start and an NCMD carrying `Node Control/Rebirth = true` to `spBv1.0/<group>/NCMD/<edge_node_id>`. Validation then requires a literal group in the topic filter (`spBv1.0/MicroFi/#`, a wildcard group is rejected) plus explicit `Node IDs`. The device side has to hold up its end. A publisher that declares `Node Control/Rebirth` in its NBIRTH but never subscribes to its own NCMD topic keeps publishing NDATA and never re-births.

**The topic filter is the Sparkplug namespace pattern.** Typically `spBv1.0/#` to catch every group, node, and device on the broker, or scoped narrower (`spBv1.0/FactoryLine1/#`) once you know which groups you care about.

### Two-Leg Process-Group Pattern

The NiFi process group for this chapter's material (exported at [`files/SparkPlug.json`](files/SparkPlug.json)) runs two independent consumer legs off the same broker, because two different kinds of publishers exist in this lab at once. A plain-JSON publisher and a Sparkplug B binary publisher.

```
ConsumeMQTT        (Topic Filter: test/sensor/data)   → PublishKafka  (topic: xiao_telemetry)
ConsumeMQTTIIoT     (Topic Filter: spBv1.0/#)          → PublishKafka  (topic: sparkplug_telemetry)
```

`ConsumeMQTT` takes plain JSON payloads on `test/sensor/data`. This is the path for any device that wants to publish JSON without adopting the full Sparkplug spec. No protobuf, no birth and death lifecycle, a flat JSON object per message. `parse.failure` routes off to a dead end for anything malformed.

`ConsumeMQTTIIoT` takes Sparkplug B binary on `spBv1.0/#`. This is the spec-compliant path. Every message on this leg went through an `NBIRTH`/`NDATA` lifecycle and protobuf encoding.

Both legs terminate in their own `PublishKafka` processor, each with its own topic.

| Leg | Chain | Kafka topic | Key |
|---|---|---|---|
| JSON | `ConsumeMQTT` → `ExtractDeviceId` (`EvaluateJsonPath`, `device_id` from `$.device_id`) → `PublishKafka-XiaoTelemetry` | `xiao_telemetry` | `${device_id}`. The JSON publisher carries its own agent-class name in the payload, so the key resolves to the device's class identity (`MicroFi-1`) |
| Sparkplug B | `ConsumeMQTTIIoT` → `PublishKafka-SparkplugTelemetry` | `sparkplug_telemetry` | Null. The device identity travels in the Sparkplug topic segments (`spBv1.0/<group>/<type>/<edge-node>`), not a `device_id` attribute |

Both point at the same broker, `my-cluster-kafka-bootstrap.cld-streaming.svc:9092`, `PLAINTEXT`, no SASL. The same Kafka connection settings the other live processors in this cluster use.

Running both legs side by side in one process group is deliberate. It lets a JSON-only device (no Sparkplug library, no protobuf dependency) and a spec-compliant Sparkplug B device coexist on the same broker and land in Kafka as two separated topics, with no need to force every edge publisher onto the heavier spec just to get data in.

Why two legs and two processors. `ConsumeMQTTIIoT` expects Sparkplug B's protobuf wire format. Point it at a topic carrying plain JSON and it fails to decode every message. `ConsumeMQTT` has no protobuf decode at all. Point it at `spBv1.0/#` and it delivers undecoded binary downstream. The topic filter is the dispatch key between spec-compliant Sparkplug and anything simpler that just wants a broker.

### Sample Flow, `files/SparkPlug.json`

The committed export, [`files/SparkPlug.json`](files/SparkPlug.json), is the process group above. Import it directly. There is no need to rebuild the two legs from scratch.

```bash
curl -k -u "$NIFI_USER:$NIFI_PASS" \
  -F "file=@files/SparkPlug.json" \
  "https://<nifi-host>/nifi-api/process-groups/<root-pg-id>/process-groups/upload"
```

> **⚠️ Never GET-then-PUT a processor with sensitive properties.** Both `ConsumeMQTT` and `ConsumeMQTTIIoT` have a `Password` property. If your broker has auth configured, check `descriptors[...].sensitive` before any full-entity PUT against a live processor. A masked value (`********`, or in this pair's case a literal `null`) written straight back destroys the credential. Bind the broker password through a Parameter Context and leave the processor entity alone. [Chapter 20](ch20-sparkplug-demo.md) covers this against this exact processor pair.

## Test Publishers

Two publisher scripts exercise the two legs independently. Both are plain Python against a port-forwarded Mosquitto. No edge hardware is required to validate the NiFi side of this pipeline.

### Plain JSON, Matches the `ConsumeMQTT` Leg

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

Sample output.

```
Connecting to Mosquitto broker at localhost:1883...
Successfully connected! Publishing data to topic 'test/sensor/data' every 2 seconds. Press Ctrl+C to stop.
Published: {'device_id': 'MacMockSensor-01', 'temperature': 22.43, 'humidity': 53.29, 'timestamp': 1781614422}
Published: {'device_id': 'MacMockSensor-01', 'temperature': 24.88, 'humidity': 51.33, 'timestamp': 1781614424}
Published: {'device_id': 'MacMockSensor-01', 'temperature': 21.82, 'humidity': 41.39, 'timestamp': 1781614426}
```

### Sparkplug B Binary, Matches the `ConsumeMQTTIIoT` Leg

This is the important one for the protobuf decode path. It constructs spec-compliant `NBIRTH`/`NDATA` messages via `pysparkplug` and publishes them binary-encoded to the namespace-prefixed topics.

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

Run it against the port-forwarded broker.

```bash
source venv/bin/activate
python sparkplug_test_publisher.py
```

Sample output.

```
Connecting to Mosquitto broker at localhost:1883...
Publishing binary Sparkplug B NBIRTH payload...
Node is ONLINE. Sending NDATA every 5 seconds. Press Ctrl+C to stop.
Sent Sparkplug NDATA (Seq: 1) -> Temp: 28.87 | Humid: 49.59
Sent Sparkplug NDATA (Seq: 2) -> Temp: 26.07 | Humid: 51.89
Sent Sparkplug NDATA (Seq: 3) -> Temp: 31.36 | Humid: 46.76
Sent Sparkplug NDATA (Seq: 4) -> Temp: 21.02 | Humid: 46.02
```

Notice the `seq` field in the printed output tracks 1, 2, 3, 4. That is the sequence number Sparkplug B's spec defines so a subscriber can detect a dropped or out-of-order message. A gap in that counter is the signal to request a rebirth. Silently trusting stale state is the failure the counter exists to catch.

## What Runs Where

| Capability | Where |
|---|---|
| Mosquitto broker | Minikube, `mqtt` namespace, reachable from NiFi and from off-cluster agents over the NodePort or a port-forward |
| Plain-JSON publish | `mqtt_test_publisher.py`, and the Seeed XIAO ESP32-S3 (`MicroFi-1`) on `test/sensor/data` |
| Sparkplug B publish | `sparkplug_test_publisher.py` (`pysparkplug`), the XIAO ESP32-S3 Sense (`MicroFi-3`, `EmbeddedSparkplugNode`/nanopb), and MiNiFi Java via the `PublishSparkplug` NAR |
| Sparkplug B decode | NiFi `ConsumeMQTTIIoT` (stock), and MiNiFi Java `ConsumeMQTTIIoT` with the CDF IIoT NAR side-loaded |
| Relay without decode | MiNiFi C++ `ConsumeMQTT` to `PublishMQTT`/`PublishKafka`, and MiNiFi Java `ConsumeMQTT` |
| Primary Host and Rebirth request | NiFi `ConsumeMQTTIIoT` publishes `STATE` and NCMD. Honored only by a device that subscribes to its own NCMD topic |
| Kafka landing | `xiao_telemetry` (JSON leg, keyed by `device_id`) and `sparkplug_telemetry` (Sparkplug leg) |

Embedded Sparkplug B is a small-footprint path. [`mkeras/EmbeddedSparkplugNode`](https://github.com/mkeras/EmbeddedSparkplugNode), a `nanopb`-based Sparkplug B encoder that is MQTT-library-agnostic, drops into an existing XIAO sketch as a second, additive publish leg while the plain-JSON leg keeps working side by side. The device publishes `NBIRTH`/`NDATA` to `spBv1.0/XiaoTelemetry/{NBIRTH,NDATA}/XiaoESP32-01` with `Sensors/Temperature` as a float32 metric (the same internal-temperature value the JSON leg reports), and NiFi's `ConsumeMQTTIIoT` routes both messages via `Message`. Check the NiFi side when you want to know whether a device's Sparkplug B is well-formed. The firmware's own serial log only shows what it tried to send. The parser routing and the raw wire bytes arriving in Kafka are what count.

Edge-side decode on a MiNiFi C++ agent via custom Python is not built. Java is the decode runtime everywhere this lab decodes at the edge, and the C++ agents relay. The custom-code path stays documented in the C++ section above if a C++-only deployment ever needs it.

## Where to Learn More

- [Chapter 20](ch20-sparkplug-demo.md) has the end-to-end demo narrative. A device, a process-group-loss incident and recovery, a topic-contamination incident and fix, and the live verification technique (do not trust a device's own serial log).
- [Chapter 19](ch19-efm-and-nvidia-jetson.md) has the `ExecuteScript`/TensorRT edge-inference pattern the Sparkplug edge-intelligence phase reuses.
- [Chapter 6](ch06-minifi-custom-python-processors.md) shows what it would take to add a Sparkplug B decoder as a MiNiFi C++ custom processor.
- [`files/SparkPlug.json`](files/SparkPlug.json) is the importable process group. Reading its processor configuration in the NiFi UI after import is often faster than re-deriving property values from prose.
- The Sparkplug B specification (Eclipse Tahu / Sparkplug specification, published by the Eclipse Foundation). This chapter covers the subset relevant to this lab's flows (`NBIRTH`/`NDATA`/`NDEATH`, the topic namespace, the sequence number). The full spec also defines `DBIRTH`/`DDATA`/`DDEATH` device-scoped semantics and the Primary Host `STATE` mechanism in more depth than reproduced here.
- `pysparkplug`'s own source and docs. Its `Metric`/`DataType`/`NBirth`/`NData` API surface is the practical on-ramp for writing another Sparkplug B publisher without hand-rolling protobuf encoding.
- [`nifi-sparkplug-bundle`](https://github.com/cldr-steven-matison/NiFi2-Processor-Playground/tree/main/nifi-sparkplug-bundle) is the native Java `PublishSparkplug` processor. Readable, unit-tested reference code for the NBIRTH/NDATA/NDEATH session state machine.

## What NOT to Do

**Do not point `ConsumeMQTTIIoT` at a topic carrying plain JSON, or `ConsumeMQTT` at `spBv1.0/#` expecting decoded output.** The two processors are not interchangeable. One expects Sparkplug B protobuf, the other has no protobuf decode at all. Match the processor to the wire format on that topic, leg by leg.

**Do not assume a MiNiFi agent has a Sparkplug-aware processor because NiFi does.** Not by default, and not on C++ at all. `ConsumeMQTT`/`PublishMQTT` on the C++ agent are generic MQTT, fine for relay and useless for decode. On MiNiFi Java, `ConsumeMQTTIIoT` is loadable via the CDF IIoT NAR drop-in and decodes Sparkplug B at the edge, but it is not in the stock CEM tarball. It is there only if you side-load the `nifi-cdf-iiot-mqtt-nar` closure. Check the NAR is present and loaded in your specific agent build before designing an edge flow around native decode.

**Do not GET-then-PUT `ConsumeMQTT`/`ConsumeMQTTIIoT` when a broker password is set.** Same rule as every other sensitive NiFi property in this guide. Check `sensitive` in the descriptor before any full-entity PUT, regardless of whether the field reads back masked or literally `null`.

**Do not treat a Sparkplug B message without a preceding `NBIRTH` as trustworthy.** The spec's state model depends on the birth certificate establishing the full metric set first. `NDATA` before `NBIRTH`, or after a missed sequence number, means a subscriber's view of that node's state may already be wrong. This is what the Primary Host and Rebirth-request mechanism exists to correct. Do not build downstream logic that ignores `seq` gaps.

**Do not declare `Node Control/Rebirth` in an NBIRTH without subscribing to your own NCMD topic.** The birth certificate advertises the rebirth control metric to every Primary Host on the broker. A publisher that declares it but never listens for the NCMD silently breaks the spec's recovery mechanism. The host's rebirth request goes nowhere and its view of the node stays stale. Either subscribe and honor the request, or do not declare the metric.

## Related Chapters

- [EFM + MicroFi](ch12-efm-and-microfi.md) (Ch12): the ESP32-class agent-enrollment side (device onboarding under EFM). This chapter assumes an already-enrolled or non-EFM edge publisher and focuses on the Sparkplug protocol and processor layer instead.
- [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md) (Ch19): the `ExecuteScript`/TensorRT edge-inference pattern referenced by the edge-intelligence design above.
- [SparkPlug B, MQTT/IIoT edge demo](ch20-sparkplug-demo.md) (Ch20): the end-to-end demo narrative this chapter is the technical reference for.
