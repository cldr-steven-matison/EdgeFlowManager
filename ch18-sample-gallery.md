# Chapter 18: Sample Gallery of MiNiFi Flows

A runnable set of MiNiFi flows collected as the guide was built. Each entry is a flow that runs somewhere in this guide. This chapter gathers them behind one consistent card and does not invent new ones. A flow gets a card here once its own chapter is complete.

The runnable home for these flows is the [`sample-gallery/`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/tree/main/sample-gallery) directory in the MiNiFi Kubernetes Playground repo. This chapter is the narrative. The Playground's `sample-gallery/README.md` is the runnable index that links the configs. Configs live once, at the repo root, and each card here links to them.

## Card Format

Every entry uses the same card so the gallery reads consistently.

| Field | What goes in it |
|---|---|
| Name | Short and googlable |
| Purpose | One line, what it is for |
| Agent | C++ or Java, version, class, standalone or EFM-managed |
| Shape | The processor chain |
| Files | `config.yml` or the exported flow JSON |
| Verify | The exact commands that show it runs |
| Chapter | Where the full walkthrough lives |

---

## 1. HTTP to Kafka and File (MiNiFi C++, Standalone)

| Field | Value |
|---|---|
| Name | `http-to-kafka-cpp` |
| Purpose | Accept an HTTP POST at the edge and fan it out to a Kafka topic and a local file in one flow. |
| Agent | MiNiFi C++ `1.26.02` (`container.repo.cloudera.com/cloudera/apacheminificpp:latest`), standalone `config.yml` baked at image build time, no EFM. |
| Files | [`config.yml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/config.yml) · [`Dockerfile`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/Dockerfile) · [`minifi-test.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test.yaml) (NodePort 30080) |
| Chapter | [Chapter 7](ch07-standalone-minifi-cpp-on-k8s.md) |

#### Shape

The flow is a fork, not a chain. Both connections carry `ListenHTTP`'s `success` relationship.

```
ListenHTTP ─┬─(success)─→ PublishKafka   (topic test-minifi, broker my-cluster-kafka-bootstrap.cld-streaming.svc:9092)
            └─(success)─→ PutFile         (/tmp/minifi-test-output)
```

#### Verify

```bash
# 1. open the network tunnel (required on macOS — NodePort not directly reachable)
minikube service minifi-test-service --url

# 2. POST a payload (use the tunnel port from step 1)
curl -i -X POST http://127.0.0.1:<TUNNEL_PORT>/contentListener \
     -H "Content-Type: application/json" \
     -d '{"test_id": "integration-success", "message": "Flow is functional"}'

# 3. confirm delivery to Kafka
kubectl run kafka-viewer -it --rm \
  --image=quay.io/strimzi/kafka:latest-kafka-3.7.0 --restart=Never \
  -- bin/kafka-console-consumer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.cld-streaming.svc:9092 \
  --topic test-minifi --from-beginning --timeout-ms 10000

# 4. confirm PutFile also wrote the payload
kubectl exec -it deployment/minifi-test -- /bin/sh -c "cat /tmp/minifi-test-output/*"
```

> **⚠️ C++ config requirements.** Every processor and connection needs an explicit `id` UUID. Class names are C++ short names (`ListenHTTP`, `PublishKafka`, `PutFile`). Java FQCNs do not work. `PublishKafka` requires a non-empty `Client Name`. The readiness probe path is `/contentListener`, not `/` or `/health`.

---

## 2. HTTP to File (MiNiFi Java, Standalone)

| Field | Value |
|---|---|
| Name | `http-to-file-java` |
| Purpose | Accept an HTTP POST at the edge and persist it to a local file. No Kafka, because the stock Java image ships no Kafka NAR. |
| Agent | MiNiFi Java `1.23.04-b15` (`container.repo.cloudera.com/cloudera/nifi-minifi-java:latest`), standalone `config-java.yml` (`MiNiFi Config Version: 3`), no EFM. |
| Files | [`config-java.yml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/config-java.yml) · [`Dockerfile.java`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/Dockerfile.java) · [`minifi-test-java.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test-java.yaml) (NodePort 30081) |
| Chapter | [Chapter 8](ch08-minifi-java-setup.md) |

#### Shape

```
ListenHTTP ─(success)─→ PutFile   (/tmp/minifi-test-output)
```

#### Verify

```bash
minikube service minifi-test-java-service --url

curl -i -X POST http://127.0.0.1:<TUNNEL_PORT>/contentListener \
     -H "Content-Type: application/json" \
     -d '{"test_id": "integration-success", "message": "Flow is functional"}'

kubectl exec -it deployment/minifi-test-java -- /bin/sh -c "cat /tmp/minifi-test-output/*"
```

> **⚠️ Java config gotchas.** Connections wire by `source id`/`destination id` UUID, not by name. Processor `class` is fully qualified. The readiness and liveness probes must be `tcpSocket`, not `httpGet`. Java's `ListenHTTP` returns `405` to a bare `GET`, and an `httpGet` probe crash-loops the pod.

---

## 3. EFM-Managed Smoke Flow (MiNiFi C++, Level 2)

| Field | Value |
|---|---|
| Name | `efm-level2-playground-cpp` |
| Purpose | Exercise EFM C2 wiring end to end in the `default` namespace using a bare Ubuntu pod. `GenerateFlowFile` emits a heartbeat every 10 seconds and `LogAttribute` shows the agent is receiving and executing EFM-published flows. |
| Agent | MiNiFi C++ `1.26.02`, EFM-managed agent class `PlaygroundCpp`, installed from bare `ubuntu:22.04` via the EFM agent-deployer script (no custom image). EFM `2.3.1.0-2` in the `cld-streaming` namespace. |
| Files | [`minifi-test-efm-cpp.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test-efm-cpp.yaml) · [`files/efm/PlaygroundCpp.json`](files/efm/PlaygroundCpp.json) (exported flow) |
| Chapter | [Chapter 9](ch09-efm-in-the-playground.md) |

#### Shape

```
GenerateFlowFile (10 sec, Custom Text: "PlaygroundCpp Level 2 heartbeat")
  ─(success)─→ LogAttribute
```

#### Verify

```bash
# confirm the agent reached ONLINE in EFM Monitor → Agents
# (check EFM UI at http://127.0.0.1:10090/efm/ui)

# confirm LogAttribute output in pod logs
kubectl logs minifi-test-efm-cpp -n default | grep LogAttribute
# expected: LogAttribute -- filename: <uuid>, content: PlaygroundCpp Level 2 heartbeat
```

> **⚠️ EFM health poll required.** On a cold start EFM takes up to two minutes to bind its Jetty listener. Both manifests poll `/efm/actuator/health` in a loop before running the deployer command. Skip the poll and the agent never enrolls. Both `flowId` and `pgId` are required in the processor-create API path. Using only `pgId` returns a misleading Spring 404.

---

## 4. EFM-Managed Smoke Flow (MiNiFi Java, Level 2)

| Field | Value |
|---|---|
| Name | `efm-level2-playground-java` |
| Purpose | The Java counterpart to Entry 3. Exercises EFM C2 enrollment and flow delivery to a MiNiFi Java agent with the same bare-pod bootstrap pattern. |
| Agent | MiNiFi Java `2.24.08.0-19`, EFM-managed agent class `PlaygroundJava`, installed from bare `ubuntu:22.04` via the EFM agent-deployer script (requires `openjdk-11-jre-headless`). EFM `2.3.1.0-2` in the `cld-streaming` namespace. |
| Files | [`minifi-test-efm-java.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test-efm-java.yaml) · [`files/efm/PlaygroundJava.json`](files/efm/PlaygroundJava.json) (exported flow) |
| Chapter | [Chapter 9](ch09-efm-in-the-playground.md) |

#### Shape

```
GenerateFlowFile (10 sec, Custom Text: "PlaygroundJava Level 2 heartbeat")
  ─(success)─→ LogAttribute
```

#### Verify

```bash
kubectl logs minifi-test-efm-java -n default | grep LogAttribute
# expected: LogAttribute -- filename: <uuid>, content: PlaygroundJava Level 2 heartbeat
```

---

## 5. S2S Source (MiNiFi C++, EFM-Managed, K8s to NiFi)

| Field | Value |
|---|---|
| Name | `s2s-cpp-to-nifi-k8s` |
| Purpose | Transmit FlowFiles from a MiNiFi C++ agent running in Kubernetes to a CFM-operator-managed NiFi in the same cluster over secure HTTP Site-to-Site. Covers the full C++ S2S path, an EFM-authored flow, a cert-mounted client identity, and `User` CR authorization on NiFi. |
| Agent | MiNiFi C++ `1.26.02`, EFM-managed (any C++ agent class with the S2S properties in `minifi.properties`). Client SSL is global, set as `nifi.security.client.certificate/private.key/ca.certificate` in `minifi.properties`. There is no per-RPG SSL context service field in C++. |
| Files | No committed C++-specific flow file. The EFM-authored flow is built in the Designer and published per agent class. [`files/site-to-site/`](files/site-to-site/) holds the shared S2S reference assets and [`files/site-to-site/SITE_TO_SITE.md`](files/site-to-site/SITE_TO_SITE.md) is the directory guide. NiFi-side manifests (cert, user CR, web service) live in [`files/site-to-site/ch11-java/`](files/site-to-site/ch11-java/) and apply unchanged to the C++ leg. |
| Chapter | [Chapter 11](ch11-site-to-site.md) |

#### Shape

```
GenerateFlowFile
  ─(success)─→ RemoteProcessGroup  (targetUris: https://nifi-web.<ns>.svc.cluster.local:8443,
                                    transportProtocol: HTTP,
                                    destination: from-minifi input port UUID)
```

#### Verify

```bash
# agent log confirms each transaction
grep "Site to Site transaction" /path/to/minifi-app.log
# expected: "sent flow 1 flow records, with total size <N>"
# expected: "peer finished transaction"

# NiFi side: queued count climbs on the from-minifi input port
curl -s --cert /certs/tls.crt --key /certs/tls.key --cacert /certs/ca.crt \
  "https://nifi-web.<ns>.svc.cluster.local:8443/nifi-api/flow/process-groups/root/status?recursive=true" \
  | grep -oE '"(flowFilesReceived|queued)":("[^"]*"|[0-9]+)'
```

> **⚠️ C++ S2S client SSL is global, not per-processor.** Set `nifi.security.client.certificate`, `nifi.security.client.private.key`, and `nifi.security.client.ca.certificate` in `minifi.properties`. The EFM deployer may overwrite `minifi.properties` on pod restart, so bake the keys into your boot script. The `from-minifi` input port must be RUNNING, and the `User` CR must reference the cert's SAN (not the subject DN) and the exact port UUID, before the agent can complete its first transaction.

---

## 6. S2S Source (MiNiFi Java, Standalone K8s to NiFi)

| Field | Value |
|---|---|
| Name | `s2s-java-to-nifi-k8s` |
| Purpose | Transmit FlowFiles from a MiNiFi Java agent running as a standalone Kubernetes pod to a CFM-operator-managed NiFi in the same cluster over secure HTTP Site-to-Site. The agent runs without EFM. Flow and config are baked into a custom image. Covers the Java S2S path, `bootstrap.conf` SSL wiring, the `flow.json.raw` bake-in, and `User` CR authorization on NiFi. |
| Agent | MiNiFi Java `2.24.08.0-19`, standalone (no EFM). Client SSL is set in `bootstrap.conf` with `nifi.minifi.flow.use.parent.ssl=true`. MiNiFi Java regenerates `minifi.properties` from `bootstrap.conf` on every start, so direct edits to `minifi.properties` are wiped. |
| Chapter | [Chapter 11](ch11-site-to-site.md) |

#### Files

| File | What it is |
|---|---|
| [`files/site-to-site/ch11-java/bootstrap.conf`](files/site-to-site/ch11-java/bootstrap.conf) | SSL config with `use.parent.ssl=true` |
| [`files/site-to-site/ch11-java/Dockerfile`](files/site-to-site/ch11-java/Dockerfile) | Bakes `flow.json.raw`, `flow.json.gz`, `flow-identifier`, `bootstrap.conf`, and the truststore |
| [`files/site-to-site/ch11-java/minifi-java-unmanaged.yaml`](files/site-to-site/ch11-java/minifi-java-unmanaged.yaml) | Pod manifest, mounts the client keystore from a Secret |
| [`files/site-to-site/ch11-java/minifi-s2s-cert.yaml`](files/site-to-site/ch11-java/minifi-s2s-cert.yaml) | cert-manager Certificate for the agent client identity (SAN `minifi-s2s`) |
| [`files/site-to-site/ch11-java/minifi-s2s-user.yaml`](files/site-to-site/ch11-java/minifi-s2s-user.yaml) | `User` CR granting write on the `from-minifi` input port and read on `/site-to-site` |
| [`files/site-to-site/ch11-java/nifi-web-svc.yaml`](files/site-to-site/ch11-java/nifi-web-svc.yaml) | The `nifi-web` ClusterIP service (the operator does not create this) |
| [`files/site-to-site/ch11-java/README.md`](files/site-to-site/ch11-java/README.md) | Build and apply sequence |

#### Shape

```
GenerateFlowFile
  ─(success)─→ RemoteProcessGroup  (targetUris: https://nifi-web.<ns>.svc.cluster.local:8443,
                                    transportProtocol: HTTP,
                                    destination: from-minifi input port UUID)
```

#### Verify

```bash
# Java agent log — peer refresh and each send
kubectl logs <minifi-java-pod> | grep -E "Successfully refreshed|Successfully sent"
# expected: "Successfully refreshed Flow Contents for RemoteProcessGroup[https://nifi-web…]"
# expected: "Successfully sent [...] (32 bytes) to …/nifi-api in <N> milliseconds"

# NiFi side: queue count on from-minifi climbs
curl -s --cert /certs/tls.crt --key /certs/tls.key --cacert /certs/ca.crt \
  "https://nifi-web.<ns>.svc.cluster.local:8443/nifi-api/flow/process-groups/root/status?recursive=true" \
  | grep -oE '"(flowFilesReceived|queued)":("[^"]*"|[0-9]+)'
```

> **⚠️ `flow.json.raw` is authoritative.** Bake only `flow.json.gz` and MiNiFi regenerates an empty default flow, recompresses over your file, and starts zero processors. Bake `flow.json.raw` and `flow-identifier` alongside the `.gz`. Do not edit `minifi.properties` directly, since every start regenerates it from `bootstrap.conf`. Set `nifi.minifi.security.*` and `nifi.minifi.flow.use.parent.ssl=true` in `bootstrap.conf`. `PKIX path building failed` means the client cannot trust the server. Chase the SSL context wiring, not the authorization policy.

---

## 7. TensorRT Inference on Jetson (MiNiFi C++, EFM-Managed)

| Field | Value |
|---|---|
| Name | `jetson-tensorrt-cpp` |
| Purpose | Accept an HTTP POST on a Jetson Orin Nano, run TensorRT inference via `ExecuteScript`, and publish the enriched payload to Kafka. EFM-managed flow delivery to aarch64 edge hardware with on-device GPU execution. |
| Agent | MiNiFi C++ `1.26.02`, EFM-managed agent class `NvidiaNano`, enrolled on a Jetson Orin Nano (aarch64). Extra-extensions injection enables `ExecuteScript`. EFM `2.3.1.0-2`. |
| Files | EFM flow export [`files/efm/NvidiaNano-TensorRT.json`](files/efm/NvidiaNano-TensorRT.json) · TensorRT script [`files/gpu_nifi_tensorRT-3.py`](files/gpu_nifi_tensorRT-3.py) · companion flows [`WindowsDesktop-TensorRT.json`](files/efm/WindowsDesktop-TensorRT.json), [`KubernetesPod-TensorRT.json`](files/efm/KubernetesPod-TensorRT.json) |
| Chapter | [Chapter 19](ch19-efm-and-nvidia-jetson.md) |

#### Shape

```
ListenHTTP (port 8080, /contentListener)
  ─(success)─→ ExecuteScript (gpu_nifi_tensorRT-3.py, Script Engine: python)
  ─(success)─→ PublishKafka  (topic agent-nvidia-tensorRT, bootstrap gaming-pc-lan-ip:31623)
```

#### Verify

```bash
# POST to the Jetson's ListenHTTP
curl -X POST http://localhost:8080/contentListener \
  -H "Content-Type: application/json" \
  -d '{"sensor":"jetson-test","value":42}'

# Consume from the Kafka topic (external bootstrap NodePort)
kafka-console-consumer.sh --bootstrap-server gaming-pc-lan-ip:31623 \
  --topic agent-nvidia-tensorRT --from-beginning --max-messages 1
# expected: {"sensor": "jetson-test", "value": 42, "tensorrt": {"version": "10.16.2.10", "status": "Active"}}
```

> **⚠️ Execute bit.** EFM delivers resources to `asset/` without the execute bit. Run `chmod +x ~/nifi-minifi-cpp-1.26.02/asset/gpu_nifi_tensorRT-3.py` on the Jetson after the resource syncs. `ExecuteScript` silently fails without it. The class assignment may change over time, so check the current class before building dependent tooling.

---

## 8. ExecuteScript Python Smoke (MiNiFi C++, EFM-Managed)

| Field | Value |
|---|---|
| Name | `executescript-python-smoke-cpp` |
| Purpose | Show that `ExecuteScript` with the Python engine is live and executing on a C++ agent. The script stamps a `python.smoke` attribute on every FlowFile and `LogAttribute` shows delivery. This is the minimal check before wiring any script logic. |
| Agent | MiNiFi C++ `1.26.02`, EFM-managed, any class with extra-extensions injection applied (`KubernetesPod`, `WindowsDesktopCpp`, `NvidiaNano`). `ExecuteScript` is not in the stock binary. [Chapter 5](ch05-executescript-availability.md) has the injection paths. |
| Files | Script delivered via the EFM Resource Manager API (`POST /efm/api/resource-manager/resources/file`, then `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` with `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[]}`). Flow exported to `files/efm/` per agent class. |
| Chapter | [Chapter 5](ch05-executescript-availability.md), [Chapter 16](ch16-how-to-ai-with-minifi.md) |

#### Shape

```
ListenHTTP (port 18080, /contentListener, Batch Size: 1, Buffer Size: 1)
  ─(success)─→ ExecuteScript (Script Engine: python)
  ─(success)─→ LogAttribute  (Log Payload: true)
```

#### Script body

```python
def onTrigger(context, session):
    flow_file = session.get()
    if flow_file:
        session.putAttribute(flow_file, "python.smoke", "edge-executescript-ok")
        session.transfer(flow_file, REL_SUCCESS)
```

#### Verify

```bash
curl -X POST http://127.0.0.1:18080/contentListener \
     -H "Content-Type: application/json" \
     -d '{"test":"smoke1"}'
# pass: LogAttribute shows python.smoke=edge-executescript-ok with the payload
# fail indicator: "Could not instantiate: PythonScriptExecutor" repeating in minifi-app.log
```

This runs on C++ Kubernetes pods (Linux x86_64) and on the Jetson (aarch64) through the extra-extensions path, and on a Windows C++ agent installed with the `ADDLOCAL=ALL` MSI.

> **⚠️ `ListenHTTP` Batch Size and Buffer Size default to 5/5.** A single request never fills the buffer and is silently dropped. Set both to `1` (MINIFICPP-2243). The C++ FQCN in EFM Designer is `org.apache.nifi.minifi.processors.ExecuteScript`. The `minifi` segment is required, and the Java NiFi FQCN fails.

---

## 9. Edge-AI Router (MiNiFi Java, EFM-Managed)

| Field | Value |
|---|---|
| Name | `starlinkai-lemonade-router-java` |
| Purpose | Front a local Lemonade Server (AMD OpenAI-compatible inference, port 13305) with a three-processor MiNiFi Java flow that proxies all five Lemonade endpoints synchronously. The agent is tiny and the GPU model runs on the adjacent box. All five endpoints work end to end. Transcription needs a multipart-reassembly branch ahead of `InvokeHTTP`. |
| Agent | MiNiFi Java `2.24.08.0-19`, EFM-managed agent class `StarlinkAIJava`, running on a Beelink SER9 (Windows). `HandleHttpRequest`/`HandleHttpResponse`, the Java-only synchronous response pair, are why this is a Java flow and not C++. |
| Files | Flow export [`files/efm/StarlinkAIJava.json`](files/efm/StarlinkAIJava.json) |
| Chapter | [Chapter 17](ch17-edge-ai-router.md) |

#### Shape

```
HandleHttpRequest-Lemonade  (port 8090, any path)
  ─(success)─→ InvokeHTTP-Lemonade  (POST http://localhost:13305${http.request.uri}, Read/Write Timeout: 10 min)
  ─(Response)─→ HandleHttpResponse-Lemonade  (Status Code: ${invokehttp.status.code:replaceEmpty('502')})
```

`Retry`, `No Retry`, and `Failure` from `InvokeHTTP` also wire to `HandleHttpResponse` (and `LogAttribute-Error`), not to `Original`, which would double-respond the HTTP context.

#### Verify

```bash
# Chat
curl -X POST http://localhost:8090/api/v1/chat/completions \
     -H "Content-Type: application/json" \
     -d @chat_body.json

# Embeddings
curl -X POST http://localhost:8090/api/v1/embeddings \
     -H "Content-Type: application/json" \
     -d '{"model":"Qwen3-Embedding-0.6B-GGUF","input":["test sentence"]}'
```

Expected. A synchronous response from Lemonade, with `invokehttp.status.code=200` on `LogAttribute`.

> **⚠️ `InvokeHTTP` socket timeouts.** LLM inference routinely takes 10 to 25 seconds, and the framework default `Socket Read Timeout` of 15 secs fails every call. Set Read and Write timeouts to `10 mins`. `HTTP Method` silently defaults to `GET`, so set it to `POST` explicitly.

---

## 10. Sparkplug / MQTT Two-Leg Ingest (MicroFi ESP32 to NiFi to Kafka)

| Field | Value |
|---|---|
| Name | `sparkplug-mqtt-to-kafka` |
| Purpose | Ingest both kinds of edge MQTT publisher at once, plain-JSON telemetry and spec-compliant Sparkplug B (`NBIRTH`/`NDATA`, protobuf), through one Mosquitto broker into per-kind Kafka topics keyed by the device's agent-class identity. |
| Agent | MicroFi (ESP32-S3 XIAO, compile-time processor registry, EFM-managed). Class `MicroFi-1` publishes the JSON leg. Class `MicroFi-3` publishes Sparkplug B via the firmware's native `PublishSparkplug` processor, built on the `EmbeddedSparkplugNode`/nanopb stack. The NiFi side is the `SparkPlug` process group on the CFM-operator NiFi (`cfm-streaming`/`mynifi`). EFM `2.3.1.0-2`. |
| Files | [`files/SparkPlug.json`](files/SparkPlug.json) (NiFi PG export) · device flows [`files/microfi/microfi-1-telemetry.json`](files/microfi/microfi-1-telemetry.json) and [`files/microfi/microfi-3-sparkplug.json`](files/microfi/microfi-3-sparkplug.json) |
| Chapter | Protocol mechanics in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md), demo narrative in [Chapter 20](ch20-sparkplug-demo.md) |

#### Shape

```
# device side (EFM-pushed)
MicroFi-1:  GenerateFlowFile ({"device_id":"MicroFi-1"}) ─(success)─→ PublishMQTT   (test/sensor/data)
MicroFi-3:  GenerateFlowFile-SpbTick ─(success)─→ PublishSparkplug  (spBv1.0/MicroFi/…/MicroFi-3)

# NiFi side (SparkPlug PG)
ConsumeMQTT     (test/sensor/data) ─(Message)─→ ExtractDeviceId (EvaluateJsonPath $.device_id)
                                   ─(matched…)─→ PublishKafka-XiaoTelemetry      (topic xiao_telemetry, key ${device_id})
ConsumeMQTTIIoT (spBv1.0/#)        ─(Message)─→ PublishKafka-SparkplugTelemetry  (topic sparkplug_telemetry)
```

#### Verify

```bash
# broker: both payload kinds arriving
kubectl exec -n mqtt deploy/mosquitto -- mosquitto_sub -v -t 'test/sensor/data' -t 'spBv1.0/#'

# Kafka: JSON leg keyed by device class
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic xiao_telemetry --property print.key=true --property key.separator=" | " --timeout-ms 15000
# expected: MicroFi-1 | {"device_id":"MicroFi-1"}

# Kafka: Sparkplug B leg (binary protobuf records — NBIRTH then NDATA)
kubectl exec -n cld-streaming my-cluster-combined-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sparkplug_telemetry --timeout-ms 15000
```

> **⚠️ Sparkplug B needs an encoder, not a topic convention.** Publishing JSON to an `spBv1.0/#` topic is not Sparkplug B. The payload must be the protobuf `Payload` with `bdSeq`/`seq` lifecycle semantics, which is what the MicroFi `PublishSparkplug` processor provides and why `ConsumeMQTT` plus `EvaluateJsonPath` cannot decode this leg.

---

## 11. Sparkplug B Publish from MiNiFi Java (`PublishSparkplug` NAR)

| Field | Value |
|---|---|
| Name | `minifi-java-publish-sparkplug` |
| Purpose | Originate spec-compliant Sparkplug B from a MiNiFi Java edge agent, the publish side the CDF IIoT NAR does not ship. One FlowFile of flat JSON metrics becomes NBIRTH then NDATA with `bdSeq`/`seq` and an NDEATH will, all managed by the processor. |
| Agent | MiNiFi Java `2.24.08.0-19`, EFM-managed (class `SparkplugJavaLab`), with the custom [`nifi-sparkplug-nar`](https://github.com/cldr-steven-matison/NiFi2-Processor-Playground/tree/main/nifi-sparkplug-bundle) (Eclipse Tahu plus Paho, self-contained) side-loaded into `extensions/`. The consumer and validator is the live `SparkPlug` PG's `ConsumeMQTTIIoT`. |
| Files | Processor source [`nifi-sparkplug-bundle`](https://github.com/cldr-steven-matison/NiFi2-Processor-Playground/tree/main/nifi-sparkplug-bundle). The agent flow is the two-node Designer flow in Shape below, built per class. |
| Chapter | Mechanics and gotchas in [Chapter 13](ch13-efm-and-sparkplug-mqtt.md) |

#### Shape

```
# device side (EFM Designer, class SparkplugJavaLab)
GenerateFlowFile ({"Sensors/Temperature": 22.5, "Sensors/Count": 1013, "Sensors/Online": true})
  ─(success)─→ PublishSparkplug (tcp://mosquitto.mqtt.svc:1883, group SparkplugLab, node MiNiFi-Java-1)

# wire: spBv1.0/SparkplugLab/NBIRTH/MiNiFi-Java-1 (seq=0), then NDATA seq 1,2,3…
# NiFi side: the existing ConsumeMQTTIIoT (spBv1.0/#) decodes it into sparkplug_telemetry
```

#### Verify

```bash
# wire: birth-first then advancing seq
mosquitto_sub -h <broker-lan-ip> -v -t 'spBv1.0/SparkplugLab/#'

# decode validates: Message-not-parse.failure into Kafka, metric names present
kubectl exec -n cld-streaming my-cluster-combined-0 -c kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sparkplug_telemetry --timeout-ms 15000
```

> **⚠️ A hot-loaded NAR does not refresh the agent's C2 manifest.** The extension loads in seconds, but the Designer palette only sees the new processor after an agent restart re-heartbeats the manifest. Then pin it with `POST /efm/api/agent-class-manifest-config`, field `agentClassName`.

---

## 12. LED Actuation Round Trip (NiFi to MicroFi `ListenHTTP` to `SetGPIO`)

| Field | Value |
|---|---|
| Name | `microfi-led-actuation` |
| Purpose | The minimal flow-to-physical-world round trip. A FlowFile on the central NiFi canvas turns an LED on or off on an ESP32 across the room. The teachable core of every actuation leg in this guide. |
| Agent | MicroFi (ESP32-S3 XIAO, class `MicroFi-1`), plus the `MicroFiLedActuation` PG on central NiFi. |
| Files | Device flow [`files/microfi/microfi-3-led-flow-backup.json`](files/microfi/microfi-3-led-flow-backup.json) |
| Chapter | Demo narrative in [Chapter 20](ch20-sparkplug-demo.md) |

#### Shape

```
# device side (EFM-pushed class flow, 2 nodes)
ListenHTTP (:8095, base path /led) ─(success)─→ SetGPIO (pin 21, level from-content, Invert)

# NiFi side (MicroFiLedActuation PG)
GenerateFlowFile (content "1" or "0") ─(success)─→ InvokeHTTP (POST http://<device-ip>:8095/led)
                                                    └─(Failure/Retry/No Retry)─→ LogAttribute
```

#### Verify

POST content `1` or `0`, directly or via the PG. Expect HTTP 200, the user LED toggles, and the `InvokeHTTP` failure legs stay empty. The FlowFile content is the pin level. No attributes survive the HTTP hop, because MiNiFi `ListenHTTP` is fire-and-forget (see the Ch16 trap list).

---

## How This Gallery Grows

A flow earns a card here after three things are true. Its chapter is complete, the config or flow export is committed to the Playground repo or `files/efm/`, and the card is added both here and to `sample-gallery/README.md` in the Playground.

The gallery's runnable index is [`sample-gallery/README.md`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/sample-gallery/README.md).
