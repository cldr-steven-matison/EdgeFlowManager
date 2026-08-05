# Chapter 18: Sample Gallery of MiNiFi Flows

A curated, runnable set of MiNiFi flows accumulated as the guide is built. Each entry is a flow that has been **field-validated** somewhere in this guide — this chapter collects and polishes them behind one consistent card, it doesn't invent new ones. A flow only earns a full card here after its own chapter is field-validated; slots whose chapters are not yet closed are listed as labeled pending entries at the bottom.

The runnable home for these flows is the [`sample-gallery/`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/tree/main/sample-gallery) directory in the MiNiFi Kubernetes Playground repo. This chapter is the plan and narrative; the Playground's `sample-gallery/README.md` is the runnable index that links the configs. Configs live once at the repo root — each card here links to them, not duplicates them.

## Card format

Every entry uses the same card so the gallery reads consistently:

- **Name** — short, googlable
- **Purpose** — one line, what it's for
- **Agent** — C++ / Java, version, class (standalone vs EFM-managed)
- **Shape** — the processor chain
- **Files** — `config.yml` and/or exported `flow.json`
- **Verification** — the exact command(s) to prove it runs
- **Status** — field-validation date and where to find the full walkthrough

---

## Entry 1 — HTTP → Kafka + File (MiNiFi C++, standalone)

- **Name:** `http-to-kafka-cpp`
- **Purpose:** Accept an HTTP POST at the edge and fan it out to a Kafka topic *and* a local file in one flow.
- **Agent:** MiNiFi **C++** `1.26.02` (`container.repo.cloudera.com/cloudera/apacheminificpp:latest`), standalone `config.yml` baked at image build time, no EFM.
- **Shape:** fan-out from a single listener — both connections carry `ListenHTTP`'s `success` relationship; the flow is a fork, not a chain:
  ```
  ListenHTTP ─┬─(success)─→ PublishKafka   (topic test-minifi, broker my-cluster-kafka-bootstrap.cld-streaming.svc:9092)
              └─(success)─→ PutFile         (/tmp/minifi-test-output)
  ```
- **Files:** [`config.yml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/config.yml) · [`Dockerfile`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/Dockerfile) · [`minifi-test.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test.yaml) (NodePort 30080)
- **Verification:**
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
- **Status:** ✅ field-validated, playground Minikube (context `minikube`). Full walkthrough: [Chapter 7](ch07-standalone-minifi-cpp-on-k8s.md).

> **⚠️ C++ config requirements.** Every processor and connection needs an explicit `id` UUID. Class names are C++ short names (`ListenHTTP`, `PublishKafka`, `PutFile`) — Java FQCNs do not work. `PublishKafka` requires a non-empty `Client Name`. The readiness probe path is `/contentListener`, not `/` or `/health`.

---

## Entry 2 — HTTP → File (MiNiFi Java, standalone)

- **Name:** `http-to-file-java`
- **Purpose:** Accept an HTTP POST at the edge and persist it to a local file. No Kafka — the stock Java image ships no Kafka NAR.
- **Agent:** MiNiFi **Java** `1.23.04-b15` (`container.repo.cloudera.com/cloudera/nifi-minifi-java:latest`), standalone `config-java.yml` (`MiNiFi Config Version: 3`), no EFM.
- **Shape:**
  ```
  ListenHTTP ─(success)─→ PutFile   (/tmp/minifi-test-output)
  ```
- **Files:** [`config-java.yml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/config-java.yml) · [`Dockerfile.java`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/Dockerfile.java) · [`minifi-test-java.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test-java.yaml) (NodePort 30081)
- **Verification:**
  ```bash
  minikube service minifi-test-java-service --url

  curl -i -X POST http://127.0.0.1:<TUNNEL_PORT>/contentListener \
       -H "Content-Type: application/json" \
       -d '{"test_id": "integration-success", "message": "Flow is functional"}'

  kubectl exec -it deployment/minifi-test-java -- /bin/sh -c "cat /tmp/minifi-test-output/*"
  ```
- **Status:** ✅ field-verified end-to-end on playground Minikube. Full walkthrough: [Chapter 8](ch08-minifi-java-setup.md).

> **⚠️ Java config gotchas.** Connections wire by `source id`/`destination id` UUID, not by name. Processor `class` is fully-qualified. The readiness/liveness probes must be `tcpSocket`, not `httpGet` — Java's `ListenHTTP` returns `405` to a bare `GET` and an `httpGet` probe crash-loops the pod.

---

## Entry 3 — EFM-managed smoke flow (MiNiFi C++, Level 2)

- **Name:** `efm-level2-playground-cpp`
- **Purpose:** Prove EFM C2 wiring end to end in the `default` namespace using a bare Ubuntu pod. `GenerateFlowFile` emits a heartbeat every 10 seconds; `LogAttribute` confirms the agent is receiving and executing EFM-published flows.
- **Agent:** MiNiFi **C++** `1.26.02`, EFM-managed agent class `PlaygroundCpp`, installed from bare `ubuntu:22.04` via the EFM agent-deployer script (no custom image). EFM `2.3.1.0-2` in `cld-streaming` namespace.
- **Shape:**
  ```
  GenerateFlowFile (10 sec, Custom Text: "PlaygroundCpp Level 2 heartbeat")
    ─(success)─→ LogAttribute
  ```
- **Files:** [`minifi-test-efm-cpp.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test-efm-cpp.yaml) · [`files/efm/PlaygroundCpp.json`](../files/efm/PlaygroundCpp.json) (exported flow)
- **Verification:**
  ```bash
  # confirm the agent reached ONLINE in EFM Monitor → Agents
  # (check EFM UI at http://127.0.0.1:10090/efm/ui)

  # confirm LogAttribute output in pod logs
  kubectl logs minifi-test-efm-cpp -n default | grep LogAttribute
  # expected: LogAttribute -- filename: <uuid>, content: PlaygroundCpp Level 2 heartbeat
  ```
- **Status:** ✅ field-validated on playground Minikube, `default` namespace. Full walkthrough: [Chapter 9](ch09-efm-in-the-playground.md).

> **⚠️ EFM health-poll required.** On cold-start EFM takes up to two minutes to bind its Jetty listener. Both manifests poll `/efm/actuator/health` in a loop before running the deployer curl — skip the poll and the agent never enrolls. Both `flowId` and `pgId` are required in the processor-create API path; using only `pgId` returns a misleading Spring 404.

---

## Entry 4 — EFM-managed smoke flow (MiNiFi Java, Level 2)

- **Name:** `efm-level2-playground-java`
- **Purpose:** The Java-flavor counterpart to Entry 3. Proves EFM C2 enrollment and flow delivery to a MiNiFi Java agent, same bare-pod bootstrap pattern.
- **Agent:** MiNiFi **Java** `2.24.08.0-19`, EFM-managed agent class `PlaygroundJava`, installed from bare `ubuntu:22.04` via the EFM agent-deployer script (requires `openjdk-11-jre-headless`). EFM `2.3.1.0-2` in `cld-streaming` namespace.
- **Shape:**
  ```
  GenerateFlowFile (10 sec, Custom Text: "PlaygroundJava Level 2 heartbeat")
    ─(success)─→ LogAttribute
  ```
- **Files:** [`minifi-test-efm-java.yaml`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/minifi-test-efm-java.yaml) · [`files/efm/PlaygroundJava.json`](../files/efm/PlaygroundJava.json) (exported flow)
- **Verification:**
  ```bash
  kubectl logs minifi-test-efm-java -n default | grep LogAttribute
  # expected: LogAttribute -- filename: <uuid>, content: PlaygroundJava Level 2 heartbeat
  ```
- **Status:** ✅ field-validated on playground Minikube, `default` namespace. Full walkthrough: [Chapter 9](ch09-efm-in-the-playground.md).

---

## Entry 5 — TensorRT inference on Jetson (MiNiFi C++, EFM-managed)

- **Name:** `jetson-tensorrt-cpp`
- **Purpose:** Accept an HTTP POST on a Jetson Orin Nano, run TensorRT inference via `ExecuteScript`, and publish the enriched payload to Kafka. Proves EFM-managed flow delivery to real aarch64 edge hardware and on-device GPU execution.
- **Agent:** MiNiFi **C++** `1.26.02`, EFM-managed agent class `NvidiaNano`, enrolled on a Jetson Orin Nano (`tunastreet`, aarch64). Extra-extensions injection enables `ExecuteScript`. EFM `2.3.1.0-2` on WindowsDesktop.
- **Shape:**
  ```
  ListenHTTP (port 8080, /contentListener)
    ─(success)─→ ExecuteScript (gpu_nifi_tensorRT-3.py, Script Engine: python)
    ─(success)─→ PublishKafka  (topic agent-nvidia-tensorRT, bootstrap gaming-pc-lan-ip:31623)
  ```
- **Files:**
  - EFM flow export: [`files/efm/NvidiaNano-TensorRT.json`](../files/efm/NvidiaNano-TensorRT.json)
  - TensorRT script: [`files/gpu_nifi_tensorRT-3.py`](../files/gpu_nifi_tensorRT-3.py)
  - Companion flows: [`WindowsDesktop-TensorRT.json`](../files/efm/WindowsDesktop-TensorRT.json), [`KubernetesPod-TensorRT.json`](../files/efm/KubernetesPod-TensorRT.json)
- **Verification:**
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
- **Status:** ✅ field-validated on real Jetson Orin Nano hardware; `tensorrt` block appended live on-device. Full walkthrough: [Chapter 19](ch19-efm-and-nvidia-jetson.md).

> **⚠️ Execute bit.** EFM delivers resources to `asset/` without the execute bit. Run `chmod +x ~/nifi-minifi-cpp-1.26.02/asset/gpu_nifi_tensorRT-3.py` on the Jetson after the resource syncs — `ExecuteScript` silently fails without it. `NvidiaNano` is the current class assignment and may change; check the current class before building dependent tooling.

---

## Entry 6 — ExecuteScript Python smoke (MiNiFi C++, EFM-managed)

- **Name:** `executescript-python-smoke-cpp`
- **Purpose:** Prove `ExecuteScript` with the Python engine is live and executing on a C++ agent. The script stamps a `python.smoke` attribute on every FlowFile; `LogAttribute` confirms delivery. This is the minimal validation pattern before wiring any real script logic.
- **Agent:** MiNiFi **C++** `1.26.02`, EFM-managed (any class with extra-extensions injection applied — `KubernetesPod`, `WindowsDesktopCpp`, `NvidiaNano`). `ExecuteScript` is not in the stock binary; see [Chapter 5](ch05-executescript-availability.md) for the injection paths.
- **Shape:**
  ```
  ListenHTTP (port 18080, /contentListener, Batch Size: 1, Buffer Size: 1)
    ─(success)─→ ExecuteScript (Script Engine: python)
    ─(success)─→ LogAttribute  (Log Payload: true)
  ```
- **Script body:**
  ```python
  def onTrigger(context, session):
      flow_file = session.get()
      if flow_file:
          session.putAttribute(flow_file, "python.smoke", "edge-executescript-ok")
          session.transfer(flow_file, REL_SUCCESS)
  ```
- **Files:** Script delivered via EFM Resource Manager API (`POST /efm/api/resource-manager/resources/file`, then `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` with `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[]}`). Flow exported to `files/efm/` per agent class.
- **Verification:**
  ```bash
  curl -X POST http://127.0.0.1:18080/contentListener \
       -H "Content-Type: application/json" \
       -d '{"test":"smoke1"}'
  # pass: LogAttribute shows python.smoke=edge-executescript-ok with the payload
  # fail indicator: "Could not instantiate: PythonScriptExecutor" repeating in minifi-app.log
  ```
- **Status:** ✅ field-validated on C++ K8s pods (Linux x86_64) and Jetson aarch64 (extra-extensions path); on WindowsDesktop C++ via `ADDLOCAL=ALL` MSI. Full breakdown: [Chapter 5](ch05-executescript-availability.md), [Chapter 16](ch16-how-to-ai-with-minifi.md).

> **⚠️ `ListenHTTP` Batch Size/Buffer Size default to 5/5.** A single request never fills the buffer and is silently dropped. Set both to `1` (MINIFICPP-2243). Also: the C++ FQCN in EFM Designer is `org.apache.nifi.minifi.processors.ExecuteScript` — the `minifi` segment is required; the Java NiFi FQCN fails.

---

## Entry 7 — Edge-AI router (MiNiFi Java, EFM-managed)

- **Name:** `starlinkai-lemonade-router-java`
- **Purpose:** Front a local Lemonade Server (AMD OpenAI-compatible inference, port 13305) with a three-processor MiNiFi Java flow that proxies all five Lemonade endpoints synchronously. The agent is tiny; the GPU model runs on the adjacent box. All five endpoints work end to end; transcription needs a multipart-reassembly branch ahead of `InvokeHTTP`.
- **Agent:** MiNiFi **Java** `2.24.08.0-19`, EFM-managed agent class `StarlinkAIJava`, running on StarlinkAI Beelink SER9 (`TunaStarlink`, Windows). `HandleHttpRequest`/`HandleHttpResponse` — the Java-only synchronous response pair — are why this is a Java flow, not C++.
- **Shape:**
  ```
  HandleHttpRequest-Lemonade  (port 8090, any path)
    ─(success)─→ InvokeHTTP-Lemonade  (POST http://localhost:13305${http.request.uri}, Read/Write Timeout: 10 min)
    ─(Response)─→ HandleHttpResponse-Lemonade  (Status Code: ${invokehttp.status.code:replaceEmpty('502')})
  ```
  `Retry`, `No Retry`, and `Failure` from `InvokeHTTP` also wire to `HandleHttpResponse` (and `LogAttribute-Error`) — not to `Original`, which would double-respond the HTTP context.
- **Files:** Flow export lives in `files/efm/` per the StarlinkAI agent class.
- **Verification:**
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
  Expected: real synchronous response from Lemonade, `invokehttp.status.code=200` on `LogAttribute`.
- **Status:** ✅ all 5 endpoints work (chat, embeddings, reranking, TTS, transcription). Transcription needs a multipart-reassembly branch ahead of `InvokeHTTP`. Full walkthrough: [Chapter 16](ch16-how-to-ai-with-minifi.md).

> **⚠️ `InvokeHTTP` socket timeouts.** LLM inference routinely takes 10–25s; the framework default `Socket Read Timeout` of 15 secs fails every real call. Set Read and Write timeouts to `10 mins`. `HTTP Method` silently defaults to `GET` — set it to `POST` explicitly.

---

## Pending entries

These flows are planned but don't yet have a folded, field-validated chapter behind them. Each becomes a full card above once its chapter lands.

- **S2S source flows — MiNiFi → NiFi K8s** (Ch10 C++, Ch11 Java): the Site-to-Site chapter pair.
- **SparkPlug / MQTT ingest** (Ch20): the SparkPlug demo chapter.

---

## How this gallery grows

A flow earns a card here after three things are true: (1) its chapter is field-validated, (2) the config or flow export is committed to the Playground repo or `files/efm/`, and (3) the card is added both here and to `sample-gallery/README.md` in the Playground.

The gallery's runnable index: [`sample-gallery/README.md`](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground/blob/main/sample-gallery/README.md).
