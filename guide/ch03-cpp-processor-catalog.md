# Chapter 3: MiNiFi C++ Processor Catalog

I pulled the complete processor catalog from a running `apacheminificpp:latest` instance — not from docs — after hitting the `ExecuteScript` wall during a live flow build. What follows is the verified set of 74 processors in the stock Cloudera C++ image (v1.26.02, Linux x86_64), the 5-processor extras already staged on the NvidiaNano Jetson (79 aarch64), the 81-processor Windows MSI count with `ADDLOCAL=ALL`, and the precise class names and gotchas you need to wire them in EFM or standalone `config.yml`.

---

## Cloudera vs Apache: what ships vs what's possible

The stock image is Cloudera-curated, Apache-licensed. The upstream source lives at `https://github.com/apache/nifi-minifi-cpp`. All 74 processors in the catalog below are Apache upstream processors — Cloudera controls which subset gets compiled into `apacheminificpp:latest`. The full Apache ceiling is in upstream `PROCESSORS.md`; getting anything beyond 74 requires the extra-extensions tarball injection or a source build.

The EFM deployer registers these agents as `agentType=cpp`. `MINIFI_HOME` inside the container is `/opt/minifi/nifi-minifi-cpp-1.26.02`. The EFM binary path for each platform follows the pattern `${agentType}/${osArch}/${agentVersion}/` — `osArch` must be `linux`, `linuxaarch64`, or `windows`; hyphens are rejected by the EFM validator.


---

## Verified stock catalog — 74 processors (Linux x86_64, v1.26.02)

Extracted from a running instance. Every name below is verbatim from that catalog — nothing added, nothing inferred from docs.

### HTTP / Networking

- **ListenHTTP** — embedded HTTP server; fire-and-forget (caller gets 200, no inline reply). See gotchas.
- **InvokeHTTP** — HTTP client; GET/POST/PUT/etc. to upstream services. See gotchas.
- **GetTCP** — receive data over a persistent TCP connection
- **ListenTCP** — listen for inbound TCP connections
- **ListenUDP** — listen for inbound UDP datagrams
- **PutTCP** — send data over TCP
- **PutUDP** — send data over UDP

### Kafka

- **ConsumeKafka** — consume from a Kafka topic
- **PublishKafka** — publish to a Kafka topic. See gotchas.

### MQTT

- **ConsumeMQTT** — subscribe to an MQTT topic
- **PublishMQTT** — publish to an MQTT topic

### File / Archive

- **FetchFile** — read a file from the local filesystem
- **GetFile** — list and transfer files from a directory
- **ListFile** — list files in a directory without consuming them
- **PutFile** — write a FlowFile to the local filesystem
- **TailFile** — tail a log file or any growing file
- **CompressContent** — compress or decompress content (gzip, lz4, etc.)
- **FocusArchiveEntry** — focus a single entry inside a `.tar` or `.zip` archive
- **ManipulateArchive** — add, remove, or modify archive entries
- **MergeContent** — merge multiple FlowFiles into one (defragment, bin-pack, or concat)
- **SegmentContent** — split content into fixed-size segments
- **SplitContent** — split FlowFile content on a delimiter
- **UnfocusArchiveEntry** — return focus to the outer archive after `FocusArchiveEntry`

### Cloud Storage — AWS

- **DeleteS3Object** — delete an object from S3
- **FetchS3Object** — download an object from S3
- **ListS3** — list objects in an S3 bucket
- **PutKinesisStream** — publish records to AWS Kinesis
- **PutS3Object** — upload an object to S3

### Cloud Storage — Azure

- **DeleteAzureBlobStorage** — delete a blob
- **DeleteAzureDataLakeStorage** — delete a file in ADLS Gen2
- **FetchAzureBlobStorage** — download a blob
- **FetchAzureDataLakeStorage** — download a file from ADLS Gen2
- **ListAzureBlobStorage** — list blobs in a container
- **ListAzureDataLakeStorage** — list files in an ADLS Gen2 path
- **PutAzureBlobStorage** — upload a blob
- **PutAzureDataLakeStorage** — upload a file to ADLS Gen2

### Cloud Storage — Google Cloud

- **DeleteGCSObject** — delete an object from GCS
- **FetchGCSObject** — download an object from GCS
- **ListGCSBucket** — list objects in a GCS bucket
- **PutGCSObject** — upload an object to GCS

### Database / SQL

- **ExecuteSQL** — run a SQL query and emit results as FlowFiles
- **GetCouchbaseKey** — fetch a document from Couchbase by key
- **PutCouchbaseKey** — store a document in Couchbase by key
- **PutSQL** — execute a SQL insert/update/delete
- **QueryDatabaseTable** — incrementally poll a database table for new rows

### Data Transformation / Routing

- **AttributesToJSON** — serialize FlowFile attributes as JSON
- **ConvertRecord** — convert records between formats (requires a Record Reader/Writer controller service)
- **DefragmentText** — reassemble text fragments produced by `SplitText`
- **EvaluateJsonPath** — extract fields from JSON content into FlowFile attributes. See gotchas.
- **ExtractText** — extract content matching a regex into attributes
- **JoltTransformJSON** — apply a JOLT spec transformation to JSON
- **ReplaceText** — replace content or attributes using a regex or literal
- **RouteOnAttribute** — route FlowFiles based on attribute expressions
- **RouteText** — route FlowFiles by matching text content
- **SplitJson** — split a JSON array into individual FlowFiles
- **SplitRecord** — split a record set into individual records
- **SplitText** — split text content by line count or delimiter
- **UpdateAttribute** — add, remove, or modify FlowFile attributes

### Observability

- **CollectKubernetesPodMetrics** — emit pod resource metrics as FlowFiles
- **ConsumeJournald** — read systemd journald log entries as FlowFiles
- **LogAttribute** — log FlowFile attributes to `minifi-app.log`
- **PostElasticsearch** — index documents into Elasticsearch
- **ProcFsMonitor** — emit Linux `/proc` system metrics (CPU, memory, disk) as FlowFiles
- **PushGrafanaLokiGrpc** — push log entries to Grafana Loki over gRPC
- **PushGrafanaLokiREST** — push log entries to Grafana Loki over HTTP
- **PutSplunkHTTP** — send events to Splunk HEC
- **QuerySplunkIndexingStatus** — check indexing status for a Splunk HEC submission

### Attributes / Host Metadata

- **AttributeRollingWindow** — maintain a rolling window of attribute values over time
- **AppendHostInfo** — append hostname and IP to FlowFile attributes

### Syslog

- **ListenSyslog** — receive syslog messages (UDP or TCP)

### Industrial Protocols

- **FetchModbusTcp** — read registers from a Modbus TCP device

### Utilities

- **GenerateFlowFile** — generate synthetic FlowFiles (load testing, warm-up)
- **HashContent** — compute a hash of FlowFile content and store it as an attribute
- **RetryFlowFile** — route a FlowFile back to a previous step up to N times

**Total: 74 processors.** Complete verified set from `apacheminificpp:latest` (v1.26.02, Linux x86_64), extracted from a running instance.

`ExecuteScript` is absent. There is no `libminifi-python-script-extension.so` in the stock image's `extensions/` directory. Cloudera docs list `ExecuteScript` for Linux because it can be built — not because it ships.

---

## Platform matrix (x86_64 / aarch64 / Windows)

| Platform | Agent binary | Stock count | Extra-extensions | ExecuteScript | Status |
|---|---|---|---|---|---|
| Linux x86_64 | `binaries/cpp/linux/1.26.02/minifi.tar.gz` | 74 | Injection recipe in [Ch2](ch02-efm-binaries.md) | Via extra-extensions or source build | Confirmed — running instance verified |
| Linux aarch64 (ARM64) | `binaries/cpp/linuxaarch64/1.26.02/minifi.tar.gz` | 79 (live re-capture) | Already staged on `NvidiaNano` (Jetson) | Confirmed present, live-executed | Field-verified |
| Windows x64 (MSI) | `binaries/cpp/windows/1.26.02/minifi.msi` | 81 (live re-capture) | `ADDLOCAL=ALL` enables Python scripting DLL | `ADDLOCAL=ALL` required, confirmed present | Field-verified |

**aarch64 detail:** Live-captured from the `NvidiaNano` agent (class `NvidiaNano`, manifest `dab61017-33fb-44e7-a159-882601f01952`, build `1.26.02`) via `GET /efm/api/agent-manifests/{id}`, committed as `files/efm/NvidiaNano-manifest.json`. 5 more than the stock 74: `ExecuteProcess`, `ExecuteScript`, `FetchOPCProcessor`, `PutOPCProcessor`, `RunLlamaCppInference` — the extra-extensions `.so` files were already staged on this device. `ExecuteScript` confirmed running live in Python engine (3 processors in the device's production flow). Kafka confirmed with a genuine end-to-end round trip: 10/10 messages, sequential offsets 0–9, topic `minifi-aarch64-test`.

**Windows detail:** Live-captured from agent `40eb2f92-94c5-4478-beed-7060e41c9d7f` (`WindowsDesktopCpp`, manifest `ad8fb2bf-a4de-49e6-92ec-4d70fcbe5519`), committed as `files/efm/WindowsDesktopCpp-manifest.json`. 5 more than the stock 74: `FetchOPCProcessor`, `PutOPCProcessor`, `GetCouchbaseKey`, `PutCouchbaseKey`, `RunLlamaCppInference` — same binary as an earlier 76-processor capture, so these were extension bundles not loaded at that time.

---

## Processors unlocked by extra-extensions injection

After injecting `extra-extensions-linux.tar.gz` into the agent's `extensions/` directory (recipe in [Chapter 2](ch02-efm-binaries.md)), these `.so` files appear:

| `.so` filename | Enables | Notes |
|---|---|---|
| `libminifi-lua-script-extension.so` | **ExecuteScript** (Lua engine) | Together with `libminifi-script-extension.so` |
| `libminifi-python-script-extension.so` + `libminifi-python-lib-loader-extension.so` + `minifi_native.so` | **ExecuteScript** (Python engine) | All three required; also enables `PythonScriptExecutor` |
| `libminifi-execute-process.so` | **ExecuteProcess** | Shell command execution |
| `libminifi-opc-extensions.so` | **FetchOPCProcessor**, **PutOPCProcessor** | OPC-UA client for industrial automation |
| `libminifi-llamacpp.so` | **RunLlamaCppInference** | On-device LLM inference via llama.cpp |
| `libminifi-script-extension.so` | Script dispatch host | Required for both Lua and Python `ExecuteScript` |

The injection is: unpack the tarball, `find -name "*.so" -exec cp {} extensions/`, re-tar, and pipe into the EFM pod before the agent deploys. Full recipe in [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md).

There is also an ARM64-specific tarball: `nifi-minifi-cpp-1.26.02-b30-extra-extensions-linux-arm64.tar.gz`. Field-verified on `NvidiaNano`: 26 `.so` files present (18 stock + 8 extra-extensions), identical basenames to the x86_64 list — no ARM64-only or missing filenames.

On Windows, the equivalent is the MSI with `ADDLOCAL=ALL` — `.dll` files compiled with MSVC, not the Linux `.so` files. Do not copy `.so` files onto a Windows agent.

---

## config.yml class names vs EFM FQCNs

Standalone `config.yml` uses short class names. EFM-deployed flows use FQCNs. They are not interchangeable.

### Short class names — standalone `config.yml`

```yaml
Flow Controller:
  name: MiNiFi HTTP to Kafka

Processors:
- name: ListenHTTP
  id: 489c62c4-2d12-11f1-baac-62f0ccd85bcd
  class: ListenHTTP
  Properties:
    Listening Port: 8080
    Batch Size: '1'
    Buffer Size: '1'

- name: PublishKafka
  id: 489c62c6-2d12-11f1-baac-62f0ccd85bcd
  class: PublishKafka
  Properties:
    Known Brokers: my-cluster-kafka-bootstrap.cld-streaming.svc:9092
    Topic Name: test-minifi
    Client Name: minifi-test-client
    Batch Size: '10'
```

Every component needs an explicit UUID `id` field. `PublishKafka` requires `Client Name`. Use C++ class names, not Java NiFi names (`ListenHTTP`, not `org.apache.nifi.processors.standard.ListenHTTP`).

### FQCNs — EFM-deployed flow format

| Processor | FQCN for EFM |
|---|---|
| ListenHTTP | `org.apache.nifi.minifi.processors.ListenHTTP` |
| InvokeHTTP | `org.apache.nifi.minifi.processors.InvokeHTTP` |
| PublishKafka | `org.apache.nifi.minifi.processors.PublishKafka` |
| EvaluateJsonPath | `org.apache.nifi.minifi.processors.EvaluateJsonPath` |
| RouteOnAttribute | `org.apache.nifi.minifi.processors.RouteOnAttribute` |
| PutFile | `org.apache.nifi.minifi.processors.PutFile` |
| ExecuteScript | `org.apache.nifi.minifi.processors.ExecuteScript` |
| UpdateAttribute | `org.apache.nifi.minifi.processors.UpdateAttribute` |
| LogAttribute | `org.apache.nifi.minifi.processors.LogAttribute` |

Always read `GET /efm/api/designer/flows/{id}` to confirm the exact FQCN and bundle version already in a flow before constructing a new `POST`. The EFM Designer API has no batch create; each processor is one `POST` call returning a server-assigned `identifier`.

---

## Flow gotchas

These are real bugs found on live instances.

### ListenHTTP — Batch Size / Buffer Size (MINFICPP-2243)

**Symptom:** You POST to `ListenHTTP` and get HTTP 200, but no FlowFile reaches the downstream processor. `minifi-app.log` shows:

```
buffer is NOT full 1/5
```

**Diagnosis:** `ListenHTTP` defaults both `Batch Size` and `Buffer Size` to `5`. A single request hits `1/5` — the buffer never fills, so it never flushes.

**Fix:** Set both to `1` in EFM or in `config.yml`. If you still see `1/1 buffer is NOT full` after that, you're hitting MINFICPP-2243, fixed in MiNiFi C++ main in December 2024. Check your agent version.

`ListenHTTP` is also fire-and-forget — the caller gets an empty HTTP 200 immediately. There is no `HandleHttpRequest`/`HandleHttpResponse` pair in MiNiFi C++. Async reply must go via Kafka keyed on a caller-supplied `request_id`. If you need synchronous request/reply in a single HTTP connection, use MiNiFi Java.

### InvokeHTTP — HTTP Method persistence

**Symptom:** `InvokeHTTP` sends GET when you configured POST, causing 405 or data loss.

**Diagnosis:** The `HTTP Method` property persists as `GET` when you create the processor in EFM's Flow Designer without explicitly touching that field.

**Fix:** Always explicitly set `HTTP Method` in EFM or `config.yml`. Never assume the default matches intent.

### PublishKafka — NodePort vs in-cluster

**Symptom:** `PublishKafka` fails with `Connection refused` or `LEADER_NOT_AVAILABLE` from an edge agent running outside Kubernetes.

**Diagnosis:** `Known Brokers` is set to the in-cluster DNS name (`my-cluster-kafka-bootstrap.cld-streaming.svc:9092`), which is only reachable from inside the cluster.

**Fix:** For edge agents outside the cluster, use the external NodePort (e.g., `<node-ip>:31623`). For in-cluster `KubernetesPod` agents, the internal DNS is correct.

### EvaluateJsonPath — path syntax

`$.request_id` extracts a top-level field. `$[0]` extracts the first array element. When `EvaluateJsonPath` produces empty attributes, check path syntax first. For multipart request bodies, `EvaluateJsonPath` cannot parse a multipart payload — set `ListenHTTP`'s `HTTP Headers to receive as Attributes (Regex)` and have the caller send the field as an HTTP header instead.

---

## When to use C++

The stock image is ~15 MB. No JVM. Memory request of ~128Mi works. It deploys as a Kubernetes sidecar in seconds. Kafka, S3, Azure, GCS, HTTP ingestion, SQL, MQTT, Modbus, and Kubernetes metrics all ship in the 74-processor stock set — no scripting required.

Use C++ when you need a lightweight agent that moves data: ingestion, routing, protocol bridging, cloud sync. Use it for production edge or K8s sidecar deployments where image size and cold-start time matter. When you need custom transformation logic that can't be expressed in the available processors, the options are: extra-extensions injection (still C++, no recompile), source build (full control, 30+ minute build), or MiNiFi Java (no build, full scripting, ~300–400 MB image, ~512Mi minimum).

---

## What NOT to do

- **Do not assume `ExecuteScript` is in the stock image.** It isn't. Cloudera docs list it for Linux because it can be built — not because `apacheminificpp:latest` ships it. The tell: `libminifi-python-script-extension.so` is absent from `extensions/`.

- **Do not copy Linux `.so` files from the extra-extensions tarball onto a Windows agent.** Linux `.so` files are ELF binaries. The Windows agent uses MSVC-compiled `.dll` files. The MSI `ADDLOCAL=ALL` path is the correct Windows mechanism.

- **Do not use Java NiFi FQCN class names in `config.yml`.** `org.apache.nifi.processors.standard.ListenHTTP` is the Java class name. The C++ standalone format uses short names like `ListenHTTP`. Wrong class names produce silent no-ops or instantiation failures.

- **Do not run the EFM Windows deployer from `C:\WINDOWS\system32`.** The deployer installs to `$PWD`. Running from system32 dumps the entire install tree into a system directory and creates permission issues on upgrade. `cd C:\minifi` first.

- **Do not skip `ADDLOCAL=ALL` on Windows and then wonder why Python doesn't work.** The EFM-generated deployer command never includes `ADDLOCAL=ALL`. Symptom: `Could not instantiate: PythonScriptExecutor` every 30 seconds. The `msiexec /i ... ADDLOCAL=ALL` repair pass is mandatory. Full recovery plan in [Chapter 5 (ExecuteScript Availability)](ch05-executescript-availability.md).

- **The `linuxaarch64` manifest does not match the x86_64 list — field-verified.** 79 processors on the Jetson vs. 74 stock, because extra-extensions were already staged on that device. No x86-only processors were missing on aarch64. See `files/efm/NvidiaNano-manifest.json`.

- **Do not confuse `ExecuteScript` (C++, post-injection) with Python custom processors in Java NiFi 2.x.** C++'s `ExecuteScript` re-reads its script file from disk on every trigger with no restart needed. Java NiFi Python custom processors require a version bump and processor switch to register a new bundle version in a running instance.

---

## Related chapters

- Ch2 — [EFM Binaries](ch02-efm-binaries.md): the extra-extensions injection recipe and staging tree.
- Ch5 — [ExecuteScript Availability](ch05-executescript-availability.md): the four `ExecuteScript` fix paths (A–D) in full.

Per-platform manifest captures are committed under `files/efm/` (e.g. `NvidiaNano-manifest.json`, `WindowsDesktopCpp-manifest.json`).
