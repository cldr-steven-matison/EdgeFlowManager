# Chapter 3: MiNiFi C++ Processor Catalog

I pulled the complete processor catalog from a running `apacheminificpp:latest` instance after hitting the `ExecuteScript` wall during a live flow build. What follows is the set of 74 processors in the stock Cloudera C++ image (v1.26.02, Linux x86_64), what an aarch64 Jetson and a Windows MSI install report once the extra extensions are staged, and the class names and gotchas you need to wire them in EFM or a standalone `config.yml`.

---

## What Ships vs What's Possible (Cloudera vs Apache)

The stock image is Cloudera-curated and Apache-licensed. The upstream source lives at `https://github.com/apache/nifi-minifi-cpp`. All 74 processors in the catalog below are Apache upstream processors. Cloudera controls which subset gets compiled into `apacheminificpp:latest`. The full Apache ceiling is in upstream `PROCESSORS.md`, and getting anything beyond 74 requires the extra-extensions tarball injection or a source build.

The EFM deployer registers these agents as `agentType=cpp`. `MINIFI_HOME` inside the container is `/opt/minifi/nifi-minifi-cpp-1.26.02`. The EFM binary path for each platform follows the pattern `${agentType}/${osArch}/${agentVersion}/`. `osArch` must be `linux`, `linuxaarch64`, or `windows`. The EFM validator rejects hyphens.

---

## The 74 Stock Processors (Linux x86_64, v1.26.02)

Every name below is verbatim from the running instance's manifest. Nothing is added from docs.

### HTTP / Networking

| Processor | What it does |
|---|---|
| `ListenHTTP` | Embedded HTTP server. Fire-and-forget, the caller gets a 200 and no inline reply. See gotchas. |
| `InvokeHTTP` | HTTP client. GET, POST, PUT and the rest to upstream services. See gotchas. |
| `GetTCP` | Receive data over a persistent TCP connection |
| `ListenTCP` | Listen for inbound TCP connections |
| `ListenUDP` | Listen for inbound UDP datagrams |
| `PutTCP` | Send data over TCP |
| `PutUDP` | Send data over UDP |

### Kafka

| Processor | What it does |
|---|---|
| `ConsumeKafka` | Consume from a Kafka topic |
| `PublishKafka` | Publish to a Kafka topic. See gotchas. |

### MQTT

| Processor | What it does |
|---|---|
| `ConsumeMQTT` | Subscribe to an MQTT topic |
| `PublishMQTT` | Publish to an MQTT topic |

### File / Archive

| Processor | What it does |
|---|---|
| `FetchFile` | Read a file from the local filesystem |
| `GetFile` | List and transfer files from a directory |
| `ListFile` | List files in a directory without consuming them |
| `PutFile` | Write a FlowFile to the local filesystem |
| `TailFile` | Tail a log file or any growing file |
| `CompressContent` | Compress or decompress content (gzip, lz4, and others) |
| `FocusArchiveEntry` | Focus a single entry inside a `.tar` or `.zip` archive |
| `ManipulateArchive` | Add, remove, or modify archive entries |
| `MergeContent` | Merge multiple FlowFiles into one (defragment, bin-pack, or concat) |
| `SegmentContent` | Split content into fixed-size segments |
| `SplitContent` | Split FlowFile content on a delimiter |
| `UnfocusArchiveEntry` | Return focus to the outer archive after `FocusArchiveEntry` |

### Cloud Storage (AWS)

| Processor | What it does |
|---|---|
| `DeleteS3Object` | Delete an object from S3 |
| `FetchS3Object` | Download an object from S3 |
| `ListS3` | List objects in an S3 bucket |
| `PutKinesisStream` | Publish records to AWS Kinesis |
| `PutS3Object` | Upload an object to S3 |

### Cloud Storage (Azure)

| Processor | What it does |
|---|---|
| `DeleteAzureBlobStorage` | Delete a blob |
| `DeleteAzureDataLakeStorage` | Delete a file in ADLS Gen2 |
| `FetchAzureBlobStorage` | Download a blob |
| `FetchAzureDataLakeStorage` | Download a file from ADLS Gen2 |
| `ListAzureBlobStorage` | List blobs in a container |
| `ListAzureDataLakeStorage` | List files in an ADLS Gen2 path |
| `PutAzureBlobStorage` | Upload a blob |
| `PutAzureDataLakeStorage` | Upload a file to ADLS Gen2 |

### Cloud Storage (Google Cloud)

| Processor | What it does |
|---|---|
| `DeleteGCSObject` | Delete an object from GCS |
| `FetchGCSObject` | Download an object from GCS |
| `ListGCSBucket` | List objects in a GCS bucket |
| `PutGCSObject` | Upload an object to GCS |

### Database / SQL

| Processor | What it does |
|---|---|
| `ExecuteSQL` | Run a SQL query and emit results as FlowFiles |
| `GetCouchbaseKey` | Fetch a document from Couchbase by key |
| `PutCouchbaseKey` | Store a document in Couchbase by key |
| `PutSQL` | Execute a SQL insert, update, or delete |
| `QueryDatabaseTable` | Incrementally poll a database table for new rows |

### Data Transformation / Routing

| Processor | What it does |
|---|---|
| `AttributesToJSON` | Serialize FlowFile attributes as JSON |
| `ConvertRecord` | Convert records between formats (requires a Record Reader/Writer controller service) |
| `DefragmentText` | Reassemble text fragments produced by `SplitText` |
| `EvaluateJsonPath` | Extract fields from JSON content into FlowFile attributes. See gotchas. |
| `ExtractText` | Extract content matching a regex into attributes |
| `JoltTransformJSON` | Apply a JOLT spec transformation to JSON |
| `ReplaceText` | Replace content or attributes using a regex or literal |
| `RouteOnAttribute` | Route FlowFiles based on attribute expressions |
| `RouteText` | Route FlowFiles by matching text content |
| `SplitJson` | Split a JSON array into individual FlowFiles |
| `SplitRecord` | Split a record set into individual records |
| `SplitText` | Split text content by line count or delimiter |
| `UpdateAttribute` | Add, remove, or modify FlowFile attributes |

### Observability

| Processor | What it does |
|---|---|
| `CollectKubernetesPodMetrics` | Emit pod resource metrics as FlowFiles |
| `ConsumeJournald` | Read systemd journald log entries as FlowFiles |
| `LogAttribute` | Log FlowFile attributes to `minifi-app.log` |
| `PostElasticsearch` | Index documents into Elasticsearch |
| `ProcFsMonitor` | Emit Linux `/proc` system metrics (CPU, memory, disk) as FlowFiles |
| `PushGrafanaLokiGrpc` | Push log entries to Grafana Loki over gRPC |
| `PushGrafanaLokiREST` | Push log entries to Grafana Loki over HTTP |
| `PutSplunkHTTP` | Send events to Splunk HEC |
| `QuerySplunkIndexingStatus` | Check indexing status for a Splunk HEC submission |

### Attributes / Host Metadata

| Processor | What it does |
|---|---|
| `AttributeRollingWindow` | Maintain a rolling window of attribute values over time |
| `AppendHostInfo` | Append hostname and IP to FlowFile attributes |

### Syslog

| Processor | What it does |
|---|---|
| `ListenSyslog` | Receive syslog messages (UDP or TCP) |

### Industrial Protocols

| Processor | What it does |
|---|---|
| `FetchModbusTcp` | Read registers from a Modbus TCP device |

### Utilities

| Processor | What it does |
|---|---|
| `GenerateFlowFile` | Generate synthetic FlowFiles (load testing, warm-up) |
| `HashContent` | Compute a hash of FlowFile content and store it as an attribute |
| `RetryFlowFile` | Route a FlowFile back to a previous step up to N times |

That is the full set of 74 from `apacheminificpp:latest` (v1.26.02, Linux x86_64).

`ExecuteScript` is absent. There is no `libminifi-python-script-extension.so` in the stock image's `extensions/` directory. Cloudera docs list `ExecuteScript` for Linux because it can be built, not because it ships.

---

## Platform Matrix (x86_64 / aarch64 / Windows)

| Platform | Agent binary | What the manifest reports | Extra-extensions | ExecuteScript |
|---|---|---|---|---|
| Linux x86_64 | `binaries/cpp/linux/1.26.02/minifi.tar.gz` | The stock 74 | Injection recipe in [Ch2](ch02-efm-binaries.md) | Via extra-extensions or source build |
| Linux aarch64 (ARM64) | `binaries/cpp/linuxaarch64/1.26.02/minifi.tar.gz` | 67 with the ARM64 extra-extensions staged | Same recipe, ARM64 tarball | Present once the bundle is staged |
| Windows x64 (MSI) | `binaries/cpp/windows/1.26.02/minifi.msi` | The stock set plus the Windows-only types | `ADDLOCAL=ALL` enables the Python scripting DLL | `ADDLOCAL=ALL` required |

**aarch64.** A Jetson agent with the ARM64 extra-extensions bundle staged reports 67 processor types in `GET /efm/api/agent-manifests/{id}`. That is the stock list plus five the bundle adds, `ExecuteProcess`, `ExecuteScript`, `FetchOPCProcessor`, `PutOPCProcessor`, and `RunLlamaCppInference`, minus twelve stock types whose bundles are not loaded on that build (the GCS set, `ConsumeJournald`, `FetchModbusTcp`, and a few others). `ExecuteScript` runs there in the Python engine. The manifest capture is committed as `files/efm/NvidiaNano-manifest.json`.

**Windows.** A Windows MSI agent reports the stock set plus `ExecuteScript`, `FetchOPCProcessor`, `PutOPCProcessor`, and three Windows-only types, `ConsumeWindowsEventLog`, `PerformanceDataMonitor`, and `TailEventLog`. The manifest capture is committed as `files/efm/WindowsDesktopCpp-manifest.json`.

Do not treat any of these counts as fixed. A processor type appears in the manifest only when its extension bundle is present and loaded, so the same binary reports a different number depending on which `.so` or `.dll` files sit in `extensions/`. `GET /efm/api/agent-manifests/{id}` for your own class is the list to build against.

---

## Processors Unlocked by Extra-Extensions Injection

After injecting `extra-extensions-linux.tar.gz` into the agent's `extensions/` directory (recipe in [Chapter 2](ch02-efm-binaries.md)), these `.so` files appear.

| `.so` filename | Enables | Notes |
|---|---|---|
| `libminifi-lua-script-extension.so` | `ExecuteScript` (Lua engine) | Together with `libminifi-script-extension.so` |
| `libminifi-python-script-extension.so` + `libminifi-python-lib-loader-extension.so` + `minifi_native.so` | `ExecuteScript` (Python engine) | All three required. Also enables `PythonScriptExecutor` |
| `libminifi-execute-process.so` | `ExecuteProcess` | Shell command execution |
| `libminifi-opc-extensions.so` | `FetchOPCProcessor`, `PutOPCProcessor` | OPC-UA client for industrial automation |
| `libminifi-llamacpp.so` | `RunLlamaCppInference` | On-device LLM inference via llama.cpp |
| `libminifi-script-extension.so` | Script dispatch host | Required for both Lua and Python `ExecuteScript` |

The injection is to unpack the tarball, `find -name "*.so" -exec cp {} extensions/`, re-tar, and pipe into the EFM pod before the agent deploys. The full recipe is in [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md).

There is also an ARM64-specific tarball, `nifi-minifi-cpp-1.26.02-b30-extra-extensions-linux-arm64.tar.gz`. On the Jetson it yields 26 `.so` files in `extensions/` (18 stock plus 8 extra-extensions), with the same basenames as the x86_64 list. There are no ARM64-only or missing filenames.

On Windows the equivalent is the MSI with `ADDLOCAL=ALL`, which installs `.dll` files compiled with MSVC, not the Linux `.so` files. Do not copy `.so` files onto a Windows agent.

---

## config.yml Class Names vs EFM FQCNs

Standalone `config.yml` uses short class names. EFM-deployed flows use FQCNs. They are not interchangeable.

### Short Class Names in a Standalone `config.yml`

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

### FQCNs in the EFM-Deployed Flow Format

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

Always read `GET /efm/api/designer/flows/{id}` to confirm the exact FQCN and bundle version already in a flow before constructing a new `POST`. The EFM Designer API has no batch create. Each processor is one `POST` call returning a server-assigned `identifier`.

---

## Flow Gotchas

These are bugs found on live instances.

### ListenHTTP Batch Size and Buffer Size (MINFICPP-2243)

**Symptom.** You POST to `ListenHTTP` and get HTTP 200, but no FlowFile reaches the downstream processor. `minifi-app.log` shows:

```
buffer is NOT full 1/5
```

**Diagnosis.** `ListenHTTP` defaults both `Batch Size` and `Buffer Size` to `5`. A single request hits `1/5`. The buffer never fills, so it never flushes.

**Fix.** Set both to `1` in EFM or in `config.yml`. If you still see `1/1 buffer is NOT full` after that, you are hitting MINFICPP-2243, fixed upstream in MiNiFi C++ main. Check your agent version.

`ListenHTTP` is also fire-and-forget. The caller gets an empty HTTP 200 immediately. There is no `HandleHttpRequest`/`HandleHttpResponse` pair in MiNiFi C++. An async reply must go via Kafka keyed on a caller-supplied `request_id`. If you need synchronous request and reply in a single HTTP connection, use MiNiFi Java.

### InvokeHTTP HTTP Method Persistence

**Symptom.** `InvokeHTTP` sends GET when you configured POST, causing 405 or data loss.

**Diagnosis.** The `HTTP Method` property persists as `GET` when you create the processor in EFM's Flow Designer without explicitly touching that field.

**Fix.** Always explicitly set `HTTP Method` in EFM or `config.yml`. Never assume the default matches intent.

### PublishKafka NodePort vs In-Cluster

**Symptom.** `PublishKafka` fails with `Connection refused` or `LEADER_NOT_AVAILABLE` from an edge agent running outside Kubernetes.

**Diagnosis.** `Known Brokers` is set to the in-cluster DNS name (`my-cluster-kafka-bootstrap.cld-streaming.svc:9092`), which is only reachable from inside the cluster.

**Fix.** For edge agents outside the cluster, use the external NodePort (for example `<node-ip>:31623`). For in-cluster `KubernetesPod` agents, the internal DNS is correct.

### EvaluateJsonPath Path Syntax

`$.request_id` extracts a top-level field. `$[0]` extracts the first array element. When `EvaluateJsonPath` produces empty attributes, check path syntax first. For multipart request bodies, `EvaluateJsonPath` cannot parse a multipart payload. Set `ListenHTTP`'s `HTTP Headers to receive as Attributes (Regex)` and have the caller send the field as an HTTP header instead.

---

## When to Use C++

The stock image is about 15 MB. No JVM. A memory request of about 128Mi works. It deploys as a Kubernetes sidecar in seconds. Kafka, S3, Azure, GCS, HTTP ingestion, SQL, MQTT, Modbus, and Kubernetes metrics all ship in the 74-processor stock set, with no scripting required.

Use C++ when you need a lightweight agent that moves data. Ingestion, routing, protocol bridging, cloud sync. Use it for production edge or K8s sidecar deployments where image size and cold-start time matter. When you need custom transformation logic that the available processors cannot express, you have three options. Extra-extensions injection (still C++, no recompile), a source build (full control, 30+ minute build), or MiNiFi Java (no build, full scripting, a 300 to 400 MB image, about 512Mi minimum).

---

## What NOT to Do

- Do not assume `ExecuteScript` is in the stock image. It isn't. Cloudera docs list it for Linux because it can be built, not because `apacheminificpp:latest` ships it. The tell is that `libminifi-python-script-extension.so` is absent from `extensions/`.

- Do not copy Linux `.so` files from the extra-extensions tarball onto a Windows agent. Linux `.so` files are ELF binaries. The Windows agent uses MSVC-compiled `.dll` files. The MSI `ADDLOCAL=ALL` path is the correct Windows mechanism.

- Do not use Java NiFi FQCN class names in `config.yml`. `org.apache.nifi.processors.standard.ListenHTTP` is the Java class name. The C++ standalone format uses short names like `ListenHTTP`. Wrong class names produce silent no-ops or instantiation failures.

- Do not run the EFM Windows deployer from `C:\WINDOWS\system32`. The deployer installs to `$PWD`. Running from system32 dumps the entire install tree into a system directory and creates permission issues on upgrade. `cd C:\minifi` first.

- Do not skip `ADDLOCAL=ALL` on Windows and then wonder why Python doesn't work. The EFM-generated deployer command never includes `ADDLOCAL=ALL`. The symptom is `Could not instantiate: PythonScriptExecutor` every 30 seconds. The `msiexec /i ... ADDLOCAL=ALL` repair pass is mandatory. The full recovery plan is in [Chapter 5 (ExecuteScript Availability)](ch05-executescript-availability.md).

- Do not expect the `linuxaarch64` manifest to match the x86_64 list. Which types appear depends on which extension bundles are loaded on that device, in both directions. Read `files/efm/NvidiaNano-manifest.json` or your own class's manifest before building a flow against it.

- Do not confuse `ExecuteScript` (C++, post-injection) with Python custom processors in Java NiFi 2.x. C++'s `ExecuteScript` re-reads its script file from disk on every trigger with no restart needed. Java NiFi Python custom processors require a version bump and processor switch to register a new bundle version in a running instance.

---

## Related Chapters

- Ch2 — [EFM Binaries](ch02-efm-binaries.md): the extra-extensions injection recipe and staging tree.
- Ch5 — [ExecuteScript Availability](ch05-executescript-availability.md): the four `ExecuteScript` fix paths (A–D) in full.

Per-platform manifest captures are committed under `files/efm/` (`NvidiaNano-manifest.json`, `WindowsDesktopCpp-manifest.json`).
