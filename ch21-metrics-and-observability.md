# Chapter 21: Metrics & Observability

Once a MiNiFi agent is running out at the edge — on a Jetson, a Windows box over Tailscale, a Kubernetes pod with no persistent identity — the next question is: how do I see it? What is EFM's own health, what are the agents doing, and how do I get all of it onto the same Prometheus/Grafana stack I already run for NiFi, Kafka, Flink, and Schema Registry via the CSO operators?

There are three metrics layers (Layer 2 covers both the C++ and Java agent variants). They are independent — you can wire up any one without the others:

1. **Layer 1 — EFM server metrics.** EFM is a Spring Boot app; it exposes an actuator Prometheus endpoint. ✅ Done.
2. **Layer 2 — MiNiFi C++ agent metrics.** The C++ agent has a native Prometheus publisher (system + processor + repository metrics). ✅ Done.
3. **Layer 2 (Java) — MiNiFi Java agent metrics.** The built-in Prometheus endpoint is blocked at the platform level, but two working alternatives exist: a Site-to-Site metrics relay into NiFi, and — the production path on the Jetson — a flow-level `HandleHttpRequest`/`HandleHttpResponse` exporter that serves Prometheus exposition format directly. 🚫 built-in endpoint / ✅ S2S relay / ✅ flow-level exporter.
4. **Layer 3 — embedded / heartbeat metrics.** The smallest agents (ESP32/XIAO class) fold storage and health counters into the C2 heartbeat instead. Field-verified 2026-08-15: EFM drops the custom payload fields and re-exports nothing from the heartbeat body, so the panelable slice is the heartbeat-transport series (already on the fleet dashboard); the storage counters need a device-egress path.

## The CSO Prometheus/Grafana Stack

All three layers target the **existing** observability stack. The `kube-prometheus-stack` Helm install that already scrapes CFM (NiFi), CSA (Flink), and CSM (Kafka/Strimzi) runs in the `cld-streaming` namespace. EFM and the edge agents become additional scrape targets on that same stack — the edge does not need its own monitoring silo.

**On WindowsDesktop this stack has to be stood up separately** — EFM and NiFi (CFM) targets confirm live once it is. Kafka (CSM) and Flink (CSA) are deliberately not wired there yet.

For contrast on the NiFi side: the old `PrometheusReportingTask` is gone in NiFi 2.x. Metrics now come from the built-in `/nifi-api/flow/metrics/prometheus` REST endpoint. EFM and MiNiFi are the edge-side complement to that datacenter endpoint.

## Layer 0 — Prerequisites and Deploy

Every layer below assumes EFM is actually deployed. On a CSO host that runs NiFi/Kafka/Flink but has never run EFM, stand it up from the `ClouderaStreamingOperators` repo. EFM is a Spring Boot app backed by Postgres; skipping a prerequisite is how you get a pod in `CrashLoopBackOff` instead of a clean metrics endpoint.

### Verify Prerequisites Before Applying Anything

The persisted deployment (`efm-deployment-persisted.yaml`) references six objects by name. Confirm each exists in `cld-streaming` first:

```bash
ns=cld-streaming
# 1+2. DB-password and encryption-password secrets
kubectl get secret -n $ns efm-db-pass efm-encryption
# 3. efm.properties override (this is where metrics export gets turned on)
kubectl get cm -n $ns efm-config
# 4+5. Two PVCs: staged agent binaries + EFM resources
kubectl get pvc -n $ns efm-agent-binaries efm-resources
# 6. The efm Postgres database inside the shared ssb-postgresql pod
kubectl exec -n $ns deploy/ssb-postgresql -- psql -U postgres -lqt | grep efm
```

Two gotchas the manifest hides:

- **`imagePullSecret: cloudera-registry`** is referenced, but if the image is already cached in minikube (`minikube image ls | grep efm` shows `container.repo.cloudera.com/cloudera/efm:2.3.1.0-2`) the default `IfNotPresent` pull policy means the kubelet never contacts the registry. A missing pull secret is harmless here. On a host without the cached image, create the secret or `minikube image load` the tarball first.
- **`EF_REGISTRY_URL=http://host.minikube.internal:18080` with `EF_REGISTRY_ENABLED=true`** points at a NiFi Registry that may not exist on the host. EFM still starts and serves metrics without a reachable registry — it just logs connection retries. Set `EF_REGISTRY_ENABLED=false` if you want the log clean; it has no effect on the metrics path.

### Deploy

```bash
cd ~/Documents/GitHub/ClouderaStreamingOperators
# PVCs first (skip if already Bound from a prior run)
kubectl apply -f efm-pvc.yaml
# EFM Deployment + Service
kubectl apply -f efm-deployment-persisted.yaml
kubectl rollout status deployment/efm -n cld-streaming --timeout=5m
```

EFM's cold start is ~2 minutes (Jetty + Spring context + DB migration) on a fresh DB. Don't trust the pod `Running` state alone — poll the health actuator until it returns `200`. The EFM image ships **no `curl`**, so port-forward and check from the host, not `kubectl exec`:

```bash
kubectl port-forward -n cld-streaming deploy/efm 10190:10090 &
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:10190/efm/actuator/health   # want 200
```

**Field-validated.** Prerequisites already present (secrets, ConfigMap, both PVCs Bound, `efm` Postgres DB, image cached in minikube) — single `kubectl apply`, pod `Running` and health-green in ~15s (DB already migrated). The MiNiFi `KubernetesPod` agent enrolled the same session.

### Why the Metrics Endpoint Already Works

The `efm-config` ConfigMap overrides `conf/efm.properties` and already carries the metrics block. You do not turn anything on at deploy time:

```properties
# Metrics Properties (from efm-config ConfigMap)
management.metrics.efm.enabled=true
management.prometheus.metrics.export.enabled=true
management.prometheus.metrics.export.descriptions=true
management.metrics.enable.efm.heartbeat=true
management.metrics.enable.efm.repo=true
management.metrics.efm.enableTag.agentClass=true
management.metrics.efm.enableTag.agentId=true
management.metrics.tags.application=efm
```

Without this ConfigMap mounted, the actuator is up but the Prometheus registry is not wired — the endpoint returns `404` and Layer 1 silently scrapes nothing.

### Deploy an Agent so There's Something to Measure

EFM's own metrics appear as soon as it's running, but the interesting agent-tagged series only appear once an agent is enrolled and heartbeating:

```bash
kubectl apply -f minifi-agent-pod.yaml
kubectl logs -f minifi-agent-k8s -n cld-streaming   # watch it wait for EFM, deploy, and enroll
```

Once it's heartbeating, `/efm/actuator/prometheus` gains `agentClass="KubernetesPod"`-tagged series, and Layer 2 (the agent's own `9936` publisher) becomes available on the pod.

## Layer 1 — EFM Server Metrics

EFM's Kubernetes `Service` exposes two named ports: `efm-ui` on `10090` (the UI/API) and `metrics` on `9092`.

> **⚠️ The metrics do not come out of the `metrics` port.** The Service *declares* `metrics/9092`, and the obvious read is "scrape 9092." That's wrong: `9092` accepts a TCP connection but returns an **empty reply**, because EFM never starts a separate management server there (`management.server.port=9092` is not set in `efm.properties`). The actuator — including the Prometheus endpoint — is served on the **main server port `10090`** under the `/efm` servlet context path. The `metrics/9092` port is a Service-definition leftover, not a live endpoint.

Confirm which port actually serves before writing the `ServiceMonitor`:

```bash
kubectl port-forward -n cld-streaming deploy/efm 10190:10090 &
# Actuator index lists "prometheus" as a registered endpoint:
curl -s http://localhost:10190/efm/actuator | python3 -m json.tool | grep prometheus
# Returns real Prometheus text (~1429 lines on the K8s EFM pod, 1965 on WindowsDesktop):
curl -s http://localhost:10190/efm/actuator/prometheus | head
```

> **⚠️ Don't `kubectl exec ... -- curl` into the EFM pod.** The image has no `curl`. Port-forward `10090` to the host and curl locally.

Sample output — `efm_*` metrics tagged `application="efm"`:

```text
efm_tasks_scheduled_execution_active_seconds_max{application="efm",code_function="run",
  code_namespace="com.cloudera.cem.efm.monitor.core.MissingAgentMonitor",...} 0.0
```

Wire it into the Prometheus Operator with a `ServiceMonitor` that selects the EFM service and scrapes the **`efm-ui`** port. The `release: prometheus` label matches the `kube-prometheus-stack` convention the other CSO ServiceMonitors use:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: efm
  namespace: cld-streaming
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: efm
  endpoints:
  - port: efm-ui                      # NOT `metrics` — 9092 serves nothing
    path: /efm/actuator/prometheus
    interval: 15s
```

```bash
kubectl apply -f efm-service-monitor.yaml
```

**Field-verified.** After applying, the target registers and goes green within ~90s (Prometheus config reload + first scrape):

```text
$ curl -s 'localhost:9490/api/v1/query?query=up{job="efm"}'
http://10.244.5.43:10090/efm/actuator/prometheus -> up
up{container="efm",endpoint="efm-ui",instance="10.244.5.43:10090",job="efm",
   namespace="cld-streaming",pod="efm-686c9c4758-mlvbw",service="efm"} = 1
```

**Also confirmed on WindowsDesktop.** `http://192.168.1.121:10090/efm/actuator/prometheus` returns 1965 lines; `ServiceMonitor` applied and `up{job="efm"}=1` confirmed.

If you want to use `metrics/9092` (cleaner separation from the UI), set `management.server.port=9092` in the `efm-config` ConfigMap and redeploy — then `port: metrics` in the `ServiceMonitor` works. Until you do that, scrape `efm-ui`.

### The Heartbeat Series — Fleet Liveness for Free

The most useful thing in EFM's actuator output is the per-class heartbeat family — it gives fleet-wide liveness for every enrolled agent without touching a single device:

- `efm_heartbeat_lastSeenTime_seconds{agentClass, agentId, agentManifestId}` — epoch of the last heartbeat. `time() - max by (agentClass)(...)` = seconds since last heartbeat, the core liveness expression.
- `efm_heartbeat_count_total{...}` — `sum by (agentClass)(rate(...[5m]))*60` = heartbeats per minute.
- `efm_heartbeat_contentLength_sum/_count` — `rate(sum)/rate(count)` = average heartbeat payload size.

Three semantics that will bite if unlearned:

- **Label churn creates duplicate series.** Every manifest change mints a new `agentManifestId` label value, so one physical device accumulates several series over its life. Always aggregate (`max by`, `sum by`) — never chart a raw series.
- **Retired agents linger in the metric registry** until an EFM pod restart, even after their `agent` row is deleted — the micrometer counters are in-memory. Filter by `agentClass`/`agentId` rather than waiting for the registry to clean itself.
- **The `last_seen` column in EFM's Postgres is not this metric.** The DB column updates only on material change; the actuator series updates on every heartbeat. For "is it alive right now," the metric wins.

### The Fleet Dashboard

Those series drive the **EFM Fleet - All Devices** Grafana dashboard ([`files/efm-fleet-dashboard.json`](files/efm-fleet-dashboard.json)): a seconds-since-heartbeat stat tile per device (green under 120s, yellow under 600s, red beyond), an all-device sawtooth graph (a healthy device saws between 0 and its heartbeat interval; a dying one just climbs), and a host row — scrape status, CPU, memory — for each device with a Layer-2 exporter (the Jetson, WindowsDesktop, and StarlinkAI legs below). Devices without an exporter get a Layer-1 row (sawtooth, heartbeats/min, average heartbeat size) instead.

Two deployment conventions on this stack:

- **Dashboards deploy as sidecar ConfigMaps, not manual imports.** Any ConfigMap labeled `grafana_dashboard=1` auto-loads and hot-reloads on `kubectl apply`; the JSON stays versioned in this repo's `files/` as the source of truth.
- **⚠️ The datasource UID trap.** kube-prometheus-stack provisions its Prometheus datasource with the deterministic UID **`PBFA97CFB590B2093`** — *not* `prometheus`. A dashboard JSON hardcoding the wrong UID renders every panel "No data" while Prometheus itself is fine, and API-side sanity checks pass because they query Prometheus directly. Verify the way panels actually query: `GET /api/datasources` for the real UID, then run a panel expression through `/api/datasources/proxy/uid/<uid>/api/v1/query`.

## Layer 2 — MiNiFi C++ Agent Metrics

MiNiFi C++ has a native Prometheus publisher — no `ExecuteScript`, no sidecar. It ships as a separate extension, `libminifi-prometheus.so`. Confirm it's present in the agent's `extensions/` directory before troubleshooting a "publisher never starts" symptom.

### Corrected Property Names

> **⚠️ The `nifi.c2.*` property names documented in earlier revisions of this guide do not exist in MiNiFi C++ 1.26.02.** Those keys are never read by the binary — confirmed by `strings` against `libminifi-prometheus.so` and by the shipped `minifi.properties` template itself, which shows the real keys commented out under a "Publish metrics to external consumers" header. The real namespace is `nifi.metrics.publisher.*`.

Drop in a new file `conf/minifi.properties.d/95-metrics.properties` — do not edit `minifi.properties` directly (the main file's own header warns it is overwritten on upgrade, and EFM writes its own `90_c2.properties` there on enrollment):

```properties
# conf/minifi.properties.d/95-metrics.properties
nifi.metrics.publisher.agent.identifier=<agent-uuid — matches nifi.c2.agent.identifier>
nifi.metrics.publisher.class=PrometheusMetricsPublisher
nifi.metrics.publisher.PrometheusMetricsPublisher.port=9936
nifi.metrics.publisher.metrics=QueueMetrics,RepositoryMetrics,DeviceInfoNode,FlowInformation
```

Notes from the field:

- **Default port is `9936`, not `9092`.** The binary accepts any free port, but `9092` collides by name with the common Kafka broker convention. The shipped template itself suggests `9936` — prefer it unless there's a specific reason not to.
- **`nifi.metrics.publisher.metrics` is a comma-separated list of metric-node classes, not a boolean toggle.** `QueueMetrics` and `RepositoryMetrics` are always available. `DeviceInfoNode` and `FlowInformation` are the general per-agent and per-processor nodes. A class tied to a specific processor (e.g. `GetFileMetrics`) only emits if a processor of that type actually exists in the agent's flow — check `config.yml` first, or it is silently a no-op.
- **The setting only takes effect on a service restart, not a config-only reload.**

### Field Validation — NvidiaNano (Real Jetson Hardware, Systemd-Managed)

After restarting the systemd-managed `minifi` service with the corrected config:

```text
[...] [PrometheusExposerWrapper] [info] Started Prometheus metrics publisher on port 9936
[...] [PrometheusMetricsPublisher] [info] Loading metric node 'flowInfo'
[...] [PrometheusMetricsPublisher] [info] Loading metric node 'deviceInfo'
[...] [PrometheusMetricsPublisher] [info] Loading metric node 'RepositoryMetrics'
[...] [PrometheusMetricsPublisher] [info] Loading metric node 'QueueMetrics'

$ ss -tlnp | grep 9936
LISTEN 0  200  0.0.0.0:9936  0.0.0.0:*  users:(("minifi",pid=203867,fd=18))

$ curl -s http://127.0.0.1:9936/metrics | wc -l
204
$ curl -s http://127.0.0.1:9936/metrics | grep minifi_is_running | head -3
minifi_is_running{metric_class="FlowInformation",component_name="FlowController",
  component_uuid="87ea1666-8b6f-11f1-bcfa-580205de1a71",
  agent_identifier="4ca82a0d-8e04-4ede-b59d-379de1495f2b"} 1
```

Binds `0.0.0.0`, so it is LAN-reachable. Series carry `agent_identifier`, `metric_class`, and per-connection/per-processor tags — exactly the shape a Grafana panel needs.

### Field Validation — WindowsDesktopCpp

Writing `95-metrics.properties` to `C:\WINDOWS\System32\nifi-minifi-cpp\conf\minifi.properties.d\` is blocked by UAC Admin Approval Mode. An admin account is in `BUILTIN\Administrators`, but the live process token returns `IsInRole(Administrator) = False` (filtered standard token). `Get-Acl` confirms `BUILTIN\Administrators` has `FullControl` but `BUILTIN\Users` (the effective group on the filtered token) only has `ReadAndExecute` — which matches the denial exactly.

**The fix is an elevated write.** `Start-Process powershell -Verb RunAs -Wait` pops the UAC consent prompt; the elevated script writes `95-metrics.properties` and runs `Restart-Service -Name "Apache NiFi MiNiFi" -Force` in the same elevated context. Confirmed live immediately after:

```powershell
Get-Service "Apache NiFi MiNiFi"  # Status: Running
Get-NetTCPConnection -LocalPort 9936  # State: Listen
curl http://127.0.0.1:9936/metrics   # returns real minifi_* Prometheus text
```

The response includes `agent_identifier="40eb2f92-94c5-4478-beed-7060e41c9d7f"` on live queue/connection metrics from the running flow. The one-time UAC prompt is the entire blocker.

### Wiring C++ Agent Metrics into CSO Prometheus (WindowsDesktop)

The agent runs on the Windows host, not as a Kubernetes pod, so use the external-target pattern — a headless `Service` + `Endpoints` object pointing at the host IP, plus a `ServiceMonitor`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: windowsdesktopcpp-minifi-metrics
  namespace: cld-streaming
  labels:
    app: windowsdesktopcpp-minifi-metrics
spec:
  ports:
  - name: metrics
    port: 9936
    targetPort: 9936
  clusterIP: None
---
apiVersion: v1
kind: Endpoints
metadata:
  name: windowsdesktopcpp-minifi-metrics
  namespace: cld-streaming
subsets:
- addresses:
  - ip: 192.168.1.121     # WindowsDesktop LAN IP
  ports:
  - name: metrics
    port: 9936
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: windowsdesktopcpp-minifi-metrics
  namespace: cld-streaming
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: windowsdesktopcpp-minifi-metrics
  endpoints:
  - port: metrics
    path: /metrics
    interval: 15s
```

Confirm the target via Prometheus's own `/api/v1/targets` (`health: "up"`) and a live PromQL query returning real per-connection series from the running flow. Job name: `windowsdesktopcpp-minifi-metrics`.

### Restarting the C++ Agent — The Real Mechanics

Applying a `minifi.properties.d/*.properties` change requires restarting the `minifi` systemd service. Three paths that look equivalent are not:

- **`sudo systemctl restart minifi` — the only path that reliably works on Linux (NvidiaNano, StarlinkAI).** Requires an interactive sudo password on NvidiaNano; with no `NOPASSWD` sudoers entry, an automated session cannot supply that password non-interactively.
- **`~/minifi-1.26.02/bin/minifi.sh restart` is not an independent alternative.** Reading the script shows its `restart_service()` just calls `systemctl restart minifi.service` on Linux — needs the exact same sudo privilege.
- **Killing the process directly does not reliably force a systemd respawn.** The unit file sets `Restart=on-failure` with `RestartForceExitStatus=3` — that rule fires on a specific exit code used by the agent's own C2-triggered restart path, not on an externally sent `SIGTERM`. Confirmed live: sending `SIGTERM` to the MiNiFi PID made the process exit cleanly, `systemctl is-active` immediately reported `inactive`, and the agent stayed down until a human ran `sudo systemctl start minifi`.

### Networking — The Port Has to Be Reachable

The publisher binds `0.0.0.0`, but:

- Confirm the host firewall allows the port (default `9936`) on the interface the scraper arrives on.
- On Windows targets a dedicated inbound allow rule for `9936` is mandatory — Defender defaults `BlockInbound` and drops silently. Both WindowsDesktop and the StarlinkAI Beelink needed `netsh advfirewall firewall add rule ... localport=9936` (elevated) before the in-cluster scrape connected; the StarlinkAI scrape additionally travels over Tailscale, not the LAN (the as-built details are in the Route 3 section below).

## Layer 2 — MiNiFi Java Agent Metrics (Prometheus endpoint blocked; unblocked via S2S relay)

> **⚠️ The *built-in Prometheus endpoint* is conclusively blocked on an EFM/C2-managed headless Java agent — a confirmed platform limit. But that is not the end of the story: the agent's metrics still reach the operator NiFi over a Site-to-Site relay. Both the block and the unblock are documented below.**

On the C++ side, enabling Prometheus metrics is a three-line properties drop-in. On the Java side (`WindowsDesktop` agent, MiNiFi Java `2.24.08.0-19`), both real paths were exhausted:

**Why there is no drop-in property equivalent.** `minifi.properties` has no `metric`/`prometheus`/`reporting` properties at all — no commented-out template to uncomment, unlike C++. `bootstrap.conf`'s only documented status reporter is `StatusLogger`, which writes to a file, not a metrics endpoint. The live `flow.json.gz` has `"reportingTasks":[]` and no Prometheus-capable reporting-task NAR ships in `lib\` or `extensions\` (`nifi-site-to-site-reporting-nar` is the only one present — that is S2S provenance reporting, not Prometheus).

**Path A — enable the embedded web API (`nifi.web.http.port`).** NiFi 2.x's built-in `/nifi-api/flow/metrics/prometheus` REST endpoint requires the embedded Jetty web server to be running. `nifi.web.http.port` is empty on this agent — it runs fully headless. Staged and tested live: `nifi.web.http.host=127.0.0.1` and `nifi.web.http.port=8998` were set directly in `conf/minifi.properties` (writable without elevation). After restart via `run-minifi.bat`, both properties were back to empty — EFM regenerates this agent's `minifi.properties` from its C2-stored config on every boot, regardless of which key changed. `/nifi-api/flow/metrics/prometheus` was never reachable (connection refused on `8998`, confirmed from both WSL2 and Windows-side `Invoke-WebRequest`).

**Path B — push `nifi.web.http.*` through EFM's C2 `UPDATE_PROPERTIES`.** Since direct file edit reverts on restart, the only remaining channel is EFM's own C2 push. Both keys were inserted directly into EFM's Postgres `property_updates` table (agent class `WindowsDesktop`) — required because `PUT /efm/api/agent-classes/WindowsDesktop` returns `200` but does not persist (confirmed via direct DB read immediately after; a separate EFM bug). After an EFM pod restart to force cache reload, both properties were pushed to the live agent every ~5s and rejected every time — `operation.state = FAILED` for both `nifi.web.http.host` and `nifi.web.http.port`. This is the same server-side C2 denylist behavior seen for `nifi.python.command`.

**Path B is also the only path.** Searching the exact-matching `2.24.08.0-19` source tarball (`~/efm-binaries/nifi-minifi-java-2.0.0.2.24.08.0-19-source.tar.gz`) end to end: the only Prometheus code anywhere — `org.apache.nifi.prometheusutil.*`, `PrometheusMetricsWriter` — lives inside `nifi-web-api` itself, wired directly to the embedded Jetty server. There is no separate `nifi-prometheus-nar` module. `/nifi-api/flow/metrics/prometheus` is only reachable by enabling the embedded web API. What looked like two independent paths was actually the same path.

**Conclusion for the built-in endpoint: on this specific platform combination — an EFM/C2-managed, headless MiNiFi Java `2.24.08.0-19` agent — there is no supported channel to get NiFi 2.x's built-in Prometheus endpoint live.** Not a config mistake, not an oversight. Direct file edit reverts on restart, the C2 protocol itself blocks the properties needed to turn the embedded web API on, and no alternative NAR-based metrics path ships in this build. The C++ Layer 2 pattern is the reference for what a working MiNiFi *Prometheus* target looks like on this stack.

But "no Prometheus endpoint" is not "no metrics." The `nifi-site-to-site-reporting-nar` that *is* present ships a Site-to-Site relay — the unblock below.

### Unblocked — Java metrics over a Site-to-Site relay

The metrics goal — get the Java agent's operational state back to the same NiFi Prometheus/Grafana stack — is achievable by relaying records over secure Site-to-Site into an operator NiFi input port, instead of opening a scrape endpoint on the agent. Two routes, both field-validated:

**Route 1 — EFM-managed, `PutRecord → SiteToSiteReportingRecordSink`.** A formal `ReportingTask` cannot be configured through EFM at all — every `reporting-tasks` Designer endpoint returns 404 and the Designer's `flowContent` has no `reportingTasks` key. The equivalent that *is* Designer-manageable is a **controller service** from the same NAR — `org.apache.nifi.reporting.sink.SiteToSiteReportingRecordSink` — driven by a stock `PutRecord`. The working flow:

```
GenerateFlowFile (30 sec) → ExecuteStreamCommand (reads /proc/loadavg + /proc/meminfo → JSON)
   → PutRecord (Record Reader = JsonTreeReader, Record Sink = SiteToSiteReportingRecordSink)
```

A record transiting into the target NiFi's input port:

```json
{"agent_id":"minifi-java-agent","timestamp":1786057534,"load1":5.24,"load5":5.89,"load15":6.75,
 "mem_total_kb":32555448,"mem_free_kb":12219880,"mem_available_kb":22280236}
```

Two wiring notes that each cost a debug cycle:

- **The RecordSink's SSL Context is explicit, not inherited.** Unlike an agent's own S2S client (`nifi.minifi.flow.use.parent.ssl`), this controller service has its own `SSL Context Service` property — leave it unset and the session is unauthenticated and rejected. Point it at a `StandardRestrictedSSLContextService` carrying the agent's client keystore + CA truststore (PKCS12). The transport-protocol property key is `s2s-transport-protocol` (set `HTTP`), not a display name. EFM also rejects literal sensitive values — keystore passwords must be a Parameter Context reference (`#{…}`).
- **`ExecuteStreamCommand` mangles an inline quoted `sh -c` script**, stripping the quote grouping so the JSON keys come out unquoted. Base64-encode the script and run `sh -c "echo <b64> | base64 -d | sh"` so no quotes reach its argument tokenizer.

**Route 2 — unmanaged agent, the real `SiteToSiteMetricsReportingTask`.** An agent whose config is authored directly (not EFM/C2-managed) can run the actual reporting task, bypassing the C2 denylist. It delivers the *full* JVM/NiFi internal metric set — `jvm.heap_used`, `loadAverage1min`, `FlowFilesQueued`, GC counters, thread states — into the same input port, richer than the managed RecordSink can produce (a stock processor cannot read the agent's internal metric registry without the embedded web API).

**Honest scope.** This is a metrics relay, not Prometheus parity: the managed route carries host/OS metrics, and neither route exposes a scrape endpoint on the agent. Both push records into NiFi — which is exactly where the CSO Prometheus stack already scrapes, so the edge metrics land on the same Grafana as everything else.

### Route 3 — the flow itself as the Prometheus exporter (production path, field-validated 2026-08-14)

The S2S relay assumes a Site-to-Site-enabled target NiFi — which the production instance deliberately does not have yet (S2S adoption is its own migration project). The route that actually shipped on the `NvidiaNano` Java agent needs no S2S, no new edge services, and no C2-blocked properties: **a fourth `HandleHttpRequest → ExecuteStreamCommand → HandleHttpResponse` leg on the agent's existing flow serves `/metrics` on port 9936** (the same port the C++ publisher used), emitting `# TYPE`-annotated gauges built from `/proc/loadavg` and `/proc/meminfo`. The agent *is* the scrape endpoint — exactly what the built-in-endpoint block was preventing — but implemented entirely as an EFM-designed, C2-pushed flow using the same synchronous HTTP pattern the agent's production inference legs already run.

Cluster side, the C++-era external-target wiring carries over unchanged: a selector-less `Service` + manual `Endpoints` → `192.168.1.197:9936` + `ServiceMonitor` (`job="nvidianano-minifi-metrics"`, 15s interval). One Prometheus 3 requirement: the flow-level responder sends no `Content-Type` header and Prometheus 3 refuses a blank one (`non-compliant scrape target sending blank Content-Type`) — set `spec.fallbackScrapeProtocol: PrometheusText0.0.4` on the `ServiceMonitor`. Verified live: `up{job="nvidianano-minifi-metrics"}=1`, six `minifi_java_host_*` series in Prometheus, rendered on the sidecar-loaded **"MiNiFi Java - NvidiaNano"** Grafana dashboard. Full flow shape, series list, and files: [Chapter 19](ch19-efm-and-nvidia-jetson.md) "Java Agent Metrics Path (Confirmed)".

**The Windows variant (field-validated on the `WindowsDesktop` Java agent, 2026-08-15).** Same fourth-leg shape; three Windows-specific substitutions:

- No `/proc` — the script runs via **`powershell.exe -NoProfile -EncodedCommand <base64-UTF-16LE>`** (the Windows equivalent of the `sh` base64 wrapper: the encoded form survives `ExecuteStreamCommand`'s `;` argument delimiter and quoting untouched). Metrics from `Get-CimInstance Win32_OperatingSystem` (memory, already KB) and `Win32_Processor` `LoadPercentage` (CPU %).
- **PowerShell emits CRLF, and Prometheus rejects it** (`invalid metric type "gauge\r"`). Build the exposition text as one string and `[Console]::Out.Write(($lines -join "`n") + "`n")` — never let default `Write-Output` line endings reach the wire.
- **Windows Defender Firewall silently drops the inbound scrape** on the LAN IP even from the same physical host — an elevated `netsh advfirewall firewall add rule ... localport=9936` allow rule is mandatory (the error signature before the rule is connection-failure; after the rule, any remaining error is the parser telling you about the payload). A WSL-side `curl` to the host's own LAN IP is *not* a valid reachability test in mirrored mode — it can keep failing after the in-cluster scrape works; test via loopback locally and via Prometheus's own target status for the real path.

**The remote variant — StarlinkAI over Tailscale (field-validated 2026-08-15).** The third replay of the same fourth-leg shape, on the `StarlinkAI` class running on the Beelink — a Windows box that isn't on the cluster's LAN path. Same `-EncodedCommand` CIM script as WindowsDesktop; what's new is the network leg, and it produced the sharpest lesson of the set:

- **The scrape target address is per-device, and only an in-cluster test decides it.** From an in-cluster pod, the Beelink's LAN IP (`192.168.1.245:9936`) times out — *even with the firewall rule verified on all three profiles* — while its Tailscale IP (`100.110.253.66:9936`) answers. That is the opposite of the Jetson and WindowsDesktop targets, which scrape over LAN. Run a `wget`-from-a-busybox-pod test against every candidate address *before* writing the `Endpoints`; a successful test from the target host itself proves nothing about the cluster's path. The selector-less `Service`/`Endpoints` therefore point at the Tailscale IP, with the same `fallbackScrapeProtocol: PrometheusText0.0.4` on the `ServiceMonitor`.
- **The CRLF trap recurs — treat the LF-join as part of the pattern, not a one-off fix.** The first StarlinkAI flow version was published minutes before the WindowsDesktop CRLF lesson landed, and the first scrape failed with the identical `invalid metric type "gauge\r"`. Republishing with the `[Console]::Out.Write` LF-joined script fixed it immediately. Any PowerShell-emitting exporter leg should ship the LF-join from the first version.

Verified live: `up{job="starlinkai-minifi-metrics"}=1` with real values, and the StarlinkAI host row (scrape status / CPU / memory) on the fleet dashboard. Flow export: [`files/efm/StarlinkAI.json`](files/efm/StarlinkAI.json).

## Layer 3 — Embedded Heartbeat Metrics (XIAO/microfi)

The ESP32-class agent is too small to run a Prometheus server. Instead it puts its own health into the **C2 heartbeat**: LittleFS durable-storage counters with watermark-based eviction — `littleFsUsedBytes`, `littleFsCapacityBytes`, `littleFsFillPercent`, `evictionCount`, `failedWrites`, `storedRecords` — emitted under `status.microfi` in every heartbeat (`CONFIG_MICROFI_STORAGE_METRICS=y` is the firmware default), alongside `queueDepth` / `produced` / `consumed` engine counters.

The plan was "EFM holds the agent state, Prometheus scrapes EFM." Field-checking that on EFM 2.3.1.0-2 killed it, at two independent points:

- **EFM drops the payload.** `GET /efm/api/agents/{agentId}` for a live MicroFi agent returns the parsed heartbeat status — `uptime`, `repositories.flowFile`, `resourceConsumption` — and no `microfi` block at all. EFM deserializes the heartbeat into its own DTO and unknown fields vanish. "EFM tolerates unknown fields" means exactly that: tolerated, not stored.
- **The actuator never re-exports payload fields anyway.** The full family list on `/efm/actuator/prometheus` carries per-agent `agentClass`/`agentId` labels only on the heartbeat-transport series — `efm_heartbeat_count_total`, `efm_heartbeat_lastSeenTime_seconds`, `efm_heartbeat_content_*` (payload size), `efm_heartbeat_time_seconds*` (processing latency). Nothing from inside the heartbeat body comes back out, for any agent class.

So the storage counters exist in exactly one place: on the wire between the device and EFM. No intermediary that polls EFM can recover them, and Prometheus has nothing to scrape. What Layer 3 gets on this stack is the heartbeat-transport series — and those are real: the fleet dashboard's MicroFi-1/2/3 rows (seconds-since-heartbeat, heartbeats/min, average heartbeat size) are built entirely from them, and average heartbeat size doubles as a coarse payload signal — a heartbeat carrying the storage block is measurably bigger than one without it.

Putting the storage counters themselves on a panel needs one of two real changes, both outside this chapter's scope: EFM re-exporting heartbeat payload fields (a vendor gap), or the device publishing metrics through its own egress path — MicroFi-3 already publishes Sparkplug B to Mosquitto, and the MQTT → NiFi → Prometheus road is already paved. That work belongs to the MicroFi R&D stream, not the observability wiring.

## What NOT to Do

**Don't assume EFM's `9092` and the agent's `9936` are the same thing.** One is the EFM pod's (non-functional) `metrics` port in `cld-streaming`; the other is the publisher port the MiNiFi C++ agent opens on the edge host. They are on different machines; the only way they conflict is if you pick the same number deliberately.

**Don't scrape the `metrics/9092` port on the EFM Service.** It is declared on the Service but serves an empty reply. EFM's actuator and `/prometheus` endpoint are on `efm-ui/10090` under `/efm`. Point the `ServiceMonitor` at `port: efm-ui`, or set `management.server.port=9092` in the `efm-config` ConfigMap and redeploy first.

**Don't `kubectl exec ... -- curl` into the EFM pod.** The image ships no `curl`. Port-forward `10090` to the host and curl locally to check health or the Prometheus endpoint.

**Don't apply the `ServiceMonitor` and call it done.** Confirm the target shows green and a value lands in Prometheus (`up{job="efm"}=1`) before trusting it. The port trap above silently yields an empty scrape if the wrong port is used.

**Don't design a metric to ride custom fields in the C2 heartbeat and expect it downstream.** EFM 2.3.1.0-2 deserializes heartbeats into a fixed DTO — unknown payload fields are dropped, not stored, and the actuator re-exports nothing from the heartbeat body. A custom heartbeat metric is visible to nobody. If a device metric must reach Prometheus, give it a real egress (a scrape endpoint or an MQTT/Kafka publish), not a heartbeat side-channel.

**Don't configure the MiNiFi C++ publisher with `nifi.c2.*` property names.** That namespace does not exist in MiNiFi C++ 1.26.02. The real keys are `nifi.metrics.publisher.*`.

**Don't edit `minifi.properties` directly on C++ agents.** The file warns it is overwritten on upgrade, and EFM writes `90_c2.properties` into `conf/minifi.properties.d/` on enrollment. Use a new drop-in file like `95-metrics.properties` instead.

**Don't treat "kill the MiNiFi C++ process" as a safe unattended restart.** `Restart=on-failure` does not catch a plain `SIGTERM` — confirmed live on NvidiaNano, the agent stayed down. Use `sudo systemctl restart minifi` (requires a human at the terminal; no passwordless sudo configured).

**Don't attempt to enable the built-in Java Prometheus endpoint by editing the agent's `minifi.properties` directly.** EFM regenerates that file from its C2-stored config on every agent boot — the edit reverts on the next restart. The C2 protocol itself also blocks `nifi.web.http.*` properties server-side. Both paths to the *built-in endpoint* are exhausted — for the working alternative, use the Site-to-Site metrics relay above, not the scrape endpoint.
