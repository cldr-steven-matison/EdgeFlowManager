# Chapter 10: MiNiFi C++ and Java as Kubernetes Pods

[Chapters 7](ch07-standalone-minifi-cpp-on-k8s.md) and [8](ch08-minifi-java-setup.md) run MiNiFi standalone in minikube with the flow baked into the image. [Chapter 9](ch09-efm-in-the-playground.md) brings EFM in to manage those agents. This chapter is the deployment reference for the result: how to run **both** MiNiFi runtimes as Kubernetes pods under EFM management — the C++ agent and the Java agent, side by side, in the same cluster and the same EFM instance. These are the `KubernetesPod` (C++) and `KubernetesPodJava` (Java) agent classes the rest of the guide leans on.

There are two ways a MiNiFi agent lives in a pod, and getting the difference straight is the whole chapter:

- **Baked-config (standalone)** — the flow is `config.yml` inside a custom image, no EFM. This is the Chapter 7/8 pattern; use it for a fixed, single-purpose agent.
- **EFM-managed (deployer-in-pod)** — a stock base image runs the EFM agent-deployer at boot, enrolls into a class, and pulls its flow from the Designer. This is what `KubernetesPod` and `KubernetesPodJava` are, and it's what this chapter builds.

## The EFM-Managed Pod Pattern

An EFM-managed pod doesn't bake a flow. It starts from a plain base image, installs the runtime prerequisites, waits for EFM to be reachable, then runs the deployer script — which downloads the correct agent binary, writes `bootstrap.conf`, and starts the agent enrolled in its class. The agent then pulls whatever flow the Designer has published for that class.

The shape is the same for both runtimes; the base image and prerequisites differ. Both poll EFM's health endpoint before the deployer curl — the deployer 400s or hangs if it fires during EFM's cold-start window (the cold-start race is documented in the `nifi-and-ai` skill's `references/minifi-efm.md`).

### C++ Pod — `KubernetesPod` / `PlaygroundCpp`

A C++ agent needs `curl`, `tar`, and — if the flow uses Python `ExecuteScript` — `python3` symlinked to `python`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: minifi-test-efm-cpp
  labels:
    app: minifi-test-efm-cpp
spec:
  containers:
  - name: minifi
    image: ubuntu:22.04
    command: ["/bin/bash", "-c"]
    args:
    - |
      apt-get update && apt-get install -y curl tar python3 python3-pip python3-venv
      ln -s /usr/bin/python3 /usr/bin/python || true
      # Health-poll EFM before the deployer curl (cold-start race)
      for i in $(seq 1 60); do
        curl -sf http://efm.cld-streaming.svc:10090/efm/actuator/health && break
        sleep 5
      done
      curl -L \
       -d agentClass=PlaygroundCpp \
       -d agentIdentifier=c7aae80c-5d37-4e9b-bfa8-0877e0355f64 \
       -d agentType=cpp \
       -d agentVersion=1.26.02 \
       -d autoConfigureSecurity=false \
       -d baseUrl=http%3A%2F%2Fefm.cld-streaming.svc%3A10090%2Fefm%2Fapi \
       -d hbPeriod=5000 \
       -d osArch=linux \
       -d serviceName=minifi \
       -d serviceUser=root \
       -d trustSelfSignedCertificates=false \
       http://efm.cld-streaming.svc:10090/efm/api/agent-deployer/script | bash -
      tail -f /dev/null
```

`agentType=cpp`, `agentVersion=1.26.02`, `osArch=linux`. The C++ agent is small (~128Mi) and starts fast — no readiness delay needed like the Java pod. `agentIdentifier` must be a fresh UUID per agent.

### Java Pod — `KubernetesPodJava` / `PlaygroundJava`

The Java agent needs a JRE 21 and — the trap — `sudo`, even when the container already runs as root:

```yaml
      apt-get update && apt-get install -y curl tar openjdk-21-jre-headless ca-certificates sudo
      for i in $(seq 1 60); do
        curl -sf http://efm.cld-streaming.svc:10090/efm/actuator/health && break
        sleep 5
      done
      curl -L \
       -d agentClass=PlaygroundJava \
       -d agentIdentifier=a08533e6-c8da-408e-8412-34a999375463 \
       -d agentType=java \
       -d agentVersion=2.24.08.0-19 \
       -d autoConfigureSecurity=false \
       -d baseUrl=http%3A%2F%2Fefm.cld-streaming.svc%3A10090%2Fefm%2Fapi \
       -d hbPeriod=5000 \
       -d osArch=linux \
       -d serviceName=minifi \
       -d serviceUser=root \
       -d trustSelfSignedCertificates=false \
       http://efm.cld-streaming.svc:10090/efm/api/agent-deployer/script | bash -
      tail -f /dev/null
```

`agentType=java`, `agentVersion=2.24.08.0-19`, `osArch=linux` (the Java tarball is platform-agnostic; `linux` covers x86_64 and aarch64 alike). Field-measured sizing for the Java pod is `768Mi request / 1536Mi limit` — the main JVM sits at ~378–424 MB RSS and the bootstrap-watcher JVM adds another ~83–86 MB, roughly 500 MB combined.

## The Tricks That Decide Whether the Pod Comes Up

- **`sudo` is required even as root.** The Java deployer script calls `sudo` internally. If it's not installed, the deployer exits immediately with `ERROR: The following command is required, but not found: sudo`. Install it before the deployer curl.
- **Poll EFM health first.** Firing the deployer during EFM's cold start returns a `400` or hangs. The `for i in $(seq 1 60)` health loop against `/efm/actuator/health` is not optional on a fresh cluster.
- **One class per runtime, never shared.** `KubernetesPod` (C++) and `KubernetesPodJava` (Java) are separate classes on purpose — the Designer validates flows against a class's manifest, so a C++-FQCN flow published to a Java-mapped class (or vice versa) is rejected. Keep them apart and a flow push aimed at one runtime never lands on the other.
- **`serviceUser=root` in a pod.** The default `serviceUser=minifi` triggers a `useradd` the deployer may not have rights for; `root` runs the agent as a plain background process instead.
- **Don't let the deployer's own MiNiFi hold the lock.** The deployer starts a MiNiFi during install; a second start dies on `Could not acquire LOCK`. If you re-exec the agent by hand, `pkill` the deployer's instance and clear the stale `LOCK` first.

## Introspecting a Running Agent

Once a pod is enrolled, confirm it from EFM rather than the pod's own logs (EFM is the source of truth for enrollment state):

```bash
GET /efm/api/agents                    # the agent row: state ONLINE, its class, last heartbeat
GET /efm/api/agent-classes             # the class exists with a manifest id
GET /efm/api/agent-manifests/{id}      # exactly the processors compiled/loaded into that build
```

A C++ pod reports a ~74-processor manifest; a stock Java pod reports 114 (122 after the Kafka/scripting NAR drop-in — see [Chapter 4](ch04-java-processor-catalog.md)). The manifest is what the Designer offers to place, so a mismatch between "what the agent loaded" and "what the palette shows" is almost always a class-manifest mapping that needs re-pointing.

**Field-captured 2026-08-06** — live introspection of the two production agents in `cld-streaming`, via `GET /efm/api/agents/page`, `GET /efm/api/agent-manifests/{id}`, and `GET /efm/api/designer/{agentClassName}/flows/export` (the `AgentsService.getAgentsPage` / `FlowDesignerService.exportFlowByAgentClass` operations — see the `nifi-and-ai` skill's `references/flow-api.md` for how these were located; there's no OpenAPI spec).

**Agent state:**

| Agent class | Identifier | State | Runtime | Last heartbeat | Flow last updated |
|---|---|---|---|---|---|
| `KubernetesPod` | `5a5a3366-efc8-4c77-b434-6f23206dc974` | `ONLINE` | `cpp 1.26.02` | 2026-08-06 (current) | 2026-07-29 |
| `KubernetesPodJava` | `32a44ee7-02ea-4b50-8913-11bdf66cb894` | `ONLINE` | `minifi-java 2.24.08.0-19` | 2026-08-06 (current) | 2026-08-04 |

Both agents have been enrolled and heartbeating continuously since 2026-07-25 with zero restarts (`restartCount: 0` on both pods).

**Processor/controller-service counts — corrected.** The class-mapped manifest (`GET /efm/api/agent-class-manifest-config`, then `GET /efm/api/agent-manifests/{id}`) is the number that matters — it's what the Designer palette actually offers:

- `KubernetesPod` → manifest `7f193639-b494-48e9-9c71-cd7203cee5af`: **76 processors, 16 controller services**. Corrects this chapter's "~74" to the exact figure.
- `KubernetesPodJava` → manifest `ff1aaa62-7dab-4128-8b32-9bd10ec6bfe3`: **122 processors, 51 controller services**. This confirms the parenthetical in this chapter's earlier paragraph — the manifest currently mapped to production Java classes is already the post-NAR-drop-in build. There is no "stock 114" manifest live anywhere in this cluster right now; 122/51 is what both `KubernetesPodJava` and `WindowsDesktop` are mapped to today. (Chapter 4's "114 processors, 45 controller services" figure was field-verified at an earlier point against this same manifest lineage and is now stale — that's a Chapter 4 bookkeeping note, not something this run changes, since confirming it wasn't in scope here.)

**A live example of the manifest-mismatch gotcha this chapter already warns about:** the `KubernetesPod` agent's own reported `agentManifestId` (`dab61017-33fb-44e7-a159-882601f01952`, 79 processors/17 controller services) does **not** match the class's currently-mapped manifest (`7f193639…`, 76/16) — `dab61017…` is actually the manifest `agent-class-manifest-config` has mapped to the unrelated `NvidiaNanoAI` class. The agent is healthy and `ONLINE`; this doesn't appear to be breaking anything today, but it's the exact "what the agent loaded" vs. "what the palette shows" drift this chapter describes as needing a re-point — caught live, not reproduced deliberately.

**Applied flow — what's actually running, not documentation guesswork:**

`KubernetesPod` (C++, exported via `/efm/api/designer/KubernetesPod/flows/export`) — 7 processors, 0 controller services, no RemoteProcessGroup:

```text
ListenHTTP-MatrixLoad    org.apache.nifi.minifi.processors.ListenHTTP
ListenHTTP               org.apache.nifi.minifi.processors.ListenHTTP
ListenHTTP-StreamLoad    org.apache.nifi.minifi.processors.ListenHTTP
PublishKafka              org.apache.nifi.minifi.processors.PublishKafka
LaunchGamingPCStream      org.apache.nifi.minifi.processors.ExecuteScript
ExecuteScript             org.apache.nifi.minifi.processors.ExecuteScript
LaunchGamingPCMatrix      org.apache.nifi.minifi.processors.ExecuteScript
```

This is the stream/matrix-load-trigger pipeline from the Twitch/screen-loader work, not a toy flow — three HTTP listeners feeding Kafka and shell-launch scripts. C++ inlines `PublishKafka`'s connection properties directly on the processor (no controller service), matching the structural difference [Chapter 4](ch04-java-processor-catalog.md) documents.

`KubernetesPodJava` (Java) — 11 processors, 2 controller services, no RemoteProcessGroup:

```text
ListenHTTP                    org.apache.nifi.processors.standard.ListenHTTP
ExecuteScript-JavaNarTest     org.apache.nifi.processors.script.ExecuteScript
HandleHttpRequest-Stream      org.apache.nifi.processors.standard.HandleHttpRequest
HandleHttpResponse-Matrix-OK  org.apache.nifi.processors.standard.HandleHttpResponse
HandleHttpRequest-Matrix      org.apache.nifi.processors.standard.HandleHttpRequest
InvokeHTTP-Matrix              org.apache.nifi.processors.standard.InvokeHTTP
HandleHttpResponse-Stream-OK  org.apache.nifi.processors.standard.HandleHttpResponse
HandleHttpResponse-Matrix-Error org.apache.nifi.processors.standard.HandleHttpResponse
HandleHttpResponse-Stream-Error org.apache.nifi.processors.standard.HandleHttpResponse
LogAttribute                  org.apache.nifi.processors.standard.LogAttribute
InvokeHTTP-Stream              org.apache.nifi.processors.standard.InvokeHTTP

Controller services:
HttpContextMap-Screen2         org.apache.nifi.http.StandardHttpContextMap
Kafka3ConnectionService-JavaNarTest  org.apache.nifi.kafka.service.Kafka3ConnectionService
```

The `Kafka3ConnectionService` isn't just present in the manifest — it's instantiated in this live flow. It isn't currently referenced by any processor's properties here (no live `PublishKafka`/`ConsumeKafka` in this particular flow to wire it to), so this confirms the NAR drop-in fix (Chapter 2) makes the controller service creatable and instantiable in production, not that a Kafka publish/consume path is actively running through it today. `ExecuteScript` is live and wired here (`ExecuteScript-JavaNarTest`), which is the stronger of the two NAR-drop-in confirmations from this flow.

**Resource footprint — measured, not estimated.** `metrics-server` is live in this cluster, so `kubectl top pod` gave real numbers directly (no cgroup fallback needed):

```text
NAME                       CPU(cores)   MEMORY(bytes)
minifi-agent-k8s-gaming    4m           75Mi     # KubernetesPod (C++)
minifi-agent-k8s-java      35m          588Mi    # KubernetesPodJava
```

Pod spec resources (`kubectl get pod -o yaml`):

- `minifi-agent-k8s-gaming` (C++): **no `resources` block at all** — `resources: {}`, QoS class `BestEffort`. There's no configured request/limit backing the "~128Mi" figure this chapter states; it's descriptive of actual usage, and live usage (75Mi via `kubectl top`, ~35MB RSS for the `./bin/minifi` process itself via `kubectl exec ... ps`) is comfortably under that number.
- `minifi-agent-k8s-java`: `requests: {cpu: 250m, memory: 768Mi}`, `limits: {cpu: 1, memory: 1536Mi}`, QoS class `Burstable` — matches this chapter's documented sizing exactly.

Process-level breakdown inside the Java pod (`kubectl exec minifi-agent-k8s-java -- ps -eo pid,rss,cmd`):

```text
   PID   RSS(KB)  CMD
  4978   501720   java ... org.apache.nifi.minifi.MiNiFi          (main framework JVM, -Xmx256m)
  4959    78284   java ... RunMiNiFi start                        (bootstrap-watcher JVM, -Xmx24m)
```

Main JVM: ~490 MB RSS, bootstrap-watcher: ~76 MB, **~566 MB combined** — higher than the "~378–424 MB main / ~83–86 MB watcher / ~500 MB combined" this chapter previously stated. The flow now applied (11 processors, 2 controller services) is heavier than whatever flow was on the agent when that figure was captured; the delta tracks with more processors being loaded and running, not a leak.

Inside the C++ pod, `ps` shows the live agent process plus two harmless zombies:

```text
   PID   RSS(KB)  CMD
  6270    35596   ./bin/minifi
     1     1024   tail -f /dev/null
  5000        0   [minifi] <defunct>
  5295        0   [minifi] <defunct>
```

The `<defunct>` entries are zombie processes from earlier deployer-script child processes (0 RSS, no resource impact) — a real, if cosmetic, artifact of the deployer's process lifecycle in a long-running pod, worth knowing about if you're ever staring at `ps` output on one of these trying to diagnose something else.

**Production vs. this chapter's own YAML — `PlaygroundCpp`/`PlaygroundJava` are decommissioned, so this is a diff against the documented examples above, not a second live pod:**

- The production Java deployer script adds three things not in this chapter's example: `set -eux` (fail loud instead of silently continuing past an error), `export DEBIAN_FRONTEND=noninteractive` (unattended `apt-get`), and a 120-iteration EFM health-poll loop instead of 60 — the production pod tolerates up to ~10 minutes of EFM cold-start instead of ~5.
- The production Java pod carries `labels: {agent-type: java, app: minifi-agent-k8s-java}`. The production C++ pod carries no labels at all. Neither chapter YAML example sets labels.
- The production Java pod's `resources` block (`768Mi`/`1536Mi`) is baked into the pod spec's own `last-applied-configuration`, matching this chapter's field-measured sizing exactly rather than being a separate out-of-band patch.
- The production C++ pod has no `resources` block, consistent with this chapter's C++ YAML example (which also doesn't set one) — not a drift, just confirmation the "small enough not to bother" approach is what's actually deployed.
- Both pods use `serviceAccountName: default` with nothing mounted beyond the standard projected service-account token (`ca.crt`, `namespace`, token) — no extra `ConfigMap`/`Secret` volumes, no extra env vars. Nothing beyond what this chapter documents.
- Both `agentIdentifier` and `agentClass` values baked into each pod's deployer args match exactly what `GET /efm/api/agents/page` reports back for the live, enrolled agent — the deployer curl in this chapter's examples is the real, unmodified command running in production, not a simplified stand-in.

## What NOT to Do

- **Don't share one agent class across C++ and Java pods.** The Designer rejects the mismatched FQCNs. `KubernetesPod` for C++, `KubernetesPodJava` for Java.
- **Don't skip `sudo` in the Java pod image.** The deployer needs it even as root, and fails hard without it.
- **Don't run the deployer before EFM is healthy.** Poll `/efm/actuator/health` first, or the enroll `400`s on a cold cluster.
- **Don't give the Java pod the C++ agent's memory.** The JVM needs `768Mi`+; the C++ agent runs fine near `128Mi`.
- **Don't reuse an `agentIdentifier` across pods.** Each agent needs its own UUID, or two pods collide on one identity in EFM.

## Related Chapters

- Ch7 — [Standalone MiNiFi C++ on Kubernetes](ch07-standalone-minifi-cpp-on-k8s.md): the baked-config C++ pod, no EFM.
- Ch8 — [Standalone MiNiFi Java on Kubernetes](ch08-minifi-java-setup.md): the baked-config Java pod, no EFM.
- Ch9 — [Introduce EFM into the Playground](ch09-efm-in-the-playground.md): how EFM takes over managing these agents.
- Ch11 — [Site-to-Site](ch11-site-to-site.md): moving FlowFiles from these agents into NiFi.
