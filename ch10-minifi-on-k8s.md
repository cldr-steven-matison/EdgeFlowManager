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

> **🟡 Held — live production-agent introspection.** A deeper side-by-side introspection of the live `KubernetesPod` and `KubernetesPodJava` agents in `cld-streaming` — dumping each agent's applied flow, exact resource footprint under load, and the differences between the playground `PlaygroundCpp`/`PlaygroundJava` pods and the production classes — is field work to be done on the cluster, not authored from here. Tracked as its own issue; this section fills in once that run lands.

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
