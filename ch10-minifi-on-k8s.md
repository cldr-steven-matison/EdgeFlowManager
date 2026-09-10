# Chapter 10: MiNiFi C++ and Java as Kubernetes Pods

[Chapters 7](ch07-standalone-minifi-cpp-on-k8s.md) and [8](ch08-minifi-java-setup.md) run MiNiFi standalone in minikube with the flow baked into the image. [Chapter 9](ch09-efm-in-the-playground.md) brings EFM in to manage those agents. This chapter is the deployment reference for the result. How to run both MiNiFi runtimes as Kubernetes pods under EFM management, the C++ agent and the Java agent, side by side, in the same cluster and the same EFM instance. These are the `KubernetesPod` (C++) and `KubernetesPodJava` (Java) agent classes the rest of the guide leans on.

There are two ways a MiNiFi agent lives in a pod, and getting the difference straight is the whole chapter.

- Baked-config (standalone). The flow is `config.yml` inside a custom image, no EFM. This is the Chapter 7/8 pattern. Use it for a fixed, single-purpose agent.
- EFM-managed (deployer-in-pod). A stock base image runs the EFM agent-deployer at boot, enrolls into a class, and pulls its flow from the Designer. This is what `KubernetesPod` and `KubernetesPodJava` are, and it is what this chapter builds.

## The EFM-Managed Pod Pattern

An EFM-managed pod does not bake a flow. It starts from a plain base image, installs the runtime prerequisites, waits for EFM to be reachable, then runs the deployer script, which downloads the correct agent binary, writes `bootstrap.conf`, and starts the agent enrolled in its class. The agent then pulls whatever flow the Designer has published for that class.

The shape is the same for both runtimes. The base image and prerequisites differ. Both poll EFM's health endpoint before the deployer command, because the deployer 400s or hangs if it fires during EFM's cold-start window.

The deployer command itself comes from EFM. Generate it on the Deploy Agent CLI screen, or with `POST /efm/api/agent-deployer/generateCommand` with `agentIdentifier` omitted, for the class and runtime you want, and paste it where the pod spec says. Do not hand-build it and do not reuse an identifier from another agent or an earlier enrollment. A reused identifier makes the C2 `UPDATE` that pushes the flow fail on the new agent.

### C++ Pod, `KubernetesPod` / `PlaygroundCpp`

A C++ agent needs `curl`, `tar`, and, if the flow uses Python `ExecuteScript`, `python3` symlinked to `python`.

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
      set -eux
      export DEBIAN_FRONTEND=noninteractive
      apt-get update && apt-get install -y curl tar python3 python3-pip python3-venv
      ln -s /usr/bin/python3 /usr/bin/python || true
      # Health-poll EFM before the deployer command (cold-start race)
      for i in $(seq 1 120); do
        curl -sf http://efm.cld-streaming.svc:10090/efm/actuator/health && break
        sleep 5
      done
      # Paste the command EFM generated for class PlaygroundCpp here
      # (agentType=cpp, agentVersion=1.26.02, osArch=linux, serviceUser=root,
      #  baseUrl=http://efm.cld-streaming.svc:10090/efm/api)
      <EFM-generated deployer command> | bash -
      tail -f /dev/null
```

`agentType=cpp`, `agentVersion=1.26.02`, `osArch=linux`. The C++ agent is small (about 75Mi in use, 35 MB RSS for the `./bin/minifi` process) and starts fast. No readiness delay needed like the Java pod, and no `resources` block is needed either.

### Java Pod, `KubernetesPodJava` / `PlaygroundJava`

The Java agent needs a JRE 21 and, the trap, `sudo`, even when the container already runs as root.

```yaml
      set -eux
      export DEBIAN_FRONTEND=noninteractive
      apt-get update && apt-get install -y curl tar openjdk-21-jre-headless ca-certificates sudo
      for i in $(seq 1 120); do
        curl -sf http://efm.cld-streaming.svc:10090/efm/actuator/health && break
        sleep 5
      done
      # Paste the command EFM generated for class PlaygroundJava here
      # (agentType=java, agentVersion=2.24.08.0-19, osArch=linux, serviceUser=root)
      <EFM-generated deployer command> | bash -
      tail -f /dev/null
```

`agentType=java`, `agentVersion=2.24.08.0-19`, `osArch=linux` (the Java tarball is platform-agnostic, and `linux` covers x86_64 and aarch64 alike). Size the Java pod at `requests: {cpu: 250m, memory: 768Mi}`, `limits: {cpu: 1, memory: 1536Mi}`. The main JVM (`-Xmx256m`) sits at 380 to 500 MB RSS depending on how many processors the applied flow loads, and the bootstrap-watcher JVM (`-Xmx24m`) adds another 75 to 85 MB, so plan on 500 to 570 MB in use. Label the pod (`agent-type: java`, `app: minifi-agent-k8s-java`) so it is selectable later.

Three lines in both scripts are there on purpose. `set -eux` makes the pod fail loud on the first error instead of continuing past it. `DEBIAN_FRONTEND=noninteractive` keeps `apt-get` from waiting on a prompt. The 120-iteration health loop tolerates about 10 minutes of EFM cold start.

## The Tricks That Decide Whether the Pod Comes Up

- `sudo` is required even as root. The Java deployer script calls `sudo` internally. If it is not installed, the deployer exits immediately with `ERROR: The following command is required, but not found: sudo`. Install it before the deployer command.
- Poll EFM health first. Firing the deployer during EFM's cold start returns a `400` or hangs. The health loop against `/efm/actuator/health` is not optional on a fresh cluster.
- One class per runtime, never shared. `KubernetesPod` (C++) and `KubernetesPodJava` (Java) are separate classes on purpose. The Designer validates flows against a class's manifest, so a C++-FQCN flow published to a Java-mapped class (or vice versa) is rejected. Keep them apart and a flow push aimed at one runtime never lands on the other.
- `serviceUser=root` in a pod. The default `serviceUser=minifi` triggers a `useradd` the deployer may not have rights for. `root` runs the agent as a plain background process instead.
- Do not let the deployer's own MiNiFi hold the lock. The deployer starts a MiNiFi during install, and a second start dies on `Could not acquire LOCK`. If you re-exec the agent by hand, `pkill` the deployer's instance and clear the stale `LOCK` first.

## Introspecting a Running Agent

Once a pod is enrolled, confirm it from EFM, which is the source of truth for enrollment state, and not from the pod's own logs.

```bash
GET /efm/api/agents/page               # the agent row: state ONLINE, its class, last heartbeat
GET /efm/api/agent-classes             # the class exists with a manifest id
GET /efm/api/agent-manifests/{id}      # exactly the processors compiled/loaded into that build
```

A C++ pod reports the stock catalog ([Chapter 3](ch03-cpp-processor-catalog.md)) plus whatever extension bundles are loaded. A stock Java pod reports 114 processors, 122 after the Kafka/scripting NAR drop-in ([Chapter 4](ch04-java-processor-catalog.md)). The manifest is what the Designer offers to place, so a mismatch between "what the agent loaded" and "what the palette shows" is almost always a class-manifest mapping that needs re-pointing.

Check that mapping directly. The manifest the agent reports in its own row (`agentManifestId` on `GET /efm/api/agents/page`) and the manifest the class is mapped to (`GET /efm/api/agent-class-manifest-config`) can drift apart, for example after a manifest change or when a class was re-pointed at a manifest that came from a different agent. The agent stays `ONLINE` and its flow keeps running, so nothing flags it. The palette is built from the class mapping. When a processor you know the agent has is missing from the palette, or the palette offers one the agent rejects, compare the two ids and re-point the class.

What a production pair looks like once flows are applied. The C++ class runs a flow of three `ListenHTTP` listeners feeding `PublishKafka` and `ExecuteScript` shell launchers, with `PublishKafka`'s connection properties inlined on the processor and no controller services, the structural difference [Chapter 4](ch04-java-processor-catalog.md) documents. The Java class runs `HandleHttpRequest`/`HandleHttpResponse` pairs around `InvokeHTTP`, a `StandardHttpContextMap` and a `Kafka3ConnectionService` as controller services, and an `ExecuteScript`, which is the NAR drop-in doing its job in production.

Inside a long-running C++ pod, `ps` shows the agent plus zombie entries from the deployer script's child processes.

```text
   PID   RSS(KB)  CMD
  6270    35596   ./bin/minifi
     1     1024   tail -f /dev/null
  5000        0   [minifi] <defunct>
  5295        0   [minifi] <defunct>
```

The `<defunct>` entries have 0 RSS and no resource impact. They are an artifact of the deployer's process lifecycle, and they are harmless when you are staring at `ps` output trying to diagnose something else.

Both pods run as `serviceAccountName: default` with nothing mounted beyond the projected service-account token. No extra ConfigMap or Secret volumes, no extra env vars.

## What NOT to Do

- Do not share one agent class across C++ and Java pods. The Designer rejects the mismatched FQCNs. `KubernetesPod` for C++, `KubernetesPodJava` for Java.
- Do not skip `sudo` in the Java pod image. The deployer needs it even as root, and fails hard without it.
- Do not run the deployer before EFM is healthy. Poll `/efm/actuator/health` first, or the enroll `400`s on a cold cluster.
- Do not give the Java pod the C++ agent's memory. The JVM needs `768Mi` and up. The C++ agent runs fine near `128Mi`.
- Do not hand-build the deployer command or reuse an `agentIdentifier` across pods. Generate it in EFM per agent. Two pods on one identity collide in EFM, and a reused identifier breaks the flow push to the new one.

## Related Chapters

- [Standalone MiNiFi C++ on Kubernetes](ch07-standalone-minifi-cpp-on-k8s.md) (Ch7): the baked-config C++ pod, no EFM.
- [Standalone MiNiFi Java on Kubernetes](ch08-minifi-java-setup.md) (Ch8): the baked-config Java pod, no EFM.
- [Introduce EFM into the Playground](ch09-efm-in-the-playground.md) (Ch9): how EFM takes over managing these agents.
- [Site-to-Site](ch11-site-to-site.md) (Ch11): moving FlowFiles from these agents into NiFi.
