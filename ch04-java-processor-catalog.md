# Chapter 4: MiNiFi Java Processor Catalog

I run MiNiFi C++ and MiNiFi Java side-by-side in the same minikube playground, same Strimzi Kafka cluster, same EFM server. The swap is a Dockerfile change, a memory bump in the K8s YAML, and a different `agentType` in the EFM deployer curl. What you get from Java is a processor set that C++ can't match out of the box: `HandleHttpRequest`/`HandleHttpResponse` (synchronous request/reply HTTP — absent in C++), a scripting engine once the right NARs are present, and a Record Reader/Writer framework. The field-verified count from a live `minifi-java` agent manifest (`2.24.08.0-19` on WindowsDesktop): **114 processors, 45 controller services**.

---

## What Java gives you that C++ doesn't

| Capability | MiNiFi C++ (`apacheminificpp:latest`) | MiNiFi Java (CEM `2.24.08.0-19` tarball) |
|---|---|---|
| **ExecuteScript** | Not in stock image; requires extra-extensions or source build | Missing from the stock `2.24.08.0-19` tarball, but NAR drop-in fix field-verified — Groovy execution confirmed working |
| **ExecuteProcess** | Not in stock image; only via extra-extensions | **[Cloudera stock]** — shell command execution |
| **HandleHttpRequest / HandleHttpResponse** | Not available — no pair exists in C++ | **[Cloudera stock]** — synchronous request-reply HTTP (Jetty-backed); both share an `HttpContextMap` controller service |
| **PublishKafka / ConsumeKafka** | Present (C++ extensions) | Missing from the stock `2.24.08.0-19` tarball, but NAR drop-in fix field-verified — real transactional Kafka producer confirmed connecting |
| **Record Reader/Writer framework** | `ConvertRecord` and `SplitRecord` present but require controller services | **[Cloudera stock]** — RecordReader/RecordWriter controller services present |
| **Scripting engines** | None without extra-extensions | Shell via `ExecuteProcess`/`ExecuteStreamCommand` in stock; Groovy/Clojure via NAR drop-in |
| **Total processors** | 74 (stock), more via extra-extensions | 114 (field-verified from live agent manifest) |
| **Image size** | ~15 MB | ~300–400 MB |
| **Memory minimum** | ~128Mi | ~512Mi |
| **JVM startup** | None | ~30–60s cold start |
| **Kubernetes sidecar use** | Production-ready | Not recommended — footprint too large |

> **⚠️ Heads up:** There is no `minifi-java` Docker image to check the "200+ processors" figure against. Field-verified: `container.repo.cloudera.com/cloudera/minifi-java:latest` does not exist in the registry (nor ~12 name variants), while `apacheminificpp:latest` resolves — Cloudera containerizes only the C++ agent; MiNiFi Java ships as the tarball. The authoritative count is the tarball's field-verified **114 processors / 45 controller services**; "200+" has no running Java manifest behind it.

---

## Footprint comparison

Real numbers from the playground, not marketing estimates.

**C++ (`apacheminificpp:latest`):**
- Image: ~15 MB compressed pull
- Memory request: `128Mi` — agents run stable at this allocation
- Startup: near-instant — agent is ready before Kubernetes' `initialDelaySeconds: 5` readiness probe fires
- No JVM, no warm-up phase

**Java (CEM `2.24.08.0-19` tarball):**
- Image: ~300–400 MB is an estimate, **not a measured playground number** — there is no published `minifi-java` image; you build one `FROM` a JRE base + the tarball
- Memory request: `512Mi` minimum; `1Gi` is safer for flows with `ExecuteScript` or Record processing
- Startup: ~30–60 seconds for JVM + agent bootstrap before EFM can push a flow
- Readiness probe `initialDelaySeconds` must match this window (see YAML reference below)

The tradeoff is real: C++ for production edge and Kubernetes sidecars; Java for dev/test, complex flows that need scripting, or anything that requires `HandleHttpRequest`/`HandleHttpResponse`.

---

## EFM deployer setup for Java

The deployer curl is the same shape as C++, with `agentType=java`, `agentVersion=2.24.08.0-19`, and `osArch=linux`. Replace `agentClass=test` with your actual agent class name and `agentIdentifier` with a fresh UUID (`uuidgen` on Linux/macOS):

```bash
curl -L \
 -d agentClass=test \
 -d agentIdentifier=e9faec53-6301-4ba1-a9e9-2403674ccdb2 \
 -d agentType=java \
 -d agentVersion=2.24.08.0-19 \
 -d autoConfigureSecurity=false \
 -d baseUrl=http%3A%2F%2F127.0.0.1%3A46663%2Fefm%2Fapi \
 -d hbPeriod=5000 \
 -d osArch=linux \
 -d serviceName=minifi \
 -d serviceUser=minifi \
 -d trustSelfSignedCertificates=false \
 http://127.0.0.1:46663/efm/api/agent-deployer/script | bash -
```

The `baseUrl` is the EFM API endpoint reachable from the machine running the deployer — adjust for your port-forward or `minikube service` tunnel address.

The EFM binary tree must have the archive at exactly this path before the deployer runs:

```
/opt/efm/efm-2.3.1.0-2/agent-deployer/binaries/java/linux/2.24.08.0-19/minifi.tar.gz
```

To verify it's staged:

```bash
EFM_POD=$(kubectl get pod -n cld-streaming -l app=efm -o jsonpath='{.items[0].metadata.name}')
kubectl exec -i $EFM_POD -n cld-streaming -- find /opt/efm/efm-2.3.1.0-2/agent-deployer/ -type f | grep java
```

Expected: `/opt/efm/efm-2.3.1.0-2/agent-deployer/binaries/java/linux/2.24.08.0-19/minifi.tar.gz`

For staging the tarball from source: see [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md) — copy local `minifi-2.24.08.0-19-bin.tar.gz` to `staging/binaries/java/linux/2.24.08.0-19/minifi.tar.gz`, then tar-pipe it into the EFM pod.

For the Dockerfile and K8s YAML that wire this together, see `Dockerfile.java` and `minifi-java-test.yaml` in the playground repo. Key differences from the C++ YAML: `resources.requests.memory: 512Mi`, `readinessProbe.initialDelaySeconds: 60`, and `nodePort: 30081` to avoid conflict with the C++ deployment on 30080.

> **⚠️ The `minifi-java` base image does not exist — `Dockerfile.java` will not build as written.** Field-verified (registry-authenticated): `container.repo.cloudera.com/cloudera/minifi-java:latest` returns `unknown: Not found` from `docker manifest inspect` (as do ~12 name variants), while `apacheminificpp:latest` and `efm:latest` resolve on the same credentials. Cloudera publishes only the C++ agent image; MiNiFi Java is the tarball. So there is no image to read a `MINIFI_HOME` or processor count from — the `Dockerfile.java` `FROM` needs replacing (build `FROM` a JRE base + unpack the tarball, or deploy via the EFM binary path). The `MINIFI_HOME` value `/opt/minifi/minifi-2.24.08.0-19` is an unverified placeholder until that new base is chosen.

The CEM tarball deployer path (`binaries/java/linux/2.24.08.0-19/minifi.tar.gz`) is fully verified and unaffected by this flag — it is the correct way to run Java MiNiFi on this stack.

---

## Controller services — the structural difference

This is the biggest structural difference between Java and C++ flows in EFM.

**C++** inlines connection properties directly on the processor. A `PublishKafka` in C++ takes `Known Brokers`, `Topic Name`, and `Client Name` as flat properties — no controller service required.

**Java** uses NiFi's controller service architecture. A `PublishKafka` in Java MiNiFi requires a `Kafka3ConnectionService` controller service. The FQCN is `org.apache.nifi.kafka.service.Kafka3ConnectionService`, sourced from `nifi-kafka-3-service-nar` — field-verified, wired via the processor's "Kafka Connection Service" property.

Note the package: Java `PublishKafka`/`ConsumeKafka` are `org.apache.nifi.kafka.processors.*`, not under `.standard.` — field-verified. Typing a bare class name in EFM may result in a no-op or a processor that fails to instantiate. Read the bundle info from `GET /efm/api/designer/flows/{id}` to confirm the exact FQCN format the agent class expects.

For SSL, the general NiFi 2.x pattern is: add a `StandardSSLContextService` controller service to the flow in EFM, configure it with your truststore/keystore paths, then reference it from the processor's `SSL Context Service` property. Same approach for Record Reader/Writer controller services (e.g., `JsonTreeReader`, `JsonRecordSetWriter`).

> **⚠️ Not yet field-verified.** SSL (`StandardSSLContextService`) and Record Reader/Writer controller service FQCNs and wiring are not yet field-verified for MiNiFi Java `2.24.08.0-19`. The Kafka3ConnectionService wiring above is field-verified. Verify the SSL/Record service FQCNs against a running EFM flow before building a production dependency on them.

---

## Flow patterns

All three patterns below require the scripting + Kafka NARs. The stock EFM-staged CEM `2.24.08.0-19` tarball (field-verified) lacks `ExecuteScript`, `PublishKafka`, and `ConsumeKafka` out of the box. The NAR drop-in fix (3 NARs, ~3 min build) is field-verified on both `KubernetesPodJava` and WindowsDesktop — see [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md) for the recipe.

**ListenHTTP → ExecuteScript → PublishKafka** — The kitchen-sink ingest pattern. HTTP listener receives a payload, Groovy script transforms or filters it, result goes to Kafka. `ExecuteScript` with `Script Engine: Groovy` works once the scripting NAR is present. Only Groovy and Clojure engines are bundled — no Jython/Python.

**HandleHttpRequest → [logic] → HandleHttpResponse** — Synchronous request/reply HTTP. Java only — C++ has no equivalent. Both processors share a `StandardHttpContextMap` controller service. `HandleHttpRequest` starts an embedded Jetty server; the caller blocks until `HandleHttpResponse` sends the reply. Use this when the caller needs the actual response body, not just a 200 ack.

**ConsumeKafka → ExecuteScript → PublishKafka** — Standard transform pipeline. Practical alternative to a full custom NiFi processor when the transform logic is contained and doesn't need to be versioned independently.

---

## When to use Java

- You need `ExecuteScript` — Groovy or Clojure, not Python — and your build includes the scripting NAR (the drop-in fix covers this; the stock CEM tarball doesn't).
- You need `HandleHttpRequest`/`HandleHttpResponse` for synchronous HTTP request/reply. C++ cannot do this at all.
- You're building flows that need the Record framework (`ConvertRecord`, `SplitRecord`, `QueryRecord`) with custom reader/writer controller services.
- You're developing and testing flow logic before committing to a C++ deployment — Java gives you the full toolkit while you figure out what you actually need.

---

## What NOT to do

- **Do not deploy Java MiNiFi as a production Kubernetes sidecar.** A ~400 MB image that takes 60 seconds to start is not a sidecar. Use C++ for that.

- **Do not assume "switch to Java and get ExecuteScript for free."** The stock EFM-staged CEM `2.24.08.0-19` tarball does not include `ExecuteScript` or Kafka processors — field-verified. That claim applies to the NAR drop-in version or full NiFi, not the base tarball.

- **Do not use Python in Java ExecuteScript.** Java `ExecuteScript` runs Groovy and Clojure. Python is not bundled in the built `nifi-scripting-nar`. If you need Python, that's C++ with extra-extensions, or a custom Python processor in full NiFi.

- **Do not skip the `initialDelaySeconds` bump in the readiness probe.** The C++ probe fires at 5 seconds and the pod is up. The Java JVM + MiNiFi bootstrap takes 30–60 seconds. A 5-second initial delay will loop-restart the pod before the agent has had a chance to connect to EFM.

- **Do not assume the Java EFM binary path matches C++.** C++ is `binaries/cpp/linux/1.26.02/minifi.tar.gz`. Java is `binaries/java/linux/2.24.08.0-19/minifi.tar.gz`. The EFM deployer resolves the binary from `agentType` + `osArch` + `agentVersion` — send the wrong combination and you get a 404 or the wrong binary.

- **Do not run the Java deployer before creating the agent class and publishing a flow in EFM.** The agent will heartbeat with no flow to apply and nothing happens.

---

## Related chapters

- Ch2 — [EFM Binaries](ch02-efm-binaries.md): the NAR drop-in build recipe and the binary-staging tree.
- Ch5 — [ExecuteScript Availability](ch05-executescript-availability.md): which runtimes ship the scripting engine, build by build.
