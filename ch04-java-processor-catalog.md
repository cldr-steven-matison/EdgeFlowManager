# Chapter 4: MiNiFi Java Processor Catalog

I run MiNiFi C++ and MiNiFi Java side by side in the same minikube playground, same Strimzi Kafka cluster, same EFM server. The swap is a Dockerfile change, a memory bump in the K8s YAML, and a different `agentType` in the EFM deployer command. What you get from Java is a processor set that C++ cannot match out of the box. `HandleHttpRequest`/`HandleHttpResponse` (synchronous request and reply HTTP, absent in C++), a scripting engine once the right NARs are present, and a Record Reader/Writer framework. The stock `minifi-java` tarball (`2.24.08.0-19`, before the NAR drop-in below) reports 114 processors and 45 controller services. With the drop-in it reports 122 and 51, and that is the build the production Java classes in this cluster run.

---

## What Java Gives You That C++ Doesn't

| Capability | MiNiFi C++ (`apacheminificpp:latest`) | MiNiFi Java (CEM `2.24.08.0-19` tarball) |
|---|---|---|
| `ExecuteScript` | Not in the stock image. Requires extra-extensions or a source build | Missing from the stock tarball. The NAR drop-in adds it, Groovy engine |
| `ExecuteProcess` | Not in the stock image. Only via extra-extensions | Cloudera stock. Shell command execution |
| `HandleHttpRequest` / `HandleHttpResponse` | Not available. No pair exists in C++ | Cloudera stock. Synchronous request-reply HTTP (Jetty-backed). Both share an `HttpContextMap` controller service |
| `PublishKafka` / `ConsumeKafka` | Present (C++ extensions) | Missing from the stock tarball. The NAR drop-in adds a transactional Kafka producer |
| Record Reader/Writer framework | `ConvertRecord` and `SplitRecord` present but require controller services | Cloudera stock. RecordReader/RecordWriter controller services present |
| Sparkplug B decode (`ConsumeMQTTIIoT`) | No IIoT/Sparkplug processor. `ConsumeMQTT` is generic MQTT relay only | Not in the stock tarball. Loadable via the Cloudera CDF `nifi-cdf-iiot-mqtt-nar` drop-in for native Sparkplug B protobuf decode at the edge (see [Chapter 13](ch13-efm-and-sparkplug-mqtt.md)) |
| Scripting engines | None without extra-extensions | Shell via `ExecuteProcess`/`ExecuteStreamCommand` in stock. Groovy/Clojure via the NAR drop-in |
| Total processors | 74 (stock), more via extra-extensions | 114 stock, 122 after the Kafka/scripting NAR drop-in |
| Image size | ~15 MB | ~300 to 400 MB |
| Memory minimum | ~128Mi | ~512Mi |
| JVM startup | None | ~30 to 60s cold start |
| Kubernetes sidecar use | Production-ready | Not recommended. The footprint is too large |

> **⚠️ There is no `minifi-java` Docker image.** `container.repo.cloudera.com/cloudera/minifi-java:latest` does not exist in the registry (nor about a dozen name variants), while `apacheminificpp:latest` resolves on the same credentials. Cloudera containerizes only the C++ agent. MiNiFi Java ships as the tarball. The baseline is the stock tarball's 114 processors and 45 controller services. Production classes run the NAR-drop-in build at 122 and 51. The "200+ processors" figure some docs quote has no running Java manifest behind it, stock or drop-in.

---

## Footprint Comparison

Numbers from the playground.

**C++ (`apacheminificpp:latest`).** Image about 15 MB compressed pull. Memory request `128Mi`, and agents run stable at this allocation. Startup is near-instant. The agent is ready before Kubernetes' `initialDelaySeconds: 5` readiness probe fires. No JVM, no warm-up phase.

**Java (CEM `2.24.08.0-19` tarball).** Image about 300 to 400 MB, an estimate. There is no published `minifi-java` image, so you build one `FROM` a JRE base plus the tarball. Memory request `512Mi` minimum, and `1Gi` is safer for flows with `ExecuteScript` or Record processing. Startup is 30 to 60 seconds for JVM plus agent bootstrap before EFM can push a flow. The readiness probe `initialDelaySeconds` must match this window (see the YAML reference below).

The tradeoff. C++ for production edge and Kubernetes sidecars. Java for dev and test, complex flows that need scripting, or anything that requires `HandleHttpRequest`/`HandleHttpResponse`.

---

## EFM Deployer Setup for Java

The deployer command comes from EFM. Use the Deploy Agent CLI screen, or `POST /efm/api/agent-deployer/generateCommand` with `agentIdentifier` omitted, and paste what it returns. For Java the command carries `agentType=java`, `agentVersion=2.24.08.0-19`, `osArch=linux`, your agent class name, and a server-minted `agentIdentifier`. Do not hand-build the command and do not reuse an identifier from an earlier enrollment. A reused identifier makes the C2 `UPDATE` that pushes the flow fail on the re-enrolled agent.

The `baseUrl` in the generated command is the EFM API endpoint reachable from the machine running the deployer. Adjust it for your port-forward or `minikube service` tunnel address.

The EFM binary tree must have the archive at exactly this path before the deployer runs.

```
/opt/efm/efm-2.3.1.0-2/agent-deployer/binaries/java/linux/2.24.08.0-19/minifi.tar.gz
```

To check it is staged.

```bash
EFM_POD=$(kubectl get pod -n cld-streaming -l app=efm -o jsonpath='{.items[0].metadata.name}')
kubectl exec -i $EFM_POD -n cld-streaming -- find /opt/efm/efm-2.3.1.0-2/agent-deployer/ -type f | grep java
```

Expected. `/opt/efm/efm-2.3.1.0-2/agent-deployer/binaries/java/linux/2.24.08.0-19/minifi.tar.gz`

For staging the tarball from source, see [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md). Copy the local `minifi-2.24.08.0-19-bin.tar.gz` to `staging/binaries/java/linux/2.24.08.0-19/minifi.tar.gz`, then tar-pipe it into the EFM pod.

For the Dockerfile and K8s YAML that wire this together, see `Dockerfile.java` and `minifi-java-test.yaml` in the playground repo. The differences from the C++ YAML are `resources.requests.memory: 512Mi`, `readinessProbe.initialDelaySeconds: 60`, and `nodePort: 30081` to avoid conflict with the C++ deployment on 30080.

> **⚠️ The `minifi-java` base image does not exist, so `Dockerfile.java` will not build as written.** `docker manifest inspect container.repo.cloudera.com/cloudera/minifi-java:latest` returns `unknown: Not found` (as do about a dozen name variants), while `apacheminificpp:latest` and `efm:latest` resolve on the same credentials. Cloudera publishes only the C++ agent image. MiNiFi Java is the tarball. There is no image to read a `MINIFI_HOME` or processor count from. The `Dockerfile.java` `FROM` needs replacing (build `FROM` a JRE base and unpack the tarball, or deploy via the EFM binary path). The `MINIFI_HOME` value `/opt/minifi/minifi-2.24.08.0-19` is a placeholder until that new base is chosen.

The CEM tarball deployer path (`binaries/java/linux/2.24.08.0-19/minifi.tar.gz`) is unaffected by this. It is the way to run Java MiNiFi on this stack.

---

## Controller Services, the Structural Difference

This is the biggest structural difference between Java and C++ flows in EFM.

C++ inlines connection properties directly on the processor. A `PublishKafka` in C++ takes `Known Brokers`, `Topic Name`, and `Client Name` as flat properties. No controller service required.

Java uses NiFi's controller service architecture. A `PublishKafka` in Java MiNiFi requires a `Kafka3ConnectionService` controller service. The FQCN is `org.apache.nifi.kafka.service.Kafka3ConnectionService`, sourced from `nifi-kafka-3-service-nar`, wired via the processor's "Kafka Connection Service" property.

Note the package. Java `PublishKafka`/`ConsumeKafka` are `org.apache.nifi.kafka.processors.*`, not under `.standard.`. Typing a bare class name in EFM may result in a no-op or a processor that fails to instantiate. Read the bundle info from `GET /efm/api/designer/flows/{id}` to confirm the exact FQCN format the agent class expects.

For SSL, the general NiFi 2.x pattern applies. Add a `StandardSSLContextService` controller service to the flow in EFM, configure it with your truststore and keystore paths, then reference it from the processor's `SSL Context Service` property. Same approach for Record Reader/Writer controller services (`JsonTreeReader`, `JsonRecordSetWriter`).

---

## Flow Patterns

All three patterns below require the scripting and Kafka NARs. The stock EFM-staged CEM `2.24.08.0-19` tarball lacks `ExecuteScript`, `PublishKafka`, and `ConsumeKafka` out of the box. The NAR drop-in fix (3 NARs, about a 3 minute build) runs on both `KubernetesPodJava` and the Windows Java agent. See [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md) for the recipe.

**`ListenHTTP` → `ExecuteScript` → `PublishKafka`.** The kitchen-sink ingest pattern. The HTTP listener receives a payload, a Groovy script transforms or filters it, and the result goes to Kafka. `ExecuteScript` with `Script Engine: Groovy` works once the scripting NAR is present. Only Groovy and Clojure engines are bundled. No Jython or Python.

**`HandleHttpRequest` → [logic] → `HandleHttpResponse`.** Synchronous request and reply HTTP. Java only. C++ has no equivalent. Both processors share a `StandardHttpContextMap` controller service. `HandleHttpRequest` starts an embedded Jetty server, and the caller blocks until `HandleHttpResponse` sends the reply. Use this when the caller needs the response body and not only a 200 ack.

**`ConsumeKafka` → `ExecuteScript` → `PublishKafka`.** The standard transform pipeline. A practical alternative to a full custom NiFi processor when the transform logic is contained and does not need to be versioned independently.

---

## When to Use Java

- You need `ExecuteScript` (Groovy or Clojure, not Python) and your build includes the scripting NAR. The drop-in fix covers this. The stock CEM tarball does not.
- You need `HandleHttpRequest`/`HandleHttpResponse` for synchronous HTTP request and reply. C++ cannot do this at all.
- You are building flows that need the Record framework (`ConvertRecord`, `SplitRecord`, `QueryRecord`) with custom reader and writer controller services.
- You are developing and testing flow logic before committing to a C++ deployment. Java gives you the full toolkit while you figure out what you need.

---

## What NOT to Do

- Do not deploy Java MiNiFi as a production Kubernetes sidecar. A 400 MB image that takes 60 seconds to start is not a sidecar. Use C++ for that.
- Do not assume "switch to Java and get `ExecuteScript` for free." The stock EFM-staged CEM `2.24.08.0-19` tarball does not include `ExecuteScript` or the Kafka processors. That claim applies to the NAR drop-in version or full NiFi, not the base tarball.
- Do not use Python in Java `ExecuteScript`. Java `ExecuteScript` runs Groovy and Clojure. Python is not bundled in the built `nifi-scripting-nar`. If you need Python, that is C++ with extra-extensions, or a custom Python processor in full NiFi.
- Do not skip the `initialDelaySeconds` bump in the readiness probe. The C++ probe fires at 5 seconds and the pod is up. The Java JVM plus MiNiFi bootstrap takes 30 to 60 seconds. A 5-second initial delay loop-restarts the pod before the agent has had a chance to connect to EFM.
- Do not assume the Java EFM binary path matches C++. C++ is `binaries/cpp/linux/1.26.02/minifi.tar.gz`. Java is `binaries/java/linux/2.24.08.0-19/minifi.tar.gz`. The EFM deployer resolves the binary from `agentType` plus `osArch` plus `agentVersion`. Send the wrong combination and you get a 404 or the wrong binary.
- Do not run the Java deployer before creating the agent class and publishing a flow in EFM. The agent heartbeats with no flow to apply and nothing happens.

---

## Related Chapters

- [EFM Binaries](ch02-efm-binaries.md) (Ch2): the NAR drop-in build recipe and the binary-staging tree.
- [ExecuteScript Availability](ch05-executescript-availability.md) (Ch5): which runtimes ship the scripting engine, build by build.
