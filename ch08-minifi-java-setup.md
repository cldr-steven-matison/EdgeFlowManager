# Chapter 8: Standalone MiNiFi Java on Kubernetes (no EFM)

This chapter proves plain Apache MiNiFi Java running in minikube on macOS with no EFM involvement at all — the Java counterpart to [Chapter 7](ch07-standalone-minifi-cpp-on-k8s.md)'s standalone C++ agent. The flow is `ListenHTTP (8080)` → `PutFile` (`/tmp/minifi-test-output`), with the flow baked into the image as a `config.yml`. Everything here comes from the [MiNiFi Kubernetes Playground](https://github.com/cldr-steven-matison/MiNiFi-Kubernetes-Playground) repo — actual scripts, actual YAML, run clean and verified. EFM does not enter the picture until [Chapter 9](ch09-efm-in-the-playground.md).

## What This Scenario Proves

Standalone MiNiFi Java, with `config.yml` baked into the image, is the same fast iteration loop the C++ agent gives you — no EFM, no agent registration, no class reconciliation. You edit `config.yml`, rebuild, and the pod restarts with the new flow. It also establishes what changes when you move from C++ to Java before EFM is anywhere in the picture: the flow config format, the class names, the image footprint, and the way the pod reports ready. Getting those right standalone means they're already right when EFM takes over the same agent in Chapter 9.

The standalone image is `container.repo.cloudera.com/cloudera/nifi-minifi-java:latest` — Apache MiNiFi Java `1.23.04-b15`, a `config.yml`-driven runtime. `MINIFI_HOME=/opt/minifi/minifi-1.23.04-b15`, verified with `docker run --rm <image> find /opt -iname "*minifi*"`. (The EFM-managed CEM agent introduced later is a different, `2.24.08.0-19` runtime — Chapter 9 onward.)

## The "Nuclear" Rebuild Script

Same destroy-everything-first loop as the C++ agent, with the Java names and a different NodePort so it can run alongside the C++ deployment:

```bash
# --- 1. DESTRUCTIVE CLEANUP ---
kubectl delete deployment minifi-test-java --force --grace-period=0
kubectl delete service minifi-test-java-service --ignore-not-found

# --- 2. ENVIRONMENT SYNC ---
# Point the terminal's Docker client at the engine INSIDE minikube.
eval $(minikube docker-env)

# --- 3. CACHE PURGE ---
docker rmi -f minifi-test-java:latest || true
docker builder prune -a -f

# --- 4. AUTHENTICATION ---
# The base image is pulled from Cloudera's registry.
docker login container.repo.cloudera.com

# --- 5. NATIVE BUILD ---
docker build --no-cache --platform linux/amd64 -f Dockerfile.java -t minifi-test-java:latest .

# --- 6. DEPLOY ---
kubectl apply -f minifi-test-java.yaml

# --- 7. MONITOR ---
kubectl get pods -w
```

Step 2 — `eval $(minikube docker-env)` — is mandatory, exactly as for C++. Skip it and the image builds on your Mac's Docker daemon, the minikube node never sees it, and the pod stays in `ErrImageNeverPull`.

## config.yml — The Three Differences from C++

MiNiFi Java's standalone flow config is `config.yml` at `MiNiFi Config Version: 3`. The full working config for this flow:

```yaml
MiNiFi Config Version: 3
Flow Controller:
  name: MiNiFi HTTP to File (Java)

Processors:
- name: ListenHTTP
  id: 6e21b840-2d12-11f1-baac-62f0ccd85bcd
  class: org.apache.nifi.processors.standard.ListenHTTP
  scheduling strategy: TIMER_DRIVEN
  scheduling period: 0 sec
  auto-terminated relationships list: []
  Properties:
    Listening Port: 8080
    Base Path: contentListener
- name: PutFile
  id: 6e21b842-2d12-11f1-baac-62f0ccd85bcd
  class: org.apache.nifi.processors.standard.PutFile
  scheduling strategy: TIMER_DRIVEN
  scheduling period: 0 sec
  auto-terminated relationships list:
  - success
  - failure
  Properties:
    Directory: /tmp/minifi-test-output

Connections:
- name: HttpToFile
  id: 6e21b844-2d12-11f1-baac-62f0ccd85bcd
  source id: 6e21b840-2d12-11f1-baac-62f0ccd85bcd
  source relationship names:
  - success
  destination id: 6e21b842-2d12-11f1-baac-62f0ccd85bcd
```

Three things trip you up coming from the C++ `config.yml`:

**1. Full Java FQCNs, not C++ short names.** C++ takes `class: ListenHTTP`; Java takes `class: org.apache.nifi.processors.standard.ListenHTTP`. A short name here fails to instantiate — the Java runtime resolves processors by fully-qualified class name.

**2. Connections wire by `source id`/`destination id`, not by name.** The C++ config references `source name`/`destination name`; the Java `config.yml` (Config Version 3) references the component UUIDs. Copy the processor `id` values into the connection's `source id`/`destination id`. Every processor and connection still needs its own explicit UUID — generate fresh ones with `uuidgen`.

**3. Terminal sinks auto-terminate their relationships.** `PutFile` is the end of the flow, so its `success` and `failure` relationships go in `auto-terminated relationships list`. Leave a relationship neither connected nor auto-terminated and the flow fails validation on load.

## Dockerfile — MINIFI_HOME and the Config Drop

```dockerfile
FROM container.repo.cloudera.com/cloudera/nifi-minifi-java:latest
USER root

# MINIFI_HOME verified via: docker run --rm <image> find /opt -iname "*minifi*"
ENV MINIFI_HOME=/opt/minifi/minifi-1.23.04-b15

# Deploy the standalone flow config
COPY config-java.yml ${MINIFI_HOME}/conf/config.yml

# Create the PutFile sink directory
RUN mkdir -p /tmp/minifi-test-output && chmod 777 /tmp/minifi-test-output

EXPOSE 8080

CMD ["/opt/minifi/minifi-1.23.04-b15/bin/minifi.sh", "run"]
```

`MINIFI_HOME=/opt/minifi/minifi-1.23.04-b15` is verified from the image itself. Pull a different version of the image and that path changes — the version number is part of the directory name, and a wrong `MINIFI_HOME` means `config.yml` lands where the agent never reads it.

## The Kubernetes Manifest — Why the Probe Must Be TCP, Not HTTP

This is the one place the C++ manifest does not translate. The C++ deployment uses an `httpGet` readiness probe against `/contentListener`. On the Java agent that probe **permanently fails** — Java's `ListenHTTP` accepts only `POST` on `/contentListener` and returns `405` to the `GET` a probe sends, so the pod never reaches `Ready`. Use a `tcpSocket` check against the Jetty listener instead:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: minifi-test-java-service
spec:
  type: NodePort
  selector:
    app: minifi-test-java
  ports:
    - protocol: TCP
      port: 8080
      targetPort: 8080
      nodePort: 30081        # different from the C++ deployment (30080)
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minifi-test-java
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minifi-test-java
  template:
    metadata:
      labels:
        app: minifi-test-java
    spec:
      containers:
      - name: minifi-java
        image: minifi-test-java:latest
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "512Mi"   # C++ runs at ~128Mi; the Java JVM needs 512Mi minimum
            cpu: "250m"
          limits:
            memory: "1Gi"
        readinessProbe:
          tcpSocket:
            port: 8080
          initialDelaySeconds: 60   # JVM + MiNiFi bootstrap is 30-60s; C++ is ready at 5s
          periodSeconds: 5
        livenessProbe:
          tcpSocket:
            port: 8080
          initialDelaySeconds: 75
          periodSeconds: 10
```

Two more numbers that differ from C++: memory (`512Mi` request / `1Gi` limit — the JVM will not run in the C++ agent's `128Mi`) and `initialDelaySeconds: 60` (the JVM plus MiNiFi bootstrap takes 30–60 seconds; a 5-second delay loop-restarts the pod before the agent is up).

## Verifying Delivery

**Step 1 — Open the network tunnel.** On macOS, minikube NodePorts are not reachable from `localhost` directly. Run this in a dedicated terminal and leave it open:

```bash
minikube service minifi-test-java-service --url
```

**Step 2 — POST a test message.** `ListenHTTP` on the Java agent takes `POST` only, on `/contentListener`:

```bash
curl -i -X POST http://127.0.0.1:<TUNNEL_PORT>/contentListener \
     -H "Content-Type: application/json" \
     -d '{"test_id": "java-standalone", "message": "Flow is functional"}'
```

You get an immediate `200`. A `GET` to the same path returns `405` — that is expected, and it is exactly why the readiness probe is TCP.

**Step 3 — Verify PutFile.** Check the internal pod storage:

```bash
kubectl exec -it deployment/minifi-test-java -- /bin/sh -c "cat /tmp/minifi-test-output/*"
```

The JSON body you POSTed appears here. That confirms `ListenHTTP → PutFile` wired correctly from the `config.yml`.

## What NOT to Do

**Use an `httpGet` readiness probe against `/contentListener`.** Java's `ListenHTTP` returns `405` to a `GET`, so the probe never passes and the pod stays `NotReady` forever. Use a `tcpSocket` probe against port 8080.

**Give the Java pod the C++ agent's `128Mi`.** The JVM will not start. `512Mi` request / `1Gi` limit is the field-measured minimum.

**Set `initialDelaySeconds: 5` like the C++ deployment.** The JVM plus MiNiFi bootstrap takes 30–60 seconds. A 5-second delay loop-restarts the pod before it finishes coming up.

**Use C++ short class names in the Java `config.yml`.** `class: ListenHTTP` fails to instantiate; the Java runtime needs the full FQCN `org.apache.nifi.processors.standard.ListenHTTP`.

**Wire connections by name in a Config Version 3 file.** Java connections reference `source id`/`destination id` (the component UUIDs), not `source name`/`destination name`. Mixing the C++ style here produces a flow that loads with no connections.

**Skip `eval $(minikube docker-env)` before `docker build`.** The image lands on your Mac's daemon, the minikube node never sees it, and the pod goes `ErrImageNeverPull`. It does not persist across terminal sessions — run it every build.

## Related Chapters

- Ch7 — [Standalone MiNiFi C++ on Kubernetes](ch07-standalone-minifi-cpp-on-k8s.md): the C++ agent this chapter mirrors, and the shared playground and rebuild loop.
- Ch9 — [Introduce EFM into the Playground](ch09-efm-in-the-playground.md): where EFM takes over managing these same agents.
- Ch4 — [MiNiFi Java Processor Catalog](ch04-java-processor-catalog.md): the full Java processor/controller-service set, and the Kafka/scripting NAR drop-in for the EFM-managed CEM agent.
