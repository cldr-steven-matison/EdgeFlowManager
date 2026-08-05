# Chapter 7: Standalone MiNiFi C++ on Kubernetes (no EFM)

This chapter proves plain Apache MiNiFi C++ v1.26.02 running in minikube on macOS with no EFM involvement at all. The flow is `ListenHTTP (8080)` → `PublishKafka` (in-cluster Strimzi topic `test-minifi`) plus `PutFile` (`/tmp/minifi-test-output`). Everything here comes from the `MiNiFi Kubernetes Playground` repo — actual scripts, actual YAML, run clean and verified.

## What this scenario proves

Running MiNiFi C++ standalone, with `config.yml` baked into the image, is the fastest iteration loop for flow development. No EFM, no agent registration, no class reconciliation. You edit `config.yml`, run the nuclear script, and the pod restarts with the new flow in under two minutes. It also confirms the in-cluster Kafka path works before you wire EFM into it: if `PublishKafka` delivers to `test-minifi` here, you know the broker address, topic, and `Client Name` are right — and you carry that exact config into the EFM flow designer.

The image is `container.repo.cloudera.com/cloudera/apacheminificpp:latest` = v1.26.02. `MINIFI_HOME=/opt/minifi/nifi-minifi-cpp-1.26.02`, verified from running-instance logs.

## The "nuclear" rebuild script

Use this every iteration. It destroys everything first so no stale image layer or `Terminating` pod survives into the next run.

```bash
# --- 1. DESTRUCTIVE CLEANUP ---
# Force delete the deployment and service to clear the namespace
kubectl delete deployment minifi-test --force --grace-period=0
kubectl delete service minifi-test-service --ignore-not-found

# --- 2. ENVIRONMENT SYNC ---
# Point your terminal's Docker client to the engine INSIDE minikube.
# Without this, docker build targets the host daemon and kubectl never sees the image.
eval $(minikube docker-env)

# --- 3. CACHE PURGE ---
# Remove the local image and wipe the build cache within minikube
docker rmi -f minifi-test:latest || true
docker builder prune -a -f

# --- 4. AUTHENTICATION ---
# Login to the registry from within the minikube Docker context
docker login container.repo.cloudera.com

# --- 5. NATIVE BUILD ---
# Build the image directly on the minikube node (bypasses 'minikube image load')
docker build --no-cache --platform linux/amd64 -t minifi-test:latest .

# --- 6. DEPLOY & INITIALIZE ---
kubectl apply -f minifi-test.yaml

# --- 7. MONITOR ---
# Wait for 1/1 READY status
kubectl get pods -w
```

Step 2 — `eval $(minikube docker-env)` — is mandatory. It is the single most common failure point. Skip it and the image builds on your Mac's Docker daemon, the minikube node never sees it, and the pod stays in `ImagePullBackOff` or `ErrImageNeverPull` indefinitely.

## config.yml — the three requirements

The full working config for this flow:

```yaml
Flow Controller:
  name: MiNiFi HTTP to Kafka

Processors:
- name: ListenHTTP
  id: 489c62c4-2d12-11f1-baac-62f0ccd85bcd
  class: ListenHTTP
  Properties:
    Listening Port: 8080

- name: PublishKafka
  id: 489c62c6-2d12-11f1-baac-62f0ccd85bcd
  class: PublishKafka
  Properties:
    Known Brokers: my-cluster-kafka-bootstrap.cld-streaming.svc:9092
    Topic Name: test-minifi
    Client Name: minifi-test-client
    Batch Size: '10'

- name: DebugLog
  id: 489c62c7-2d12-11f1-baac-62f0ccd85bcd
  class: PutFile
  Properties:
    Directory: /tmp/minifi-test-output

Connections:
- name: HttpToKafka
  id: 489c62c8-2d12-11f1-baac-62f0ccd85bcd
  source name: ListenHTTP
  destination name: PublishKafka
  source relationship name: success

- name: HttpToLog
  id: 489c62ca-2d12-11f1-baac-62f0ccd85bcd
  source name: ListenHTTP
  destination name: DebugLog
  source relationship name: success

Remote Processing Groups: []
```

Three requirements that catch everyone the first time:

**1. Explicit UUID `id` fields on every component.** The C++ agent does not generate IDs for you. Every processor and every connection needs its own `id` field. Copy real UUIDs — generate them with `uuidgen` if you need fresh ones. Omit `id` and the agent fails to load the config silently.

**2. C++ short class names, not Java FQCNs.** `class: ListenHTTP`, `class: PublishKafka`, `class: PutFile` — these are the C++ short names. `org.apache.nifi.processors.standard.ListenHTTP` is the Java NiFi FQCN. It does not work here. Using a Java FQCN produces a silent no-op: the processor fails to instantiate and nothing flows through it.

**3. `Client Name` is mandatory for `PublishKafka`.** Without it, Kafka rejects the connection. `minifi-test-client` is the value here; any non-empty string works. The in-cluster broker address for Strimzi in the `cld-streaming` namespace is `my-cluster-kafka-bootstrap.cld-streaming.svc:9092` — that address is only reachable from inside the cluster, which is where this pod runs.

## Dockerfile — MINIFI_HOME and the readiness probe

```dockerfile
FROM container.repo.cloudera.com/cloudera/apacheminificpp:latest
USER root

# Set home directory verified via agent logs
ENV MINIFI_HOME=/opt/minifi/nifi-minifi-cpp-1.26.02

# Deploy configuration
COPY config.yml ${MINIFI_HOME}/conf/config.yml

# Create local sink directory for PutFile (DebugLog)
RUN mkdir -p /tmp/minifi-test-output && chmod 777 /tmp/minifi-test-output

EXPOSE 8080

CMD ["/opt/minifi/nifi-minifi-cpp-1.26.02/bin/minifi.sh", "run"]
```

`MINIFI_HOME=/opt/minifi/nifi-minifi-cpp-1.26.02` is verified from running-instance logs. If you pull a different version of the image, that path changes — the version number is part of the directory name.

The Kubernetes manifest (`minifi-test.yaml`) includes a Service and Deployment. The `readinessProbe` path must be `/contentListener`, not `/`, not `/health`. That is the endpoint `ListenHTTP` registers internally:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: minifi-test-service
spec:
  type: NodePort
  selector:
    app: minifi-test
  ports:
    - protocol: TCP
      port: 8080
      targetPort: 8080
      nodePort: 30080
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minifi-test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minifi-test
  template:
    metadata:
      labels:
        app: minifi-test
    spec:
      serviceAccountName: minifi-controller
      containers:
      - name: minifi
        image: minifi-test:latest
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8080
        readinessProbe:
          httpGet:
            path: /contentListener
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

`serviceAccountName: minifi-controller` is required for the pod to reach other cluster services. `imagePullPolicy: IfNotPresent` is correct here because the image was built directly into minikube's daemon — there is no registry to pull from.

## Verifying Kafka delivery and PutFile

**Step 1 — Open the network tunnel.** On macOS, minikube NodePorts are not directly reachable from `localhost`. Run this in a dedicated terminal and leave it open:

```bash
minikube service minifi-test-service --url
```

It prints something like `http://127.0.0.1:53314`. That is your endpoint.

**Step 2 — POST a test message.** Use the tunnel URL from step 1:

```bash
curl -i -X POST http://127.0.0.1:<TUNNEL_PORT>/contentListener \
     -H "Content-Type: application/json" \
     -d '{"test_id": "integration-success", "message": "Flow is functional"}'
```

You get an immediate HTTP 200. `ListenHTTP` is fire-and-forget — the 200 means the FlowFile was accepted, not that Kafka received it.

**Step 3 — Verify Kafka delivery.** Run a temporary consumer pod against the in-cluster Strimzi broker:

```bash
kubectl run kafka-viewer -it --rm \
  --image=quay.io/strimzi/kafka:latest-kafka-3.7.0 \
  --restart=Never \
  -- bin/kafka-console-consumer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.cld-streaming.svc:9092 \
  --topic test-minifi \
  --from-beginning \
  --timeout-ms 10000
```

The JSON body you POSTed appears as a Kafka message. If nothing appears, the broker address or topic name is wrong — check both against your Strimzi cluster resources.

**Step 4 — Verify PutFile.** Check the internal pod storage:

```bash
kubectl exec -it deployment/minifi-test -- /bin/sh -c "cat /tmp/minifi-test-output/*"
```

The same payload appears here. Both sinks receiving the same message confirms the fan-out connection wiring in `config.yml` is correct.

## What NOT to do

**Skip `eval $(minikube docker-env)` and you build on the wrong daemon.** The image lands in your Mac's Docker cache. The minikube node has no copy. The pod goes `ErrImageNeverPull` immediately. Run `eval $(minikube docker-env)` before every `docker build` in this workflow — it does not persist across terminal sessions.

**Omit UUID `id` fields and the agent silently rejects the config.** There is no parse error, no crash, no log line that says "missing id." The agent either fails to start or starts with an empty flow. Every processor and every connection needs its own UUID.

**Use Java FQCNs in `config.yml` and the processor never instantiates.** `org.apache.nifi.processors.standard.PublishKafka` is not a C++ class name. The agent reports no error — the processor just never starts. Use `PublishKafka`, `ListenHTTP`, `PutFile`.

**Set the `readinessProbe` path to `/` or `/health` and the pod never reaches `Ready`.** Kubernetes marks the pod `NotReady` indefinitely. The correct path is `/contentListener` — that is the path `ListenHTTP` registers.

**Omit `Client Name` from `PublishKafka` and Kafka rejects the connection.** The property is not marked required in the schema but the broker refuses the connection without a client identifier. Every `PublishKafka` instance needs a non-empty `Client Name`.

## Source

Source doc: `MiNiFi Kubernetes Playground` repo `readme.md` (196 lines) — all scripts, config, Dockerfile, and verification steps in this chapter are drawn verbatim or adapted from that file.
