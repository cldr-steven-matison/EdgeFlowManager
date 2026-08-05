# Chapter 1: EFM on Kubernetes (incl. persistence)

EFM — Cloudera Edge Flow Manager — is the central manager that turns a bare MiNiFi binary into a
managed agent. It owns agent Classes, Resources, and Edge Flows, and it pushes configuration down
to every registered agent on heartbeat. Without EFM there is no central place to author a flow,
upload a Python script, or watch an agent's status. This chapter covers deploying EFM 2.3.1.0-2 on
minikube in namespace `cld-streaming` and making its full state survive a pod restart. It folds in
what was originally a separate persistence chapter — the two topics are inseparable in practice.

---

## What EFM is

EFM exposes a UI at `http://127.0.0.1:10090/efm/ui/` where I author flows for each agent Class, stage
agent installers, and upload Resources (Python scripts, JARs) that get pushed to agents on their next
heartbeat. The agent side — MiNiFi C++ or Java — polls EFM, downloads the flow, and runs it. EFM is
not in the data path; it is the control plane.

Three kinds of state need to survive a pod restart for EFM to be useful day-to-day:

1. **Metadata** (agent classes, manifests, flows, agents) — PostgreSQL
2. **Agent binaries** (C++ and Java installers) — a dedicated PVC
3. **Uploaded resources / assets** (Python scripts, JARs) — a second PVC

The third one is the trap. A bare EFM install has no PVC for resources. The DB rows survive, but the
actual file bytes live on ephemeral disk and disappear on restart. Every flow that references an
uploaded script silently breaks.

---

## Storage layout — what lives where

| State | Backing store | Path in pod | Survives restart? |
|---|---|---|---|
| Agent classes, manifests, flows, agents | Postgres `efm` DB (`ssb-postgresql`) | — | Yes (via `ssb-postgresql-db` PVC) |
| Uploaded agent binaries | PVC `efm-agent-binaries` (2 Gi) | `/opt/efm/efm-2.3.1.0-2/agent-deployer/binaries` | Yes |
| Uploaded resources / assets | PVC `efm-resources` (1 Gi) | `/opt/efm/efm-2.3.1.0-2/resources` | Yes |
| EFM properties | ConfigMap `efm-config` | `/opt/efm/efm-2.3.1.0-2/conf/efm.properties` (subPath) | Yes |
| DB credentials | Secret `efm-db-pass` | env var | Yes |
| Encryption password | Secret `efm-encryption` | env var | Yes |
| Image pull auth | Secret `cloudera-registry` | imagePullSecrets | Yes |

The property that governs where resources land is `efm.resourcemanager.repositoryPath`. It defaults
to `./resources`, which resolves to `/opt/efm/efm-2.3.1.0-2/resources` given EFM's working directory.
Mounting `efm-resources` at that exact path is all it takes to persist those bytes.

All three YAMLs live in `~/ClouderaStreamingOperators/`: `efm-configMap.yaml`, `efm-pvc.yaml`, and
`efm-deployment-persisted.yaml`.

---

## The 8-phase deploy

This section keeps the key command per phase.

### Phase 0 — cluster up check

```bash
kubectl get pods -n cld-streaming | grep -E "postgres|kafka|efm"
```

`ssb-postgresql-*` must be Running before proceeding. Kafka pods only matter if flows publish there.

### Phase 1 — PostgreSQL one-time setup

Skip if the `efm` DB and user already exist.

```bash
PG=$(kubectl get pods -n cld-streaming | grep postgres | awk '{print $1}' | head -1)
kubectl exec $PG -n cld-streaming -- psql -U postgres -c "CREATE DATABASE efm;"
kubectl exec $PG -n cld-streaming -- psql -U postgres -c "CREATE USER efm WITH PASSWORD 'efm_password';"
kubectl exec $PG -n cld-streaming -- psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE efm TO efm;"
kubectl exec $PG -n cld-streaming -- psql -U postgres -c "ALTER DATABASE efm OWNER TO efm;"
```

Verify with `psql -U postgres -c "\l" | grep efm`.

### Phase 2 — secrets

```bash
kubectl create secret generic efm-db-pass \
  --from-literal=password=efm_password \
  --namespace cld-streaming

kubectl create secret generic efm-encryption \
  --from-literal=encryption.password=efm_encryption_key \
  --namespace cld-streaming

source ~/.env
kubectl create secret docker-registry cloudera-registry \
  --docker-server=container.repo.cloudera.com \
  --docker-username=$CLOUDERA_USER \
  --docker-password=$CLOUDERA_PASS \
  --namespace=cld-streaming
```

`already exists` errors from a prior session are fine.

### Phase 3 — pull image into minikube

```bash
eval $(minikube docker-env)
docker login container.repo.cloudera.com
docker pull container.repo.cloudera.com/cloudera/efm:2.3.1.0-2
```

Match the tag to your CSO / CEM entitlement.

### Phase 4 — deploy with persistence

```bash
cd ~/ClouderaStreamingOperators
kubectl apply -f efm-configMap.yaml -n cld-streaming
kubectl apply -f efm-pvc.yaml         -n cld-streaming
kubectl apply -f efm-deployment-persisted.yaml -n cld-streaming
kubectl rollout status deployment/efm -n cld-streaming --timeout=180s
```

Spot-check after rollout:

```bash
EFM_POD=$(kubectl get pod -n cld-streaming -l app=efm -o jsonpath='{.items[0].metadata.name}')
kubectl exec $EFM_POD -n cld-streaming -- mount | grep efm-2.3.1.0-2
# Expect two ext4 lines: agent-deployer/binaries and resources
```

If `grep db.url` shows `h2`, the ConfigMap didn't mount — re-apply `efm-configMap.yaml` and restart.

### Phase 5 — stage agent binaries (one-time per PVC)

If the binaries directory is already populated, skip this. Otherwise see [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md) for the
full build. The streaming copy:

```bash
EFM_POD=$(kubectl get pod -n cld-streaming -l app=efm -o jsonpath='{.items[0].metadata.name}')
cd ~/efm-binaries/staging/ && tar -cf - binaries/ | \
  kubectl exec -i $EFM_POD -n cld-streaming -- tar -xf - -C /opt/efm/efm-2.3.1.0-2/agent-deployer/
kubectl rollout restart deployment/efm -n cld-streaming
```

### Phase 6 — reach the UI

```bash
kubectl port-forward -n cld-streaming svc/efm 10090:10090
```

Open `http://127.0.0.1:10090/efm/ui/`. Check for a stale port-forward first (`lsof -iTCP:10090 -sTCP:LISTEN`)
— a forward bound to a dead pod after a rollout returns HTTP 000 silently.

> **⚠️ Check before port-forwarding.** The canonical port-forwards run as zellij panes (`kube-service-ports-efm.kdl`). A duplicate forward on the same target silently orphans or hangs.

### Phase 7 — upload resources

EFM UI → **Resources** → Upload. Set **File / Name** to match whatever the flow's `Script File`
property expects (e.g. `cpu_nifi_tensorRT.py`). Set **Agent Class** to the target class
(`KubernetesPod`, `WindowsDesktop`, `NvidiaNano`). Leave relative path blank.

Verify both DB row and file on the PVC:

```bash
kubectl exec $PG -n cld-streaming -- psql -U postgres -d efm -c \
  "SELECT name, file_name, resource_type FROM resource_metadata;"

kubectl exec $EFM_POD -n cld-streaming -- ls -la /opt/efm/efm-2.3.1.0-2/resources/
```

Both should exist. The file syncs to `<minifi-install>/asset/<file_name>` on the agent's next heartbeat.

### Phase 8 — persistence test

```bash
kubectl rollout restart deployment/efm -n cld-streaming
kubectl rollout status deployment/efm -n cld-streaming --timeout=180s

kubectl exec $PG -n cld-streaming -- psql -U postgres -d efm -c \
  "SELECT 'agent_class', count(*) FROM agent_class
   UNION ALL SELECT 'flow', count(*) FROM flow
   UNION ALL SELECT 'resource_metadata', count(*) FROM resource_metadata;"

EFM_POD=$(kubectl get pod -n cld-streaming -l app=efm -o jsonpath='{.items[0].metadata.name}')
kubectl exec $EFM_POD -n cld-streaming -- ls -la /opt/efm/efm-2.3.1.0-2/resources/
```

Refresh EFM UI → **Resources**. The upload should still be there. Counts should match pre-restart.

After the YAMLs are applied once, a cold `minikube stop` / `minikube start` only needs
`kubectl rollout status` and a port-forward — everything reloads from Postgres + PVCs automatically.

---

## Postgres + 2-PVC persistence

The two PVCs exist for different reasons:

- **`efm-agent-binaries` (2 Gi)** backs `agent-deployer/binaries/`. EFM serves the C++ and Java
  installers from here to agents that request an upgrade. Without it, I re-stage four platform
  tarballs every time the pod restarts.

- **`efm-resources` (1 Gi)** backs the `resources/` directory governed by
  `efm.resourcemanager.repositoryPath`. This is the trap a bare install hits: the DB tables
  `resource_metadata` and `asset` track names and UUIDs, but the actual bytes live at whatever
  `repositoryPath` resolves to. Without the PVC mounted there, the DB says the file exists but the
  bytes are gone after a restart, and the next agent heartbeat gets a 404 for the script it was
  told to download.

Postgres (`ssb-postgresql`) handles everything else: agent class registrations, flow definitions,
flow content, manifests, agent heartbeat metadata. That state is already durable because
`ssb-postgresql` itself has its own PVC (`ssb-postgresql-db`). EFM just needs to point at it via
the `efm.db.url` property in `efm-configMap.yaml`.

---

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| EFM pod crashes on startup | `efm-encryption` or `efm-db-pass` secret missing | Recreate secrets (Phase 2) |
| EFM logs `Connection refused` to PostgreSQL | `ssb-postgresql` not running | Phase 0 — wait for Postgres |
| EFM UI shows H2-style URLs (no persistence) | ConfigMap not mounted at correct subPath | Verify `volumeMount subPath: efm.properties`, re-apply ConfigMap, restart |
| Uploaded resource disappears after restart | `efm-resources` PVC not mounted | `kubectl describe pod efm-... \| grep -A1 Volumes` — confirm both PVCs present |
| Agent: `Script File ... does not exist` | Resource `file_name` in EFM doesn't match `Script File` in flow | Rename resource in EFM to match, or fix the flow property |
| Port-forward returns HTTP 000 / RST | Stale forward bound to dead pod after rollout | `lsof -iTCP:10090 -sTCP:LISTEN`, kill, re-forward |
| Postgres: `remaining connection slots are reserved` | Too many idle EFM connections | `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle' AND datname='efm';` |

---

## What NOT to do

**Don't deploy from `efm-deployment.yaml` (the non-persisted variant).** It's in
`~/ClouderaStreamingOperators/` too and looks identical to `efm-deployment-persisted.yaml` at a
glance. The difference is the two `volumeMounts` and `volumes` blocks for the PVCs. Deploy the
wrong one and EFM runs fine — right up until the first restart, when all uploaded resources vanish
and the agents start failing to download scripts with no obvious error in the EFM UI.

**Don't skip the `efm-resources` PVC.** The `efm-agent-binaries` PVC failure is obvious — agents
can't download their installer. The `efm-resources` failure is silent: the DB rows are there, the
UI shows the upload, but the bytes are gone from disk. The only symptom shows up on the agent side
as a missing file on the next heartbeat. The fix is `efm-pvc.yaml` + remounting, not a re-upload
(though a re-upload after fixing the mount is the quickest way to repopulate).

**Don't start a port-forward without checking for an existing one.** See Phase 6 above.

---

## Related chapters

- Ch2 — [EFM Binaries & staging tree](ch02-efm-binaries.md): stocking the agent-binary tree that the
  deploy above expects, so the `Deploy Agent` button stops returning `400`.
