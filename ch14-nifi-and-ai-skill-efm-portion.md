# Chapter 14: The NiFi and AI Skill — EFM Portion

Every flow in this Part — NiFi + Python (Ch15), MiNiFi AI at the edge (Ch16), the StarlinkAI
router (Ch17) — was built the same way: not by hand-dragging processors around a canvas, but by
driving NiFi, MiNiFi, and EFM programmatically, with an AI agent holding a written playbook of what
breaks and how. That playbook is a Claude Code skill called `nifi-and-ai`. This chapter exposes it
in full, then goes deep on the part that matters most for this guide: the **EFM portion** — the
machinery of managing agents from a central manager, which is exactly the undocumented territory the
rest of the guide lives in.

> **Why a chapter about a tool, in a guide about EFM?** Because the tool *is* how the EFM work got
> done, and everything it knows about EFM was learned the expensive way — one silent-drop,
> corrupted-credential, empty-install-dir bug at a time. Reading the skill is reading the
> post-mortems without paying for them. The rest of Part V then puts it to work.

Everything here is field-verified against a live EFM `2.3.1.0-2`, MiNiFi C++ `1.26.02`, and MiNiFi
Java `2.24.08.0-19`.

---

## What the skill actually is

`nifi-and-ai` is a Claude Code skill — a `SKILL.md` and six reference files that install into
`~/.claude/skills/` and load on demand when a session touches NiFi, MiNiFi, or EFM. It is not
documentation *about* NiFi; the Apache docs already exist. It is the distilled residue of building
this array: each rule is one bug that cost real time, written down so the next session doesn't pay
for it again.

The skill is organized as a small always-loaded core plus reference files loaded only when the task
calls for one:

| File | What it covers |
|---|---|
| `SKILL.md` | The 9 rules, the three deployment shapes, and the map to everything else. Always loaded. |
| `references/flow-api.md` | Deploying and editing flows via the **NiFi** REST API — auth, uploading a Process Group JSON, re-exporting to keep a checked-in copy current, safe live edits. |
| `references/minifi-efm.md` | **The edge side.** Staging agent binaries, EFM persistence, the deployer curl, Windows+Python, the undocumented EFM Designer API, and recovering an agent whose heartbeat has gone dark. This is the EFM portion. |
| `references/custom-processors.md` | Writing custom Python/Java processors, the mixed-template EL trap, and rebuild→redeploy discipline. |
| `references/patterns.md` | Flow patterns that ship: NiFi-as-HTTP-API, the MiNiFi fire-and-forget router, ingest→Kafka→transform→sink (RAG), and the GUI-less edge→host bridge. |
| `references/debugging.md` | Cross-cutting wire-up gotchas and a 10-step debugging checklist. |
| `references/layout.md` | Canvas layout: the coordinate model, spacing constants, per-shape placement rules, and a worked example. |

### The 9 rules

The core of the skill is nine rules you read before touching any live flow. They aren't NiFi
trivia — they're the ones that, ignored, cost an afternoon each:

1. **Live UI / `flow.json` is truth. Docs and memory lag.** Dump the running flow before editing;
   never edit from a remembered description.
2. **Never GET-then-PUT a processor with sensitive properties.** NiFi returns `"********"` on GET;
   PUT it back and you write that literal over the real credential. Bind secrets to a Parameter
   Context, or use a narrow-scope endpoint like `/run-status`.
3. **Don't hand-patch a live Process Group while it's actively posting/queueing.** Route the change
   through the API, or rebuild → redeploy.
4. **Keep changes scoped.** Make the change asked for, not the adjacent "obvious improvement."
5. **Every flow change gets exported + committed.** A running canvas that isn't in version control
   is one restart from gone.
6. **`ListenHTTP` on MiNiFi C++ is fire-and-forget; MiNiFi Java is not.** C++ has no
   `HandleHttpRequest`/`HandleHttpResponse` pair — the caller gets an empty `200` ack and the real
   reply must exit over Kafka keyed on a `request_id`. Java ships both processors and
   `StandardHttpContextMap`. This single fact decides C++ vs Java for any HTTP-fronted inference
   proxy (Ch16, Ch17).
7. **`Retry` is not `Failure`.** Auto-terminating `InvokeHTTP`'s `Retry` silently drops every
   transient 5xx/429. Self-loop `Retry` with a bounded `FlowFile Expiration`.
8. **Build new logic in its own new, finite Process Group — never inline inside a live one.**
9. **Decompose into a FlowFile chain of small native processors — don't put timers, state, and
   branching inside one custom Python processor.** A leaked internal-timer thread once kept
   re-logging stale state after a restart; a stock-processor chain can't, because NiFi owns all
   scheduling.

Rules 1, 2, 5, and 6 are the ones that bite hardest at the edge, and they carry through the whole
of Part V.

### The three deployment shapes

The skill frames every flow by where it runs, because auth and lifecycle differ by shape:

| Shape | Where it lives | Auth | When |
|---|---|---|---|
| **Operator-managed on Kubernetes** | A `Nifi` CR → StatefulSet pod | Operator-issued mTLS user cert, or Single-User Auth via a k8s secret | In-cluster flows |
| **Host-native NiFi** | A tarball install, `bin/nifi.sh start` | Single-user login | A single VM / public host |
| **MiNiFi C++/Java agent (EFM-deployed)** | Windows service, Linux `minifi.service`, or a K8s pod | Unauthenticated agent→EFM heartbeat by default | Edge / desktop flows driven from EFM |

The canonical AI array runs all three at once: **EFM + MiNiFi agents on the edge + Kafka in the
middle + NiFi doing the heavier lift.**

---

## EFM-directed vs direct-on-agent: the two ways to drive an edge flow

The single most useful distinction the skill draws is *how* a change reaches a running agent. There
are two paths, and confusing them is how work gets silently lost.

**EFM-directed** — you change the flow in EFM (Designer API or UI), validate, and `publish`. EFM
pushes the new flow to the agent on its next heartbeat. This is authoritative: a `publish`
overwrites even a hand-edited agent-local `config.yml` on the next heartbeat. Resources (scripts,
JARs) travel the same way — uploaded to EFM's Resource Manager, assigned to the agent class, synced
to the agent over the C2 asset-sync command. This is the production path, it's restart-durable
(given the right PVCs), and it's tracked.

**Direct-on-agent** — you bypass EFM entirely: `kubectl cp` a script onto the pod, edit `config.yml`
by hand, or `kill` and relaunch the `minifi` process inside the container. This is fast for
iterating — a running C++ agent's `ExecuteScript` re-reads its script from disk on every trigger, so
a raw `kubectl cp` takes effect on the next call with no republish — but it's invisible to EFM,
won't survive a pod restart, and gets **overwritten the instant anyone does an EFM-directed
publish**.

> **The trap:** a hand-edited local config is never authoritative once you use the real API. If you
> iterate direct-on-agent and someone (or a later session) publishes from EFM, your changes are gone
> with no error. Use direct-on-agent for a tight edit loop; promote to EFM-directed the moment the
> change is worth keeping.

The rest of this chapter is the EFM-directed machinery, because that's the part with no OpenAPI
spec, no Swagger UI, and nothing else written down.

---

## Staging agent binaries into EFM

EFM deploys agents from a binaries tree with a **strict** validator: it rejects hyphens in `osArch`
and more than one archive per leaf directory. Layout for the common four:

```text
binaries/cpp/linux/<ver>/minifi.tar.gz            # x86_64 Linux
binaries/cpp/linuxaarch64/<ver>/minifi.tar.gz     # ARM64 Linux
binaries/cpp/windows/<ver>/minifi.msi             # Windows
binaries/java/linux/<ver>/minifi.tar.gz           # Java MiNiFi
```

Inject any Linux `.so` extra-extensions and extra-python-components **inside** the tarball's
`extensions/` dir before re-tarring, then tar-pipe into the EFM pod:

```bash
EFM_POD=$(kubectl get pod -n $NS -l app=efm -o jsonpath='{.items[0].metadata.name}')
tar -cf - binaries/ | kubectl exec -i $EFM_POD -n $NS -- tar -xf - -C /opt/efm/<efm-dir>/agent-deployer/
kubectl rollout restart deployment/efm -n $NS
```

The full staging tree — including the Windows MSI Python black hole and the missing Java
scripting NAR — is Chapter 2 (EFM Binaries). If your `Deploy Agent` button returns `400`, that's the
chapter, not this one.

## EFM persistence — three layers, or a restart wipes state

1. **Postgres** — metadata: `agent_class`, `flow`, `flow_content`, `agent`, `agent_manifest`,
   `asset`, `resource_metadata`.
2. **A binaries PVC** → the agent archives from above.
3. **A resources PVC** → uploaded Resources (Python scripts, JARs). The DB tracks the metadata; the
   file *bytes* live here.

Skip layer 3 and every uploaded script vanishes on pod restart even though the DB rows survive — a
confusing failure where the resource "exists" but has no content.

## The agent pod boot race

A MiNiFi agent pod downloads the deployer script from EFM at startup. EFM's Jetty takes ~2 minutes
to bind its port on a cold start. A one-shot `curl` races that and exits silently — the pod stays
`Running 1/1`, but the MiNiFi install dir is empty, with a single `curl: (7) Failed to connect` at
the top of the pod log and nothing after.

**Fix:** health-poll `/efm/actuator/health` (e.g. 120 × 5s = a 10-minute ceiling) *before* running
the deployer. Diagnose with `kubectl exec <agent-pod> -- ls /nifi-minifi-cpp-<ver>/` — empty means
the deployer never ran.

## The deployer curl

Same shape for every arch — swap `agentType` / `agentVersion` / `osArch`:

```bash
curl -L \
 -d agentClass=MyClass \
 -d agentIdentifier=$(cat /proc/sys/kernel/random/uuid) \
 -d agentType=cpp \
 -d agentVersion=<ver> \
 -d autoConfigureSecurity=false \
 -d baseUrl=http%3A%2F%2F127.0.0.1%3A<port>%2Fefm%2Fapi \
 -d hbPeriod=5000 \
 -d osArch=linuxaarch64 \
 -d serviceName=minifi -d serviceUser=minifi \
 -d trustSelfSignedCertificates=false \
 http://<efm-host>:10090/efm/api/agent-deployer/script | bash -
```

On **Windows** run the equivalent `Invoke-WebRequest ... | Invoke-Expression` from PowerShell **as
Administrator**, and `cd` to a clean dir first — the deployer installs to `$PWD`, and running it from
`C:\WINDOWS\system32` is a permission nightmare.

---

## The EFM Designer API — no OpenAPI, recover it from the UI bundle

EFM exposes **no** OpenAPI/Swagger doc for its flow-editing REST API — `/efm/api-docs`,
`/v3/api-docs`, and `/efm/swagger-ui` all `404`. Guessing at body shapes produces generic `500`s or,
worse, silent no-ops: Jackson deserializes an unrecognized shape into a default/empty DTO without
erroring, so a `200 OK` does **not** mean the call did anything.

The way in is EFM's own Angular UI. It ships an OpenAPI-generated TypeScript client, so the compiled
JS has every operation name, URL, and body shape verbatim — even minified:

```bash
curl -s http://<efm-host>:10090/efm/ui/ | grep -oE 'src="[^"]*main[^"]*\.js"'   # find the hashed bundle
curl -s http://<efm-host>:10090/efm/ui/main.<hash>.js -o /tmp/efm_main.js
grep -oE '"[A-Za-z]+Service\.[a-zA-Z]+"' /tmp/efm_main.js | sort -u            # every real operation
```

The confirmed working contract, recovered this way and proven against the live array:

- `GET /efm/api/designer/client-identifier` → `{"clientId": "<uuid>"}` — required in every write's
  `revision.clientId`.
- `GET /efm/api/designer/flows/summaries` → one entry per agent class with `identifier` /
  `rootProcessGroupIdentifier`; `GET .../flows/{id}` for the full live flow doc. **Read this before
  editing — it is ground truth over any doc or memory (rule 1).**
- `POST .../process-groups/{pgId}/processors` — create one processor. Properties and
  `autoTerminatedRelationships` can be set in this one call; the server assigns the real
  `identifier`.
- `POST .../connections` — wire one connection, referencing the server-assigned processor ids.
- `PUT .../processors/{id}` — update one processor; `revision.version` must match current.
- `GET .../flows/{id}/validate` → must return `{"validationErrors":[]}` before publishing.
- `POST .../flows/{id}/publish` — **the real push-to-agent step.** This is what overwrites a
  hand-edited local `config.yml` on the next heartbeat.
- `DELETE /efm/api/agents/{id}` — removes a stale/`MISSING` agent record EFM never garbage-collects.

> **There is no whole-flow-document `PUT`. Don't guess one.** `PUT /efm/api/designer/flows/{flowId}`
> with the full modified `flowContent` fails at the routing layer
> (`HttpRequestMethodNotSupportedException: Request method 'PUT' is not supported` — a `500` before
> any business logic, nothing written). The only write path is one `POST` per processor and one
> `POST` per connection, each returning the identifier you use to wire the next. There is no
> batch/bulk create. This is the same contract Ch16 and Ch17 build against.

**The Designer validates against the agent class → manifest mapping, not against whatever agent is
online.** Put a Java agent on a class whose flow was authored for C++ and the processors are rejected
because the FQCNs differ (`org.apache.nifi.minifi.processors.ListenHTTP` vs the Java equivalent).
Keep mixed runtimes as parallel classes — `WindowsDesktopCpp` separate from `WindowsDesktop`,
`KubernetesPodJava` separate from `KubernetesPod` — so a Java agent never lands on a C++ canvas.

### Layout at the Designer pitch

Building via the API means you also place every processor by `position:{x,y}` — there is no
auto-layout. On an EFM Designer build the row pitch is **300** (not the NiFi canvas's 200), branch/
column pitch is **~600–900** (not ~300–480), and a linear chain runs **vertical** (constant `x`,
`y += 300`). A `(0,0)→(400,0)` sideways pair is the flagged-bad shape that reads cramped. Decide your
intended shape and pitch *before* the first `POST .../processors`, not after the build reads cramped.

---

## The Resource Manager API — getting scripts and assets onto an agent

The tracked, restart-durable way to put a script or asset on an agent (vs `kubectl cp`-ing it
directly):

- `POST /efm/api/resource-manager/resources/file` — multipart; query params `name` /
  `resourceType` (`ASSET`|`EXTENSION`) / `relativePathOnAgent` / `notes`, field `file`. Returns a
  SHA-512 `digest` — diff it against local `sha512sum` to confirm no drift.
- `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` — body **must** be exactly
  `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[...]}`. A bare array or
  `{"resourceIds":[...]}` is silently swallowed (`200 OK`, nothing assigned).
- **No in-place asset update exists** (API or UI). Changing an assigned script's content is:
  unassign → delete the old resource → upload as new → reassign. A same-named re-upload does not
  overwrite the old bytes.

This is the EFM-directed half of the two-paths distinction above. The direct-on-agent shortcut —
`kubectl cp` onto the `ExecuteScript` path, which re-reads on every trigger — is faster for
iteration but bypasses all of this tracking and dies with the pod.

---

## Status is in Postgres, not the REST heuristics

EFM's `operation` table has no automatic retention. A crash-looping agent can flood it — thousands of
rows in hours — which hangs `/efm/api/operations` entirely and breaks anything reconstructing "which
agents are online" from it, **including EFM's own UI.** For reliable online/offline status, query
Postgres directly: `agent`(`agent_class`, `agent_state`, `last_seen`) joined to `device`
(`ip_address`, `hostname`) is the durable source of truth.

And **an agent-class name is not guaranteed to map to one physical machine.** A single class can have
multiple separately-registered deployments — e.g. one GPU host and one CPU host running a stub with
the same output schema. Don't call a hardware/script mismatch in an exported flow a bug without
checking which agent identifier — which physical machine — you're actually looking at.

---

## When a `KubernetesPod`-class agent goes silently dark

A real incident, and the sharpest EFM-portion lesson: a pod's MiNiFi agent (`KubernetesPod` class)
had **not heartbeated to EFM in 6 days** — `last_seen` in Postgres was stale — but the pod showed
`Running 1/1`, 0 restarts, and its already-deployed flow kept working the whole time. That's the key
fact: **MiNiFi C++ doesn't need EFM once a flow is deployed.** Only *new* pushes need a live
heartbeat channel. So a `200` from the resource-manager or flow-publish API only means EFM accepted
the write — not that any agent received it. Check `agent.last_seen` before assuming a push will land.

**Recovering a bare pod isn't a one-line restart.** If the pod has no `Deployment`/`StatefulSet`/
`ReplicaSet` owner (`kubectl get pod ... -o jsonpath='{.metadata.ownerReferences}'` returns empty),
`kubectl delete` does *not* get it rescheduled. Before deleting, save the exact original manifest
from the `kubectl.kubernetes.io/last-applied-configuration` annotation — that's the full
`kubectl apply`-able JSON, deployer-curl args (including the agent's `agentIdentifier`) and all, so
`apply` brings it back as the *same* EFM agent record rather than a new one.

And a fresh boot doesn't guarantee the flow's resources land on disk in time. Even with a correct
`config.yml`, the assigned asset *files* can still be missing (`/nifi-minifi-cpp-<ver>/asset/` empty,
`.state` shows `{"digest":"","assets":{}}`) — every `ExecuteScript` referencing them fails to start,
retries a **fixed 3 times, 30s apart, then gives up** (not an infinite loop). Fix: `kubectl cp` the
asset file(s) onto the pod, then restart just the `minifi` process inside the container (find its
PID, `kill` it, relaunch `./bin/minifi &`) — much cheaper than another full pod delete, and it
re-reads the already-correct `config.yml` cleanly now that the files exist.

Finally, **a bare pod's IP changes on every restart** (no stable `Service` in front of it). Any NiFi
processor with that IP hardcoded in an `HTTP URL` breaks silently until updated — grep for the old IP
across the flow, or budget for a real `Service` if the pod will restart more than once.

---

## What NOT to do

- **Don't iterate direct-on-agent and assume it sticks.** A hand-edited `config.yml` or a
  `kubectl cp`'d script is overwritten by the next EFM `publish`, with no error. Promote real changes
  to EFM-directed.
- **Don't trust a `200` from a Designer or Resource Manager write.** Jackson swallows an unrecognized
  body into an empty DTO and returns `200` having done nothing. Verify: re-`GET` the flow, diff the
  SHA-512 digest, or check `validationErrors`.
- **Don't `PUT` a whole flow to the Designer.** There is no whole-flow PUT (`405`) — build component
  by component.
- **Don't reconstruct agent online/offline status from `/efm/api/operations`.** It has no retention
  and a crash-looper hangs it. Query the `agent` table.
- **Don't GET-then-PUT a processor with sensitive properties.** The `********` mask writes back as a
  literal and destroys the credential (rule 2).
- **Don't `kubectl delete` a bare agent pod without first saving its
  `last-applied-configuration`.** It won't reschedule, and you'll have lost the manifest that
  re-registers it as the same agent.
- **Don't assume an agent-class name is one machine.** Check the agent identifier before calling a
  mismatch a bug.

---

## Related chapters

- Ch2 — [EFM Binaries](ch02-efm-binaries.md): the full binary-staging tree, Windows MSI Python, and
  the missing Java scripting NAR.
- Ch5 — [ExecuteScript Availability](ch05-executescript-availability.md): which runtimes ship the
  Python engine.
- Ch16 — [How to AI with MiNiFi](ch16-how-to-ai-with-minifi.md): what you make agents *do* once this
  machinery is in place; builds against the same Designer contract.

The `nifi-and-ai` skill (`skills/nifi-and-ai/`) is the working toolkit this chapter documents.
