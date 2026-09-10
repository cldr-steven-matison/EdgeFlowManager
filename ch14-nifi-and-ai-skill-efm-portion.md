# Chapter 14: The NiFi and AI Skill — EFM Portion

Every flow in this Part was built the same way. The NiFi + Python flows in Ch15, the MiNiFi edge AI in Ch16, and the StarlinkAI router in Ch17 all came from driving NiFi, MiNiFi, and EFM programmatically, with an AI agent holding a written playbook of what breaks and how. That playbook is a Claude Code skill called [`nifi-and-ai`](https://github.com/cldr-steven-matison/NiFiandAi), published as its own repo you can clone into `~/.claude/skills/nifi-and-ai/`. This chapter shows the whole skill, then goes deep on the part that matters most for this guide, the EFM portion. That is the machinery of managing agents from a central manager, and it is the undocumented territory the rest of the guide lives in.

> **Why a chapter about a tool, in a guide about EFM?** Because the tool is how the EFM work got done. Everything it knows about EFM was learned the expensive way, one silent-drop, corrupted-credential, empty-install-dir bug at a time. Reading the skill is reading the post-mortems without paying for them. The rest of Part VI puts it to work.

Everything here applies to EFM `2.3.1.0-2`, MiNiFi C++ `1.26.02`, and MiNiFi Java `2.24.08.0-19`.

---

## What the Skill Is

`nifi-and-ai` is a Claude Code skill, a `SKILL.md` and eight reference files that install into `~/.claude/skills/` and load on demand when a session touches NiFi, MiNiFi, or EFM. It is not documentation about NiFi. The Apache docs already exist. It is the distilled residue of building this array. Each rule is one bug that cost time, written down so the next session does not pay for it again.

The skill is a small always-loaded core plus reference files that load only when the task calls for one.

| File | What it covers |
|---|---|
| `SKILL.md` | The 10 rules, the three deployment shapes, and the map to everything else. Always loaded. |
| `references/flow-api.md` | Deploying and editing flows via the **NiFi** REST API. Auth, uploading a Process Group JSON, re-exporting to keep a checked-in copy current, safe live edits. |
| `references/minifi-efm.md` | **The edge side.** Staging agent binaries, EFM persistence, the deployer command, Windows+Python, the undocumented EFM Designer API, and recovering an agent whose heartbeat has gone dark. This is the EFM portion. |
| `references/custom-processors.md` | Writing custom Python/Java processors, the mixed-template EL trap, and the rebuild-then-redeploy discipline. |
| `references/patterns.md` | Flow patterns that ship. NiFi-as-HTTP-API, the MiNiFi fire-and-forget router, ingest to Kafka to transform to sink (RAG), and the GUI-less edge-to-host bridge. |
| `references/debugging.md` | Cross-cutting wire-up gotchas and a 10-step debugging checklist. |
| `references/layout.md` | Canvas layout. The coordinate model, spacing constants, direction and sprawl rules (route and add down, never up; new work to the right of the existing canvas; one test funnel; per-branch terminal logs), per-shape placement rules, a worked example, and the running list of what a programmatic build still needs a human pass on. |
| `references/flow-registry.md` | Add or update a Process Group **without ever reading the root `flow.json`**. The committed export is the registry. PG upload and upsert via the API, Parameter Context pre-create from k8s Secrets, a complete k8s Job template, and the secret-manager options. |
| `references/site-to-site.md` | Site-to-Site and secure-cluster rollout on the CFM operator. `userCertAuth` at CR creation, identity = cert SAN, the one-CA issuer chain, peers as `User` CRs, transport keys, and the symptom, cause, fix traps table. |

### The 10 Rules

The core of the skill is ten rules you read before touching any live flow. They are not NiFi trivia. Each one, ignored, costs an afternoon.

1. **Live UI / `flow.json` is truth. Docs and memory lag.** Dump the running flow before editing. Never edit from a remembered description.
2. **Never GET-then-PUT a processor with sensitive properties.** NiFi returns `"********"` on GET. PUT it back and you write that literal over the credential. Bind secrets to a Parameter Context, or use a narrow-scope endpoint like `/run-status`.
3. **Don't hand-patch a live Process Group while it is posting or queueing.** Route the change through the API, or rebuild and redeploy.
4. **Keep changes scoped.** Make the change asked for, not the adjacent "obvious improvement."
5. **Every flow change gets exported and committed.** A running canvas that is not in version control is one restart from gone.
6. **`ListenHTTP` on MiNiFi C++ is fire-and-forget. MiNiFi Java is not.** C++ has no `HandleHttpRequest`/`HandleHttpResponse` pair. The caller gets an empty `200` ack and the reply has to exit over Kafka keyed on a `request_id`. Java ships both processors and `StandardHttpContextMap`. This single fact decides C++ vs Java for any HTTP-fronted inference proxy (Ch16, Ch17).
7. **`Retry` is not `Failure`.** Auto-terminating `InvokeHTTP`'s `Retry` silently drops every transient 5xx/429. Self-loop `Retry` with a bounded `FlowFile Expiration`.
8. **Build new logic in its own new, finite Process Group. Never inline inside a live one.**
9. **Decompose into a FlowFile chain of small native processors. Don't put timers, state, and branching inside one custom Python processor.** A leaked internal-timer thread once kept re-logging stale state after a restart. A stock-processor chain cannot, because NiFi owns all scheduling.
10. **Never read `flow.json.gz` to add a component.** The committed JSON export in git is the source of truth. POST it to the parent PG's `upload` endpoint and only the new PG is touched. The rest of the canvas is never read or modified. The full registry pattern lives in `references/flow-registry.md`.

Rules 1, 2, 5, and 6 are the ones that bite hardest at the edge, and they carry through the whole of Part VI.

### The Three Deployment Shapes

The skill frames every flow by where it runs, because auth and lifecycle differ by shape.

| Shape | Where it lives | Auth | When |
|---|---|---|---|
| **Operator-managed on Kubernetes** | A `Nifi` CR, a StatefulSet pod | Operator-issued mTLS user cert, or Single-User Auth via a k8s secret | In-cluster flows |
| **Host-native NiFi** | A tarball install, `bin/nifi.sh start` | Single-user login | A single VM / public host |
| **MiNiFi C++/Java agent (EFM-deployed)** | Windows service, Linux `minifi.service`, or a K8s pod | Unauthenticated agent-to-EFM heartbeat by default | Edge / desktop flows driven from EFM |

The canonical AI array runs all three at once. EFM and MiNiFi agents on the edge, Kafka in the middle, NiFi doing the heavier lift.

---

## EFM-Directed vs Direct-on-Agent: The Two Ways to Drive an Edge Flow

The single most useful distinction the skill draws is how a change reaches a running agent. There are two paths, and confusing them is how work gets silently lost.

**EFM-directed.** You change the flow in EFM (Designer API or UI), validate, and `publish`. EFM pushes the new flow to the agent on its next heartbeat. This is authoritative. A `publish` overwrites even a hand-edited agent-local `config.yml` on the next heartbeat. Resources (scripts, JARs) travel the same way. They are uploaded to EFM's Resource Manager, assigned to the agent class, and synced to the agent over the C2 asset-sync command. This is the production path. It is restart-durable given the right PVCs, and it is tracked.

**Direct-on-agent.** You bypass EFM entirely. `kubectl cp` a script onto the pod, edit `config.yml` by hand, or `kill` and relaunch the `minifi` process inside the container. This is fast for iterating. A running C++ agent's `ExecuteScript` re-reads its script from disk on every trigger, so a raw `kubectl cp` takes effect on the next call with no republish. But it is invisible to EFM, it will not survive a pod restart, and it gets **overwritten the instant anyone does an EFM-directed publish**.

> **The trap.** A hand-edited local config is never authoritative once you use the API. If you iterate direct-on-agent and someone (or a later session) publishes from EFM, your changes are gone with no error. Use direct-on-agent for a tight edit loop. Promote to EFM-directed the moment the change is worth keeping.

The rest of this chapter is the EFM-directed machinery, because that is the part with no OpenAPI spec, no Swagger UI, and nothing else written down.

---

## Staging Agent Binaries into EFM

EFM deploys agents from a binaries tree with a **strict** validator. It rejects hyphens in `osArch` and more than one archive per leaf directory. Layout for the common four:

```text
binaries/cpp/linux/<ver>/minifi.tar.gz            # x86_64 Linux
binaries/cpp/linuxaarch64/<ver>/minifi.tar.gz     # ARM64 Linux
binaries/cpp/windows/<ver>/minifi.msi             # Windows
binaries/java/linux/<ver>/minifi.tar.gz           # Java MiNiFi
```

Inject any Linux `.so` extra-extensions and extra-python-components **inside** the tarball's `extensions/` dir before re-tarring, then tar-pipe into the EFM pod:

```bash
EFM_POD=$(kubectl get pod -n $NS -l app=efm -o jsonpath='{.items[0].metadata.name}')
tar -cf - binaries/ | kubectl exec -i $EFM_POD -n $NS -- tar -xf - -C /opt/efm/<efm-dir>/agent-deployer/
kubectl rollout restart deployment/efm -n $NS
```

The full staging tree, including the Windows MSI Python black hole and the missing Java scripting NAR, is Chapter 2 (EFM Binaries). If your `Deploy Agent` button returns `400`, that is the chapter, not this one.

## EFM Persistence: Three Layers, or a Restart Wipes State

1. **Postgres** holds the metadata. `agent_class`, `flow`, `flow_content`, `agent`, `agent_manifest`, `asset`, `resource_metadata`.
2. **A binaries PVC** holds the agent archives from above.
3. **A resources PVC** holds the uploaded Resources (Python scripts, JARs). The DB tracks the metadata. The file bytes live here.

Skip layer 3 and every uploaded script vanishes on pod restart even though the DB rows survive. The resource "exists" but has no content.

## The Agent Pod Boot Race

A MiNiFi agent pod downloads the deployer script from EFM at startup. EFM's Jetty takes about 2 minutes to bind its port on a cold start. A one-shot `curl` races that and exits silently. The pod stays `Running 1/1`, but the MiNiFi install dir is empty, with a single `curl: (7) Failed to connect` at the top of the pod log and nothing after.

The fix is to health-poll `/efm/actuator/health` (for example 120 × 5s, a 10-minute ceiling) before running the deployer. Diagnose with `kubectl exec <agent-pod> -- ls /nifi-minifi-cpp-<ver>/`. Empty means the deployer never ran.

## The Deployer Command: Get It From EFM, Never Hand-Build It

**The only sanctioned way to obtain a deployer command is EFM's own Deploy Agent CLI screen, or its backing API, `POST /efm/api/agent-deployer/generateCommand`.** Both return a full, ready-to-run command carrying a **server-minted `agentIdentifier`**. Do not hand-construct the `curl` or `Invoke-WebRequest`, and do not copy a previous deployment's command and tweak the fields for a new enrollment. That is exactly how a stale identifier gets reused and two pods collide on one EFM identity.

```bash
curl -s -X POST http://<efm-host>:10090/efm/api/agent-deployer/generateCommand \
 -H 'Content-Type: application/json' \
 -d '{
   "agentClass": "MyClass",
   "agentType": "cpp",
   "agentVersion": "<ver>",
   "osArch": "linuxaarch64",
   "baseUrl": "http://127.0.0.1:<port>/efm/api",
   "hbPeriod": 5000,
   "serviceUser": "minifi",
   "serviceName": "minifi",
   "autoConfigureSecurity": false,
   "trustSelfSignedCertificates": false
 }'
```

Omit `agentIdentifier` from that body. The server generates a fresh, collision-free one. The returned command has the same shape as any deployer curl (`agentClass`, `agentType`, `osArch`, and so on, piped into `bash -` on Linux or `Invoke-Expression` on Windows), but its `agentIdentifier` field is server-supplied, not something to pick or copy.

> **Don't reuse an identifier across a class migration or a new enrollment.** Re-enroll an agent under a new class with a hand-built command that carries the retired class's `agentIdentifier`, and two identities claim one agent record. The EFM C2 `UPDATE` pushing the flow to the re-enrolled agent fails on every attempt and the class's dashboard status turns red. Re-enrolling through `generateCommand` with its own fresh identifier clears it. **The one place reusing an identifier is correct is restoring the exact same bare pod that was never de-registered** (see the dark-agent recovery below). A new enrollment or a class migration is never that case.

On **Windows** run the generated command via `Invoke-WebRequest ... | Invoke-Expression` from PowerShell **as Administrator**, and `cd` to a clean dir first. The deployer installs to `$PWD`, and running it from `C:\WINDOWS\system32` is a permission nightmare.

---

## The EFM Designer API: No OpenAPI, Recover It from the UI Bundle

EFM exposes **no** OpenAPI/Swagger doc for its flow-editing REST API. `/efm/api-docs`, `/v3/api-docs`, and `/efm/swagger-ui` all `404`. Guessing at body shapes produces generic `500`s or, worse, silent no-ops. Jackson deserializes an unrecognized shape into a default empty DTO without erroring, so a `200 OK` does **not** mean the call did anything.

The way in is EFM's own Angular UI. It ships an OpenAPI-generated TypeScript client, so the compiled JS has every operation name, URL, and body shape verbatim, even minified:

```bash
curl -s http://<efm-host>:10090/efm/ui/ | grep -oE 'src="[^"]*main[^"]*\.js"'   # find the hashed bundle
curl -s http://<efm-host>:10090/efm/ui/main.<hash>.js -o /tmp/efm_main.js
grep -oE '"[A-Za-z]+Service\.[a-zA-Z]+"' /tmp/efm_main.js | sort -u            # every real operation
```

The working contract, recovered this way:

- `GET /efm/api/designer/client-identifier` returns `{"clientId": "<uuid>"}`, required in every write's `revision.clientId`.
- `GET /efm/api/designer/flows/summaries` returns one entry per agent class with `identifier` / `rootProcessGroupIdentifier`. `GET .../flows/{id}` returns the full live flow doc. **Read this before editing. It is ground truth over any doc or memory (rule 1).**
- `POST .../process-groups/{pgId}/processors` creates one processor. Properties and `autoTerminatedRelationships` can be set in this one call. The server assigns the `identifier`.
- `POST .../connections` wires one connection, referencing the server-assigned processor ids.
- `PUT .../processors/{id}` updates one processor. `revision.version` must match current.
- `GET .../flows/{id}/validate` must return `{"validationErrors":[]}` before publishing.
- `POST .../flows/{id}/publish` is **the push-to-agent step.** This is what overwrites a hand-edited local `config.yml` on the next heartbeat.
- `DELETE /efm/api/agents/{id}` removes a stale or `MISSING` agent record EFM never garbage-collects.

> **There is no whole-flow-document `PUT`. Don't guess one.** `PUT /efm/api/designer/flows/{flowId}` with the full modified `flowContent` fails at the routing layer (`HttpRequestMethodNotSupportedException: Request method 'PUT' is not supported`, a `500` before any business logic, nothing written). The only write path is one `POST` per processor and one `POST` per connection, each returning the identifier you use to wire the next. There is no batch or bulk create. This is the same contract Ch16 and Ch17 build against.

A read-only MCP server now wraps this same Designer API, along with the agent-class, manifest, and resource endpoints. The [Edge Flow Manager MCP Server](https://github.com/cldr-steven-matison/edge-flow-manager-mcp-server) exposes twelve GET-only tools over the surfaces above, and [Ch16](ch16-how-to-ai-with-minifi.md#let-the-ai-drive-the-flow-mcp-servers) covers it and the NiFi-side companion. It is a client of this contract, not part of the skill.

**The Designer validates against the agent class to manifest mapping, not against whatever agent is online.** Put a Java agent on a class whose flow was authored for C++ and the processors are rejected because the FQCNs differ (`org.apache.nifi.minifi.processors.ListenHTTP` vs the Java equivalent). Keep mixed runtimes as parallel classes, a C++ class separate from its Java sibling, so a Java agent never lands on a C++ canvas.

### Layout at the Designer Pitch

Building via the API means you also place every processor by `position:{x,y}`. There is no auto-layout. On an EFM Designer build the row pitch is **300** (not the NiFi canvas's 200), branch and column pitch is **600 to 900** (not 300 to 480), and a linear chain runs **vertical** (constant `x`, `y += 300`). A `(0,0)` then `(400,0)` sideways pair is the flagged-bad shape that reads cramped. Decide your intended shape and pitch before the first `POST .../processors`, not after the build reads cramped.

---

## The Resource Manager API: Getting Scripts and Assets onto an Agent

The tracked, restart-durable way to put a script or asset on an agent (vs `kubectl cp`-ing it directly):

- `POST /efm/api/resource-manager/resources/file` is multipart. Query params `name` / `resourceType` (`ASSET`|`EXTENSION`) / `relativePathOnAgent` / `notes`, field `file`. Returns a SHA-512 `digest`. Diff it against local `sha512sum` to check for drift.
- `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` takes a body that **must** be exactly `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[...]}`. A bare array or `{"resourceIds":[...]}` is silently swallowed (`200 OK`, nothing assigned).
- **No in-place asset update exists** (API or UI). Changing an assigned script's content is unassign, delete the old resource, upload as new, reassign. A same-named re-upload does not overwrite the old bytes.

This is the EFM-directed half of the two-paths distinction above. The direct-on-agent shortcut, `kubectl cp` onto the `ExecuteScript` path, which re-reads on every trigger, is faster for iteration but bypasses all of this tracking and dies with the pod.

---

## Status Is in Postgres, Not the REST Heuristics

EFM's `operation` table has no automatic retention. A crash-looping agent can flood it with thousands of rows in hours, which hangs `/efm/api/operations` entirely and breaks anything reconstructing "which agents are online" from it, **including EFM's own UI.** For reliable online/offline status, query Postgres directly. `agent` (`agent_class`, `agent_state`, `last_seen`) joined to `device` (`ip_address`, `hostname`) is the durable source of truth.

And **an agent-class name is not guaranteed to map to one physical machine.** A single class can have multiple separately-registered deployments, for example one GPU host and one CPU host running a stub with the same output schema. Don't call a hardware or script mismatch in an exported flow a bug without checking which agent identifier, which physical machine, you are looking at.

---

## When a `KubernetesPod`-class Agent Goes Silently Dark

The symptom: a pod's MiNiFi agent (`KubernetesPod` class) has not heartbeated to EFM in days. `last_seen` in Postgres is stale. But the pod shows `Running 1/1`, 0 restarts, and its already-deployed flow keeps working the whole time.

The reason: **MiNiFi C++ does not need EFM once a flow is deployed.** Only new pushes need a live heartbeat channel. So a `200` from the resource-manager or flow-publish API only means EFM accepted the write, not that any agent received it. Check `agent.last_seen` before assuming a push will land.

**Recovering a bare pod is not a one-line restart.** If the pod has no `Deployment`/`StatefulSet`/`ReplicaSet` owner (`kubectl get pod ... -o jsonpath='{.metadata.ownerReferences}'` returns empty), `kubectl delete` does not get it rescheduled. Before deleting, save the exact original manifest from the `kubectl.kubernetes.io/last-applied-configuration` annotation. That is the full `kubectl apply`-able JSON, deployer-curl args (including the agent's `agentIdentifier`) and all, so `apply` brings it back as the same EFM agent record and not a new one.

A fresh boot does not guarantee the flow's resources land on disk in time. Even with a correct `config.yml`, the assigned asset files can still be missing (`/nifi-minifi-cpp-<ver>/asset/` empty, `.state` shows `{"digest":"","assets":{}}`). Every `ExecuteScript` referencing them fails to start, retries a **fixed 3 times, 30s apart, then gives up** (not an infinite loop). The fix is to `kubectl cp` the asset file(s) onto the pod, then restart just the `minifi` process inside the container (find its PID, `kill` it, relaunch `./bin/minifi &`). That is much cheaper than another full pod delete, and it re-reads the already-correct `config.yml` cleanly now that the files exist.

Finally, **a bare pod's IP changes on every restart** (no stable `Service` in front of it). Any NiFi processor with that IP hardcoded in an `HTTP URL` breaks silently until updated. Grep for the old IP across the flow, or budget for a `Service` if the pod will restart more than once.

---

## Orphaned Resources, and Why the "Updated Agents" Badge Lies

A class showing red on the dashboard has a second, unrelated cause besides the deployer-command mistake above, and the fix for one does nothing for the other.

The symptom: an agent class has migrated from a C++ agent to a Java one, but a handful of Python `ExecuteScript` assets from the old C++ agent stay **assigned** to the class in the Resource Manager. Java `ExecuteScript` cannot run Python at all, so those assets are dead weight the moment the migration happens. Every heartbeat cycle, the live agent's `SYNC RESOURCE` operation fails with an HTTP 500 fetching resource content that nothing needs anymore.

**Unassigning the stale resources through the Resource Manager API is necessary but not sufficient.** Every read endpoint shows the unassignment landed. The resource no longer shows as assigned to the class, and its own reverse lookup shows no class association at all. But the very next `SYNC RESOURCE` operation dispatched to the agent still carries the byte-identical resource list and hash digest as every failure before. EFM caches the per-class resource digest it uses to build that operation somewhere the assign/unassign API call does not reach. The read path is honest. The operation-generation path is not.

**The fix is restarting the EFM pod itself** (`kubectl rollout restart deployment/efm`). Not the agent, not the class, just EFM's own process, to force it to drop its in-memory cache and reload from Postgres. Nothing is lost doing this. EFM's persistent state lives in Postgres and its PVCs, not in the pod's memory (see "EFM Persistence" above). The next sync after the restart succeeds.

Even with the underlying sync now succeeding on every individual operation, **the dashboard's "Updated Agents" badge stays red.** That badge is not a live health indicator. It reflects the class's most recent bulk operation, a different, coarser record that is only created by a class-wide action like publishing a flow. Routine per-agent sync retries never touch it. So a class can have every individual operation succeeding and still show red indefinitely, simply because nothing has run a fresh bulk action since the last one failed. Don't read a green badge as proof of health either. Check the underlying per-agent operations directly.

![Monitor → Agents "Updated Agents" column still showing a red warning icon at 100%. The badge tracks the last bulk operation, not live per-agent sync health](images/efm-orphaned-resources-updated-agents-badge.png)

Clicking through the warning surfaces the coarser bulk-operation record itself, not the live per-agent state:

![Updated Agents drill-down. "1 failed to update. 0 of 1 agents (100%) have received the last update." with a View Recent Alerts link](images/efm-orphaned-resources-sync-alert-detail.png)

**Clearing the badge is cheap. Republish the flow, even with zero content changes.** EFM's publish endpoint accepts a republish of an already-current, non-dirty flow, bumps its version number anyway, and pushes a fresh configuration update to every agent in the class. That alone creates a new, successful bulk operation and flips the badge to green. No delete-and-recreate needed, and the live agent's own identity and running processors are unaffected.

**If a plain republish does not clear it**, the class is more deeply broken than a stale badge and the fallback is delete the agent, delete the class, recreate. The trap in that fallback: **don't point the freshly recreated class at the old, retired class's flow definition.** That flow belongs to an identity that is gone. Reusing it either fails outright or drags forward whatever made the original class unhealthy. Build the new class's flow fresh, and, combined with the deployer-command rule above, always mint a fresh `agentIdentifier` too. A class recreation that reuses either the old flow or the old identifier is liable to reproduce the exact failure it was meant to fix.

---

## What NOT to Do

- Don't iterate direct-on-agent and assume it sticks. A hand-edited `config.yml` or a `kubectl cp`'d script is overwritten by the next EFM `publish`, with no error. Promote changes you want to keep to EFM-directed.
- Don't trust a `200` from a Designer or Resource Manager write. Jackson swallows an unrecognized body into an empty DTO and returns `200` having done nothing. Re-`GET` the flow, diff the SHA-512 digest, or check `validationErrors`.
- Don't `PUT` a whole flow to the Designer. There is no whole-flow PUT (`405`). Build component by component.
- Don't reconstruct agent online/offline status from `/efm/api/operations`. It has no retention and a crash-looper hangs it. Query the `agent` table.
- Don't GET-then-PUT a processor with sensitive properties. The `********` mask writes back as a literal and destroys the credential (rule 2).
- Don't hand-build an agent-deployer command, or reuse an `agentIdentifier` for a new enrollment. Get it from `generateCommand` every time. A stale identifier collides two pods on one EFM identity.
- Don't assume unassigning a stale Resource stops EFM syncing it. The read endpoints can be honest while the operation-generation cache stays stale. Watch the next sync succeed, and restart the EFM pod if it does not.
- Don't trust the "Updated Agents" dashboard badge either way. It tracks the class's last bulk operation, not live per-agent health. Query the underlying operations directly before concluding a class is broken or fixed.
- Don't `kubectl delete` a bare agent pod without first saving its `last-applied-configuration`. It will not reschedule, and you will have lost the manifest that re-registers it as the same agent.
- Don't assume an agent-class name is one machine. Check the agent identifier before calling a mismatch a bug.

---

## Related Chapters

- [EFM Binaries](ch02-efm-binaries.md) (Ch2): the full binary-staging tree, Windows MSI Python, and the missing Java scripting NAR.
- [ExecuteScript Availability](ch05-executescript-availability.md) (Ch5): which runtimes ship the Python engine.
- [How to AI with MiNiFi](ch16-how-to-ai-with-minifi.md) (Ch16): what you make agents do once this machinery is in place. Builds against the same Designer contract.

The `nifi-and-ai` skill is published at [cldr-steven-matison/NiFiandAi](https://github.com/cldr-steven-matison/NiFiandAi). Install it with

```bash
git clone https://github.com/cldr-steven-matison/NiFiandAi ~/.claude/skills/nifi-and-ai
```

and the next Claude Code session that touches NiFi, MiNiFi, or EFM loads it automatically. That repo is the working toolkit this chapter documents.
