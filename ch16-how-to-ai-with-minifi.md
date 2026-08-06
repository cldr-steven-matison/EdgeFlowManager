# Chapter 16: How to AI with MiNiFi

[Chapter 15](ch15-how-to-ai-with-nifi-and-python.md) runs Python inference *inside* NiFi on a Kubernetes cluster with room to spare. This chapter is the opposite end of the wire: a MiNiFi agent on a small edge box — a Beelink mini PC, a Windows desktop, a Jetson, a bare Kubernetes pod — that has no business hosting a model but still needs to do AI work. The lesson that took the longest to accept: **the agent almost never runs the model.** It routes, transforms, enrolls, and ships results. Get that straight and edge AI stops being a hardware problem and becomes a flow problem.

This is the "how to" chapter — the method, the tools, and the traps. The fully worked case study (the StarlinkAI router: Tailscale, Lemonade, the live router flow, and the transcription reassembly branch) is [Chapter 17](ch17-edge-ai-router.md). Read this for the pattern; read Ch17 for the build.

> **⚠️ This is the "using" chapter, not the "installing" chapter.** Staging agent binaries, the five-leaf EFM directory layout, the Windows MSI Python black hole, and the missing Java NARs all belong to [Chapter 2](ch02-efm-binaries.md). Read that first if your `Deploy Agent` button still returns `400`. This chapter assumes agents are online and asks the next question: what do you make them do.

## The Four Ways to Do AI at the Edge

Edge AI is not one approach. Each has a different footprint and a different fit — pick by where the model actually lives and how stable the logic is:

| Option | What the agent does | When to use it |
|---|---|---|
| **Route to a nearby inference server** | Accepts HTTP, forwards to a local LLM, returns the real answer | Agent is tiny; a GPU box is nearby |
| **`ExecuteScript` (Python)** | Runs an inline Python script on every FlowFile | Fast iteration, single-processor enrichment |
| **Custom Python processors** | Executes an authored processor type that ships with the agent | Reusable, stable edge logic that deserves its own palette entry |
| **On-device model execution** | Runs TensorRT / llama.cpp inside the flow | The edge device has a real GPU (Jetson, discrete-GPU box) |

The first three are this chapter. On-device execution (TensorRT on the Jetson, `RunLlamaCppInference` in the C++ manifest) is [Chapter 19](ch19-efm-and-nvidia-jetson.md).

## Route to a Nearby Inference Server

The most useful edge-AI shape is a MiNiFi **Java** flow that fronts a local inference server: the agent is tiny, the GPU box next to it holds the model. A pure pass-through does it — `HandleHttpRequest → InvokeHTTP → HandleHttpResponse`, where `InvokeHTTP` forwards `${http.request.uri}` unchanged so one flow fronts every endpoint on the model server, no per-endpoint branching. The agent doesn't know what a model is; it accepts a POST, forwards it, and hands the real response straight back. The value is the flow, the enrollment, and the transport — not the inference.

**Why MiNiFi Java, not C++, for this.** MiNiFi C++'s `ListenHTTP` has no synchronous request/response pair — the caller always gets an empty `200` ack, and the real answer has to come back out-of-band over Kafka keyed on a client-supplied `request_id`. MiNiFi **Java** ships `HandleHttpRequest`/`HandleHttpResponse` — a real synchronous response, no Kafka detour. For an HTTP-fronted inference proxy that is the decisive difference. Two settings on that flow are load-bearing and default wrong: `InvokeHTTP`'s `HTTP Method` silently stays `GET` (set it to `POST`), and its `Socket Read Timeout` defaults to `15 secs` while real LLM inference takes 10–25s+ (raise it to match your slowest endpoint, or every call fails with a silent `SocketTimeoutException`).

The complete build of this pattern — the exact processor settings, error routing, all five endpoints, and the multipart reassembly the transcription endpoint needs — is [Chapter 17](ch17-edge-ai-router.md).

## `ExecuteScript` — Inline Python on the Agent

`ExecuteScript` is the fastest way to run arbitrary Python at the edge. Wire it inline, set `Script Engine: python`, and paste a body that implements `onTrigger`:

```text
ListenHTTP :18080 /contentListener
  → ExecuteScript (Script Engine: python)
  → LogAttribute (Log Payload = true)
```

```python
def onTrigger(context, session):
    flow_file = session.get()
    if flow_file:
        session.putAttribute(flow_file, "python.smoke", "edge-executescript-ok")
        session.transfer(flow_file, REL_SUCCESS)
```

POST a payload and the attribute appears on `LogAttribute` — proof the extension loaded and executed. The property that makes `ExecuteScript` useful for iteration: a running C++ agent **re-reads the script from disk on every trigger**. Edit the script, POST again, the new logic runs — no restart, no republish.

In EFM Designer flows the C++ FQCN is `org.apache.nifi.minifi.processors.ExecuteScript` — note `minifi` in the path. It is *not* the Java NiFi `org.apache.nifi.processors.standard.ExecuteScript`.

### `ExecuteScript` Availability

`ExecuteScript` is **not in any stock Cloudera binary** — not the C++ image, not the CEM Java tarball, not the default Windows MSI feature set. The tell is `Could not instantiate: PythonScriptExecutor` repeating every 30s in `minifi-app.log`, or an EFM Designer "not a valid Processor type" rejection.

| Runtime | Path to the engine | Python engine? |
|---|---|---|
| C++ Kubernetes pod | Extra-extensions injection (`MINIFI_EXTRA_EXTENSIONS_COMMA_SEPARATED`) | Yes — confirmed on arm64 K8s pods and Jetson |
| C++ source build | Include the Python extension at build time | Yes |
| Java CEM tarball | Drop the scripting NAR into `lib/` | **No** — CEM `2.24.08.0-19` gives Groovy/Clojure only, no Python |
| Windows MSI | `ADDLOCAL=ALL` during install | Yes (C++ MSI only) |

Python `ExecuteScript` at the edge means a **C++ agent**, not the Java agent. See [Chapter 5](ch05-executescript-availability.md) for the full build-by-build breakdown.

### Getting Scripts onto the Agent

Two mechanisms, and the choice is the same one you make for every resource at the edge:

- **EFM Resource Manager API** — the tracked, restart-durable path. `POST /efm/api/resource-manager/resources/file` to upload, then `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` with the exact body `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[...]}` (a bare array is silently swallowed). This path needs the `efm-resources` PVC, or the uploaded bytes die with the pod while the DB row survives pointing at nothing.
- **Raw `kubectl cp`** onto the agent's script path — takes effect on the next trigger, bypasses EFM tracking, does not survive a pod restart. Correct for fast iteration; not the production path.

### Windows Session 0 Caveat

An `ExecuteScript` that shells out to launch a GUI window runs green — `200`, attributes set, the target process even spawns — but the window never appears. A default `LocalSystem` Windows service lives in Session 0 with no interactive desktop. Run the agent in process-mode (Session 1) for anything that must show up on screen.

## Custom Python Processors — Author a New Edge Processor Type

When the logic is worth keeping, write it as a real processor. A custom Python processor is a new *type*: it appears in the agent's manifest under its own name, with its own properties and relationships, and wires into a flow like any stock processor. The same four rules from [Chapter 15](ch15-how-to-ai-with-nifi-and-python.md) apply — you own the framework skeleton, the AI writes the logic inside it.

On the C++ agent, subclass the pre-shipped `nifiapi` framework:

```python
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class EdgeTagger(FlowFileTransform):
    class ProcessorDetails:
        version = "0.0.1"
        description = "Tags a FlowFile with an edge attribute and passes it through."

    def transform(self, context, flowfile):
        return FlowFileTransformResult(
            relationship="success",
            attributes={"edge.tag": "field-test"},
        )
```

Drop the `.py` into the agent's configured processor directory (`nifi.python.processor.dir`, default `${MINIFI_HOME}/minifi-python/`; authored processors go in the sibling `nifi_python_processors/` package) and restart. The agent's `PythonCreator` scans the directory once at boot and registers the type under its own FQCN — `org.apache.nifi.minifi.processors.nifi_python_processors.EdgeTagger` appears in `GET /efm/api/agent-manifests/{id}` with the exact text from `ProcessorDetails.description` in the `typeDescription` field, confirmation the authored `describe()` ran, not a placeholder. From there it wires into an EFM Designer flow (`ListenHTTP → EdgeTagger → LogAttribute`) exactly like a stock processor.

> **⚠️ A custom processor is not a hot patch.** `PythonCreator` scans at boot; a `.py` dropped in or edited after the agent is running is not picked up until the agent restarts. This is the sharp difference from `ExecuteScript`, which re-reads every trigger.

### `ExecuteScript` vs Custom Processor — Pick One

| | `ExecuteScript` (Python engine) | Custom Python processor |
|---|---|---|
| What it is | One generic processor; paste a script body | A new processor type you author in Python |
| Identity in the flow | Always shows as `ExecuteScript` | Shows under its own name |
| Reload | Re-reads script every trigger — hot-edit, no restart | `PythonCreator` scans at boot — needs agent restart |
| Best for | Fast iteration, per-box one-offs | Reusable, stable edge capability |

Conflating these two is the most common mistake. They are different processors with different reload behavior: use `ExecuteScript` while you're iterating, author a processor once the capability is stable.

## Building the Flows — The Skill, the API, and Using AI to Test

Everything above gets published to agents the same way, and the method is worth stating once because it's what makes AI-assisted edge work fast instead of frustrating.

**Use the `nifi-and-ai` skill, not raw recall.** [Chapter 14](ch14-nifi-and-ai-skill-efm-portion.md) is the skill's EFM machinery. It carries the flow-build API shapes, the manifest-mapping rules, and the silent-drop failure catalog so an AI building these flows works from the proven contract instead of hallucinating one.

**Build flows component by component — there is no whole-flow PUT.** `PUT /efm/api/designer/flows/{flowId}` returns `405`. The loop is always the same:

```bash
POST /efm/api/designer/flows/{flowId}/process-groups/{pgId}/processors   # one per processor
POST /efm/api/designer/flows/{flowId}/connections                        # one per connection
GET  /efm/api/designer/flows/{flowId}/validate                           # must return "validationErrors": []
POST /efm/api/designer/flows/{flowId}/publish
```

The Designer validates against the agent class → manifest mapping, not against whatever agent happens to be online. New processors don't get their relationships auto-terminated (an unterminated `failure`/`unmatched` blocks `/publish` with a `409`), and a single orphaned processor anywhere on the canvas blocks the whole publish.

**Test locally before you trust it.** The agent is an HTTP endpoint — POST a real payload to `ListenHTTP`/`HandleHttpRequest` and read what comes out of `LogAttribute` or `PutFile`. This is where AI-authored logic earns trust: prove the framework loads (an empty pass-through), then add the logic, then send the real request. Never publish an AI-generated processor straight to a live agent — run the skeleton first.

**Deliver resources the way that matches the job.** Restart-durable delivery is the EFM Resource Manager (needs the `efm-resources` PVC); fast-iteration delivery is `kubectl cp` (gone on the next pod restart). Pick per whether you're shipping or still iterating — the same split as scripts above.

## Traps — The Ones That Drop Data Silently

These error nowhere. They just quietly do nothing, which is worse:

- **`ListenHTTP` `Batch Size`/`Buffer Size` default to `5/5`.** A single request never fills the buffer and is dropped with `buffer is NOT full 1/5`. Set both to `1` (MINIFICPP-2243 off-by-one). First thing to check when an edge flow "does nothing."
- **`InvokeHTTP`'s `HTTP Method` silently stays `GET`.** Even when you meant `POST`. A bodyless GET is a common "why did the model get nothing" cause.
- **`Socket Read Timeout` defaults to `15 secs`.** LLM inference takes 10–25s+. Silent `SocketTimeoutException`, nothing returned, caller hangs.
- **Kafka bootstrap: external NodePort vs in-cluster port.** From outside the cluster (an edge agent over Tailscale) it's the NodePort, not the internal `:9092`. On Strimzi, per-broker `advertisedHost` must be a reachable hostname or brokers hand clients a raw LAN IP they can't route to.
- **`Retry` is not `Failure`.** Auto-terminating `InvokeHTTP`'s `Retry` relationship silently drops every transient 5xx/429. Self-loop `Retry` with a bounded `FlowFile Expiration`, route `Failure` to a log processor.
- **Never GET-then-PUT a processor with sensitive properties.** EFM/NiFi returns `********` on read; PUT it back and you write that literal over the real credential. Bind secrets to a Parameter Context or use a narrow-scope endpoint.
- **Live flow is truth.** Before editing a running agent's flow, pull what's actually there (`GET /efm/api/designer/flows/{id}`, or dump `config.yml`). The running canvas has drifted from your notes more often than not.

## What NOT to Do

- **Don't wait on the `ListenHTTP` response for model output when using C++.** MiNiFi C++ is fire-and-forget; the answer comes back on Kafka keyed by `request_id`. Use MiNiFi Java (`HandleHttpRequest`/`HandleHttpResponse`) for synchronous inference proxying.
- **Don't conflate `ExecuteScript` with a custom Python processor.** One hot-reloads every trigger; the other needs a restart. Reaching for the wrong one makes your iteration loop fight you.
- **Don't expect `ExecuteScript` to exist in a stock agent.** Getting the Python engine onto the agent is an install problem — see Ch2 and Ch5 first.
- **Don't publish AI-generated logic straight to a live agent.** Prove the empty skeleton, then add logic, then send a real request.
- **Don't `PUT` a whole flow to the Designer.** There is no whole-flow PUT (`405`) — build component by component.
- **Don't publish onto a class whose manifest doesn't match the agent's runtime.** C++ FQCNs on a Java-mapped class get rejected.
- **Don't leave `ListenHTTP` at `5/5` or `InvokeHTTP` at `GET`.** The two defaults that drop or neuter more edge flows than anything else.

## Related Chapters

- Ch14 — [The NiFi and AI Skill — EFM Portion](ch14-nifi-and-ai-skill-efm-portion.md): building these flows through the `nifi-and-ai` skill rather than by hand.
- Ch15 — [How to AI with NiFi and Python](ch15-how-to-ai-with-nifi-and-python.md): the four rules for AI-authored processors, in full, on the NiFi side.
- Ch17 — [Edge-AI Router Case Study — StarlinkAI](ch17-edge-ai-router.md): the fully worked router build — Tailscale, Lemonade, the live flow, error routing, and the transcription reassembly branch.
- Ch5 — [ExecuteScript Availability](ch05-executescript-availability.md): which runtimes ship the Python engine, build by build.
- Ch19 — [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md): on-device model execution via TensorRT / llama.cpp.
