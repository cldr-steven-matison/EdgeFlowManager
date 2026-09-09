# Chapter 16: How to AI with MiNiFi

[Chapter 15](ch15-how-to-ai-with-nifi-and-python.md) runs Python inference inside NiFi on a Kubernetes cluster with room to spare. This chapter is the opposite end of the wire. A MiNiFi agent on a small edge box (a Beelink mini PC, a Windows desktop, a Jetson, a bare Kubernetes pod) has no business hosting a model but still needs to do AI work. The thing that took longest to accept: **the agent almost never runs the model.** It routes, transforms, enrolls, and ships results. Get that straight and edge AI stops being a hardware problem and becomes a flow problem.

This is the "how to" chapter. The method, the tools, and the traps. The complete case study (the StarlinkAI router with Tailscale, Lemonade, the live router flow, and the transcription reassembly branch) is [Chapter 17](ch17-edge-ai-router.md). Read this for the pattern. Read Ch17 for the build.

> **⚠️ This is the "using" chapter, not the "installing" chapter.** Staging agent binaries, the five-leaf EFM directory layout, the Windows MSI Python black hole, and the missing Java NARs all belong to [Chapter 2](ch02-efm-binaries.md). Read that first if your `Deploy Agent` button still returns `400`. This chapter assumes agents are online and asks the next question. What do you make them do?

## The Four Ways to Do AI at the Edge

Edge AI is not one approach. Each has a different footprint and a different fit. Pick by where the model lives and how stable the logic is.

| Option | What the agent does | When to use it |
|---|---|---|
| **Route to a nearby inference server** | Accepts HTTP, forwards to a local LLM, returns the answer | Agent is tiny; a GPU box is nearby |
| **`ExecuteScript` (Python)** | Runs an inline Python script on every FlowFile | Fast iteration, single-processor enrichment |
| **Custom Python processors** | Executes an authored processor type that ships with the agent | Reusable, stable edge logic that deserves its own palette entry |
| **On-device model execution** | Runs TensorRT / llama.cpp inside the flow | The edge device has a GPU (Jetson, discrete-GPU box) |

The first three are this chapter. On-device execution (TensorRT on the Jetson, `RunLlamaCppInference` in the C++ manifest) is [Chapter 19](ch19-efm-and-nvidia-jetson.md).

## Route to a Nearby Inference Server

The most useful edge-AI shape is a MiNiFi **Java** flow that fronts a local inference server. The agent is tiny. The GPU box next to it holds the model. A pure pass-through does it, `HandleHttpRequest → InvokeHTTP → HandleHttpResponse`, where `InvokeHTTP` forwards `${http.request.uri}` unchanged so one flow fronts every endpoint on the model server with no per-endpoint branching. The agent does not know what a model is. It accepts a POST, forwards it, and hands the response straight back. The value is the flow, the enrollment, and the transport, not the inference.

**Why MiNiFi Java, not C++, for this.** MiNiFi C++'s `ListenHTTP` has no synchronous request/response pair. The caller always gets an empty `200` ack, and the answer has to come back out-of-band over Kafka keyed on a client-supplied `request_id`. MiNiFi **Java** ships `HandleHttpRequest`/`HandleHttpResponse`, a synchronous response with no Kafka detour. For an HTTP-fronted inference proxy that is the decisive difference. Two settings on that flow are load-bearing and default wrong. `InvokeHTTP`'s `HTTP Method` silently stays `GET` (set it to `POST`), and its `Socket Read Timeout` defaults to `15 secs` while LLM inference takes 10 to 25 seconds or more (raise it to match your slowest endpoint, or every call fails with a silent `SocketTimeoutException`).

The complete build of this pattern, with the exact processor settings, error routing, all five endpoints, and the multipart reassembly the transcription endpoint needs, is [Chapter 17](ch17-edge-ai-router.md).

## `ExecuteScript`: Inline Python on the Agent

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

POST a payload and the attribute appears on `LogAttribute`. That is the extension loading and executing. The property that makes `ExecuteScript` useful for iteration is that a running C++ agent **re-reads the script from disk on every trigger**. Edit the script, POST again, the new logic runs. No restart, no republish.

In EFM Designer flows the C++ FQCN is `org.apache.nifi.minifi.processors.ExecuteScript`. Note `minifi` in the path. It is not the Java NiFi `org.apache.nifi.processors.standard.ExecuteScript`.

### `ExecuteScript` Availability

`ExecuteScript` is **not in any stock Cloudera binary**. Not the C++ image, not the CEM Java tarball, not the default Windows MSI feature set. The tell is `Could not instantiate: PythonScriptExecutor` repeating every 30s in `minifi-app.log`, or an EFM Designer "not a valid Processor type" rejection.

| Runtime | Path to the engine | Python engine? |
|---|---|---|
| C++ Kubernetes pod | Extra-extensions injection (`MINIFI_EXTRA_EXTENSIONS_COMMA_SEPARATED`) | Yes, on arm64 K8s pods and the Jetson |
| C++ source build | Include the Python extension at build time | Yes |
| Java CEM tarball | Drop the scripting NAR into `lib/` | **No**. CEM `2.24.08.0-19` gives Groovy/Clojure only, no Python |
| Windows MSI | `ADDLOCAL=ALL` during install | Yes (C++ MSI only) |

Python `ExecuteScript` at the edge means a **C++ agent**, not the Java agent. See [Chapter 5](ch05-executescript-availability.md) for the full build-by-build breakdown.

### Getting Scripts onto the Agent

Two mechanisms, and the choice is the same one you make for every resource at the edge.

- **EFM Resource Manager API** is the tracked, restart-durable path. `POST /efm/api/resource-manager/resources/file` to upload, then `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` with the exact body `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[...]}` (a bare array is silently swallowed). This path needs the `efm-resources` PVC, or the uploaded bytes die with the pod while the DB row survives pointing at nothing.
- **Raw `kubectl cp`** onto the agent's script path takes effect on the next trigger, bypasses EFM tracking, and does not survive a pod restart. Correct for fast iteration. Not the production path.

### Windows Session 0 Caveat

An `ExecuteScript` that shells out to launch a GUI window runs green (`200`, attributes set, the target process even spawns), but the window never appears. A default `LocalSystem` Windows service lives in Session 0 with no interactive desktop. Run the agent in process-mode (Session 1) for anything that must show up on screen.

## Custom Python Processors: Author a New Edge Processor Type

When the logic is worth keeping, write it as a processor. A custom Python processor is a new type. It appears in the agent's manifest under its own name, with its own properties and relationships, and wires into a flow like any stock processor. The same four rules from [Chapter 15](ch15-how-to-ai-with-nifi-and-python.md) apply. You own the framework skeleton, the AI writes the logic inside it.

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

Drop the `.py` into the agent's configured processor directory (`nifi.python.processor.dir`, default `${MINIFI_HOME}/minifi-python/`; authored processors go in the sibling `nifi_python_processors/` package) and restart. The agent's `PythonCreator` scans the directory once at boot and registers the type under its own FQCN. `org.apache.nifi.minifi.processors.nifi_python_processors.EdgeTagger` appears in `GET /efm/api/agent-manifests/{id}` with the exact text from `ProcessorDetails.description` in the `typeDescription` field, which tells you the authored `describe()` ran and not a placeholder. From there it wires into an EFM Designer flow (`ListenHTTP → EdgeTagger → LogAttribute`) exactly like a stock processor.

> **⚠️ A custom processor is not a hot patch.** `PythonCreator` scans at boot. A `.py` dropped in or edited after the agent is running is not picked up until the agent restarts. This is the sharp difference from `ExecuteScript`, which re-reads every trigger.

### `ExecuteScript` vs Custom Processor: Pick One

| | `ExecuteScript` (Python engine) | Custom Python processor |
|---|---|---|
| What it is | One generic processor; paste a script body | A new processor type you author in Python |
| Identity in the flow | Always shows as `ExecuteScript` | Shows under its own name |
| Reload | Re-reads script every trigger. Hot-edit, no restart | `PythonCreator` scans at boot. Needs agent restart |
| Best for | Fast iteration, per-box one-offs | Reusable, stable edge capability |

Conflating these two is the most common mistake. They are different processors with different reload behavior. Use `ExecuteScript` while you are iterating. Author a processor once the capability is stable.

## Building the Flows: The Skill, the API, and Using AI to Test

Everything above gets published to agents the same way, and the method deserves stating once because it is what makes AI-assisted edge work fast and not frustrating.

**Use the `nifi-and-ai` skill, not raw recall.** [Chapter 14](ch14-nifi-and-ai-skill-efm-portion.md) is the skill's EFM machinery, and the skill itself is public. `git clone https://github.com/cldr-steven-matison/NiFiandAi ~/.claude/skills/nifi-and-ai` installs it. It carries the flow-build API shapes, the manifest-mapping rules, and the silent-drop failure catalog so an AI building these flows works from the known contract and does not hallucinate one.

**Build flows component by component. There is no whole-flow PUT.** `PUT /efm/api/designer/flows/{flowId}` returns `405`. The loop is always the same:

```bash
POST /efm/api/designer/flows/{flowId}/process-groups/{pgId}/processors   # one per processor
POST /efm/api/designer/flows/{flowId}/connections                        # one per connection
GET  /efm/api/designer/flows/{flowId}/validate                           # must return "validationErrors": []
POST /efm/api/designer/flows/{flowId}/publish
```

The Designer validates against the agent class to manifest mapping, not against whatever agent happens to be online. New processors do not get their relationships auto-terminated (an unterminated `failure`/`unmatched` blocks `/publish` with a `409`), and a single orphaned processor anywhere on the canvas blocks the whole publish.

**Test locally before you trust it.** The agent is an HTTP endpoint. POST a payload to `ListenHTTP`/`HandleHttpRequest` and read what comes out of `LogAttribute` or `PutFile`. AI-authored logic earns trust here. Show the framework loads (an empty pass-through), then add the logic, then send the request you care about. Never publish an AI-generated processor straight to a live agent. Run the skeleton first.

**Deliver resources the way that matches the job.** Restart-durable delivery is the EFM Resource Manager (needs the `efm-resources` PVC). Fast-iteration delivery is `kubectl cp` (gone on the next pod restart). Pick per whether you are shipping or still iterating, the same split as scripts above.

## Let the AI Drive the Flow: MCP Servers

Everything in this chapter so far treats the AI as the thing inside the flow. There is a second seat. The AI can also be the operator of the flow, reading live state and building components through the same APIs a person uses, and the Model Context Protocol is how it gets there. An MCP server is a thin process that wraps an API you already own and exposes it as named tools an assistant (Claude Code, Claude Desktop, LangChain, Agent Studio) can call. The assistant stops guessing from stale docs and starts asking the running system.

Cloudera ships one for NiFi. The [NiFi MCP Server](https://github.com/cloudera/NiFi-MCP-Server) wraps the NiFi REST API with automatic 1.x/2.x detection and Knox authentication, and exposes 24 read-only tools out of the box (`list_processors`, `get_processor_details`, `check_connection_queue`, `get_bulletins`, `get_flow_health_status`, `search_flow`, and the rest). Set `NIFI_READONLY=false` and another 42 write tools switch on, enough to create processors, connections, controller services and parameter contexts and to start or stop them. It installs the same way as the other Cloudera MCP servers ([Iceberg](https://github.com/cloudera/iceberg-mcp-server), [AI Workbench](https://github.com/cloudera/CAI_Workbench_MCP_Server), [Data Visualization](https://github.com/cloudera/CDV-MCP-Server)). `uvx` pulls it straight from the git source, the transport is stdio, and the credentials ride in environment variables. With it wired into Claude Code, "show me every queue over 10,000 FlowFiles on the NiFi side of the edge router" is one tool call instead of five API reads.

### The Gap at the Edge

Point the same assistant at EFM and there is nothing to call. Every surface this chapter used by hand is a plain REST endpoint under `/efm/api/`, and none of it has an MCP server yet.

| EFM surface | What an agent would ask it |
|---|---|
| `/efm/api/agent-classes`, `/efm/api/agents` | Which classes exist, which agents are enrolled, when each last heartbeated |
| `/efm/api/agent-manifests/{id}` | Which processor types a class can run, so a flow is built against the manifest and not a guess |
| `/efm/api/designer/flows/{flowId}/...` | Read the live canvas, add a processor, add a connection, validate, publish |
| `/efm/api/resource-manager/...` | Which scripts and assets are assigned to a class |

MiNiFi itself has no REST API to wrap. The agent talks to EFM over C2 heartbeats and its state lives in EFM, so a server over EFM covers the agents too. The read side alone would answer most of the questions that send you into `minifi-app.log` today. "Which agents in this class have not heartbeated in an hour" is one read of `/agents` and a timestamp. "Does this class's manifest contain `ExecuteScript`" is one read of the manifest, and it is the check the Availability section above tells you to do by hand. On the write side, the component-by-component build loop from the previous section (one POST per processor, one per connection, validate, publish) is already the shape an MCP tool set wants. Each step is a single narrow call with a checkable result.

### Build It the Way the Cloudera Manager One Was Built

The pattern exists. The [Cloudera Manager MCP Server](https://github.com/cldr-steven-matison/cloudera-manager-mcp-server) wraps the CM, YARN, Ranger and Atlas REST APIs on FastMCP, every tool GET-only, one `*_BASE_URL` per surface so an estate with only one of them exposes only that surface's tools, and it runs under `uvx` from the git URL with no clone. An EFM server is the same build with different endpoints.

What it needs beyond the CM one is a single decision, whether to expose the write side of the Designer at all. Start read-only. A tool that publishes a flow to a class pushes it onto every agent in that class on their next heartbeat, and the Traps section below lists the ways a bad publish goes silent. Add `efm_publish_flow` last, behind a flag that defaults off, after the read tools have earned trust the same way the custom processors did. That server is the next thing to build in this series, and it is where the edge-AI story turns from "the agent does AI work" to "the AI runs the agents."

## Traps: The Ones That Drop Data Silently

These error nowhere. They quietly do nothing, which is worse.

- **`ListenHTTP` `Batch Size`/`Buffer Size` default to `5/5`.** A single request never fills the buffer and is dropped with `buffer is NOT full 1/5`. Set both to `1` (the MINIFICPP-2243 off-by-one). First thing to check when an edge flow "does nothing."
- **`InvokeHTTP`'s `HTTP Method` silently stays `GET`.** Even when you meant `POST`. A bodyless GET is a common "why did the model get nothing" cause.
- **`Socket Read Timeout` defaults to `15 secs`.** LLM inference takes 10 to 25 seconds or more. Silent `SocketTimeoutException`, nothing returned, caller hangs.
- **Kafka bootstrap, external NodePort vs in-cluster port.** From outside the cluster (an edge agent over Tailscale) it is the NodePort, not the internal `:9092`. On Strimzi, per-broker `advertisedHost` must be a reachable hostname or brokers hand clients a raw LAN IP they cannot route to.
- **`Retry` is not `Failure`.** Auto-terminating `InvokeHTTP`'s `Retry` relationship silently drops every transient 5xx/429. Self-loop `Retry` with a bounded `FlowFile Expiration`, route `Failure` to a log processor.
- **Never GET-then-PUT a processor with sensitive properties.** EFM/NiFi returns `********` on read. PUT it back and you write that literal over the credential. Bind secrets to a Parameter Context or use a narrow-scope endpoint.
- **Live flow is truth.** Before editing a running agent's flow, pull what is there (`GET /efm/api/designer/flows/{id}`, or dump `config.yml`). The running canvas has drifted from your notes more often than not.

## What NOT to Do

- Don't wait on the `ListenHTTP` response for model output when using C++. MiNiFi C++ is fire-and-forget. The answer comes back on Kafka keyed by `request_id`. Use MiNiFi Java (`HandleHttpRequest`/`HandleHttpResponse`) for synchronous inference proxying.
- Don't conflate `ExecuteScript` with a custom Python processor. One hot-reloads every trigger. The other needs a restart. Reaching for the wrong one makes your iteration loop fight you.
- Don't expect `ExecuteScript` to exist in a stock agent. Getting the Python engine onto the agent is an install problem. See Ch2 and Ch5 first.
- Don't publish AI-generated logic straight to a live agent. Run the empty skeleton, then add logic, then send a live request.
- Don't `PUT` a whole flow to the Designer. There is no whole-flow PUT (`405`). Build component by component.
- Don't publish onto a class whose manifest does not match the agent's runtime. C++ FQCNs on a Java-mapped class get rejected.
- Don't leave `ListenHTTP` at `5/5` or `InvokeHTTP` at `GET`. Those two defaults drop or neuter more edge flows than anything else.
- Don't hand an assistant a write-enabled MCP server over EFM before its read tools have been in use. A published flow lands on every agent in the class on the next heartbeat, and nothing in the tool call tells you which agents it broke.

## Related Chapters

- [The NiFi and AI Skill — EFM Portion](ch14-nifi-and-ai-skill-efm-portion.md) (Ch14): building these flows through the `nifi-and-ai` skill and not by hand.
- [How to AI with NiFi and Python](ch15-how-to-ai-with-nifi-and-python.md) (Ch15): the four rules for AI-authored processors, in full, on the NiFi side.
- [Edge-AI Router Case Study — StarlinkAI](ch17-edge-ai-router.md) (Ch17): the complete router build. Tailscale, Lemonade, the live flow, error routing, and the transcription reassembly branch.
- [ExecuteScript Availability](ch05-executescript-availability.md) (Ch5): which runtimes ship the Python engine, build by build.
- [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md) (Ch19): on-device model execution via TensorRT / llama.cpp.
