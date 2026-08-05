# Chapter 17: Edge-AI Router Case Study — StarlinkAI

Chapter 16 introduced the four AI-at-edge options and used the StarlinkAI router as its canonical
"route to a nearby inference server" example. This chapter is the deep dive behind that summary: one
real node, end to end — the hardware, why it runs the stack it does, how it joins the array, the
exact router flow, and the one endpoint that needed real engineering rather than pass-through
plumbing. Where Chapter 16 states the shape, this chapter is the field record of building it,
including the two bugs behind it.

> **⚠️ Read Chapter 16 first for the generalized patterns.** The "why MiNiFi Java, not C++"
> reasoning, the EFM Designer write contract (no whole-flow PUT), and the edge traps are covered
> there and only summarized here. This chapter is the case study; Chapter 16 is the playbook.

Everything below runs on the live node against EFM `2.3.1.0-2` and the MiNiFi **Java** agent
`2.24.08.0-19`.

---

## The node and its role

StarlinkAI is the third inference node in the array (alongside WindowsDesktop and the Mac), hostname
`TunaStarlink`, a Beelink SER9 MAX (H260):

| | |
|---|---|
| CPU | AMD Ryzen 7 260 — 8C/16T, 3.8 GHz base |
| GPU | Radeon 780M iGPU (RDNA3, 12 CUs) — **no NPU** |
| RAM | 64 GB |
| Network | Starlink uplink; the Windows host also runs an OBS/OBSBOT Tiny 3 Twitch stream |

Its job in the array: use the iGPU for local inference and expose it to every other array machine as
an HTTP endpoint over Tailscale, fronted by an EFM/MiNiFi **Java** agent that does **routing only** —
the agent never runs a model, it forwards to the inference server sitting next to it on `localhost`.

**Why Vulkan, not ROCm/vLLM.** This chip has no NPU, and AMD's ROCm does not support this iGPU.
`llamacpp:vulkan` drives the standard GPU driver stack directly — no special driver package, no ROCm
install, no NPU runtime. It is the path that actually offloads to this particular silicon.

**Why MiNiFi Java, not C++.** The decisive reason (detailed in Chapter 16): MiNiFi C++'s `ListenHTTP`
has no synchronous request/response pair — the caller gets an empty ack and the real answer must
return out-of-band over Kafka keyed on a `request_id` — and it silently drops multipart POSTs at its
buffer-full check. MiNiFi Java ships `HandleHttpRequest`/`HandleHttpResponse`, returning a real
response inline with no Kafka detour. For an HTTP-fronted inference proxy that is the whole ballgame.

---

## Architecture

```
Other array machines (over Tailscale)
        │
        ▼
Tailscale (Windows host) — stable tailnet IP for this box
        │
        ▼
EFM / MiNiFi Java agent  (StarlinkAIJava class, Windows-native process)
  - HandleHttpRequest   : single entry point, port 8090, all 5 Lemonade
                          endpoints on one port, distinguished by path
  - InvokeHTTP          : pure reverse-proxy pass-through —
                          HTTP URL = http://localhost:13305${http.request.uri}
  - HandleHttpResponse  : returns Lemonade's real answer synchronously
        │
        ▼
Lemonade Server  (Windows-native, localhost:13305)
  - iGPU inference via llamacpp:vulkan backend
  - OpenAI-compatible API: /v1/chat/completions, /v1/embeddings,
    /v1/reranking, /v1/audio/speech, /v1/audio/transcriptions
```

Three processors, one port, no Kafka, no `request_id` correlation — the caller gets Lemonade's real
response directly and synchronously. Everything in the serving path runs **natively on Windows** —
no containers, no WSL2 (WSL2 on this box is only used for repo/doc access).

![HandleHttpRequest-Lemonade → InvokeHTTP-Lemonade → HandleHttpResponse-Lemonade, live per-processor throughput in the EFM Flow Designer](assets/images/efm-starlink-ai-unified-lemonade-flow.png)

The deployed router in the EFM Flow Designer with monitoring active — real per-processor throughput
(In / Read-Write / Out / Tasks) on the three-processor primary path, plus an error-observability
branch off `InvokeHTTP`'s `Failure`/`Retry`/`No Retry`/`Original` relationships.

---

## Standing it up

### 1. Tailscale (Windows host)

```powershell
winget install tailscale.tailscale
tailscale up
```

`tailscale up` opens an interactive browser auth — run it by hand and join the same tailnet as the
rest of the array. This box gets a stable tailnet IP; the EFM host (WindowsDesktop, hostname
`MINI-Gaming-G1`) tailnet IP is the `baseUrl` target for the agent-deployer call below.

### 2. Lemonade Server (Windows host)

```powershell
winget install --id AMD.LemonadeServer --silent --accept-package-agreements --accept-source-agreements
lemonade backends install llamacpp:vulkan
```

Five models loaded, one concurrent per category:

| Category | Model |
|---|---|
| Chat | `Qwen3-4B-GGUF` |
| Embeddings | `Qwen3-Embedding-0.6B-GGUF` |
| Reranking | `jina-reranker-v1-tiny-en-GGUF` |
| Transcription | `Whisper-Large-v3-Turbo` |
| TTS | `kokoro-v1` (`device: cpu` — the backend installed only as `kokoro:cpu`) |

Manage with `lemonade list` / `lemonade pull <model>`. Once a model is loaded, confirm Vulkan GPU
offload is actually active — `GET /api/v1/health` should return `"device": "gpu"`, not a silent CPU
fallback.

### 3. JDK + MiNiFi Java agent (router only)

```powershell
winget install Microsoft.OpenJDK.21
$env:JAVA_HOME = 'C:\Program Files\Microsoft\jdk-21.0.12.8-hotspot'
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
```

Deploy via the EFM agent-deployer script, targeting the **Java** agent type and a **dedicated
class** (`StarlinkAIJava`, kept separate from any C++ class so the two never share a canvas — see
Chapter 16's "EFM Designer write contract" on why mixed runtimes stay parallel classes):

```powershell
Invoke-WebRequest -Uri 'http://<EFM_HOST>:10090/efm/api/agent-deployer/script' -Method Post -Body @{
  agentClass   = 'StarlinkAIJava'
  agentType    = 'java'
  agentVersion = '2.24.08.0-19'
  osArch       = 'windows'
  baseUrl      = 'http://<EFM_HOST>:10090/efm/api'
  hbPeriod     = '5000'
} -OutFile deploy.ps1
.\deploy.ps1
```

The agent lands at `~\minifi-java\minifi-2.24.08.0-19\` and runs as a plain background process via
`bin\run-minifi.bat` — no Windows-service install needed on this box. A fresh class picks up the Java
manifest automatically on first heartbeat; no manual class-manifest re-pointing.

---

## The router flow

Built via the EFM Designer's per-component API (`POST .../processors`, `POST .../connections`,
`GET .../validate`, `POST .../publish` — there is no whole-flow `PUT`; Chapter 16 covers the contract).
The three processors that carry the traffic:

**`HandleHttpRequest-Lemonade`**

| Property | Value |
|---|---|
| Listening Port | `8090` |
| HTTP Context Map | `StandardHttpContextMap` (shared controller service) |
| Allowed Paths | unset — accepts any path, distinguished downstream by `${http.request.uri}` |

**`InvokeHTTP-Lemonade`**

| Property | Value |
|---|---|
| HTTP URL | `http://localhost:13305${http.request.uri}` — pure pass-through, no per-endpoint branching |
| HTTP Method | `POST` |
| Request Content-Type | `${mime.type}` — forwards the client's real content type; JSON and single-part binary bodies pass through unchanged |
| Request Body Enabled | `true` |
| **Socket Read Timeout / Socket Write Timeout** | **`10 mins`** |
| Connection Timeout | `30 secs` |

> **⚠️ The 10-minute read/write timeout is load-bearing, not cosmetic.** LLM inference routinely
> takes 10–25s+; the framework default (`15 secs`) fails every real call with a
> `SocketTimeoutException` that auto-terminates on `Failure` with nothing routed back — the client
> just sits until `StandardHttpContextMap`'s own 60s expiration gives up with a generic 503. Match
> this to the slowest endpoint on the box, not the framework default.

**`HandleHttpResponse-Lemonade`**

| Property | Value |
|---|---|
| HTTP Status Code | `${invokehttp.status.code:replaceEmpty('502')}` |
| HTTP Context Map | same shared `StandardHttpContextMap` |

### Error routing — the flowVersion 23 fix

The first working flow wired only `InvokeHTTP[success/Response] → HandleHttpResponse`, so anything
that wasn't a clean 2xx (a 404, a 500, a connection failure) never got answered — the caller hung for
the full 60s context-map expiration. Fixed at flowVersion 23:

```text
HandleHttpRequest[success] → InvokeHTTP[success/Response] → HandleHttpResponse
InvokeHTTP[Retry]          → HandleHttpResponse   (also → LogAttribute-Error)
InvokeHTTP[No Retry]       → HandleHttpResponse   (also → LogAttribute-Error)
InvokeHTTP[Failure]        → HandleHttpResponse   (also → LogAttribute-Error)
InvokeHTTP[Original]       → LogAttribute-Error only
                             (NOT to HandleHttpResponse — wiring Original in too
                              delivers a second FlowFile to the same HTTP context
                              and double-responds)
```

`HandleHttpResponse-Lemonade`'s status code moved from a hardcoded `"200"` to
`${invokehttp.status.code:replaceEmpty('502')}` in the same change, so the caller sees the **real**
upstream status on every outcome: 2xx via `Response`, the real 4xx/5xx via `Retry`/`No Retry`, or
`502` via `Failure` when no upstream response came back at all (that relationship carries no
`invokehttp.status.code` attribute). `Original` — the pass-through duplicate that always fires
alongside whichever real outcome relationship fires — stays deliberately unconnected to
`HandleHttpResponse`; wiring it in would double-respond to the same HTTP context.

Verified live: a GET-turned-POST health probe (`curl http://localhost:8090/api/v1/health`) now
returns a real `404` in well under a second instead of hanging for the 60s expiration.

---

## Endpoints

All five Lemonade services on one port and one flow — the path the client POSTs to is forwarded
verbatim to Lemonade:

| Service | Path | Confirmed |
|---|---|---|
| Chat | `/api/v1/chat/completions` | Yes — real client, real content, real synchronous answer (`200`, ~12–37s by response length) |
| Embeddings | `/api/v1/embeddings` | Yes — real 200, real embedding vector (`Qwen3-Embedding-0.6B-GGUF`), ~0.2s |
| Reranking | `/api/v1/reranking` | Yes — real 200, real relevance scores, correctly ranked the on-topic document highest, ~2.5s |
| Speech (TTS) | `/api/v1/audio/speech` | Yes — real 200, real Kokoro MP3 (valid ID3/MPEG, 78 KB), ~7s |
| Transcription | `/api/v1/audio/transcriptions` | **Yes** — real 200, real transcript; needs the reassembly branch below |

```powershell
# Chat — use --data @file.json, not inline -d '{...}': PowerShell/curl.exe has silently
# stripped quotes out of inline JSON on this box.
curl.exe -X POST http://localhost:8090/api/v1/chat/completions `
  -H 'Content-Type: application/json' `
  --data '@chat_body.json'
```

---

## The transcription multipart reassembly fix

Four of the five endpoints are pure pass-through — same three processors, same code path, only the
URL differs. Transcription was the holdout, and it is the reason this node earned its own chapter.

**The bug.** `HandleHttpRequest` splits a multipart request into **one FlowFile per form field**
(confirmed in `minifi-app.log`: `http.multipart.fragments.total.number: 2` — one fragment for
`model`, one for `file`). Each fragment is then forwarded to `InvokeHTTP` **independently**, as its
own request, still carrying the *original* multipart `Content-Type` header
(`multipart/form-data; boundary=...`) but a body that is only that one fragment's raw bytes — never
valid multipart. Lemonade correctly rejected it:
`invokehttp.response.body: {"error":{"message":"Bad request","type":"bad_request"}}`. A control probe
sending the identical multipart POST straight to Lemonade on `:13305` returned a real `200` and a
real transcript — proving the request and the model were fine all along; only the MiNiFi
pass-through leg was broken.

**The per-fragment attributes** `HandleHttpRequest` actually sets (read from `minifi-app.log`, not
guessed — a `curl.exe -F model=... -F file=...` probe diffed against the log):

| Attribute | `model` fragment | `file` fragment |
|---|---|---|
| `http.context.identifier` | `016f4c23-…` | **same value** — the correlation key across fragments of one request |
| `http.multipart.fragments.sequence.number` | `1` | `2` (**1-indexed**) |
| `http.multipart.fragments.total.number` | `2` | `2` |
| `http.multipart.name` | `model` | `file` |
| `http.multipart.filename` | *(absent)* | `test-audio.wav` |
| `http.multipart.content.type` | *(absent — note the dot, not a hyphen)* | `audio/wav` |
| `http.headers.multipart.Content-Disposition` | `form-data; name="model"` | `form-data; name="file"; filename="test-audio.wav"` |
| `http.headers.multipart.Content-Type` | *(absent)* | `audio/wav` |

`http.headers.multipart.Content-Disposition` / `.Content-Type` carry the **original raw per-part
header text** — reuse them directly instead of hand-reconstructing the header from
`http.multipart.name`/`.filename`, which sidesteps the conditional-filename expression-language
problem entirely.

**The reassembly chain**, built ahead of `InvokeHTTP` on a separate port (`:8095`) before wiring it
into the live flow:

| Processor | Key config |
|---|---|
| `HandleHttpRequest-TranscriptionTest` | Listening Port `8095`, POST only |
| `UpdateAttribute-FragmentKeys` | `fragment.identifier`=`${http.context.identifier}`, `fragment.count`=`${http.multipart.fragments.total.number}`, `fragment.index`=`${http.multipart.fragments.sequence.number:minus(1)}` (**0-indexed** — `MergeContent` Defragment requires `fragment.index` in `0 … count-1`, one off from the 1-indexed `sequence.number`) |
| `RouteOnAttribute-HasContentType` | `hasType` = `${'http.multipart.content.type':isEmpty():not()}` |
| `ReplaceText-PrependPartHeaderWithType` | Prepend. Header text goes in **`Replacement Value`, not `Text to Prepend`** (gotcha below): `--ClaudeStarlinkBoundary7f3a2b91\r\nContent-Disposition: ${'http.headers.multipart.Content-Disposition'}\r\nContent-Type: ${'http.headers.multipart.Content-Type'}\r\n\r\n` |
| `ReplaceText-PrependPartHeaderNoType` | same, in `Replacement Value`, without the `Content-Type:` line |
| `MergeContent-Multipart` | Merge Strategy `Defragment`, Delimiter Strategy `Text`, Demarcator `\r\n`, Footer `\r\n--ClaudeStarlinkBoundary7f3a2b91--\r\n`, `original` auto-terminated |
| `UpdateAttribute-SetMultipartContentType` | `Content-Type` = `multipart/form-data; boundary=ClaudeStarlinkBoundary7f3a2b91` |
| `InvokeHTTP-TranscriptionTest` | same as prod (10-min timeouts) except `Request Content-Type` = `${Content-Type}`, not `${mime.type}` |
| `HandleHttpResponse-TranscriptionTest` | `HTTP Status Code` = `${invokehttp.status.code:replaceEmpty('502')}` |

`Delimiter Strategy: Text` lets `MergeContent`'s Demarcator/Footer be literal property values (the
`Filename` mode reads a file on disk — not needed here).

> **⚠️ Two gotchas to know before you build this — both traced against `:8095` in isolation.**
>
> 1. **`ReplaceText` prepends `Replacement Value`, not `Text to Prepend`** (on this
>    `minifi-standard-nar 2.24.08.0-19` build with `Replacement Strategy = Prepend`). The real
>    boundary/header text was sitting in the intuitively-correct `Text to Prepend` field while
>    `Replacement Value` kept its literal default `$1` — so the rebuilt body came out missing its
>    opening boundary with every part starting with a literal `$1`. Moving the text into
>    `Replacement Value` on both `ReplaceText` processors fixed it.
> 2. **MiNiFi's `InvokeHTTP` does not replace FlowFile content with the HTTP response body on a
>    non-2xx** — the `Response` relationship's content stays the original *outgoing* request bytes.
>    Reproduced against production with a deliberately bad chat request, so it's router-wide, not
>    branch-specific; it just never surfaced because earlier testing only exercised the success path.
>    Useful side effect: it let me read the exact bytes MiNiFi sent to Lemonade — which is how gotcha
>    #1 was found.

**Isolated test:** a real `curl` against `:8095` returns `200`, `{"text":" .\n"}` — a genuine
Whisper response (the test WAV is a pure 1s tone, not speech, so minimal text is expected; the round
trip is the point). Note the repo's original `test-audio.wav` is an 18-byte placeholder
(`RIFF….WAVEtest`) — generate a real 1s tone to test.

**Wiring it into the live flow:** a `RouteOnAttribute-HasFragments` gate
(`hasFragments` = `${http.multipart.fragments.total.number:isEmpty():not()}`) was inserted between
`HandleHttpRequest-Lemonade` and `InvokeHTTP-Lemonade`. Multipart requests fork into the
reassembly branch; everything else (`unmatched` — chat/embeddings/reranking/speech, none of which
carry multipart fragment attributes) continues straight to `InvokeHTTP-Lemonade`, unchanged. No
new response-side wiring was needed: both `HandleHttpResponse` processors share the same
`StandardHttpContextMap`, and NiFi correlates the reply to the original caller via
`http.context.identifier` — not by which `HandleHttpResponse` instance fires — so a request that
arrives on `:8090` is answered correctly even when it routes through the `:8095`-branch's response
processor.

**On the live `:8090` flow:**

```powershell
curl.exe -X POST http://localhost:8090/api/v1/audio/transcriptions `
  -F "model=Whisper-Large-v3-Turbo" -F "file=@test-audio.wav"
# → 200, {"text":" .\n"}
```

Re-test chat/embeddings/reranking/speech after adding the gate — inserting a `RouteOnAttribute`
ahead of the shared `InvokeHTTP` is a real wiring change to their path even though their configs
don't change. All five Lemonade endpoints round-trip real data through `:8090`.

One caveat: this router has been exercised with local `curl` against `:8090`. A cross-Tailscale call
from a second array machine follows the same path but hasn't been run here yet.

---

## Related chapters

- Ch16 — [How to AI with MiNiFi](ch16-how-to-ai-with-minifi.md): the generalized edge-AI playbook
  this case study instantiates — the four options, why MiNiFi Java over C++, and the EFM Designer
  write contract.
- Ch2 — [EFM Binaries](ch02-efm-binaries.md): staging the agent binaries and the deployer the setup
  steps rely on.
- Ch19 — [EFM + NVIDIA Jetson use case](ch19-efm-and-nvidia-jetson.md): the on-device
  model-execution counterpart to this route-to-a-server case study.
</content>
