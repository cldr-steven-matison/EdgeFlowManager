# Chapter 12: EFM and MicroFi

MicroFi is a clean-room, from-scratch reimplementation of the MiNiFi C2 protocol contracts — FlowFile semantics, C2 heartbeat/ack, flow-definition apply — written in C++ against ESP-IDF for the Seeed XIAO ESP32-S3. It is **not** a fork of `nifi-minifi-cpp`, and it does not behave like one once you're inside it. This chapter is the field record of turning a private research repo (`Christopheraburns/MicroFi`) into a real EFM agent class running real processors on real hardware: chip identification, the hardware/flash-size trap that ate most of a session, EFM enrollment and the implicit-ack question, building three new processors into a compile-time registry, two real engine bugs found by running actual flows against actual hardware, and a hard architectural answer to "can this run Python."

Everything in this chapter ran on real hardware — a single physical XIAO ESP32-S3 **Sense** unit, MAC `e0:72:a1:fb:fd:04` — moved between `StarlinkAI` and `WindowsDesktop` over the course of the work. Where SparkPlug B payload decoding and the JSON-telemetry-to-Kafka pipeline are concerned, that content lives in [Chapter 13 — EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md) and [Chapter 20 — SparkPlug Demo](ch20-sparkplug-demo.md). This chapter owns the EFM/MicroFi-agent side only: getting a device to enroll, verifying the manifest, pushing flows, and extending the firmware itself.

## Scope — read this first

**This chapter is about MicroFi, the custom C2 agent.** It is not about MiNiFi C++, and the two should not be conflated:

| | MiNiFi C++ (the real agent) | MicroFi (this chapter) |
|---|---|---|
| Binary size / RAM | ~3.2 MB idling at ~5 MB RAM — Raspberry-Pi-class | Fits a 2 MB ESP32 flash budget |
| Processor loading | `dlopen` plugin loading at runtime | Compile-time static registry, resolved by name |
| Storage | Heap-centric, RocksDB repositories | LittleFS with watermark eviction |
| Python | Full CPython via `libminifi-python-script-extension.so` | None — architecturally impossible, see below |
| Target | Linux/ARM64, Jetson-class, k8s pods | Seeed XIAO ESP32-S3 (and C3), microcontroller-class |
| Processors available (this work) | Dozens, catalog in [Chapter 3](ch03-cpp-processor-catalog.md) | 5, built by hand in this chapter |

The rationale for building something new rather than porting MiNiFi C++ down is in the repo's own `docs/MICROFI_ASSESSMENT.md`: MiNiFi C++'s design — heap-centric, RocksDB repositories, `dlopen` plugin loading — doesn't shrink to a microcontroller. So MicroFi resolves processors by name against a **compile-time static registry** instead. The property names are deliberately kept MiNiFi-C++-compatible (`generate_flowfile.cpp` declares `File Size` / `Batch Size` / `Data Format`; `log_attribute.cpp` declares `Log Level` / `Log Payload` / `Log prefix` / `Attributes to Log`) so an EFM flow definition written against MiNiFi C++ resolves against MicroFi's registry unchanged. That compatibility bet is the thing every processor built in this chapter had to respect.

The repo's own `docs/Processor-Inventory-And-Roadmap.md` lists 48 proposed processors, including a WiFi-CSI sensing cluster (`GetWiFiCSI`, `WindowCSI`, `DetectMotionCSI`, `RunBistaticPair`) that is the project's actual research thesis. **None of that is built.** At the start of this work, exactly two processors existed: `GenerateFlowFile` and `LogAttribute`. Treat the roadmap as a plan, not an inventory.

## Access

`MicroFi` is private, readable as `steven-matison`:

```bash
gh api repos/Christopheraburns/MicroFi -q '.full_name, .private, .permissions'
# Christopheraburns/MicroFi
# true
# {"admin":false,"maintain":false,"pull":true,"push":true,"triage":true}
```

The token has `push`, not just `pull` — real, but not used against the upstream repo. All dev work in this chapter lives on a fork, `steven-matison/MicroFi`, with `origin` on the fork and `upstream`/`fork` remotes pointed appropriately depending on the host. `Christopheraburns/MicroFi` stayed read-only throughout — no PR opened upstream ("when the time is right," per direction received mid-project).

## The hardware problem — the XIAO's flash size is not what the docs assume

MicroFi ships three PlatformIO build environments:

| Env | Board | Flash | LittleFS | PSRAM |
|---|---|---|---|---|
| `esp32s3` (default) | Lonely Binary ESP32-S3 N16R8 | 16 MB | ~11.5 MB | 8 MB OPI |
| `esp32s3-4mb` | Generic S3 (DevKitC-1 / `esp32s3box`) | 4 MB | ~2.4 MB | — |
| `esp32-c3` | ESP32-C3 DevKitM-1 | 4 MB | none | — |

The chip's USB VID (`303a:1001`, "USB JTAG/serial debug unit," Espressif) does **not** discriminate S3 from C3 from C6 — that's the whole native Espressif USB-JTAG signature across the family. `chip-id` (or the now-deprecated `chip_id`) has to run before anything else is decidable:

```bash
python -m esptool --port COM5 chip-id
```

```
Chip type:          ESP32-S3 (QFN56) (revision v0.2)
Features:           Wi-Fi, BT 5 (LE), Dual Core + LP Core, 240MHz, Embedded PSRAM 8MB (AP_3v3)
MAC:                e0:72:a1:fb:fd:04
```

8 MB embedded PSRAM confirmed this was an S3, not a C6 (which would have been a hard stop — MicroFi has no environment for it at all). Per the hardware table, the confirmed-S3 unit should build with `esp32s3-4mb` (4 MB layout on 8 MB flash, giving up OTA) rather than the bare `esp32s3` env, which assumes a 16 MB board and would overflow.

That reasoning turned out to be half right. `esp32s3-4mb` built and flashed clean — but the upload step logged:

```
Warning! Flash memory size mismatch detected. Expected 4MB, found 2MB!
```

**This specific XIAO unit's physical flash is 2 MB, not 8 MB.** `partitions_4mb.csv`'s `littlefs` partition is declared `0x1A0000`–`0x400000`, ending exactly at the 4 MB boundary — roughly 2 MB of that declared range doesn't physically exist on this chip. Most SPI NOR flash aliases addresses past the physical chip boundary back to low addresses on a wrap; a write aimed at the "high" end of the declared LittleFS space could land on the bootloader, partition table, or app image instead of failing cleanly. The device booted and mounted LittleFS fine for a read-only check, which does not prove writes into the out-of-bounds region are safe — so the decision at that point was to stop rather than push a flow (which persists to `/littlefs/.flowdef`) onto an unverified partition layout.

**Correction found along the way**: this unit has a camera, so it's the **XIAO ESP32-S3 Sense** variant, not the base board. It also has a microSD slot — small, push-type, on the back of the camera expansion board, easy to miss visually — which means `CONFIG_MICROFI_SD_OVERFLOW` is a real option for this unit, not a dead end, if a card is ever available.

**Fix**: a from-scratch `esp32s3-2mb` PlatformIO environment with a matching `partitions_2mb.csv`, sized to the unit's actual 2 MB flash. Built and flashed clean, no mismatch warning. This is the environment every subsequent build in this chapter uses. It was pushed to `steven-matison/MicroFi`'s `xiao-s3-2mb-partition` branch — no upstream PR opened.

**The lesson generalizes**: neither of MicroFi's two sub-16 MB S3 environments fits every XIAO S3 unit. `chip-id` tells you the silicon family; it does not tell you the physical flash size the board was actually built with. Confirm both, separately, before trusting a shipped partition table.

## EFM + config requirements

The array runs **EFM 2.3.1.0-2**, MiNiFi C++ `1.26.02`. MicroFi's README claims it targets "Cloudera EFM 2.x" and that the ack is *implicit* — EFM 2.x is expected to treat a heartbeat whose `flowInfo.flowId` matches the pushed flow UUID as the acknowledgement, so `CONFIG_MICROFI_C2_ACK_URL` is configured but never POSTed to. **That claim against 2.3.1.0-2 specifically was, at the start of this work, entirely unverified** — the single highest-risk assumption in the whole integration, because if EFM actually waits for an explicit POST to `/efm/api/c2-protocol/acknowledge`, a flow push looks accepted server-side and never completes on the device. (Field-validation Task 7, below, answers this for real.)

Config lives in `sdkconfig.defaults.local`, gitignored, copied from `.example`:

```
CONFIG_MICROFI_WIFI_SSID="..."
CONFIG_MICROFI_WIFI_PASSWORD="..."
CONFIG_MICROFI_C2_HEARTBEAT_URL="http://<efm-host>:10090/efm/api/c2-protocol/heartbeat"
CONFIG_MICROFI_C2_ACK_URL="http://<efm-host>:10090/efm/api/c2-protocol/acknowledge"
```

Those are the same two URLs the CEM/C++ agents already set as `nifi.c2.rest.url` / `nifi.c2.rest.url.ack`, so the endpoint shape was known-good against this EFM before the first boot.

**`CONFIG_MICROFI_AGENT_CLASS` matters more than it looks.** Its real shipped default is `"ESP32"` (not `"default"`, as first assumed from the README — minor doc drift, corrected during the build). Whatever it's set to, **it must not be the same class an existing live agent already uses.** `StarlinkAI`, the host the XIAO was physically plugged into for most of this work, already runs a live, Online MiNiFi agent under an agent class of the same name. If MicroFi registered under that class, an EFM flow push aimed at one device would land on both. `CONFIG_MICROFI_AGENT_CLASS` was set to `MicroFi` — a distinct class — before the first boot, every time, and the existing `StarlinkAI`-class agent was re-checked after every registration to confirm it stayed untouched.

`localhost` in MicroFi's default heartbeat URL cannot work from a real ESP32 — the device needs a routable address for EFM. Two were used across this work, for different reasons covered in the toolchain section below: `efm-host-ip:10090` over Tailscale, and later a LAN-direct `192.168.1.121:10090` when the device joined the same WiFi AP as the Windows host running EFM.

## Toolchain — this runs on Windows, not WSL2

The XIAO was plugged into `StarlinkAI`'s front-facing USB. Claude Code sessions on that host run in WSL2, but the board enumerates on the **Windows** side as a `COM` port (`COM5`, `USB\VID_303A&PID_1001&MI_00`). WSL2 has no native USB passthrough — reaching the device from Ubuntu would mean `usbipd-win` attach-per-boot, a workaround, not the path of least resistance. Every build/flash/monitor command in this chapter is a native-Windows PlatformIO CLI command; WSL2 was used for editing only.

Neither `esptool` nor PlatformIO Core was actually installed on the Windows host at the start (despite an earlier assumption of "VS Code + PlatformIO" as a prerequisite) — closed with:

```powershell
pip install esptool platformio
# esptool 5.3.1, PlatformIO 6.1.19
```

No VS Code needed; every step below is a `pio`/`esptool` CLI command.

**Upload and monitor must be chained in one invocation.** Attaching `monitor` after a separate `upload` misses the boot sequence — the device has already reset and moved on by the time a second command attaches:

```powershell
pio run -e esp32s3-2mb -t upload -t monitor
```

## Field validation — the 8-task run

Eight tasks, run against the real hardware, to confirm the whole chain from chip identification through EFM registration, manifest correctness, the implicit-ack question, and reboot persistence.

### Task 1 — pin the chip

Covered above: `esptool chip-id` confirmed ESP32-S3, 8 MB embedded PSRAM, MAC `e0:72:a1:fb:fd:04`.

### Task 2 — confirm EFM is reachable

First attempt **failed, 4/4 retries** — `Invoke-WebRequest` to `http://100.68.113.126:10090/efm/ui/` timed out from `StarlinkAI`. Tailscale itself was fine (`tailscale ping mini-gaming-g1` returned in ~56ms); the failure was TCP to port 10090 specifically — a flapping `kubectl port-forward` pane for `svc/efm` on `WindowsDesktop`. Per the field doc's own instruction ("if it fails, Tailscale is down or the port-forward pane died — fix that before flashing anything"), this run stopped here: no firmware built, no flash attempted, nothing done blind. Tasks 3–8 all either need EFM to actually confirm registration or would be flashing toward a C2 endpoint that can't be reached.

Once `WindowsDesktop` restarted the flapping port-forwards, EFM was reachable again (4/4 clean `200`s from `StarlinkAI`), and the run resumed.

**Per direction received mid-run, the resumed session went LAN-direct rather than over Tailscale** — the XIAO joined a WiFi network (`ATTyjuHfEi`) on the same subnet as `WindowsDesktop`'s LAN IP (`192.168.1.121`), bypassing Starlink and Tailscale for the device's own path entirely. `sdkconfig.defaults.local` pointed both C2 URLs at `http://192.168.1.121:10090/efm/api/c2-protocol/...`. The WiFi password went directly into the gitignored local file on the Windows host, never through chat.

### Task 3 — build

```powershell
pio run -e esp32s3-4mb
```

Succeeded, but over MicroFi's own stated "under 50% flash" success criterion: **Flash 66.4% (1,044,597 / 1,572,864 bytes), RAM 36.1%.** (This was before the 2 MB partition-mismatch was discovered — see the hardware section above. The eventual `esp32s3-2mb` build came in at 88.6% of the *actual* 2 MB layout's app slot, the number to trust.)

### Task 4 — flash and first heartbeat

```powershell
pio run -e esp32s3-4mb -t upload -t monitor
```

WiFi associated with `ATTyjuHfEi` in ~2.5s (one retry), got `192.168.1.198`. First heartbeat, real:

```
I (7575) microfi.c2: heartbeat #0 -> 200 (sent 5677 bytes, manifest=yes, recv 28 bytes)
```

The full manifest (3840 bytes) went out inline on this first heartbeat, exactly as designed — subsequent heartbeats send only a hash.

### Task 5 — EFM registration

```bash
GET /efm/api/agent-classes
```

listed a new `MicroFi` class with a real manifest id. Confirmed the existing `StarlinkAI` class's agent was present with the same manifest id it had before this test, and its `minifi-app.log` showed no renewed heartbeat failures since the connectivity fix — the live production agent was untouched by the new class registering.

### Task 6 — verify the manifest

```bash
GET /efm/api/agent-manifests/{id}
```

returned exactly `GenerateFlowFile` and `LogAttribute` with their full property descriptors — no more, no less. This confirmed the clean-room registry design bet directly: MicroFi advertises only what's actually compiled in.

### Task 7 — push a flow, test the implicit ack

Built `GenerateFlowFile → LogAttribute` via the EFM Designer's real per-component API. This is where the exact request shapes matter, and where a wrong one produces an unhelpful error:

```bash
GET  .../client-identifier                          # get the write-clientId
POST .../process-groups/{pgId}/processors            # x2
POST .../process-groups/{pgId}/connections
GET  .../validate
POST .../publish
```

The envelope needs `{"revision":{...},"componentConfiguration":{...},"requestId":...}` — **not** a flat body, and not a `component`-wrapped body. A flat or wrong-wrapper body 400s with `"Component details must be specified."`, regardless of which wrapper is actually missing — that message doesn't discriminate the two failure modes, so it's worth remembering as its own trap. Every new component's create-request needs `revision.version: 0` — that field tracks the *component's own* revision line, not the flow's overall version. `LogAttribute` needed `success` explicitly auto-terminated (no downstream consumer). `GET .../validate` returned `{"validationErrors":[]}` before publish.

Published clean (`flowVersion: 1`, `dirty: false`). EFM itself went down mid-test — the same flapping-connectivity shape from Task 2, confirmed unrelated to the publish via both this session's own Tailscale checks and the live `StarlinkAI` agent's own heartbeat timeouts — and came back once restarted. Once stable:

```
GET /efm/api/agents/microfi_1
state: ONLINE
flowId: e9aac4e6-4124-45a6-92d3-ce09505974d1   # boot-default all-zero placeholder, replaced by real UUID
flowUpdateDate matching lastSeen
GenerateFlowFile: running: true
LogAttribute: running: true
```

**With MicroFi never once POSTing to `/acknowledge`** — confirmed absent from every log across the whole session. **This answers the load-bearing question from the config section above: EFM 2.3.1.0-2 does accept the implicit ack.** A heartbeat whose `flowInfo.flowId` matches the published flow is sufficient on its own; no explicit acknowledgement POST is required or expected.

### Task 8 — power-cycle persistence

Two real physical unplug/replugs (a hand on the cable, not a soft reset) both resumed `GenerateFlowFile`/`LogAttribute` cleanly with reset FlowFile counters — genuine reboots. Capturing the exact boot-log line across a real disconnect turned out to be its own tooling problem: `pio device monitor` attached *before* a physical unplug reliably lost everything from the disconnect gap through the reconnect burst, across three separate attempts, even via PlatformIO's own `log2file` filter (which writes to a different file than the redirect — same loss). Root cause not nailed down; not a COM-port renumber (confirmed same `COM5` after reconnect). Reads like an internal buffer that doesn't survive the physical link actually dropping mid-session, as opposed to a tool-triggered reset which keeps the OS-level handle alive throughout.

**Fix that actually worked**: re-run `pio run -t upload -t monitor` instead of relying on a physical unplug for the capture. Re-flashing identical firmware leaves LittleFS untouched (a separate partition), and the upload's own RTS-pin hard-reset is a real full reboot. That caught it cleanly:

```
microfi.flowstore: flow def loaded: 2872 bytes from /littlefs/.flowdef
microfi.flowstore: flow_id loaded: e9aac4e6-4124-45a6-92d3-ce09505974d1
```

Exact match to the published flow's `flowId`. Persistence confirmed for real, not inferred from an absence of errors.

### Summary — all 8 tasks

Chip: XIAO ESP32-S3 **Sense** (camera + microSD, corrected mid-session, not the base board). Actual flash: 2 MB. Custom `esp32s3-2mb`/`partitions_2mb.csv` env built for it. Firmware 1,044,597 bytes (88.6% of the 2 MB layout's app slot). Agent class `MicroFi`, isolated from the live `StarlinkAI` agent throughout. Manifest: exactly `GenerateFlowFile` + `LogAttribute`. Implicit ack: confirmed working on EFM 2.3.1.0-2. Persistence: confirmed working via LittleFS on the corrected partition table.

## Building processors — design specs and build order

Registration alone only ever exercised MicroFi's two built-in processors, and both are synthetic: `GenerateFlowFile` fabricates payload from nothing, `LogAttribute` writes it to the serial log. The flow round-trips entirely inside the device — nothing enters from a real source and nothing leaves the board. That's enough to prove registration, the implicit ack, and persistence. It is not enough to test MicroFi as a data agent, which needs real ingress and real egress — new processors compiled into the static registry.

Two constraints carried over from the validation and bound all of this work:

- Processors are **compile-time-embedded and resolved by name against a static registry** — no `dlopen`, no runtime plugin load. Adding a processor means a rebuild and a reflash, every time.
- Property names must stay **MiNiFi-C++-compatible**, matching the shipped built-ins' naming pattern (`generate_flowfile.cpp` → `File Size`/`Batch Size`/`Data Format`; `log_attribute.cpp` → `Log Level`/`Log Payload`/`Log prefix`/`Attributes to Log`, all Title Case) so an EFM flow definition written against MiNiFi C++ resolves against MicroFi unchanged.

Desk-eval spec, written before any code, against the pinned `nifi-minifi-cpp` `PROCESSORS.md` property lists — with the explicit caveat that property sets drift between upstream releases and need re-confirming at build time:

### 1. `PublishMQTT` — P0, the egress gap

Upstream MiNiFi C++ `PublishMQTT` declares: **Broker URI**, **Client ID**, **MQTT Version**, **Topic**, **Quality of Service**, **Connection Timeout**, **Keep Alive Interval**, **Last Will Topic**, **Last Will Message**, **Last Will QoS**, **Last Will Retain**, **Last Will Content Type**, **Username**, **Password**, **Security Protocol**, **Security CA**, **Security Cert**, **Security Private Key**, **Security Pass Phrase**. Relationship: **success**.

Minimal ESP32 subset scoped for a first cut: **Broker URI**, **Client ID**, **Topic**, **Quality of Service**, plus **Username**/**Password** for an auth'd Mosquitto. The `Security *` (TLS) props were deferred deliberately — the target Mosquitto is plaintext on the LAN, and ESP32 TLS is heavier weight than a first cut needs. This is the processor that turns the XIAO from a loopback into a real publisher: XIAO → Mosquitto → `ConsumeMQTT` → Kafka.

### 2. A real ingress source

No MiNiFi C++ equivalent exists to mirror, so there's no upstream property schema to match — the design instead follows the existing convention (Title Case, `GenerateFlowFile`-shaped so flows stay familiar). The XIAO ESP32-S3 **Sense** variant this array actually runs has onboard sensors (mic, IMU on some carriers) and a camera. Scoped as a scheduled source: properties like **Read Interval** / **Batch Size**, emitting one FlowFile per read with the sensor value as content and/or a Title-Case attribute. Simplest proof-of-life: a periodic read of one onboard value, published via `PublishMQTT` — real ingress plus real egress, the round-trip the loopback flows couldn't test.

### 3. `UpdateAttribute` — cheap, do it alongside ingress

Upstream model: **no fixed properties** — it takes **dynamic properties** (`attribute name` → `value`, Expression-Language-capable) and writes each as a FlowFile attribute. Relationships: **success**, **failure**. Scoped to accept user-defined dynamic properties from the flow def and set them as attributes; if there's no EL engine (there isn't — see the `RouteOnAttribute` deferral), support **literal values first**, which covers nearly all branch-logic test cases without an evaluator.

### 4. `RouteOnAttribute` — deferred, it hides an EL dependency

Upstream model: property **Routing Strategy** + dynamic properties mapping `relationship name` → an Expression-Language predicate; relationships: **unmatched**, **failure**, one per dynamic property. **The finding that killed this for a first pass**: `RouteOnAttribute` is fundamentally an Expression-Language evaluator — evaluating predicates against attributes is its entire job. MicroFi's tiny runtime almost certainly has no EL engine, making this the most expensive of the four to embed. Deferred until a minimal predicate evaluator (equals / exists / contains on a named attribute) is separately scoped — `PublishMQTT`, ingress, and `UpdateAttribute` already deliver "real ingress + egress + attribute mutation" without one.

Build/verify order settled on: `PublishMQTT` (unblocks egress) → real ingress source → `UpdateAttribute` (literal values) → `RouteOnAttribute` (deferred). Per processor: add to the static registry at compile time, keep the Title-Case MiNiFi-C++ property names, rebuild + reflash on the fork, then on-hardware verify — register in EFM, push a flow exercising the processor, confirm the implicit ack and real data movement.

## PublishMQTT — built, registered, and a real engine bug found

`src/processors/publish_mqtt.cpp`, built on a `feature/publish-mqtt` branch off `xiao-s3-2mb-partition`, pushed to the fork. Minimal ESP32 subset per the design spec — Broker URI, Client ID, Topic, Quality of Service, Username, Password, `success` relationship — using ESP-IDF's `esp_mqtt_client_*` API. The client starts lazily on the first `on_trigger` call, once Broker URI/Topic are known from `on_configure`; a FlowFile that arrives before the broker's `CONNECTED` event lands is logged and dropped rather than retried — MicroFi's engine has no session commit/rollback, so a sink that doesn't explicitly transfer a FlowFile out loses it regardless. Acceptable for a periodic ingress source (the next tick just republishes); flagged in the file's own header comment as worth revisiting before production use.

**Toolchain surprise**: PlatformIO's `espressif32` platform now resolves to ESP-IDF **6.0.1**, which no longer bundles `mqtt` in-tree — `components/mqtt` exists but is an empty stub, component-manager territory now. Fixed by adding `espressif/mqtt: "*"` to `src/idf_component.yml`. The naming matters: `espressif/esp-mqtt`, guessed from the GitHub repo name, does not exist as a registry package — confirmed by checking the component registry directly rather than assuming.

Build on `esp32s3-2mb`: **Flash 91.1% (1,074,733 / 1,179,648 bytes)**, up from 88.6% pre-MQTT. Still fits, margin thin. RAM 36.1%, unchanged.

Flashed and confirmed on real hardware: boots into the previously-persisted `GenerateFlowFile → LogAttribute` flow (LittleFS untouched by a reflash), heartbeats clean, and **the manifest now advertises three processors** — `GenerateFlowFile`, `LogAttribute`, `PublishMQTT` — with `PublishMQTT`'s property descriptors matching the design spec exactly (`Broker URI`/`Topic` required, `Quality of Service` allowable values `0`/`1`/`2`, `Username`/`Password` optional). Confirmed via `GET /efm/api/agents/microfi_1`: `state: ONLINE`, `agentManifestHash` matching the new build. The existing `StarlinkAI`-class agent was re-checked and confirmed unaffected — unchanged manifest, agent still `ONLINE`, the Windows MiNiFi service still `Running`.

### The manifest-config pin trap

Even with the manifest advertising `PublishMQTT`, **the EFM Designer never offered it to place.** `agent-classes/MicroFi` had no `agent-class-manifest-config` mapping, so the Designer kept resolving the class to its *original* manifest (`GenerateFlowFile` + `LogAttribute` only), even though the live agent had already registered the newer manifest that included `PublishMQTT` — confirmed via `GET /efm/api/agent-classes/MicroFi/manifest-diff` returning `newManifestAvailable: true`. Fixed with `POST /efm/api/agent-class-manifest-config`, pinning `MicroFi` to the manifest that includes `PublishMQTT`. This is the same "a manifest refresh alone doesn't expose a new type" trap [Chapter 6](ch06-minifi-custom-python-processors.md) documents for custom Python processors on real MiNiFi C++ — same failure shape, different agent runtime.

A later processor (`ListenHTTP`, below) hit the same trap in a slightly different form: `POST` on the manifest-config returned "mapping already exists," and a `PUT` was needed instead — though the processor-create API itself worked before the pin took effect either way. The pin's effect seems scoped to the Designer's palette, not the write API.

### Real data movement, and the `Session::transfer()` fan-out bug

Built `GenerateFlowFile → PublishMQTT` in the Designer (`Broker URI: mqtt://192.168.1.121:1883`, `Topic: test/sensor/data`, `QoS 0`) and published. Confirmed via live serial (not the EFM REST view — `GET /efm/api/agents/{id}` **froze on a stale snapshot** across real heartbeats and a real reboot during this check, consistent with the existing "query Postgres or live serial, not the REST heuristics" caution documented for MiNiFi/EFM generally) that the agent fetched and applied the new flow.

**A real engine bug turned up here.** `Session::transfer()` (`src/session.cpp`) matches a relationship name against `bindings_` and returns on the *first* match. A one-relationship, multi-connection fan-out — the pre-existing `GenerateFlowFile → LogAttribute` connection plus the new `→ PublishMQTT` one, both bound to `success` — silently starved every connection registered after the first. `PublishMQTT` never received a single FlowFile as long as `LogAttribute` stayed bound to the same relationship. Confirmed by reading the source directly, not inferred from logs.

**Workaround applied (not a real fix)**: deleted the `GenerateFlowFile → LogAttribute` connection and the now-orphaned `LogAttribute` node, leaving `GenerateFlowFile → PublishMQTT` as the flow's only connection. Republished; `PublishMQTT` then received every FlowFile. The real fix — patching `transfer()` to keep scanning `bindings_` instead of returning on the first match, followed by a rebuild and reflash — is still open. Any flow needing two consumers on one relationship (bringing `LogAttribute` back alongside `PublishMQTT` for debugging, for instance) will hit this again until it's patched.

The first retest after the workaround still failed at the transport layer:

```
transport_base: Failed to open a new connection
```

repeated disconnects, even though EFM's own LAN pane (port `10090`) was working fine — isolating the gap to Mosquitto's LAN pane (`192.168.1.121:1883`) specifically, not general LAN reachability. The Windows firewall had no allow rule for port `1883`; opening one fixed it, confirmed on the next retest.

**Real data movement confirmed end-to-end.** Live serial showed `published 32 bytes to 'test/sensor/data'` every ~1s; an independent subscriber (a Node `mqtt` client against `mqtt://100.68.113.126:1883`, run separately, not reading the firmware's own log) received 60 consecutive `MicroFi GenerateFlowFile payload` messages on `test/sensor/data`. XIAO → Mosquitto: proven.

## UpdateAttribute — shipped

`feature/update-attribute` (fork commit `ad53dcf`). Literal-value attribute writes via 4 declared `Attribute N Name`/`Attribute N Value` property slots — **not** true dynamic properties. EFM's flow validation rejects any property not in a processor's declared list, confirmed on real hardware, so upstream's arbitrary-key-per-flow shape (any property name at all, resolved at publish time) isn't reachable through the Designer API today. The declared-slot design is the workaround.

Verified via `GenerateFlowFile → UpdateAttribute → LogAttribute`: `verify_key = verify_value` appeared in serial output exactly as expected. This build was, at the time, the one flashed on the unit.

## GetGPIO — memory corruption, root-caused and cleared

Attempted as the real-ingress-source candidate — reads the onboard BOOT/GPIO0 button. Code was correct in isolation and compiled clean, but linking the ESP-IDF `driver` component regressed the *whole binary's* stability: `PublishMQTT`, which had run error-free for many consecutive minutes elsewhere in the same session, started throwing MQTT transport errors once `driver` was linked in, and `GetGPIO`'s own state showed what looked like memory corruption — a `bool` reverting without any code path that should have touched it. Root cause not found in that session; no debugger or heap-corruption instrumentation was available to go further safely. Code sat on `feature/get-gpio` (fork commit `553688b`), pushed but **not flashed** — the device was reverted to the last known-stable build and confirmed clean (236 error-free MQTT publishes over 60s) before that session stopped.

**Resolved in a follow-up session, no regression found.** `CONFIG_FREERTOS_CHECK_STACKOVERFLOW_CANARY` and `CONFIG_COMPILER_STACK_CHECK_MODE_STRONG` were added alongside heap poisoning (already tried previously, which correctly doesn't cover `FlowEngine::nodes_[]`'s static/BSS storage — that gap was flagged then and held this time). ~35 minutes of clean runtime across multiple boots, surviving a real chip reset and a real MQTT transport disconnect with the *exact* original error signature (`esp_mqtt_handle_transport_read_error... errno=128`) and zero corruption. First real physical validation: held the BOOT/GPIO0 button, confirmed `payload: 0` while held and `1` on release — proving a genuine hardware read, not just "doesn't crash." Root-cause confirmation via a hardware GDB watchpoint (staged by a prior session; the JTAG driver binding is host-specific and wasn't redone here) is the only thing that would make this airtight — closed per direct instruction anyway, with the understanding it reopens if it resurfaces.

A small real bug was found and fixed along the way: `wifi.cpp`'s disconnect handler had no visibility into *why* a disconnect happened. Adding `ESP_LOGW` on `WIFI_EVENT_STA_DISCONNECTED`'s `reason`/`rssi` fields caught `WIFI_REASON_NO_AP_FOUND` (201, `rssi=-128`, a real "scan came back empty") during this same session's own WiFi setup — distinguishing it cleanly from an auth failure, which the previous silent handler couldn't do.

## ListenHTTP — shipped

`src/processors/listen_http.cpp` — inbound HTTP ingress, `esp_http_server`-backed, MiNiFi C++-compatible property names (`Listening Port`, `Base Path`). Fire-and-forget ack, matching MiNiFi C++'s real `ListenHTTP` — not the synchronous request/response pairing built elsewhere in the array for a different use case, which needs a request/response correlation model this single-task engine doesn't have.

The httpd server runs on its own FreeRTOS task, which can't safely touch `Session`/`Queue` state directly — engine state is single-task-owned. So the URI handler only ever pushes a fixed-size item onto a small `xQueueCreate`d FreeRTOS queue; `on_trigger` (the engine task, every tick) drains it. This is the same cross-task bridge shape `FlowEngine::apply()` already uses for the C2 task, reused rather than invented fresh.

Verified end-to-end on hardware:

```bash
curl -X POST http://192.168.1.198:8095/test -d "hello from windowsdesktop"
# real 200 in 205ms
```

`LogAttribute` logged `payload: hello from windowsdesktop` — exact content preserved.

### The `kMaxFlowNodes=4` silent-drop bug

Pushing the `ListenHTTP` pair on top of an existing 4-node repro flow (6 processors total) silently dropped `LogAttribute-Repro58` and `PublishMQTT` — nothing but a `WARN` log line. This is a hard-coded flow-node ceiling (`kMaxFlowNodes=4`) in the engine, and it fails silently rather than rejecting the publish or surfacing an error anywhere a Designer user would see it. **Still unfixed.** Any flow with more than 4 processors needs this checked for explicitly — count nodes before publishing, and don't trust "publish succeeded" as proof every processor in the flow actually got applied.

## Capacity

Flash on this unit's 2 MB layout reached **96.8% (1,141,317 / 1,179,648 bytes)** with `ListenHTTP` added — very little headroom left before either trimming a processor or moving to a bigger-flash unit. Anyone adding a sixth processor to this specific board should expect to hit the wall immediately, not eventually.

## Net result against the original 3-item build order

Of the original build list (`PublishMQTT` → real ingress source → `UpdateAttribute`), **2 of 3 shipped and verified on hardware** (`PublishMQTT`, `UpdateAttribute`). The real-ingress-source slot ended up filled by `GetGPIO` after the memory-corruption detour, plus `ListenHTTP` as a bonus ingress path beyond the original three. `RouteOnAttribute` remains deferred, per the original design spec, pending a scoped Expression-Language evaluator this runtime doesn't have.

**Two real engine/infra bugs found and documented, both still open, low urgency** (worked around, not blocking anything currently running):

- `Session::transfer()` only delivers to the first relationship binding that matches by name — a fan-out to two connections on the same relationship silently starves every connection after the first.
- EFM's manifest store doesn't refresh a processor's property descriptors when its name is already known to the agent class, even on a genuine new manifest hash — only a fresh processor *name* reliably gets a new manifest record. This bit the `UpdateAttribute` property redesign; the workaround (temporarily rename, verify, rename back) is in the fork's commit history.
- `kMaxFlowNodes=4` silently drops processors beyond the fourth in a flow, with no user-visible error.

## Can the XIAO run custom Python processors?

Short answer: **not the way NiFi and MiNiFi C++ do it. Custom processors on the XIAO are C++, compiled into the static registry — the "Python" part is exactly the piece that doesn't port.** Three layers to the answer.

**1. The MiNiFi C++ Python extension cannot run on an ESP32, full stop.** MiNiFi C++ supports Python processors (and `ExecuteScript` with Python) by embedding a full CPython interpreter — it dynamically links `libpython` at runtime, needs a system Python install (`libpython3-dev` / `python3-libs`), and loads `libminifi-python-script-extension.so` through the same `.so`/`dlopen` extension mechanism (confirmed against Apache's own `extensions/python/PYTHON.md`). None of that exists on an ESP32: there is no `libpython` build for ESP-IDF, no `.so`/`dlopen` loader, and no room — CPython plus stdlib is many megabytes, and this specific unit's *entire* flash is 2 MB with firmware already north of 1 MB. It doesn't fit, and there's no port that would make it fit.

**2. MicroFi's architecture rules out the delivery model, not just the size.** MicroFi resolves processors by name against a compile-time static registry — no `dlopen`, no runtime plugin load, the design bet the whole project is built on. The entire point of a NiFi/MiNiFi Python processor is *ship a script in the flow definition, no rebuild.* MicroFi is the exact opposite: adding a processor is a firmware rebuild plus a reflash, every time. So even if CPython somehow fit, the "push Python without reflashing" property — the actual reason anyone reaches for a Python processor — is precisely what MicroFi's model doesn't offer. MicroFi exists *because* MiNiFi C++'s heavier machinery doesn't shrink to a microcontroller; embedding CPython would be strictly heavier than any of that machinery.

**3. What is possible.**

- **Custom processors: yes, in C++, compile-time.** That's exactly the processor-dev track this chapter documents — `PublishMQTT`, `UpdateAttribute`, `GetGPIO`, `ListenHTTP`. If the goal is "extend the XIAO with our own logic," the answer is yes. Just not in Python.
- **An embedded MicroPython scripting processor is buildable in principle, but it would not be "MiNiFi Python."** MicroPython runs on ESP32 as its own standalone firmware. A C++ MicroFi processor could in principle embed a MicroPython VM and run a script string carried in the flow definition, restoring the "push logic without reflashing" property. Three caveats keep this a distinct feature rather than a port: it's from-scratch MicroFi work, real engineering; MicroPython is not CPython — reduced stdlib, different C API, so existing NiFi/MiNiFi Python processors would not run unchanged; and its property/script contract wouldn't match MiNiFi C++'s real `ExecuteScript`, which breaks the compatibility bet this whole project is built on — an EFM flow def written for MiNiFi C++ would no longer resolve unchanged against it. Worth scoping on its own terms if wanted, but it is a MicroFi-specific scripting capability, not "MiNiFi Python on the XIAO."

This is the same shape as the `RouteOnAttribute` deferral above: the tiny runtime has no embedded interpreter — whether that's a NiFi Expression-Language evaluator or a Python VM — and every "just run a script/predicate" feature hits that same wall. Ingress, egress, and attribute mutation clear it because they're native C++; anything script-driven doesn't, until an interpreter is deliberately embedded as its own project.

## What NOT to do

- **Don't push to `Christopheraburns/MicroFi`.** The fork token allows it; the work doesn't. Every dev branch lives on `steven-matison/MicroFi`.
- **Don't register MicroFi under an agent class an existing live agent already uses** (e.g. `StarlinkAI`). A shared class means an EFM flow push aimed at one device reaches both. Use a distinct class (`MicroFi`) and verify the existing agent afterward, every time.
- **Don't try to flash from WSL2.** No native USB passthrough; the board enumerates on the Windows side as a `COM` port. Run PlatformIO natively on Windows. `usbipd-win` is a workaround, not the path of least resistance.
- **Don't treat the 48-processor roadmap as available.** At most 5 processors are built at any point in this chapter (`GenerateFlowFile`, `LogAttribute`, `PublishMQTT`, `UpdateAttribute`, plus whichever of `GetGPIO`/`ListenHTTP` is currently flashed). Every capability beyond what's actually in the static registry is a plan, not a feature.
- **Don't flash the default `esp32s3` env onto a XIAO S3 unit sight-unseen.** Its `partitions.csv` assumes 16 MB. Confirm the *actual physical flash size* via the upload-step warning or a direct read, separately from confirming the silicon family via `chip-id` — they are not the same fact.
- **Don't commit `sdkconfig.defaults.local`.** It holds the WiFi passphrase. Gitignored upstream; keep it that way in any clone.
- **Don't trust EFM's `GET /efm/api/agents/{id}` REST view as proof a flow push landed.** It has frozen on stale snapshots across real heartbeats and a real reboot during this work. Live serial output (or Postgres directly) is the reliable source.
- **Don't assume a manifest hash bump alone makes a changed processor's properties visible in the Designer.** Pin the agent class to the new manifest explicitly (`POST`/`PUT /efm/api/agent-class-manifest-config`), and if a processor's *properties* changed under an unchanged *name*, expect to need the temporary-rename workaround.
- **Don't fan a single relationship out to two connections on this engine.** `Session::transfer()`'s first-match bug silently starves every connection after the first. Keep flows to one connection per relationship until `transfer()` is patched.
- **Don't push a flow with more than 4 processors without checking for the `kMaxFlowNodes` ceiling.** It drops silently, with only a `WARN` log — "publish succeeded" is not proof every processor in the flow was actually applied.

## Related chapters

- [Chapter 3 — C++ Processor Catalog](ch03-cpp-processor-catalog.md): the real MiNiFi C++ processor set MicroFi's property naming deliberately mirrors.
- [Chapter 6 — MiNiFi Custom Python Processors](ch06-minifi-custom-python-processors.md): the real Python-processor delivery model on MiNiFi C++ — the thing this chapter's Python evaluation concludes MicroFi architecturally cannot do.
- [Chapter 13 — EFM and SparkPlug MQTT](ch13-efm-and-sparkplug-mqtt.md): the SparkPlug B payload and MQTT-topology side of this same hardware pass.
- [Chapter 19 — EFM and NVIDIA Jetson](ch19-efm-and-nvidia-jetson.md): a second real-hardware EFM agent-class chapter, same enrollment/manifest/flow-push pattern applied to a very different device class.
- [Chapter 20 — SparkPlug Demo](ch20-sparkplug-demo.md): the field-run demo this device's `PublishMQTT` work feeds into — XIAO → Mosquitto → NiFi → Kafka, including the incident where a leftover MicroFi debug rig polluted the same MQTT topic the real device publishes on.
