# Chapter 6: MiNiFi Custom Python Processors

A **custom Python processor** is a *new processor type* you write in Python and load into a MiNiFi agent at the edge. It appears in the agent's manifest under its own name, with its own properties and relationships, and is wired into a flow like any stock processor. It is the MiNiFi counterpart to the NiFi 2.x custom Python processors covered in *How to AI with NiFi and Python* — the same authoring model pushed down to the edge agent.

---

## Scope — read this first

**This chapter is about custom Python *processors*. It is NOT about `ExecuteScript`.** They are two different processors and two different concepts:

| | `ExecuteScript` (Python engine) | Custom Python processor |
|---|---|---|
| What it is | **One** built-in, generic processor you paste a script *body* into (or point at a script file) | **A new processor type** you author in Python and add to the agent |
| Identity in the flow | Always shows as `ExecuteScript` | Shows under its own name (e.g. `EdgeTagger`) with its own properties/relationships |
| Reload behavior | **Re-reads the script every trigger** — hot-edit, no restart | **Not a hot patch** — a type-signature change needs an agent restart |
| Where it's covered | [ExecuteScript Availability](ch05-executescript-availability.md) | this chapter |

`ExecuteScript` belongs to the [ExecuteScript Availability](ch05-executescript-availability.md) chapter — don't look for its availability paths or script-body howtos here. The phantom `ExecutePythonProcessor` from Cloudera's C++ docs does not exist in any live manifest — don't propagate it either.

---

## Where it runs

A custom Python processor loads on any MiNiFi agent that has the Python extension runtime — the same `.so`/`.pyd` pair `ExecuteScript` uses. It works across the C++ builds and on the CEM Java agent:

| Runtime | Platform | Delivery |
|---|---|---|
| C++ | Linux x86_64 / arm64 / aarch64 (k8s, Jetson) | EFM Resources → asset directory |
| C++ | Windows (MSI) | Direct file placement, or EFM Resources |
| Java | CEM Java agent | `bootstrap.conf` + a `python3` in the image (see the Java section) |

---

## Prerequisites

- **A C++ agent with the Python extension present** — the same `.so`/`.pyd` pair `ExecuteScript` uses, so any agent that can run `ExecuteScript` Python already has the runtime:
  - Linux/ARM64: `libminifi-python-script-extension.so` + `minifi_native.so`
  - Windows MSI: `minifi-python-script-extension.dll` + `minifi_native.pyd`

  How to get these onto an agent is the ExecuteScript-availability problem — see the [ExecuteScript Availability](ch05-executescript-availability.md) chapter (Path A for Linux/ARM64, Path D for Windows MSI).

- **A processor directory in `minifi.properties`** pointed at the `.py` files: `nifi.python.processor.dir` (default `${MINIFI_HOME}/minifi-python/`). Confirm the exact key on the running agent — Apache `PYTHON.md` is the authority for the current name.

---

## Two styles of processor — and why the choice matters for delivery

There are two ways to author a custom Python processor, and the choice determines which **delivery mechanism** works:

- **Function-style (`minifi_native`)** — module-level `describe()` / `onInitialize()` / `onTrigger()` functions against the `minifi_native` C-extension API. **No `nifiapi` import.** This is the portable, low-friction style.
- **Class-style (`nifiapi`)** — a class subclassing `nifiapi.flowfiletransform.FlowFileTransform`, implementing `transform(self, context, flowfile) -> FlowFileTransformResult(...)`, with a nested `ProcessorDetails` class for metadata and `getPropertyDescriptors()` for properties. Same package name and shape as full NiFi 2.x's Python processor API. The base class auto-registers `success` / `failure` / `original` and drives `describe()`/`onInitialize()`.

The class-style version **depends on the `nifiapi` framework package** shipping as a sibling directory (`minifi-python/nifiapi/`) under the processor-dir. That dependency is the crux of the delivery section below.

---

## How it loads — scan-once-at-boot, not a hot patch

MiNiFi's `PythonCreator` scans the processor directory **once, at agent boot** — not on every trigger the way `ExecuteScript` re-reads its Script File. A `.py` present at boot registers (the log shows `Registering MiNiFi python processor: <name>`); a `.py` dropped in *after* the agent is running is not picked up until the next restart.

Once registered, an **`onTrigger`-only code change hot-reloads** on the next trigger with no restart. But a **`describe()` / `onInitialize()` (type-signature) change requires a restart** to take effect.

> **Rule of thumb:** logic-only edits are cheap; changing the processor's *shape* — its properties, relationships, or declared type — means an agent restart. This is the biggest behavioral difference from `ExecuteScript`.

One subtlety: EFM's tracked manifest content-hash is *structural* (type/property/relationship names) and ignores the freeform `typeDescription` text. A description-only edit is a real, restart-required change that is invisible to EFM's manifest-diff, so a Designer palette tooltip can go stale without the "new manifest available" signal firing.

---

## Getting the processor onto the agent

Two delivery mechanisms, the same split as `ExecuteScript`'s Script File:

### 1. Direct file placement

Copy the `.py` into `minifi-python/` (or wherever `nifi.python.processor.dir` points) and restart. Fast, no EFM involvement, but it bypasses EFM tracking and doesn't survive a fresh pod/agent rebuild.

The default layout the agent ships is a **sibling-package** one: `minifi-python/nifiapi/` (the framework) alongside an empty `minifi-python/nifi_python_processors/` (where your authored `.py` goes). Both styles work here, because the `nifiapi` framework is on the scanned path.

### 2. EFM Resources → asset directory (managed, restart-durable)

Upload the authored `.py` as an **EFM Resource** and let EFM push it to the agent's **asset directory** over the asset-sync C2 command — no image rebuild, no manual SCP, the same managed path flows already ride. This is the tracked, C2-managed delivery.

Mechanics (`CONFIGURE.md#asset-directory` is the authority):

- Property **`nifi.asset.directory`** — default `${MINIFI_HOME}/asset`.
- Assets are tracked in a **`.state`** file; the **asset-sync C2 command** downloads/updates/deletes them. Requires **`AssetInformation`** in the agent's **`nifi.c2.root.classes`**, or asset sync silently won't run.
- Upload: `POST /efm/api/resource-manager/resources/file` (multipart), then assign via the agent-class resource manager with exactly `{"resourceIdsToBeAssigned":[…],"resourceIdsToBeUnassigned":[…]}` — a bare array is silently swallowed.

**Type discovery on the C++ builds:** point `nifi.python.processor.dir` **into** the asset directory so a synced `.py` is discovered as a processor *type*. (`@{asset-id:…}` is only a property-*value* resolver — a path handed to an already-loaded processor, not a new-type discovery mechanism.)

> **⚠️ Class-style processors do NOT work via asset-directory delivery.** Pointing `nifi.python.processor.dir` at the asset dir removes `minifi-python/nifiapi/` (the framework, normally a sibling of the default processor-dir) from the scanned path, so a class-style `.py` fails to load with `ModuleNotFoundError: No module named 'nifiapi'`. Function-style (`minifi_native`) processors have no such import and are unaffected. **Practical rule:** use EFM-Resources delivery for function-style processors; a class-style processor needs direct placement into `minifi-python/`.

---

## Wiring it into a flow

A custom type is referenced from an EFM Designer flow exactly like any stock processor — no special-casing. Build `ListenHTTP → EdgeTagger → LogAttribute` (or `PutFile`), POST a payload, and confirm the attribute lands with no drops (**set ListenHTTP Batch/Buffer Size = 1**, per MINIFICPP-2243).

**The one recurring Designer gotcha — a manifest refresh isn't enough.** After delivering a new type, the agent class's bound manifest must be re-pointed at the manifest that now contains it (`PUT /agent-classes/{name}`, confirm first with `GET /agent-classes/{name}/manifest-diff`). But an *already-created* processor component keeps its cached `propertyDescriptors` resolved against the old manifest — you must **delete and recreate the processor component** for it to pick up the refreshed type. A `PUT` on an existing component also silently ignores a bundle change; a wrong bundle likewise needs delete + recreate. After that, validation goes green and publish hot-reloads the agent on its next heartbeat.

---

## The Java (CEM) leg

Custom Python processors are not C++-only. The CEM Java agent (`2.24.08.0-19`) ships the full **py4j-based** Python processor framework — the `nifiapi` package, a bundled `py4j/`, the `nifi-py4j-nar`, and the `nifi-python-framework-api` JAR — and loads authored code. The Java-specific difference: a class must declare a `class Java: implements = ['org.apache.nifi.python.processor.FlowFileTransform']` inner class for the gateway.

Python is gated by a single property. On boot, `FlowController` logs `Python Extensions disabled because the nifi.python.command property has not been configured in nifi.properties`. The four `nifi.python.*` source/working/max-processes keys are necessary but **not sufficient** — `nifi.python.command` (the path to the interpreter) is the on/off switch. Setting it has two wrinkles:

1. **A direct edit to `minifi.properties` does not survive a restart** — MiNiFi-Java regenerates that file on every start.
2. **The C2 `UPDATE_PROPERTIES` push is refused** — `nifi.python.command` is on a server-side denylist (it names an arbitrary executable path).

**The durable channel is `bootstrap.conf`.** MiNiFi-Java regenerates `minifi.properties` *from* `bootstrap.conf` on each start and passes arbitrary `nifi.*` keys through — the same path the Site-to-Site `nifi.security.*` properties ride. Add `nifi.python.command=/usr/bin/python3` to `bootstrap.conf` and it lands in the regenerated `minifi.properties` and survives restarts.

One prerequisite the C++ legs don't have: **the stock MiNiFi-Java image ships no Python interpreter**, so add a `python3` for `nifi.python.command` to point at.

With both in place, the py4j framework launches at boot (`Launching Python Process /usr/bin/python3 …/Controller.py` → `Successfully started and pinged Python Server`) and an authored processor registers as a first-class type (`Discovered Python Processor <name>`). From there it wires into a Designer flow exactly like the C++ types.

---

## What NOT to do

- **Don't conflate this with `ExecuteScript`.** Different processor, different reload semantics, different chapter. See the scope table above.
- **Don't expect an edited `.py` to take effect without a restart** when the edit changes the type signature (properties/relationships/`describe()`). Only `onTrigger`-body edits hot-reload.
- **Don't deliver a class-style (`nifiapi`) processor via the asset directory** — it loses the framework package and fails with `ModuleNotFoundError: nifiapi`. Use direct placement, or use function-style for asset delivery.
- **Don't assume a manifest refresh alone exposes a new type in Designer** — delete and recreate the processor component so its cached descriptors re-resolve.
- **Don't propagate `ExecutePythonProcessor`** — it's a Cloudera doc phantom, absent from every live manifest.
- **Watch two EFM-Resources traps:** `relativePathOnAgent` must be an empty string (omitting it serializes as `null`, which some builds reject); and a single *failed* resource-sync can permanently stall an agent's resource channel, cleared only by rotating `nifi.c2.agent.identifier` (`DELETE /efm/api/agents/{id}` + a fresh id). On non-EFM-scripted installs, also set `nifi.c2.rest.path.base` explicitly — the agent derives the asset-download base from it, not by trimming the heartbeat URL.

---

## Runnable scenario

Both recipes are packaged as a lift-and-run scenario — each `.py`, its `minifi.properties`/`bootstrap.conf` snippet, and an exported EFM flow (plus a one-`apply` disposable Java agent pod) — in the MiNiFi Kubernetes Playground under `sample-gallery/python-processors/`.

## References

- Apache `nifi-minifi-cpp` `PYTHON.md` — the custom-Python-processor API and directory config.
- Apache `nifi-minifi-cpp` `CONFIGURE.md#asset-directory` — the asset directory, the `.state` file, the asset-sync C2 command, and `AssetInformation`.
- [ExecuteScript Availability](ch05-executescript-availability.md) — the adjacent, different concept.
