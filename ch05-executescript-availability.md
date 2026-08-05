# Chapter 5: ExecuteScript Availability

`ExecuteScript` is the single generic processor where you paste a script body directly into the flow and the agent executes it on every FlowFile. It is absent from every stock Cloudera binary — C++ image, CEM Java tarball, and Windows MSI default install alike. There are four field-verified paths to add it, and this chapter maps all of them so you pick the right one and skip the failed attempts.

---

## The short answer

Field-verified in this lab (a Windows box and a Kubernetes agent), not from vendor docs:

| Build | Version | ExecuteScript in stock? | How to get it |
|---|---|---|---|
| C++ image `apacheminificpp:latest` | 1.26.02 | ❌ — 74-processor production set, no scripting `.so` | Extra-extensions injection (Path A) or source build (Path B) |
| CEM Java tarball (EFM-staged), stock | 2.24.08.0-19 | ❌ — 114 processors, no scripting NAR (verified) | NAR drop-in (Path C) — see next row |
| CEM Java tarball (EFM-staged), + NAR drop-in | 2.24.08.0-19 | ✅ — 122 processors, real Groovy ExecuteScript + real Kafka producer | Build `nifi-scripting-nar`/`nifi-kafka-nar`/`nifi-kafka-3-service-nar` from the exact-matching source tarball, drop into the agent's autoload dir |
| C++ Windows MSI | 1.26.02 | ⚠️ feature level=2 (optional) | Path D — ✅ field-verified on Windows: process-mode and Windows service + `ADDLOCAL=ALL` + ExecuteScript Python smoke. ❌ An agent installed before the `ADDLOCAL=ALL` recipe won't have it — missing `minifi-python-script-extension.dll`, 0-byte `minifi_native.pyd`; needs the `ADDLOCAL=ALL` reinstall + a service restart |
| C++ source build | 1.26.02 tag | ✅ if compiled with the flags | `-DENABLE_PYTHON_SCRIPTING=ON -DENABLE_LUA_SCRIPTING=ON` (Path B) |
| Docker `minifi-java:latest` | — | 🚫 n/a — image does not exist (verified) | No such image in the registry; run Java via the CEM tarball + NAR drop-in row above |

The old claim — "switch to Java and you get ExecuteScript for free" — is dead. The stock CEM `2.24.08.0-19` binary EFM deploys has no scripting NAR. "Java has no ExecuteScript, period" is equally stale: the NAR drop-in solves it. The accurate statement is: **none of the four stock binaries include it; all four paths to add it are now field-verified in this lab.**

---

## Why the stock builds don't have it

`ExecuteScript` is a build-time or feature-time capability, not a runtime one. Cloudera ships production-minimal binaries:

- **C++ image:** compiled without `-DENABLE_PYTHON_SCRIPTING=ON` or `-DENABLE_LUA_SCRIPTING=ON`. No scripting `.so` in `extensions/`.
- **CEM Java 2.24.08.0-19 tarball:** the scripting NAR and the Kafka NAR are not packaged. 114 processors, 45 controller services, none of them a script engine.
- **Windows MSI:** the DLLs are physically in the MSI as an optional feature at Level 2. The EFM deployer never selects it.

**Symptom (C++)** — every 30 seconds in `minifi-app.log`, processor stuck in `SCHEDULED`, nothing flows:

```
Failed to start processor <uuid> (ExecuteScript):
Process Schedule Operation: Could not instantiate: PythonScriptExecutor.
Make sure that the python scripting extension is loaded
```

The tell is a missing file: no `libminifi-python-script-extension.so` on Linux, no `minifi-python-script-extension.dll` + `minifi_native.pyd` on Windows.

**Symptom (Java)** — EFM's designer refuses the processor type before you even push a flow:

```
Processor is of type org.apache.nifi.processors.standard.ExecuteScript, but this is not a valid Processor type
```

That's not a config error. The agent-class manifest genuinely doesn't contain it.

**Docs vs. reality:** the Cloudera CEM 2.4.0 C++ *Supported processors* page tallies ~90 processors and names `ExecuteScript` with no note that scripting is an optional build-time extension. The stock image field-verifies at 74 with no scripting `.so`. Trust the running manifest, not the doc table. (`docs.cloudera.com/cem/2.4.0/release-notes-minifi-cpp/topics/cem-cpp-processors.html`)

---

## Path A — C++ extra-extensions injection (the proven path)

No compile needed. Cloudera ships a separate `extra-extensions-linux.tar.gz` (and an ARM64 variant). Inject its `.so` files into the agent tarball's `extensions/` dir before the tarball lands on the EFM binaries PVC, so every agent EFM deploys from that coordinate already has scripting.

The injection is wired into the staging recipe in [Chapter 2 (EFM Binaries)](ch02-efm-binaries.md):

```bash
# unpack base agent tarball into the staging leaf, then:
mkdir -p /tmp/efm-ext-linux
tar -xf ~/efm-binaries/nifi-minifi-cpp-1.26.02-b30-extra-extensions-linux.tar.gz \
    -C /tmp/efm-ext-linux
find /tmp/efm-ext-linux -name "*.so" -exec cp {} \
  ~/efm-binaries/staging/binaries/cpp/linux/1.26.02/nifi-minifi-cpp-1.26.02/extensions/ \;

# Python engine also needs the extra-python-components:
unzip -o ~/efm-binaries/nifi-minifi-cpp-1.26.02-b30-extra-python-components.zip \
  -d ~/efm-binaries/staging/binaries/cpp/linux/1.26.02/nifi-minifi-cpp-1.26.02/

# re-tar to minifi.tar.gz, tar-pipe into the EFM pod, rollout restart efm
```

**Verified present on-agent (pod `minifi-agent-k8s`, `ls -al extensions/`):**

```
libminifi-script-extension.so
libminifi-lua-script-extension.so
libminifi-python-script-extension.so
libminifi-python-lib-loader-extension.so
minifi_native.so
```

The same tarball also lands `libminifi-execute-process.so`, `libminifi-opc-extensions.so` (OPC-UA), and `libminifi-llamacpp.so` (on-device LLM inference).

This is the settled path. ExecuteScript has been running in service on C++ K8s pods (Linux x86_64) and on NvidiaNano (Jetson aarch64) continuously. Path A is done for Linux and ARM64. Windows is Path D.

A property worth knowing: a running C++ agent's `ExecuteScript` re-reads its Script File from disk on every trigger — no restart, no republish needed to iterate on script content. The Java custom-processor path requires a bundle version bump; this doesn't.

---

## Path B — C++ multi-stage source build

Full control, ~20–40 min on an M4. Build from Apache source at the matching tag with the scripting flags on, then overlay the built `bin/` and `extensions/` onto the stock image:

```dockerfile
FROM ubuntu:24.04 AS builder
RUN apt-get update && apt-get install -y \
    build-essential cmake git python3-dev lua5.3-dev \
    libssl-dev libcurl4-openssl-dev libarchive-dev
RUN git clone --branch v1.26.02 \
    https://github.com/apache/nifi-minifi-cpp.git /src
RUN cmake -S /src -B /build \
    -DENABLE_LUA_SCRIPTING=ON -DENABLE_PYTHON_SCRIPTING=ON \
    -DENABLE_AWS=ON -DENABLE_AZURE=ON -DENABLE_GCP=ON -DENABLE_KAFKA=ON \
    -DCMAKE_BUILD_TYPE=Release
RUN cmake --build /build --parallel $(nproc)

FROM container.repo.cloudera.com/cloudera/apacheminificpp:latest
COPY --from=builder /build/bin/        /opt/minifi/nifi-minifi-cpp-1.26.02/bin/
COPY --from=builder /build/extensions/ /opt/minifi/nifi-minifi-cpp-1.26.02/extensions/
```

Reach for this if the extra-extensions tarball is unavailable or a version mismatch bites. Path A gets the same result with Cloudera-built binaries and no compile — prefer it.

---

## Path C — Java NAR drop-in (solved) and the Docker open item

The stock EFM-staged CEM Java binary `2.24.08.0-19` has neither `ExecuteScript` nor Kafka. Confirmed against the live agent manifest (`files/efm/java-minifi-2.24.08.0-19-processors.txt`): 114 processors, 45 controller services. Stock Java gives you `ExecuteProcess` / `ExecuteStreamCommand` — shell execution, not a script engine.

**Solved.** Build `nifi-scripting-nar`, `nifi-kafka-nar`, and `nifi-kafka-3-service-nar` from the exact-matching `2.24.08.0-19` source tarball, then drop them into the agent's `nifi.nar.library.autoload.directory`. The manifest goes 114 → 122 and `ExecuteScript` runs real Groovy (no Python/Jython in this build) on both `KubernetesPodJava` and the `WindowsDesktop` agent. Field-verified twice. Full recipe: [Chapter 8 (MiNiFi Java Setup)](ch08-minifi-java-setup.md).

> **⚠️ Version match is mandatory.** The NAR must be built from the source tarball at exactly `2.24.08.0-19`. A version mismatch causes silent class-loading failure.

Docker `minifi-java:latest` — **the image does not exist.** `docker manifest inspect container.repo.cloudera.com/cloudera/minifi-java:latest` returns `unknown: Not found` (as do ~12 name variants), while `apacheminificpp:latest` and `efm:latest` resolve on the same credentials — so it is the image being absent, not an auth problem. Cloudera containerizes only the C++ agent; MiNiFi Java is distributed as the tarball. There is no Docker manifest to check the "200+" count against or to shortcut the NAR build — the tarball + NAR drop-in (Path C above) is the only Java scripting path, and it stays field-verified at 122 processors.

---

## Path D — Windows MSI ADDLOCAL=ALL

**Status: works, but only where the recipe was actually followed.** Field-verified on WindowsDesktop.

| Mode | Result |
|---|---|
| Process-mode (`bin\minifi.exe`, no service) | ✅ ExecuteScript Python smoke |
| Windows service (`Apache NiFi MiNiFi` + `ADDLOCAL=ALL`) | ✅ ExecuteScript Python smoke after C2 enable |
| An agent installed before the `ADDLOCAL=ALL` recipe | ❌ Not present — only the generic `minifi-script-extension.dll` exists, no `minifi-python-script-extension.dll`, and `minifi_native.pyd` is a 0-byte stub. Fixing it needs the `ADDLOCAL=ALL` reinstall plus a service restart, with a drain plan if the agent runs a live flow (e.g. the Lemonade routing flow) |

### MSI facts (1.26.02-b30 x64)

| Fact | Detail |
|---|---|
| Python feature | `CM_C_python_script_extension` Feature Level 2 — EFM deployer never selects it |
| How to force Python | `ADDLOCAL=ALL` on `msiexec /i`, or `msiexec /a` administrative extract |
| `minifi_native.pyd` | Not a separate package — CustomAction does `mklink extensions\minifi_native.pyd minifi-python-script-extension.dll`. If missing after install: copy the DLL to that name |
| Host Python | 3.14.4 x64 at `C:\Python314` worked; agent creates `minifi-python-env` on first boot |
| Non-elevated `msiexec /i` | Exit 1625 (system policy) — service install needs real Admin PowerShell |

### Preferred how-to — Windows service + ADDLOCAL=ALL (production)

**Requires:** interactive **Administrator PowerShell** (UAC). Do not leave `$PWD` as `C:\WINDOWS\system32`.

```powershell
# 0) Always cd out of system32 first (Admin shells start there)
cd C:\minifi
# if dir missing:
New-Item C:\minifi -ItemType Directory -Force | Out-Null
Set-Location C:\minifi

# 1) Download MSI from EFM (adjust host/port)
$efm = 'http://127.0.0.1:10090'   # StarlinkAI: http://efm-host-ip:10090
Invoke-WebRequest `
  "$efm/efm/api/agent-deployer/binary?agentType=cpp&agentVersion=1.26.02&osArch=windows" `
  -OutFile C:\minifi\minifi.msi -UseBasicParsing

# 2) Install ALL features including Python (the line that matters)
$pythonDir = 'C:\Python314'   # directory containing python.exe
Start-Process msiexec.exe -ArgumentList `
  "/i `"C:\minifi\minifi.msi`" ADDLOCAL=ALL AUTOSTART=0 INSTALL_ROOT=`"C:\minifi`" INSTALLPYTHONDIR=`"$pythonDir`" /quiet /L*v `"C:\minifi\msi_service_addlocal.log`"" `
  -PassThru -Wait
# expect exit 0; log: "Configuration completed successfully"
```

**Post-install checks:**

```powershell
# Where did it actually land? (MSI may ignore INSTALL_ROOT and use system32)
sc.exe qc "Apache NiFi MiNiFi"
# BINARY_PATH_NAME tells you the real tree

$tree = 'C:\minifi\nifi-minifi-cpp'   # or C:\WINDOWS\system32\nifi-minifi-cpp if MSI stuck it there
Test-Path "$tree\extensions\minifi-python-script-extension.dll"   # must True
Test-Path "$tree\extensions\minifi_native.pyd"                    # must True
if (-not (Test-Path "$tree\extensions\minifi_native.pyd")) {
  Copy-Item "$tree\extensions\minifi-python-script-extension.dll" `
            "$tree\extensions\minifi_native.pyd" -Force
}
```

**Enable C2** (stock MSI leaves `nifi.c2.*` commented — service will not heartbeat until you set these):

```properties
# conf\minifi.properties — uncomment/set:
nifi.c2.enable=true
nifi.c2.agent.class=<YourClass>
nifi.c2.agent.identifier=<uuid>
nifi.c2.agent.heartbeat.period=5000
nifi.c2.rest.path.base=http://127.0.0.1:10090/efm/api
nifi.c2.rest.url=http://127.0.0.1:10090/efm/api/c2-protocol/heartbeat
nifi.c2.rest.url.ack=http://127.0.0.1:10090/efm/api/c2-protocol/acknowledge
nifi.c2.rest.path.heartbeat=/c2-protocol/heartbeat
nifi.c2.rest.path.acknowledge=/c2-protocol/acknowledge
```

```powershell
Start-Service 'Apache NiFi MiNiFi'   # or Restart-Service after editing props
# EFM: agent ONLINE within ~5-15s
```

### Smoke test — pass criteria

Flow on the agent class (C++ FQCNs):

```
ListenHTTP :18080 /contentListener
  -> ExecuteScript (Script Engine: python)
  -> LogAttribute (Log Payload = true)
```

Script body:

```python
def onTrigger(context, session):
    flow_file = session.get()
    if flow_file:
        session.putAttribute(flow_file, "python.smoke", "windows-cpp-executescript-ok")
        session.transfer(flow_file, REL_SUCCESS)
```

```powershell
Invoke-WebRequest -Uri http://127.0.0.1:18080/contentListener -Method Post `
  -ContentType 'application/json' `
  -Body '{"test":"hello-from-windows-cpp-python","ts":"smoke1"}' -UseBasicParsing
```

**Pass:** `POST` returns 200; LogAttribute shows `python.smoke=windows-cpp-executescript-ok` with the payload. No repeating `Could not instantiate: PythonScriptExecutor` in the log. The same test passed on WindowsDesktop in both process-mode and Windows service.

---

## Getting the script onto the agent

Having the engine is half of it — the Script File still has to reach the agent and survive a pod restart. Two mechanisms:

- **EFM Resource Manager API** — `POST /efm/api/resource-manager/resources/file` (multipart), then `PUT /efm/api/agent-class-resource-manager/{agentClass}/save` with exactly `{"resourceIdsToBeAssigned":[...],"resourceIdsToBeUnassigned":[...]}`. A bare array is silently swallowed. This is the tracked, restart-durable path. The `efm-resources` PVC must exist (mounted at `/opt/efm/efm-2.3.1.0-2/resources`) — without it, the DB row survives a pod restart but the bytes don't, leaving a phantom resource with no content.

- **Raw `kubectl cp`** onto the agent's script path — takes effect on the next `ExecuteScript` trigger (the C++ agent re-reads from disk on every trigger). Fast for iteration. Does not survive a pod restart and bypasses EFM tracking.

Use the Resource Manager path for anything running in service; use `kubectl cp` for active development iterations.

---

## What NOT to do

**Do not assume `ExecuteScript` is in any stock Cloudera binary.** Neither the C++ image (74 processors, no scripting `.so`), nor the CEM Java 2.24.08.0-19 tarball (114 processors, no scripting NAR), nor the Windows MSI default install includes it. All four paths to add it are now field-verified in this lab.

**Do not propagate `ExecutePythonProcessor` into any catalog or summary.** The Cloudera CEM docs list a phantom `ExecutePythonProcessor` that does not exist in any live manifest captured — not in the 74-processor C++ stock set, not in the 76-processor Windows set, not in the 114-processor Java tarball. It is a Cloudera doc error. The only scripting processor is `ExecuteScript` (Script Engine: python).

**Do not expect a Windows-service (LocalSystem) agent to launch a visible GUI window.** Session 0 isolation means the process spawns and its process tree looks normal, but no interactive desktop exists in Session 0 for a window to render into — confirmed with a real Chrome-launch test that ran green end-to-end yet never produced a discoverable window. For any `ExecuteScript` that needs to drive a visible UI on Windows, run the agent in process-mode or configure the service under a real interactive logon — not `LocalSystem`.

> **⚠️ Danger:** Do not run the Windows MSI installer from `C:\WINDOWS\system32`. Admin PowerShell defaults to that directory; the MSI may install the service tree under system32 even when you pass `INSTALL_ROOT=C:\minifi`. Always `cd C:\minifi` before invoking `msiexec`.

Additional traps from field work:

- Do not copy Linux `.so` extra-extensions onto a Windows agent. They are ELF binaries; Windows needs MSVC-built `.dll` files.
- Do not assume stock MSI enables C2. After service install, `nifi.c2.*` is typically still commented — the agent runs but never heartbeats until you set class/id/EFM URLs and restart.
- Do not treat "the `.so`/`.dll` is present" as proof that `ExecuteScript` works. Instantiation can still fail on a wrong Python ABI or a missing pyd. The real proof is a FlowFile through `LogAttribute` with your script's attribute.
- Do not put a scripting flow on an agent class whose EFM manifest doesn't include the processor. The designer validates against the class-to-manifest mapping, not against whatever agent happens to be online.
- PowerShell 5.1 chokes on Unicode em-dashes in `.ps1` files. Save as ASCII or add a BOM, or you get bogus "string missing terminator" parse errors.

---

## Related chapters

- Ch2 — [EFM Binaries](ch02-efm-binaries.md): the binary-staging tree and extra-extensions injection recipe.
- Ch8 — [MiNiFi Java Setup](ch08-minifi-java-setup.md): the Java NAR drop-in recipe (Path C).
- Ch16 — [How to AI with MiNiFi](ch16-how-to-ai-with-minifi.md): using `ExecuteScript` for inline Python at the edge.
