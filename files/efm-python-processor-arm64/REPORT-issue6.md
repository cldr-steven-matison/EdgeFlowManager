## Python-processor delivery via EFM Resources — k8s (KubernetesPod, C++ arm64)

- **Result:** works
- **nifi.asset.directory (live):** `${MINIFI_HOME}/asset` → `/nifi-minifi-cpp-1.26.02/asset`
- **AssetInformation in nifi.c2.root.classes:** yes — `DeviceInfoNode,AgentInformation,FlowInformation,AssetInformation`
- **python processor-dir key (live):** `nifi.python.processor.dir` — default `${MINIFI_HOME}/minifi-python/`; set to `${MINIFI_HOME}/asset` for the crux test
- **Delivery: EFM Resource → asset dir landed?** yes — uploaded via `POST /resource-manager/resources/file` (resourceType=ASSET, id `aced1ad1-cc90-4664-b799-8f1e33411e29`), assigned to class via `PUT /agent-class-resource-manager/KubernetesPod/save`, asset-synced to `asset/EdgeTagger.py` in ~5s. `.state` entry present; SHA-512 digest matched local upload (no drift).
- **Type discovery mechanism that worked:** **processor-dir into asset dir**. With `nifi.python.processor.dir=${MINIFI_HOME}/asset` and the control copy removed, the agent logged `Adding /asset/EdgeTagger.py to paths` → `Registering MiNiFi python processor: EdgeTagger`. `@{asset-id:…}` is a **property-value resolver only** — it hands a resolved path to an already-loaded processor (e.g. ExecuteScript's *Script File*), it does **not** register a new processor *type*. Corroborated live: the leftover `asset/cpu_nifi_tensorRT.py` (an ExecuteScript script asset) was scanned by the type loader and rejected — `Required Function 'describe' is not found`.
- **EdgeTagger in manifest under own name:** yes — `org.apache.nifi.minifi.processors.EdgeTagger`; property **`Tag Value`**; relationships **success, failure, original**. Manifest id `6388cf87-b074-4a24-83b3-3286bb087cbc` (identical whether loaded from `minifi-python/` or `asset/` — EFM content-hashes the manifest).
- **Flow ListenHTTP → EdgeTagger → PutFile:** green. (Ran `ListenHTTP → EdgeTagger → LogAttribute → PutFile` so the attribute is observable.) 3 POSTs → HTTP 200 ×3; LogAttribute logged `key:edge.tag value:field-test-arm64` ×3; PutFile wrote exactly 3 files with the correct bodies (`hello-edge-1/2/3`). **3 in → 3 out, no drops** (ListenHTTP Batch/Buffer Size = 1).
- **Restart needed to pick up a changed .py:** **NO** for `onTrigger` logic — a code change (added `edge.reload` attribute) took effect on the very next POST with **no restart and no re-registration** (contradicts the "custom processors are not-a-hot-patch" prior; on build 1.26.02 C++ the trigger body is re-read like ExecuteScript). **YES** for the type signature — `describe()`/`onInitialize()` (name, properties, relationships, description) run once at load and populate the manifest at registration, so changing those needs an agent restart to re-register.
- **Artifacts (staged for review — NOT yet committed):**
  - `files/efm-python-processor-arm64/EdgeTagger.py` — the processor (minifi_native API)
  - `files/efm-python-processor-arm64/config.yml` — the flow (v3 MiNiFi config)
  - `files/efm-python-processor-arm64/minifi.properties.snippet` — live keys
  - `files/efm-python-processor-arm64/agent-manifest.json` + `edgetagger-manifest-entry.json`
  - `ClouderaStreamingOperators/minifi-agent-pod-arm64.yaml` — the arm64 agent pod (new)
  - Commit sha: **pending** (stop-before-commit per plan; awaiting go-ahead)
- **Surprises / next:**
  - **Node-arch trap:** the minikube node IS arm64 (EFM Cloudera image runs aarch64), but minikube's cached `ubuntu:22.04` was **amd64** → the first agent ran as an *emulated amd64* container and the native aarch64 `minifi` binary failed with "No such file or directory" (missing aarch64 ELF interpreter). Fixed by pinning `ubuntu:22.04-arm64` (retagged in minikube docker) + `imagePullPolicy: Never`. `osArch=linuxaarch64` in the deployer.
  - **Hot-reload of `onTrigger`** is the headline finding vs the doc — worth folding into the skill/doc.
  - The agent-deployer's systemd install fails in-container (`systemctl: command not found`); `minifi` must be started by hand (`cd $MINIFI_HOME && ./bin/minifi`) — matches the §11 recovery note.
  - Runtime was proven via a **local config.yml** (C2 temporarily disabled) + the resource/asset-sync/type-registration path via the EFM API. The **EFM Designer wire-and-publish of a Python type** was not exercised (EdgeTagger is in the palette/manifest, so it should wire like any type) — good candidate for a follow-up.
  - Remaining legs (separate tickets): C++ x86_64 (WindowsDesktop), Windows MSI, Jetson aarch64 (high-confidence given this pass), Java.

### Current live state (this device, left running)
- EFM deployed (`deployment/efm`, `svc/efm`) in `cld-streaming`; port-forward `127.0.0.1:10090`.
- Agent pod `minifi-agent-k8s-arm64` (native aarch64) Online, C2 enabled, heartbeating.
- `nifi.python.processor.dir` currently = `${MINIFI_HOME}/asset` (the validated managed config); EdgeTagger delivered as an EFM Resource assigned to `KubernetesPod`. Canonical v1 `EdgeTagger.py` restored on the pod.
