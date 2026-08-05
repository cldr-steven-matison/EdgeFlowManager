"""NvidiaNano ExecuteScript — real TensorRT inference, replacing the version stub.

gpu_nifi_tensorRT-3.py imported `tensorrt`, reported its version string, and
called that AI. This does actual GPU inference: it forwards the flowfile to the
resident TensorRT daemon (files/trt_infer_server.py, loopback :5910) and merges
the classification back into the flowfile.

Note there is no `import tensorrt` here at all, and that is the point. MiNiFi
C++ `ExecuteScript` **re-reads this file on every trigger**, so nothing can stay
resident in it — a `.engine` deserialized per request would cost far more than
the inference. The daemon owns the engine, the execution context and the CUDA
buffers for the life of its own process; this script is a thin forwarder.

Exactly the same shape as agent-NvidiaNano-launch_stream.py, which forwards to
the mpv launcher on :5902. One local-daemon convention on this device, not two.

Accepted flowfile content, all three passed straight through to the daemon:
  - raw JPEG/PNG bytes
  - {"image_b64": "<base64>"}            optional "top_k"
  - {"source": "camera"}                 captures /dev/video0 on the Jetson

On success the flowfile becomes the daemon's JSON response, and the top-1
prediction is also lifted into attributes so downstream processors can route on
it without parsing content.
"""

import json
import urllib.error
import urllib.request

# Resident TensorRT daemon. MiNiFi runs natively on this Jetson (unlike the
# KubernetesPod agent, which needs host.docker.internal), so plain loopback.
INFER_URL = "http://127.0.0.1:5910/classify"

# ExecuteScript runs on MiNiFi's shared scheduling thread, so this must never be
# unbounded — an unbounded wait here is what produces the
# "onTrigger has been running for 23053 ms" warnings already in this agent's log.
# Measured on this box: ~15 ms end-to-end for a 640px JPEG, ~39 ms for a 1546px
# one (CPU-side decode dominates, not the GPU). 10s is far above the worst case
# while still failing fast if the daemon is down.
INFER_TIMEOUT_S = 10


class ReadContentCallback:
    def __init__(self):
        self.content = b""

    def process(self, input_stream):
        self.content = input_stream.read()
        return len(self.content)  # MiNiFi C++ needs this integer return


class WriteContentCallback:
    def __init__(self, data):
        self.data = data

    def process(self, output_stream):
        encoded = self.data.encode('utf-8')
        output_stream.write(encoded)
        return len(encoded)  # <--- CRITICAL: MiNiFi C++ needs this integer return!


# This is the exact entrypoint MiNiFi C++ calls on every loop execution
def onTrigger(context, session):

    flow_file = session.get()

    if flow_file:
        try:
            # 1. Read upstream payload — bytes, not str: it may be a raw JPEG,
            #    which is not valid UTF-8. The -3 stub decoded here and would
            #    have thrown on any real image.
            reader = ReadContentCallback()
            session.read(flow_file, reader)
            if not reader.content:
                raise ValueError("empty flowfile — expected an image or a JSON payload")

            # 2. Content-Type is inferred, not assumed, so the same script
            #    serves a raw image and a JSON envelope without a property.
            stripped = reader.content.lstrip()
            content_type = ("application/json" if stripped[:1] in (b"{", b"[")
                            else "application/octet-stream")

            req = urllib.request.Request(
                INFER_URL, data=reader.content, method="POST",
                headers={"Content-Type": content_type})
            try:
                with urllib.request.urlopen(req, timeout=INFER_TIMEOUT_S) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
            except urllib.error.HTTPError as http_err:
                # The daemon answers 4xx/5xx with a JSON body that says *why*
                # (e.g. "camera_unavailable" plus which port to use). urllib
                # raises before that body is read, so read it off the exception
                # — otherwise the flowfile only ever learns "503".
                try:
                    result = json.loads(http_err.read().decode('utf-8'))
                except Exception:
                    raise http_err

            if not result.get("ok"):
                raise RuntimeError(f"inference daemon reported failure: "
                                   f"{result.get('error')} {result.get('detail', '')}".strip())

            # 3. Flowfile content becomes the full result; the headline goes to
            #    attributes so downstream can route without parsing content.
            session.write(flow_file, WriteContentCallback(json.dumps(result)))

            predictions = result.get("predictions") or []
            if predictions:
                session.putAttribute(flow_file, "inference.label", str(predictions[0]["label"]))
                session.putAttribute(flow_file, "inference.confidence",
                                     str(predictions[0]["confidence"]))
            session.putAttribute(flow_file, "inference.model", str(result.get("model", "")))
            session.putAttribute(flow_file, "inference.source", str(result.get("source", "")))
            session.putAttribute(flow_file, "inference.ms", str(result.get("inference_ms", "")))
            session.putAttribute(flow_file, "python.tensorrt.execution", "Success")

            # 4. Route to success relationship
            session.transfer(flow_file, REL_SUCCESS)

        except Exception as e:
            # If it breaks, append the error message to an attribute and fail it
            session.putAttribute(flow_file, "python.error", str(e))
            session.transfer(flow_file, REL_FAILURE)
