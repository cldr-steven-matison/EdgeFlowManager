"""Resident TensorRT inference daemon — NvidiaNano (Jetson Orin Nano).

Holds a TensorRT engine, an execution context, and its CUDA device buffers open
for the life of the process, and serves inference over loopback HTTP. Same shape
and the same reasoning as mpv_stream_launcher_linux.py on :5902 — a persistent
daemon that MiNiFi talks to, rather than work done inside the agent.

Why a daemon instead of doing this inside the processor:

1. MiNiFi C++ `ExecuteScript` **re-reads its script file on every trigger**
   (minifi-python-processors.md). Nothing can stay resident in it, so a
   `.engine` would be deserialized per request — which costs far more than the
   inference itself.
2. A custom Python processor *could* hold the engine in its instance, but that
   puts a CUDA context inside the agent process that also drives this device's
   desktop automation, and every model change then needs a full agent restart
   (`PythonCreator` scans once, at boot).
3. The Java agent has no Python at all — its `ExecuteScript` is Groovy/Clojure
   only. An HTTP endpoint is the only thing that reaches all three front doors.

So the model's lifecycle is decoupled from the agent's: `systemctl --user
restart trt-infer` reloads the model without touching MiNiFi.

**No new packages.** `tensorrt`, `numpy` and `cv2` are already on this box, and
device memory is allocated with `ctypes` straight against `libcudart` — which is
what lets this avoid torch (427 MB, not a JetPack build) and pycuda entirely.

Runs as a systemd *user* service; see trt-infer.service alongside this file.

    POST /classify   raw JPEG/PNG bytes, or {"image_b64": "..."},
                     or {"source": "camera"} to capture from /dev/video0
    GET  /health     engine, TensorRT version, uptime, free device memory,
                     camera presence
"""

import base64
import ctypes
import json
import os
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import cv2
import numpy as np
import tensorrt as trt

# Loopback only. MiNiFi runs natively on this host — unlike the KubernetesPod
# and Windows cases that needed 0.0.0.0 to cross a container boundary.
BIND_HOST = "127.0.0.1"
PORT = 5910

MODEL_DIR = "/home/tunastreet/trt-infer/models"
ENGINE_PATH = os.path.join(MODEL_DIR, "mobilenetv2.fp16.engine")
LABELS_PATH = os.path.join(MODEL_DIR, "imagenet_classes.txt")

# The .engine is built on this box and is bound to this GPU + this TensorRT
# version, which is why it is not in git. Rebuild:
#   trtexec --onnx=mobilenetv2-12.onnx \
#           --saveEngine=mobilenetv2.fp16.engine --fp16
MODEL_NAME = "mobilenetv2-12 (ImageNet-1k, FP16)"

# torchvision/ONNX-zoo preprocessing for this model: resize shorter side to 256,
# centre-crop 224, RGB, scale to [0,1], then normalize.
RESIZE_SHORT = 256
CROP = 224
MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32).reshape(3, 1, 1)
STD = np.array([0.229, 0.224, 0.225], dtype=np.float32).reshape(3, 1, 1)

CAMERA_DEVICE = "/dev/video0"
# The OBSBOT Tiny 3 offers MJPEG at 1280x720; raw YUYV only goes to 640x480.
CAMERA_WIDTH, CAMERA_HEIGHT = 1280, 720
# First frames off a UVC camera are underexposed while auto-exposure settles.
CAMERA_WARMUP_FRAMES = 5

DEFAULT_TOP_K = 5
MAX_REQUEST_BYTES = 16 * 1024 * 1024

LOG_PATH = "/home/tunastreet/trt_infer_server.log"

CUDA_MEMCPY_HOST_TO_DEVICE = 1
CUDA_MEMCPY_DEVICE_TO_HOST = 2


def log(msg):
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"[{time.strftime('%Y-%m-%dT%H:%M:%S')}] {msg}\n")
    except Exception:
        pass  # logging must never take the service down


# --------------------------------------------------------------------------
# CUDA runtime via ctypes
#
# The gap in "run TensorRT from Python" is device memory allocation, which
# normally means torch or pycuda. It does not have to: libcudart's C API is
# four functions wide for this use case and is already installed.
# --------------------------------------------------------------------------

class Cuda:
    def __init__(self):
        self.lib = None
        for name in ("libcudart.so", "libcudart.so.13", "libcudart.so.12"):
            try:
                self.lib = ctypes.CDLL(name)
                break
            except OSError:
                continue
        if self.lib is None:
            raise RuntimeError("libcudart not loadable — is CUDA installed?")

        self.lib.cudaMalloc.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t]
        self.lib.cudaFree.argtypes = [ctypes.c_void_p]
        self.lib.cudaMemcpyAsync.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t,
            ctypes.c_int, ctypes.c_void_p,
        ]
        self.lib.cudaStreamCreate.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        self.lib.cudaStreamSynchronize.argtypes = [ctypes.c_void_p]
        self.lib.cudaMemGetInfo.argtypes = [
            ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t),
        ]

    def _check(self, rc, what):
        if rc != 0:
            raise RuntimeError(f"{what} failed, cudaError={rc}")

    def malloc(self, nbytes):
        ptr = ctypes.c_void_p()
        self._check(self.lib.cudaMalloc(ctypes.byref(ptr), nbytes), "cudaMalloc")
        return ptr

    def free(self, ptr):
        self.lib.cudaFree(ptr)

    def stream(self):
        s = ctypes.c_void_p()
        self._check(self.lib.cudaStreamCreate(ctypes.byref(s)), "cudaStreamCreate")
        return s

    def h2d(self, dst, host_array, stream):
        src = host_array.ctypes.data_as(ctypes.c_void_p)
        self._check(
            self.lib.cudaMemcpyAsync(dst, src, host_array.nbytes,
                                     CUDA_MEMCPY_HOST_TO_DEVICE, stream),
            "cudaMemcpyAsync H2D")

    def d2h(self, host_array, src, stream):
        dst = host_array.ctypes.data_as(ctypes.c_void_p)
        self._check(
            self.lib.cudaMemcpyAsync(dst, src, host_array.nbytes,
                                     CUDA_MEMCPY_DEVICE_TO_HOST, stream),
            "cudaMemcpyAsync D2H")

    def sync(self, stream):
        self._check(self.lib.cudaStreamSynchronize(stream), "cudaStreamSynchronize")

    def mem_info(self):
        free_b, total_b = ctypes.c_size_t(), ctypes.c_size_t()
        if self.lib.cudaMemGetInfo(ctypes.byref(free_b), ctypes.byref(total_b)) != 0:
            return None, None
        return free_b.value, total_b.value


# --------------------------------------------------------------------------
# The resident engine
# --------------------------------------------------------------------------

TRT_TO_NUMPY = {
    trt.DataType.FLOAT: np.float32,
    trt.DataType.HALF: np.float16,
    trt.DataType.INT32: np.int32,
    trt.DataType.INT8: np.int8,
}


class Classifier:
    """Engine, execution context and device buffers — allocated once, reused."""

    def __init__(self):
        self.cuda = Cuda()
        self.logger = trt.Logger(trt.Logger.WARNING)

        with open(ENGINE_PATH, "rb") as f:
            engine_bytes = f.read()
        self.runtime = trt.Runtime(self.logger)
        self.engine = self.runtime.deserialize_cuda_engine(engine_bytes)
        if self.engine is None:
            raise RuntimeError(f"could not deserialize {ENGINE_PATH} — "
                               "rebuild it with trtexec on this box")
        self.context = self.engine.create_execution_context()

        # Tensor names are read off the engine rather than hardcoded, so a
        # different ONNX model can be dropped in without editing this file.
        self.input_name = None
        self.output_name = None
        for i in range(self.engine.num_io_tensors):
            name = self.engine.get_tensor_name(i)
            if self.engine.get_tensor_mode(name) == trt.TensorIOMode.INPUT:
                self.input_name = name
            else:
                self.output_name = name
        if self.input_name is None or self.output_name is None:
            raise RuntimeError("engine does not expose one input and one output")

        self.input_shape = tuple(self.engine.get_tensor_shape(self.input_name))
        self.context.set_input_shape(self.input_name, self.input_shape)
        self.output_shape = tuple(self.context.get_tensor_shape(self.output_name))

        in_dtype = TRT_TO_NUMPY[self.engine.get_tensor_dtype(self.input_name)]
        out_dtype = TRT_TO_NUMPY[self.engine.get_tensor_dtype(self.output_name)]

        # Pinned-by-convention host staging buffers, allocated once. np.ascontiguous
        # copies into these rather than reallocating per request.
        self.host_in = np.zeros(self.input_shape, dtype=in_dtype)
        self.host_out = np.zeros(self.output_shape, dtype=out_dtype)

        self.dev_in = self.cuda.malloc(self.host_in.nbytes)
        self.dev_out = self.cuda.malloc(self.host_out.nbytes)
        self.stream = self.cuda.stream()

        self.context.set_tensor_address(self.input_name, int(self.dev_in.value))
        self.context.set_tensor_address(self.output_name, int(self.dev_out.value))

        with open(LABELS_PATH) as f:
            self.labels = [line.strip() for line in f if line.strip()]

        log(f"engine loaded: {ENGINE_PATH} in={self.input_name}{self.input_shape} "
            f"out={self.output_name}{self.output_shape} labels={len(self.labels)}")

    def preprocess(self, image_bgr):
        h, w = image_bgr.shape[:2]
        scale = RESIZE_SHORT / min(h, w)
        resized = cv2.resize(image_bgr, (int(round(w * scale)), int(round(h * scale))),
                             interpolation=cv2.INTER_LINEAR)
        rh, rw = resized.shape[:2]
        top, left = (rh - CROP) // 2, (rw - CROP) // 2
        crop = resized[top:top + CROP, left:left + CROP]

        rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0
        chw = np.transpose(rgb, (2, 0, 1))
        return ((chw - MEAN) / STD)[np.newaxis, ...]

    def infer(self, batch):
        """Run one inference. Caller must have preprocessed to the input shape."""
        np.copyto(self.host_in, batch.astype(self.host_in.dtype, copy=False))
        self.cuda.h2d(self.dev_in, self.host_in, self.stream)
        if not self.context.execute_async_v3(int(self.stream.value)):
            raise RuntimeError("execute_async_v3 returned false")
        self.cuda.d2h(self.host_out, self.dev_out, self.stream)
        self.cuda.sync(self.stream)
        return np.array(self.host_out, dtype=np.float32).reshape(-1)

    def top_k(self, logits, k):
        # This model emits raw logits, so softmax here rather than trusting the
        # graph to have done it. Shift by the max for numerical stability.
        exps = np.exp(logits - logits.max())
        probs = exps / exps.sum()
        idx = np.argsort(probs)[::-1][:k]
        return [
            {"label": self.labels[i] if i < len(self.labels) else f"class_{i}",
             "class_id": int(i),
             "confidence": round(float(probs[i]), 6)}
            for i in idx
        ]


def camera_available():
    return os.path.exists(CAMERA_DEVICE)


def capture_frame():
    """Grab one frame from the USB camera. Raises if it isn't attached."""
    if not camera_available():
        raise FileNotFoundError(
            f"{CAMERA_DEVICE} does not exist — no camera attached. POST an image "
            "instead, or plug the camera into a Type-A port (the USB-C port does "
            "not enumerate it on this board).")
    cap = cv2.VideoCapture(0, cv2.CAP_V4L2)
    try:
        if not cap.isOpened():
            raise RuntimeError(f"{CAMERA_DEVICE} exists but could not be opened")
        cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, CAMERA_WIDTH)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT)
        frame = None
        for _ in range(CAMERA_WARMUP_FRAMES):
            ok, frame = cap.read()
            if not ok:
                raise RuntimeError("camera opened but returned no frame")
        return frame
    finally:
        cap.release()


def decode_request(body, content_type):
    """Turn a request body into a BGR image plus the source it came from.

    Three accepted shapes, so the same endpoint serves a MiNiFi flowfile, a
    base64 JSON payload from a small device, and a local capture.
    """
    stripped = body.lstrip()
    if content_type.startswith("application/json") or stripped[:1] in (b"{", b"["):
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")

        if payload.get("source") == "camera":
            return capture_frame(), "camera", payload

        b64 = payload.get("image_b64") or payload.get("image")
        if not b64:
            raise ValueError(
                'JSON body needs "image_b64" (base64 image) or "source":"camera"')
        raw = base64.b64decode(b64)
        return decode_image_bytes(raw), "image_b64", payload

    return decode_image_bytes(body), "body", {}


def decode_image_bytes(raw):
    img = cv2.imdecode(np.frombuffer(raw, np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("body is not a decodable image (expected JPEG or PNG)")
    return img


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.rstrip("/") not in ("/health", ""):
            self._respond(404, {"ok": False, "error": f"no such path: {self.path}"})
            return
        free_b, total_b = CLASSIFIER.cuda.mem_info()
        self._respond(200, {
            "ok": True,
            "model": MODEL_NAME,
            "engine": ENGINE_PATH,
            "tensorrt": str(trt.__version__),
            "input": {"name": CLASSIFIER.input_name, "shape": list(CLASSIFIER.input_shape)},
            "output": {"name": CLASSIFIER.output_name, "shape": list(CLASSIFIER.output_shape)},
            "labels": len(CLASSIFIER.labels),
            "camera": {"device": CAMERA_DEVICE, "present": camera_available()},
            "device_memory_free_mb": None if free_b is None else free_b // (1024 * 1024),
            "device_memory_total_mb": None if total_b is None else total_b // (1024 * 1024),
            "uptime_s": round(time.time() - STARTED_AT, 1),
            "requests_served": SERVED["count"],
        })

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.rstrip("/") != "/classify":
            self._respond(404, {"ok": False, "error": f"no such path: {self.path}"})
            return
        # top_k also lives in the query string, not just the JSON envelope: a
        # caller POSTing a raw JPEG body has nowhere else to put it.
        query = urllib.parse.parse_qs(parsed.query)

        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_REQUEST_BYTES:
            self._respond(413, {"ok": False,
                                "error": f"body exceeds {MAX_REQUEST_BYTES} bytes"})
            return
        body = self.rfile.read(length) if length else b""
        if not body:
            self._respond(400, {"ok": False, "error": "empty body"})
            return

        try:
            t0 = time.perf_counter()
            image, source, payload = decode_request(
                body, (self.headers.get("Content-Type") or "").lower())
            batch = CLASSIFIER.preprocess(image)
            t1 = time.perf_counter()
            logits = CLASSIFIER.infer(batch)
            t2 = time.perf_counter()

            k = int(query.get("top_k", [payload.get("top_k", DEFAULT_TOP_K)])[0])
            SERVED["count"] += 1
            self._respond(200, {
                "ok": True,
                "model": MODEL_NAME,
                "source": source,
                "image": {"width": int(image.shape[1]), "height": int(image.shape[0])},
                "predictions": CLASSIFIER.top_k(logits, max(1, min(k, 20))),
                "preprocess_ms": round((t1 - t0) * 1000, 2),
                "inference_ms": round((t2 - t1) * 1000, 2),
            })
        except FileNotFoundError as e:
            # Camera asked for but not attached — a real, expected condition on
            # this box, so it gets its own machine-readable code rather than 500.
            self._respond(503, {"ok": False, "error": "camera_unavailable",
                                "detail": str(e)})
        except (ValueError, json.JSONDecodeError) as e:
            self._respond(400, {"ok": False, "error": str(e)})
        except Exception as e:
            log(f"classify FAILED: {e}")
            self._respond(500, {"ok": False, "error": str(e)})

    def _respond(self, code, body):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        pass  # rely on the JSON responses and LOG_PATH for status


if __name__ == "__main__":
    STARTED_AT = time.time()
    SERVED = {"count": 0}
    log(f"starting on {BIND_HOST}:{PORT}")
    try:
        CLASSIFIER = Classifier()
    except Exception as e:
        log(f"FAILED TO LOAD ENGINE: {e}")
        raise
    try:
        # Single-threaded deliberately: one execution context and one set of
        # device buffers are shared, and TensorRT execution contexts are not
        # thread-safe. HTTPServer serializes requests, which is exactly right
        # here — inference is milliseconds, and the callers are a MiNiFi flow,
        # not a crowd.
        HTTPServer((BIND_HOST, PORT), Handler).serve_forever()
    except Exception as e:
        log(f"CRASHED: {e}")
        raise
