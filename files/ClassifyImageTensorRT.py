#!/usr/bin/env python
# ClassifyImageTensorRT — MiNiFi C++ custom Python processor (minifi_native API).
#
# A first-class processor *type*, not an ExecuteScript body: it appears in the
# agent manifest under its own name with its own properties, and is wired in the
# EFM Designer like any stock processor. Same delivery path proven on this exact
# Jetson in issue #65 (EFM Resource -> nifi.asset.directory -> nifi.python.processor.dir).
#
# It runs GPU inference by calling the resident TensorRT daemon
# (files/trt_infer_server.py on 127.0.0.1:5910), rather than loading a .engine
# itself. That is deliberate:
#   - a custom processor is NOT a hot patch (PythonCreator scans once, at boot),
#     so an in-processor engine makes every model change an agent restart;
#   - it would put a CUDA context inside the agent process that also drives this
#     device's desktop automation;
#   - the daemon is shared with the ExecuteScript and Java front doors, so all
#     three answer identically instead of drifting apart.
#
# API shape confirmed live against the agent's own minifi-python-examples on
# build 1.26.02 (MoveContentToJson.py for the read/write callbacks,
# AddPythonAttribute.py / EdgeTagger.py for describe/onInitialize/getProperty).
# The processor TYPE name is the module (file) name: "ClassifyImageTensorRT".

import json
import urllib.error
import urllib.parse
import urllib.request


class ReadCallback:
    def __init__(self):
        self.content = b""

    def process(self, input_stream):
        # Bytes, not text: the flowfile is usually a raw JPEG, which is not
        # valid UTF-8.
        self.content = input_stream.read()
        return len(self.content)


class WriteCallback:
    def __init__(self, text):
        self.text = text

    def process(self, output_stream):
        encoded = self.text.encode('utf-8')
        output_stream.write(encoded)
        return len(encoded)  # MiNiFi C++ needs this integer return


def describe(processor):
    processor.setDescription(
        "Classifies an image on the Jetson GPU via TensorRT and replaces the "
        "FlowFile content with the prediction JSON. Accepts a raw JPEG/PNG "
        "FlowFile, a {\"image_b64\": ...} envelope, or captures a frame from the "
        "locally attached camera. Inference runs in the resident trt-infer "
        "daemon; this processor is the flow-side client."
    )


def onInitialize(processor):
    # addProperty(name, description, defaultValue, required, expressionLanguageSupported)
    processor.addProperty(
        "Inference Endpoint",
        "URL of the resident TensorRT inference daemon's classify endpoint",
        "http://127.0.0.1:5910/classify",
        True,
        False,
    )
    processor.addProperty(
        "Top K",
        "How many ranked predictions to return",
        "5",
        False,
        False,
    )
    processor.addProperty(
        "Request Timeout",
        "Seconds to wait for the inference daemon. Never leave this unbounded — "
        "the processor runs on MiNiFi's scheduling thread.",
        "10",
        False,
        False,
    )
    processor.addProperty(
        "Image Source",
        "'flowfile' classifies the FlowFile's own content; 'camera' ignores the "
        "content and has the daemon capture a frame from the local camera.",
        "flowfile",
        False,
        False,
    )


def onTrigger(context, session):
    flow_file = session.get()
    if flow_file is None:
        return

    try:
        endpoint = context.getProperty("Inference Endpoint")
        top_k = int(context.getProperty("Top K") or 5)
        timeout = float(context.getProperty("Request Timeout") or 10)
        source = (context.getProperty("Image Source") or "flowfile").strip().lower()

        if source == "camera":
            body = json.dumps({"source": "camera", "top_k": top_k}).encode('utf-8')
            content_type = "application/json"
        else:
            read_callback = ReadCallback()
            session.read(flow_file, read_callback)
            body = read_callback.content
            if not body:
                raise ValueError("empty FlowFile — expected an image or a JSON payload")
            # Inferred rather than configured, so one property fewer to get wrong:
            # a JSON envelope and a raw image are told apart by their first byte.
            stripped = body.lstrip()
            content_type = ("application/json" if stripped[:1] in (b"{", b"[")
                            else "application/octet-stream")

        # Top K goes on the query string, not in the body: when the FlowFile is
        # a raw JPEG there is no envelope to put it in, and this way one code
        # path serves both.
        separator = "&" if urllib.parse.urlparse(endpoint).query else "?"
        request = urllib.request.Request(
            "{}{}top_k={}".format(endpoint, separator, top_k),
            data=body, method="POST",
            headers={"Content-Type": content_type})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                result = json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as http_error:
            # The daemon answers 4xx/5xx with a JSON body saying why. urllib
            # raises before that body is read, so take it off the exception —
            # otherwise the FlowFile only ever learns "503".
            try:
                result = json.loads(http_error.read().decode('utf-8'))
            except Exception:
                raise http_error

        if not result.get("ok"):
            raise RuntimeError("inference daemon reported failure: {} {}".format(
                result.get("error"), result.get("detail", "")).strip())

        session.write(flow_file, WriteCallback(json.dumps(result)))

        predictions = result.get("predictions") or []
        if predictions:
            flow_file.addAttribute("inference.label", str(predictions[0]["label"]))
            flow_file.addAttribute("inference.confidence", str(predictions[0]["confidence"]))
        flow_file.addAttribute("inference.model", str(result.get("model", "")))
        flow_file.addAttribute("inference.source", str(result.get("source", "")))
        flow_file.addAttribute("inference.ms", str(result.get("inference_ms", "")))

        session.transfer(flow_file, REL_SUCCESS)

    except Exception as e:
        flow_file.addAttribute("inference.error", str(e))
        session.transfer(flow_file, REL_FAILURE)
