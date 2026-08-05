#!/usr/bin/env python
# EdgeTagger — minimal MiNiFi C++ custom Python processor (minifi_native API).
#
# Field-validation processor for issue #6 (device:FTF3XR2065, arm64 C++ k8s leg):
# proves that a .py delivered as an EFM Resource into nifi.asset.directory and seen
# by nifi.python.processor.dir loads as a first-class processor *type* — visible in
# the agent manifest under its own name (EdgeTagger), wireable in the Designer.
#
# API shape confirmed live against the agent's own minifi-python-examples
# (AddPythonAttribute.py, google/SentimentAnalyzer.py) on build 1.26.02:
#   describe(processor)      -> setDescription
#   onInitialize(processor)  -> addProperty(name, desc, default, required, elSupported)
#   onTrigger(context, sess) -> context.getProperty(name) returns the value string
#   REL_SUCCESS is a runtime-provided global.
# The processor TYPE name is the module (file) name: "EdgeTagger".


def describe(processor):
    processor.setDescription(
        "Stamps every FlowFile with a fixed 'edge.tag' attribute. "
        "Field-validation processor delivered via EFM Resources (asset directory)."
    )


def onInitialize(processor):
    # addProperty(name, description, defaultValue, required, expressionLanguageSupported)
    processor.addProperty(
        "Tag Value",
        "Value written to the edge.tag attribute on each FlowFile",
        "efm-resource-arm64",
        False,
        False,
    )


def onTrigger(context, session):
    flow_file = session.get()
    if flow_file is not None:
        tag = context.getProperty("Tag Value")
        flow_file.addAttribute("edge.tag", tag)
        session.transfer(flow_file, REL_SUCCESS)
