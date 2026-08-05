# windesktop-custom-processor-EdgeChromeLoader.py
#
# Custom MiNiFi C++ Python *processor* (not ExecuteScript) for issue #4 / minifi-python-processors.md's
# "Scenario to build" — a first-class processor type authored in Python via the minifi_native API,
# distinct from ExecuteScript's generic script-body engine (see minifi-python-processors.md's scope
# table). This is deliberately the bare skeleton per the doc's "prove a bare skeleton loads before
# injecting real logic" discipline — it just tags a FlowFile and transfers it, proving registration.
#
# Deployed live at:
#   C:\Windows\System32\nifi-minifi-cpp\minifi-python\nifi_python_processors\EdgeChromeLoader.py
# (direct file placement — the EFM-Resources/asset-directory delivery mechanism from
# minifi-python-processors.md's "Field-validation task" was not used this session; that field test
# is explicitly scoped to k8s/FTF3XR2065 first, this is the separate Windows leg. See the report in
# minifi-python-processors.md for why this leg used a direct copy instead.)
#
# nifi.python.processor.dir (confirmed live in this agent's minifi.properties):
#   nifi.python.processor.dir=${MINIFI_HOME}/minifi-python/
# which resolves to C:\Windows\System32\nifi-minifi-cpp\minifi-python\ on this box. That directory
# already ships the nifiapi framework package (base classes) and an empty nifi_python_processors
# package (just __init__.py + utils/dependency_installer.py) — the natural place for an authored
# processor to live alongside the framework, so this file was placed there rather than invent a new
# directory.
#
# Once registration is proven (see minifi-python-processors.md's report-back for the actual result),
# the real Chrome launch/reposition logic from windesktop-launch_stream.py would replace the trivial
# tag-and-transfer body below — the transform() method is the only thing that would change, the
# class/property/relationship scaffolding stays the same.

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


class EdgeChromeLoader(FlowFileTransform):
    class ProcessorDetails:
        version = "0.1.0"
        description = ("Skeleton custom Python processor for issue #4 — proves minifi_native "
                        "custom-processor registration on WindowsDesktopCpp before porting the "
                        "real Chrome launch/reposition logic from windesktop-launch_stream.py.")

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return []

    def transform(self, context, flowfile):
        # Attributes are returned via FlowFileTransformResult, not set directly on the
        # FlowFile proxy — nifiapi/properties.py's FlowFile class only exposes
        # getAttribute/getAttributes/getContentsAsBytes/getSize, no setter; the base
        # class's onTrigger() applies result.getAttributes() to the real flow file itself.
        return FlowFileTransformResult(
            relationship="success",
            attributes={"edgechromeloader.registered": "true"},
        )
