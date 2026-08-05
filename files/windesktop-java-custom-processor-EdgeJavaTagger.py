# windesktop-java-custom-processor-EdgeJavaTagger.py
#
# Custom MiNiFi Java (CEM) Python *processor* for issue #4 item 3 — proves the py4j-based Python
# processor framework shipped in the CEM Java MiNiFi tarball (python/api/nifiapi/, python/framework/
# incl. bundled py4j/, lib/nifi-python-framework-api-*.jar) is functionally usable, not just
# structurally present. minifi-python-processors.md flagged this framework as "structurally
# present, not yet tested" after the Windows C++ leg (issue #4) and the k8s x86_64 leg (issue #10)
# — this file is the first functional exercise of it.
#
# Deliberately the same bare-skeleton shape as the C++ leg's EdgeChromeLoader.py (tag-and-transfer,
# no real logic) — the point is proving registration/instantiation, not doing real work yet.
#
# Deployed live at:
#   C:\Users\tunas\minifi-java\minifi-2.24.08.0-19\python\extensions\EdgeJavaTagger.py
# (direct file placement — nifi.python.extensions.source.directory.default=./python/extensions is
# the configured scan root; no EFM-Resources delivery attempted for the Java leg this session,
# same as the original C++ leg's scope call for issue #4.)
#
# API shape differs from the C++ `minifi_native`/nifiapi variant in one required way: py4j needs an
# explicit `class Java: implements = [...]` stanza on the processor class itself (confirmed live by
# reading this exact install's python/framework/ProcessorInspection.py, which hardcodes
# PROCESSOR_INTERFACES = ['org.apache.nifi.python.processor.FlowFileTransform', ...] as the set of
# interface strings it recognizes). The C++ leg's EdgeChromeLoader.py has no such stanza — the two
# frameworks are structurally parallel (same nifiapi package name/shape) but not identical; this is
# the concrete difference found by inspecting the live py4j framework code rather than guessing.

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


class EdgeJavaTagger(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.1.0'
        description = ('Skeleton custom Python processor for issue #4 item 3 — proves the py4j '
                        'Python processor framework registers/instantiates on the real WindowsDesktop '
                        'Java MiNiFi CEM agent (2.24.08.0-19).')
        dependencies = []

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return []

    def transform(self, context, flowfile):
        return FlowFileTransformResult(
            relationship='success',
            attributes={'edgejavatagger.registered': 'true'},
        )
