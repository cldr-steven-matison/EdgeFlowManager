from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class EdgeJavaTagger(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1'
        description = 'Ch6 Java-leg proof: minimal py4j custom Python processor'
    def __init__(self, **kwargs):
        super().__init__()
    def transform(self, context, flowfile):
        return FlowFileTransformResult(relationship='success', attributes={'edge.java.tag': 'minikube-java-py4j-ok'})
