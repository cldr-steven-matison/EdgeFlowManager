import tensorrt as trt
import json

# Callback class for reading the session stream
class ReadContentCallback:
    def __init__(self):
        self.content = ""
    def process(self, input_stream):
        self.content = input_stream.read().decode('utf-8')
        return len(self.content) # Good practice to return bytes read

# Callback class for writing the session stream
class WriteContentCallback:
    def __init__(self, data):
        self.data = data
    def process(self, output_stream):
        encoded_data = self.data.encode('utf-8')
        output_stream.write(encoded_data)
        return len(encoded_data)  # <--- CRITICAL: MiNiFi C++ needs this integer return!


# This is the exact entrypoint MiNiFi C++ calls on every loop execution
def onTrigger(context, session):
    
    flow_file = session.get()
    
    if flow_file:
        try:
            # 1. Read upstream payload
            reader = ReadContentCallback()
            session.read(flow_file, reader)
            
            if reader.content.strip():
                payload = json.loads(reader.content)
            else:
                payload = {}
                
            # 2. Extract TensorRT Properties
            logger = trt.Logger(trt.Logger.INFO)
            tensorrt_info = {
                "version": str(trt.__version__),
                "status": "Active"
            }
            
            # 3. Append to JSON structure cleanly
            if isinstance(payload, dict):
                payload['tensorrt'] = tensorrt_info
            elif isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        item['tensorrt'] = tensorrt_info
            
            updated_json = json.dumps(payload)
            
            # 4. Write back to the flow file and update attributes
            # In MiNiFi C++, session.write modifies the flow_file in place or handles it internally.
            session.write(flow_file, WriteContentCallback(updated_json))
            
            session.putAttribute(flow_file, "python.tensorrt.execution", "Success")
            
            # 5. Route to success relationship
            session.transfer(flow_file, REL_SUCCESS)
            
        except Exception as e:
            # If it breaks, append the error message to an attribute and fail it
            session.putAttribute(flow_file, "python.error", str(e))
            session.transfer(flow_file, REL_FAILURE)
