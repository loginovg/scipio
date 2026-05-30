import json
import os
import threading
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory

events = []
events_lock = threading.Lock()
grpc_ready = threading.Event()


def load_messages(descriptor_path):
    descriptor_data = Path(descriptor_path).read_bytes()
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_data)

    pool = descriptor_pool.DescriptorPool()
    for file_descriptor in file_descriptor_set.file:
        pool.Add(file_descriptor)

    request_descriptor = pool.FindMessageTypeByName("scipio.saga.v1.ExecuteStepRequest")
    response_descriptor = pool.FindMessageTypeByName("scipio.saga.v1.ExecuteStepResponse")
    return (
        message_factory.GetMessageClass(request_descriptor),
        message_factory.GetMessageClass(response_descriptor),
    )


def main():
    request_cls, response_cls = load_messages(os.getenv("STEP_EXECUTOR_DESCRIPTOR", "/app/saga.pb"))
    http_port = int(os.getenv("STEP_EXECUTOR_HTTP_PORT", "18080"))

    def execute_step(request, _context):
        payload = {}
        if request.context:
            payload = json.loads(request.context.decode("utf-8"))

        event = {
            "saga_id": request.saga_id,
            "workflow": request.workflow,
            "step_name": request.step_name,
            "attempt": request.attempt,
            "context": payload,
        }

        with events_lock:
            events.append(event)

        return response_cls()

    class CallsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/healthz" and self.path != "/calls":
                self.send_response(404)
                self.end_headers()
                return

            if self.path == "/healthz":
                if not grpc_ready.is_set():
                    self.send_response(503)
                    self.end_headers()
                    return
                body = b'{"status":"ok"}'
            else:
                with events_lock:
                    body = json.dumps(events).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_DELETE(self):
            if self.path != "/calls":
                self.send_response(404)
                self.end_headers()
                return

            with events_lock:
                events.clear()

            self.send_response(204)
            self.end_headers()

        def log_message(self, _format, *_args):
            return

    http_server = ThreadingHTTPServer(("0.0.0.0", http_port), CallsHandler)
    http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    http_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    handler = grpc.method_handlers_generic_handler(
        "scipio.saga.v1.SagaStepExecutor",
        {
            "ExecuteStep": grpc.unary_unary_rpc_method_handler(
                execute_step,
                request_deserializer=request_cls.FromString,
                response_serializer=lambda response: response.SerializeToString(),
            )
        },
    )
    server.add_generic_rpc_handlers((handler,))
    server.add_insecure_port("[::]:50051")
    server.start()
    grpc_ready.set()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
