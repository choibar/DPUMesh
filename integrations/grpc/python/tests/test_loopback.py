"""A grpcio server and channel over libdpumesh_stream_loopback.so, which routes
the process's streams to its own listener. Needs the DPUMesh grpcio wheel
(build_wheel.sh) and the loopback library in build/grpc."""

import os
import pathlib
import struct
import threading
import unittest
from concurrent import futures


def _find_loopback():
    for directory in pathlib.Path(__file__).resolve().parents:
        candidate = directory / "build" / "grpc" / "libdpumesh_stream_loopback.so"
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError("libdpumesh_stream_loopback.so not found")


os.environ.setdefault("DPUMESH_STREAM_LIBRARY", _find_loopback())
os.environ["DPUMESH_SERVICE"] = "echo.test:50051"
os.environ["DPUMESH_ENABLE"] = "1"

import grpc  # noqa: E402

import dpumesh_grpc  # noqa: E402

_UNLIMITED = [("grpc.max_receive_message_length", -1), ("grpc.max_send_message_length", -1)]


def _pattern(size):
    return bytes((i * 131 + size) & 0xFF for i in range(size))


def _source(request, context):
    count, size = struct.unpack(">ii", request)
    chunk = _pattern(size)
    for _ in range(count):
        yield chunk


def _sleep(request, context):
    (delay_ms,) = struct.unpack(">i", request)
    context.add_callback(lambda: None)
    threading.Event().wait(delay_ms / 1000)
    return request


_HANDLERS = grpc.method_handlers_generic_handler(
    "test.Echo",
    {
        "Unary": grpc.unary_unary_rpc_method_handler(lambda request, context: request),
        "Source": grpc.unary_stream_rpc_method_handler(_source),
        "Sleep": grpc.unary_unary_rpc_method_handler(_sleep),
    },
)


class LoopbackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), options=_UNLIMITED)
        cls.server.add_generic_rpc_handlers((_HANDLERS,))
        dpumesh_grpc.serve(cls.server, None)
        cls.channel = dpumesh_grpc.insecure_channel("echo.test:50051", options=_UNLIMITED)
        cls.unary = cls.channel.unary_unary("/test.Echo/Unary")
        cls.source = cls.channel.unary_stream("/test.Echo/Source")
        cls.sleep = cls.channel.unary_unary("/test.Echo/Sleep")

    @classmethod
    def tearDownClass(cls):
        cls.channel.close()
        cls.server.stop(None).wait()

    def test_unary_echoes_across_sizes(self):
        for size in (0, 1, 8063, 8064, 8065, 65536, 1 << 20, 4 << 20):
            data = _pattern(size)
            self.assertEqual(data, self.unary(data, timeout=30), "size %d" % size)

    def test_server_streaming_delivers_sixteen_mib(self):
        replies = list(self.source(struct.pack(">ii", 64, 256 * 1024), timeout=30))
        self.assertEqual(64, len(replies))
        chunk = _pattern(256 * 1024)
        for reply in replies:
            self.assertEqual(chunk, reply)

    def test_concurrent_calls_share_the_connection(self):
        def calls(i):
            for _ in range(20):
                data = _pattern(64 + i)
                self.assertEqual(data, self.unary(data, timeout=30))

        with futures.ThreadPoolExecutor(max_workers=64) as pool:
            list(pool.map(calls, range(64)))

    def test_deadline_then_reuse(self):
        with self.assertRaises(grpc.RpcError) as raised:
            self.sleep(struct.pack(">i", 3000), timeout=0.1)
        self.assertEqual(grpc.StatusCode.DEADLINE_EXCEEDED, raised.exception.code())
        self.assertEqual(b"again", self.unary(b"again", timeout=30))

    def test_many_channels_open_and_close(self):
        for _ in range(20):
            channel = dpumesh_grpc.insecure_channel("echo.test:50051")
            self.assertEqual(b"x" * 1000, channel.unary_unary("/test.Echo/Unary")(b"x" * 1000, timeout=30))
            channel.close()


class DisabledTest(unittest.TestCase):
    def test_disabled_channel_uses_tcp_target(self):
        os.environ.pop("DPUMESH_ENABLE")
        try:
            self.assertFalse(dpumesh_grpc.enabled())
            channel = dpumesh_grpc.insecure_channel("127.0.0.1:1")
            channel.close()
        finally:
            os.environ["DPUMESH_ENABLE"] = "1"


if __name__ == "__main__":
    unittest.main()
