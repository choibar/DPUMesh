"""gRPC over DPUMesh for grpcio built with the DPUMesh patch (see README.md).

With DPUMESH_ENABLE=1, `serve` puts a server on this process's DPUMESH_SERVICE
and `insecure_channel` reaches "<host>:<port>" services through the DPU;
otherwise both keep grpcio's TCP behavior, so one program runs either way.
"""

import atexit
import os
import signal
import threading

import grpc
from grpc._cython import cygrpc

__all__ = ["enabled", "insecure_channel", "serve"]

_DPUMESH_GRPCIO = hasattr(cygrpc, "close_dpumesh")


def enabled():
    """True when DPUMESH_ENABLE is "1"."""
    return os.environ.get("DPUMESH_ENABLE") == "1"


def _require_patched_grpcio():
    if not _DPUMESH_GRPCIO:
        raise ImportError(
            "grpcio %s lacks DPUMesh support; install the wheel from "
            "integrations/grpc/python/build_wheel.sh" % grpc.__version__
        )


def insecure_channel(target, options=None, compression=None):
    """grpc.insecure_channel, over DPUMesh when enabled."""
    if enabled():
        _require_patched_grpcio()
        _close_on_sigterm()
        target = "dpumesh:" + target
    return grpc.insecure_channel(target, options, compression)


def serve(server, address):
    """Starts `server`: on DPUMESH_SERVICE over DPUMesh when enabled, or on
    the TCP `address`. It replaces add_insecure_port() and start()."""
    if not enabled():
        server.add_insecure_port(address)
        server.start()
        return
    _require_patched_grpcio()
    _close_on_sigterm()
    core = server._state.server
    core.add_dpumesh_listener()
    server.start()
    core.serve_dpumesh()


_sigterm_installed = False


def _close_on_sigterm():
    # SIGTERM ends a Python process without atexit. When nothing else handles
    # it, close the channel first, then end the process as the default would.
    global _sigterm_installed
    if _sigterm_installed or threading.current_thread() is not threading.main_thread():
        return
    _sigterm_installed = True
    if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        return

    def handler(signum, frame):
        _close()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGTERM)

    signal.signal(signal.SIGTERM, handler)


@atexit.register
def _close():
    # A process closes its channel before it exits, or the DPU sees it vanish
    # mid-stream.
    if _DPUMESH_GRPCIO:
        cygrpc.close_dpumesh()
