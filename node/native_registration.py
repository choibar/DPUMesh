"""Sealed Pod identity for native Host Comch sessions, without a broker process.

Only registration goes through this socket. Payload and DMA memory stay in the
native Host transport. The retained socket is a session-lifetime reference.
"""
from __future__ import annotations

import socket
import struct
import sys
import time
from types import SimpleNamespace

REQUEST = struct.Struct('<8s32s64s')
MAGIC = b'DMNATIV1'


def wait_metadata(verifier, worker, nonce, incarnation, stop, deadline):
    """Wait only for container startup; every attempt repeats authorization.

    A process can connect before kubelet publishes its running container ID.
    Never relax the ID/allocation check, or retry a permanent identity error.
    """
    from node.workload_identity import IdentityNotReady
    while True:
        try:
            return verifier.metadata(worker, nonce, incarnation)
        except IdentityNotReady:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or stop.wait(min(0.1, remaining)):
                raise


def parse_request(packet: bytes) -> tuple[bytes, str]:
    if len(packet) != REQUEST.size:
        raise ValueError('invalid native registration request length')
    magic, nonce, field = REQUEST.unpack(packet)
    if magic != MAGIC or not any(nonce):
        raise ValueError('invalid native registration challenge')
    name, separator, padding = field.partition(b'\0')
    if not separator or any(padding):
        raise ValueError('noncanonical Service field')
    service = name.decode('ascii')
    from node.workload_identity import SERVICE_NAME_RE
    if service and SERVICE_NAME_RE.fullmatch(service) is None:
        raise ValueError('invalid Service name')
    return nonce, service


def serve_slot(daemon, slot) -> None:
    from node.dpumeshd import PEERCRED, kernel_evidence, process_starttime
    local, verifier = daemon.supervisor.local, daemon.supervisor.verifier
    while not daemon.stop.is_set():
        try:
            connection, _ = slot.listener.accept()
        except socket.timeout:
            continue
        except OSError:
            break

        with slot.lock:
            if slot.state != 'FREE_LISTENING' or slot.worker is not None:
                connection.close()
                continue
            slot.state = 'AUTHORIZING'
            slot.native_active = True
            slot.generation += 1
            generation = slot.generation

        issued = False
        nonce = None
        try:
            connection.settimeout(daemon.args.request_timeout)
            nonce, service = parse_request(connection.recv(REQUEST.size + 1))
            pid, _, _ = PEERCRED.unpack(connection.getsockopt(socket.SOL_SOCKET,
                                                             socket.SO_PEERCRED, PEERCRED.size))
            started = process_starttime(pid)
            evidence = kernel_evidence(connection)
            worker = SimpleNamespace(slot=slot.number, generation=generation,
                                     pod_uid=evidence.pod_uid, container_id=evidence.container_id,
                                     service=service)
            # Includes the kubelet allocation, Pod UID, namespace, Service,
            # node, running container ID and ServiceAccount checks.
            record = wait_metadata(verifier, worker, nonce, daemon.incarnation,
                                   daemon.stop, time.monotonic() + 5.0)
            if process_starttime(pid) != started:
                raise ValueError('process changed while verifying registration')
            sealed, control_session = local.seal(record)
            issued = True
            print(f'dpumeshd: native {slot.device_id}: identity sealed '
                  f'pod={worker.pod_uid} generation={generation}', file=sys.stderr, flush=True)
            if connection.send(sealed) != len(sealed):
                raise ValueError('incomplete native registration reply')
            with slot.lock:
                slot.state = 'RUNNING'

            connection.settimeout(1.0)
            while not daemon.stop.is_set():
                try:
                    # No additional messages are accepted on this lifetime reference.
                    connection.recv(1)
                    break
                except socket.timeout:
                    pass
                if local.session != control_session or process_starttime(pid) != started:
                    break
                snapshot = verifier.observed()
                if snapshot is None:
                    raise ValueError('Kubernetes watch is no longer authoritative')
                verifier.resolve(worker, snapshot, check_allocation=False)
        except Exception as error:
            print(f'dpumeshd: native {slot.device_id}: {error}', file=sys.stderr, flush=True)
        finally:
            connection.close()
            with slot.lock:
                slot.state = 'CLEANUP_WAIT' if issued else 'FREE_LISTENING'
            cleaned = not issued
            while issued and not daemon.stop.is_set():
                try:
                    # Never recycle on a timeout or an arbitrary grace period.
                    # A replacement control session can query this old nonce.
                    local.request(3, nonce)
                    if local.request(4, nonce) == 0:
                        cleaned = True
                        break
                except (OSError, ValueError):
                    pass
                daemon.stop.wait(0.1)
            with slot.lock:
                slot.native_active = False
                if cleaned:
                    slot.state = 'FREE_LISTENING' if daemon.registry.runtime_ready else 'UNHEALTHY'
            if issued and cleaned:
                print(f'dpumeshd: native {slot.device_id}: retirement confirmed '
                      f'pod={worker.pod_uid} generation={generation}', file=sys.stderr, flush=True)
            daemon.registry.notify()
