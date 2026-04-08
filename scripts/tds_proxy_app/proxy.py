from __future__ import annotations

import socket
import threading
from typing import Optional

from .common import PacketLogger, PhaseState, RunLogger


class TdsProxyServer:
    def __init__(
        self,
        runlog: RunLogger,
        phase: PhaseState,
        host: str,
        port: int,
        stop_event: threading.Event,
    ) -> None:
        self.runlog = runlog
        self.phase = phase
        self.host = host
        self.port = port
        self.stop_event = stop_event
        self.server_socket: Optional[socket.socket] = None
        self.conn_id = 0

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(16)
        sock.settimeout(1.0)
        self.server_socket = sock
        self.runlog.line(f"TDS proxy listening on {self.host}:{self.port}")
        self.runlog.line("Connect SSMS to the proxy endpoint, not the backend ports")
        while not self.stop_event.is_set():
            try:
                client, addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.conn_id += 1
            conn_id = self.conn_id
            phase_name, backend = self.phase.get()
            self.runlog.line(
                f"[{phase_name}] accepted connection #{conn_id} from "
                f"{addr[0]}:{addr[1]} -> {backend.host}:{backend.port}"
            )
            try:
                server = socket.create_connection((backend.host, backend.port), timeout=5)
            except Exception as exc:
                self.runlog.line(
                    f"[{phase_name}] backend connect failed {backend.host}:{backend.port}: {exc}"
                )
                client.close()
                continue
            packet_logger = PacketLogger(self.runlog, self.phase, conn_id)
            self._bridge(conn_id, phase_name, client, server, packet_logger)
        self.runlog.line("TDS proxy listener stopped")

    def stop(self) -> None:
        self.stop_event.set()
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except Exception:
                pass

    def _bridge(
        self,
        conn_id: int,
        phase_name: str,
        client: socket.socket,
        server: socket.socket,
        packet_logger: PacketLogger,
    ) -> None:
        def forward(src: socket.socket, dst: socket.socket, direction: str) -> None:
            try:
                while not self.stop_event.is_set():
                    data = src.recv(65535)
                    if not data:
                        break
                    packet_logger.feed(direction, data)
                    dst.sendall(data)
            except Exception as exc:
                self.runlog.line(f"[{phase_name}] conn={conn_id} {direction} closed: {exc}")
            finally:
                packet_logger.flush(direction)
                for sock in (src, dst):
                    try:
                        sock.close()
                    except Exception:
                        pass

        threading.Thread(
            target=forward,
            args=(client, server, "C2S"),
            daemon=True,
            name=f"tds-{conn_id}-c2s",
        ).start()
        threading.Thread(
            target=forward,
            args=(server, client, "S2C"),
            daemon=True,
            name=f"tds-{conn_id}-s2c",
        ).start()
