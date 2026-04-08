from __future__ import annotations

import datetime as dt
import os
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path


TDS_PACKET_NAMES = {
    0x01: "SQL_BATCH",
    0x02: "PRE_TDS7_LOGIN",
    0x03: "RPC",
    0x04: "TABULAR_RESULT",
    0x06: "ATTENTION",
    0x10: "LOGIN7",
    0x12: "PRELOGIN",
}


def now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def q(cmd: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(cmd)
    return " ".join(cmd)


def clean(text: str) -> str:
    return "".join(c for c in text if c.isprintable() or c in "\r\n\t")


@dataclass(frozen=True)
class BackendSpec:
    name: str
    host: str
    port: int


class RunLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._fh = path.open("a", encoding="utf-8", newline="\n")

    def close(self) -> None:
        with self._lock:
            self._fh.flush()
            self._fh.close()

    def line(self, msg: str) -> None:
        text = f"[{ts()}] {msg}"
        with self._lock:
            self._fh.write(text + "\n")
            self._fh.flush()
        print(text, flush=True)

    def blank(self) -> None:
        with self._lock:
            self._fh.write("\n")
            self._fh.flush()
        print("", flush=True)

    def block(self, prefix: str, text: str) -> None:
        for line in text.splitlines():
            line = line.strip()
            if line:
                self.line(prefix + line)


class PhaseState:
    def __init__(self, backend: BackendSpec) -> None:
        self._lock = threading.Lock()
        self._backend = backend
        self._phase = backend.name

    def get(self) -> tuple[str, BackendSpec]:
        with self._lock:
            return self._phase, self._backend

    def set(self, phase: str, backend: BackendSpec) -> None:
        with self._lock:
            self._phase = phase
            self._backend = backend


class PacketLogger:
    def __init__(self, runlog: RunLogger, phase: PhaseState, conn_id: int) -> None:
        self.runlog = runlog
        self.phase = phase
        self.conn_id = conn_id
        self.buffers = {"C2S": bytearray(), "S2C": bytearray()}

    def feed(self, direction: str, data: bytes) -> None:
        buf = self.buffers[direction]
        buf.extend(data)
        while True:
            if len(buf) < 8:
                return
            pkt_len = int.from_bytes(buf[2:4], "big")
            if pkt_len < 8 or pkt_len > 65535:
                self._emit_raw(direction, bytes(buf))
                buf.clear()
                return
            if len(buf) < pkt_len:
                return
            packet = bytes(buf[:pkt_len])
            del buf[:pkt_len]
            self._emit_packet(direction, packet)

    def flush(self, direction: str) -> None:
        buf = self.buffers[direction]
        if buf:
            self._emit_raw(direction, bytes(buf), partial=True)
            buf.clear()

    def _emit_raw(self, direction: str, data: bytes, partial: bool = False) -> None:
        phase, _ = self.phase.get()
        kind = "partial " if partial else ""
        self.runlog.line(
            f"[{phase}] conn={self.conn_id} {direction} {kind}chunk {len(data)} bytes"
        )

    def _emit_packet(self, direction: str, packet: bytes) -> None:
        phase, _ = self.phase.get()
        pkt_type = packet[0]
        status = packet[1]
        pkt_len = int.from_bytes(packet[2:4], "big")
        name = TDS_PACKET_NAMES.get(pkt_type, f"0x{pkt_type:02X}")
        self.runlog.line(
            f"[{phase}] conn={self.conn_id} {direction} {name} "
            f"type=0x{pkt_type:02X} status=0x{status:02X} len={pkt_len}"
        )
        preview = self._preview(packet[8:])
        if preview:
            self.runlog.line(f"[{phase}] conn={self.conn_id} {direction} preview: {preview}")

    def _preview(self, payload: bytes) -> str:
        if not payload:
            return ""
        try:
            text = clean(payload.decode("utf-16le", errors="replace")).strip()
        except Exception:
            return ""
        if not text:
            return ""
        upper = text.upper()
        if any(tok in upper for tok in ("SELECT", "EXEC", "DECLARE", "USE ", "SET ", "IF ")):
            return text[:600]
        if len(payload) < 256:
            return text[:240]
        return ""
