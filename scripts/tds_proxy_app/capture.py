from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

from .common import RunLogger, now_tag


class CaptureSession:
    def __init__(
        self,
        runlog: RunLogger,
        capture_dir: Path,
        phase: str,
        local_port: int,
        backend_port: int,
        interface_override: Optional[str],
    ) -> None:
        self.runlog = runlog
        self.capture_dir = capture_dir
        self.phase = phase
        self.local_port = local_port
        self.backend_port = backend_port
        self.interface_override = interface_override
        self.proc: Optional[subprocess.Popen[str]] = None
        self.outfile: Optional[Path] = None

    def start(self) -> None:
        tool = shutil.which("dumpcap") or shutil.which("tshark")
        if tool is None:
            self.runlog.line(f"[{self.phase}] no dumpcap/tshark found; proxy logs only")
            return
        interface = self._resolve_interface(tool)
        if interface is None:
            self.runlog.line(f"[{self.phase}] no loopback interface found; proxy logs only")
            return
        self.outfile = self.capture_dir / f"{self.phase}_{now_tag()}.pcapng"
        flt = f"tcp port {self.local_port} or tcp port {self.backend_port}"
        cmd = [tool, "-i", interface, "-f", flt, "-w", str(self.outfile), "-q"]
        self.runlog.line(
            f"[{self.phase}] starting capture on interface {interface} -> {self.outfile.name}"
        )
        self.runlog.line(f"[{self.phase}] capture filter: {flt}")
        self.proc = subprocess.Popen(
            cmd,
            cwd=str(self.capture_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        time.sleep(0.5)
        if self.proc.poll() is not None:
            out, err = self.proc.communicate(timeout=5)
            self.runlog.line(f"[{self.phase}] capture tool exited early; disabling pcap")
            self.runlog.block(f"[{self.phase}] capture> ", out)
            self.runlog.block(f"[{self.phase}] capture> ", err)
            self.proc = None
            self.outfile = None

    def stop(self) -> None:
        if self.proc is None:
            return
        self.runlog.line(f"[{self.phase}] stopping capture")
        proc = self.proc
        self.proc = None
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    out, err = proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate(timeout=5)
            else:
                out, err = proc.communicate(timeout=5)
        except Exception as exc:
            self.runlog.line(f"[{self.phase}] capture shutdown error: {exc}")
            return
        self.runlog.block(f"[{self.phase}] capture> ", out)
        self.runlog.block(f"[{self.phase}] capture> ", err)
        if self.outfile is not None:
            self.runlog.line(f"[{self.phase}] pcap written to {self.outfile}")

    def _resolve_interface(self, tool: str) -> Optional[str]:
        if self.interface_override:
            return self.interface_override
        proc = subprocess.run([tool, "-D"], capture_output=True, text=True, check=False)
        for line in (proc.stdout or "").splitlines():
            raw = line.strip()
            if "loopback" not in raw.lower():
                continue
            digits = []
            for ch in raw:
                if ch.isdigit():
                    digits.append(ch)
                elif digits:
                    break
            if digits:
                return "".join(digits)
        return None
