from __future__ import annotations

import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

from .common import RunLogger, load_sql_credentials, q


class PlaygroundServerManager:
    def __init__(self, runlog: RunLogger, root: Path, run_dir: Path) -> None:
        self.runlog = runlog
        self.root = root
        self.run_dir = run_dir
        self.proc: Optional[subprocess.Popen[str]] = None
        self.reader: Optional[threading.Thread] = None
        self.credentials = load_sql_credentials(root)
        self.binary = self._find_binary()

    def build(self) -> None:
        cargo = shutil.which("cargo")
        if cargo is None:
            home = Path(os.environ.get("USERPROFILE", str(Path.home())))
            cargo = str(home / ".cargo" / "bin" / "cargo.exe")
        cargo_path = Path(cargo)
        if not cargo_path.exists():
            raise RuntimeError(f"cargo not found at {cargo}")
        self.runlog.line("Building tsql-server playground binary")
        self._run(
            [str(cargo_path), "build", "--package", "tsql_server", "--bin", "tsql-server"],
            cwd=str(self.root),
        )
        self.binary = self._find_binary()

    def start(self, tls_enabled: bool, port: int) -> None:
        if self.proc is not None:
            self.stop()
        if self.binary is None:
            raise RuntimeError("tsql-server playground binary not found")
        args = [
            str(self.binary),
            "--playground",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--user",
            self.credentials.user,
            "--password",
            self.credentials.password,
        ]
        args.append("--tls-gen" if tls_enabled else "--no-tls")
        self.runlog.line(
            f"Starting tsql-server playground on 127.0.0.1:{port} "
            + ("with TLS" if tls_enabled else "without TLS")
        )
        self.runlog.line(
            f"Playground credentials: {self.credentials.user} / {self.credentials.password}"
        )
        self.runlog.line(f"Playground binary: {self.binary}")
        self.proc = subprocess.Popen(
            args,
            cwd=str(self.run_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.reader = self._start_stdout_reader(self.proc)
        self._wait_for_port("127.0.0.1", port, "playground")

    def stop(self) -> None:
        if self.proc is None:
            return
        self.runlog.line("Stopping tsql-server playground")
        proc = self.proc
        self.proc = None
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        except Exception as exc:
            self.runlog.line(f"Playground shutdown error: {exc}")
        if self.reader is not None:
            self.reader.join(timeout=2)
            self.reader = None

    def cleanup(self) -> None:
        self.stop()

    def _find_binary(self) -> Optional[Path]:
        exe = "tsql-server.exe" if os.name == "nt" else "tsql-server"
        for candidate in [self.root / "target" / "debug" / exe, self.root / "target" / "release" / exe]:
            if candidate.exists():
                return candidate
        return self.root / "target" / "debug" / exe

    def _run(
        self,
        cmd: list[str],
        cwd: Optional[str] = None,
    ) -> subprocess.CompletedProcess[str]:
        self.runlog.line("RUN: " + q(cmd))
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
        if proc.stdout:
            self.runlog.block("stdout> ", proc.stdout)
        if proc.stderr:
            self.runlog.block("stderr> ", proc.stderr)
        if proc.returncode != 0:
            raise RuntimeError(f"Command failed with exit code {proc.returncode}: {q(cmd)}")
        return proc

    def _wait_for_port(self, host: str, port: int, label: str) -> None:
        self.runlog.line(f"Waiting for {label} listener on {host}:{port}")
        deadline = time.time() + 60
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            try:
                with socket.create_connection((host, port), timeout=2):
                    self.runlog.line(f"{label} is ready on {host}:{port}")
                    return
            except OSError:
                self.runlog.line(f"{label} readiness retry {attempt}")
                time.sleep(1)
        raise RuntimeError(f"Timed out waiting for {label} on {host}:{port}")

    def _start_stdout_reader(self, proc: subprocess.Popen[str]) -> threading.Thread:
        def reader() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    self.runlog.block("[playground] ", line)

        thread = threading.Thread(target=reader, name="playground-log-reader", daemon=True)
        thread.start()
        return thread
