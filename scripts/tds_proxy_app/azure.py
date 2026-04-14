from __future__ import annotations

import subprocess
import time
from typing import Optional

from .common import RunLogger, load_sql_credentials, q


class AzureSqlEdgeManager:
    def __init__(self, runlog: RunLogger) -> None:
        self.runlog = runlog
        self.container_name = "iridium_test_sqlserver"
        self.credentials = load_sql_credentials()
        self.sa_password = self.credentials.password

    def start(self) -> None:
        self.ensure_podman_machine()
        self._ensure_container_running()
        self._wait_for_ready()
        self.seed_minimal()

    def stop(self) -> None:
        self.runlog.line("Stopping Azure SQL Edge container")
        self._run(["podman", "stop", self.container_name], allow_failure=True)

    def cleanup(self) -> None:
        self.stop()

    def ensure_podman_machine(self) -> None:
        self.runlog.line("Checking Podman machine state")
        proc = self._run(["podman", "machine", "list"], allow_failure=True)
        if "Currently running" in (proc.stdout or ""):
            self.runlog.line("Podman machine already running")
            return
        self.runlog.line("Starting Podman machine")
        self._run(["podman", "machine", "start"])
        time.sleep(3)

    def seed_minimal(self) -> None:
        self.runlog.line("Seeding Azure SQL Edge with a minimal probe database")
        sqlcmd = self._resolve_container_sqlcmd()
        self._run(
            [
                "podman",
                "exec",
                self.container_name,
                sqlcmd,
                "-S",
                "localhost",
                "-U",
                "sa",
                "-P",
                self.sa_password,
                "-b",
                "-Q",
                "IF DB_ID(N'iridium_probe') IS NULL CREATE DATABASE [iridium_probe];",
            ]
        )
        self._run(
            [
                "podman",
                "exec",
                self.container_name,
                sqlcmd,
                "-S",
                "localhost",
                "-U",
                "sa",
                "-P",
                self.sa_password,
                "-d",
                "iridium_probe",
                "-b",
                "-Q",
                (
                    "IF OBJECT_ID(N'dbo.capture_probe', N'U') IS NULL "
                    "BEGIN CREATE TABLE dbo.capture_probe ("
                    "id INT NOT NULL CONSTRAINT PK_capture_probe PRIMARY KEY, "
                    "note NVARCHAR(100) NOT NULL"
                    "); END; "
                    "IF NOT EXISTS (SELECT 1 FROM dbo.capture_probe WHERE id = 1) "
                    "BEGIN INSERT INTO dbo.capture_probe (id, note) VALUES (1, N'probe'); END;"
                ),
            ]
        )

    def _ensure_container_running(self) -> None:
        ps = self._run(["podman", "ps", "-a", "--format", "{{.Names}}"], allow_failure=True)
        containers = {line.strip() for line in (ps.stdout or "").splitlines() if line.strip()}
        if self.container_name not in containers:
            self.runlog.line("Creating Azure SQL Edge container")
            self._run(
                [
                    "podman",
                    "run",
                    "-d",
                    "--name",
                    self.container_name,
                    "-e",
                    "ACCEPT_EULA=Y",
                    "-e",
                    f"MSSQL_SA_PASSWORD={self.sa_password}",
                    "-p",
                    "11433:1433",
                    "--memory=512m",
                    "mcr.microsoft.com/azure-sql-edge:latest",
                ]
            )
        else:
            current_password = self._inspect_container_password()
            if current_password is not None and current_password != self.sa_password:
                self.runlog.line("Azure SQL Edge container has stale password; recreating it")
                self._run(["podman", "rm", "-f", self.container_name], allow_failure=True)
                self._run(
                    [
                        "podman",
                        "run",
                        "-d",
                        "--name",
                        self.container_name,
                        "-e",
                        "ACCEPT_EULA=Y",
                        "-e",
                        f"MSSQL_SA_PASSWORD={self.sa_password}",
                        "-p",
                        "11433:1433",
                        "--memory=512m",
                        "mcr.microsoft.com/azure-sql-edge:latest",
                    ]
                )
                return
            running = self._run(
                ["podman", "ps", "--filter", f"name={self.container_name}", "--format", "{{.Names}}"],
                allow_failure=True,
            )
            if self.container_name not in (running.stdout or ""):
                self.runlog.line("Starting existing Azure SQL Edge container")
                self._run(["podman", "start", self.container_name])
            else:
                self.runlog.line("Azure SQL Edge container already running")

    def _resolve_container_sqlcmd(self) -> str:
        for candidate in ["/opt/mssql-tools/bin/sqlcmd", "/opt/mssql-tools18/bin/sqlcmd"]:
            probe = self._run(
                ["podman", "exec", self.container_name, "sh", "-lc", f"test -x {candidate} && echo found"],
                allow_failure=True,
            )
            if "found" in (probe.stdout or ""):
                return candidate
        raise RuntimeError("sqlcmd not found inside Azure SQL Edge container")

    def _inspect_container_password(self) -> Optional[str]:
        proc = self._run(
            [
                "podman",
                "inspect",
                "--format",
                "{{range .Config.Env}}{{println .}}{{end}}",
                self.container_name,
            ],
            allow_failure=True,
        )
        for line in (proc.stdout or "").splitlines():
            if line.startswith("MSSQL_SA_PASSWORD="):
                return line.split("=", 1)[1]
        return None

    def _wait_for_ready(self) -> None:
        self.runlog.line("Waiting for Azure SQL Edge to become ready")
        sqlcmd = self._resolve_container_sqlcmd()
        deadline = time.time() + 90
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            try:
                proc = self._run(
                    [
                        "podman",
                        "exec",
                        self.container_name,
                        sqlcmd,
                        "-S",
                        "localhost",
                        "-U",
                        "sa",
                        "-P",
                        self.sa_password,
                        "-Q",
                        "SELECT 1",
                    ],
                    allow_failure=True,
                )
                if proc.returncode == 0:
                    self.runlog.line("Azure SQL Edge is ready on localhost:11433")
                    return
            except Exception as exc:
                self.runlog.line(f"Azure readiness probe failed: {exc}")
            self.runlog.line(f"Azure readiness retry {attempt}")
            time.sleep(2)
        raise RuntimeError("Timed out waiting for Azure SQL Edge")

    def _run(
        self,
        cmd: list[str],
        cwd: Optional[str] = None,
        allow_failure: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        self.runlog.line("RUN: " + q(cmd))
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
        if proc.stdout:
            self.runlog.block("stdout> ", proc.stdout)
        if proc.stderr:
            self.runlog.block("stderr> ", proc.stderr)
        if proc.returncode != 0 and not allow_failure:
            raise RuntimeError(f"Command failed with exit code {proc.returncode}: {q(cmd)}")
        return proc

