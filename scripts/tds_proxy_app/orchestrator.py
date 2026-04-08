from __future__ import annotations

import argparse
import atexit
import signal
import threading
from pathlib import Path
from typing import Optional

from .capture import CaptureSession
from .common import (
    BackendSpec,
    PhaseState,
    RunLogger,
    ansi_green,
    load_sql_credentials,
    now_tag,
    repo_root,
)
from .azure import AzureSqlEdgeManager
from .proxy import TdsProxyServer
from .playground import PlaygroundServerManager


class Orchestrator:
    def __init__(
        self,
        root: Path,
        run_dir: Path,
        proxy_host: str,
        proxy_port: int,
        azure_port: int,
        playground_port: int,
        use_pcap: bool,
        interface_override: Optional[str],
        playground_tls: bool,
    ) -> None:
        self.root = root
        self.run_dir = run_dir
        self.capture_dir = run_dir / "captures"
        self.capture_dir.mkdir(parents=True, exist_ok=True)
        self.logger = RunLogger(run_dir / "tds_proxy.log")
        self.credentials = load_sql_credentials(root)
        self.stop_event = threading.Event()
        self.phase = PhaseState(BackendSpec("azure", "localhost", azure_port))
        self.proxy = TdsProxyServer(self.logger, self.phase, proxy_host, proxy_port, self.stop_event)
        self.azure_manager = AzureSqlEdgeManager(self.logger)
        self.playground_manager = PlaygroundServerManager(self.logger, root, run_dir)
        self.azure = BackendSpec("azure", "localhost", azure_port)
        self.playground = BackendSpec("playground", "127.0.0.1", playground_port)
        self.use_pcap = use_pcap
        self.interface_override = interface_override
        self.playground_tls = playground_tls
        self.capture: Optional[CaptureSession] = None
        self.proxy_thread = threading.Thread(target=self.proxy.start, daemon=True, name="tds-proxy")
        self.cleaned = False

    def run(self) -> None:
        self._register_cleanup()
        self.logger.line(f"Run directory: {self.run_dir}")
        self.logger.line(f"Log file: {self.run_dir / 'tds_proxy.log'}")
        self.logger.line(f"Proxy endpoint: {self.proxy.host}:{self.proxy.port}")
        self.logger.line(f"Azure backend: {self.azure.host}:{self.azure.port}")
        self.logger.line(f"Playground backend: {self.playground.host}:{self.playground.port}")
        self.logger.line_console(
            f"SSMS login: {self.credentials.user} / {self.credentials.password}",
            f"SSMS login: {self.credentials.user} / {ansi_green(self.credentials.password)}",
        )
        self.logger.line("Same credentials apply to both phases.")
        self.logger.blank()
        self.proxy_thread.start()
        self._phase_azure()
        self._prompt(
            f"Phase 1 is live. Connect SSMS to 127.0.0.1:{self.proxy.port}, inspect the tree, "
            "then press Enter to switch to the playground phase..."
        )
        self._phase_playground()
        self._prompt(
            f"Phase 2 is live. Reconnect SSMS to 127.0.0.1:{self.proxy.port}, inspect again, "
            "then press Enter to finish..."
        )

    def cleanup(self) -> None:
        if self.cleaned:
            return
        self.cleaned = True
        self.stop_event.set()
        if self.capture is not None:
            self.capture.stop()
            self.capture = None
        self.proxy.stop()
        try:
            self.proxy_thread.join(timeout=5)
        except Exception:
            pass
        self.playground_manager.cleanup()
        self.azure_manager.cleanup()
        self.logger.line("Cleanup finished")
        self.logger.close()

    def _phase_azure(self) -> None:
        self.logger.line("=== PHASE 1: Azure SQL Edge ===")
        self.phase.set("azure", self.azure)
        self.capture = self._start_capture("azure", self.azure.port)
        self.azure_manager.start()
        self._log_capture()

    def _phase_playground(self) -> None:
        self.logger.line("=== PHASE 2: tsql-server playground ===")
        if self.capture is not None:
            self.capture.stop()
            self.capture = None
        self.azure_manager.stop()
        self.phase.set("playground", self.playground)
        self.capture = self._start_capture("playground", self.playground.port)
        self.playground_manager.build()
        self.playground_manager.start(self.playground_tls, self.playground.port)
        self._log_capture()

    def _start_capture(self, phase: str, backend_port: int) -> CaptureSession:
        capture = CaptureSession(
            self.logger,
            self.capture_dir,
            phase,
            self.proxy.port,
            backend_port,
            self.interface_override,
        )
        if self.use_pcap:
            capture.start()
        else:
            self.logger.line(f"[{phase}] pcap capture disabled by flag")
        return capture

    def _log_capture(self) -> None:
        if self.capture is not None and self.capture.outfile is not None:
            self.logger.line(f"pcap output: {self.capture.outfile}")

    def _prompt(self, msg: str) -> None:
        self.logger.line(msg)
        try:
            input()
        except EOFError:
            self.logger.line("stdin closed; continuing")

    def _register_cleanup(self) -> None:
        atexit.register(self.cleanup)
        for sig in ("SIGINT", "SIGTERM", "SIGBREAK"):
            value = getattr(signal, sig, None)
            if value is not None:
                signal.signal(value, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:  # noqa: ANN001
        self.logger.line(f"Received signal {signum}; shutting down")
        self.cleanup()
        raise SystemExit(130)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Two-phase TDS proxy and capture orchestrator for SSMS",
    )
    parser.add_argument("--proxy-host", default="127.0.0.1")
    parser.add_argument("--proxy-port", type=int, default=1433)
    parser.add_argument("--azure-port", type=int, default=11433)
    parser.add_argument("--playground-port", type=int, default=14330)
    parser.add_argument("--run-dir", default=None, help="Output directory for logs and pcap files")
    parser.add_argument(
        "--capture-interface",
        default=None,
        help="Wireshark interface number or name. Defaults to loopback auto-detection.",
    )
    parser.add_argument(
        "--no-pcap",
        action="store_true",
        help="Disable dumpcap/tshark capture and keep proxy logs only.",
    )
    parser.add_argument(
        "--playground-no-tls",
        action="store_true",
        help="Start the playground without TLS.",
    )
    return parser.parse_args()


def run_from_args(args: argparse.Namespace) -> int:
    root = repo_root()
    run_dir = Path(args.run_dir) if args.run_dir else root / "logs" / "tds_proxy_runs" / now_tag()
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "captures").mkdir(parents=True, exist_ok=True)

    orchestrator = Orchestrator(
        root=root,
        run_dir=run_dir,
        proxy_host=args.proxy_host,
        proxy_port=args.proxy_port,
        azure_port=args.azure_port,
        playground_port=args.playground_port,
        use_pcap=not args.no_pcap,
        interface_override=args.capture_interface,
        playground_tls=not args.playground_no_tls,
    )

    try:
        orchestrator.run()
    except KeyboardInterrupt:
        orchestrator.logger.line("Interrupted by user")
        orchestrator.cleanup()
        return 130
    except Exception as exc:
        try:
            orchestrator.logger.line(f"Fatal error: {exc}")
        finally:
            orchestrator.cleanup()
        return 1
    finally:
        orchestrator.cleanup()
    return 0
