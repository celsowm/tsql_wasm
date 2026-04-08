"""Compatibility layer for older imports.

The real responsibilities now live in:
- `azure.py` for Azure SQL Edge lifecycle
- `playground.py` for tsql-server playground lifecycle
"""

from .azure import AzureSqlEdgeManager
from .playground import PlaygroundServerManager


class SqlServerController:
    """Backward-compatible wrapper around the split managers."""

    def __init__(self, runlog, root, run_dir):  # noqa: ANN001
        self.azure = AzureSqlEdgeManager(runlog)
        self.playground = PlaygroundServerManager(runlog, root, run_dir)

    def start_azure(self) -> None:
        self.azure.start()

    def stop_azure(self) -> None:
        self.azure.stop()

    def build_playground(self) -> None:
        self.playground.build()

    def start_playground(self, tls_enabled: bool) -> None:
        self.playground.start(tls_enabled)

    def stop_playground(self) -> None:
        self.playground.stop()

    def cleanup(self) -> None:
        self.playground.cleanup()
        self.azure.cleanup()
