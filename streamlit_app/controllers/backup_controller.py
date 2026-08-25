"""Controller that wraps the existing backup orchestrator for local UI use."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterator, Optional
import logging
import os
import threading

from quicksight_backup.models.backup_result import BackupReport
from quicksight_backup.orchestrator import QuickSightBackupOrchestrator

_PROFILE_LOCK = threading.RLock()


@dataclass
class BackupExecution:
    report: BackupReport
    manifest: dict[str, Any]
    manifest_path: Path
    report_path: Path
    output_directory: Path


class _ProgressHandler(logging.Handler):
    def __init__(self, callback: Callable[[str], None]):
        super().__init__(level=logging.INFO)
        self.callback = callback

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith("quicksight_backup"):
            try:
                self.callback(record.getMessage())
            except Exception:
                return


@contextmanager
def _selected_profile(profile: str) -> Iterator[None]:
    if not profile:
        raise ValueError("an AWS profile is required")
    with _PROFILE_LOCK:
        names = ("AWS_PROFILE", "AWS_DEFAULT_PROFILE")
        previous = {name: os.environ.get(name) for name in names}
        os.environ["AWS_PROFILE"] = profile
        os.environ["AWS_DEFAULT_PROFILE"] = profile
        try:
            yield
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


class BackupController:
    """Run existing backup operations without changing backend behavior."""

    def __init__(self, progress: Optional[Callable[[str], None]] = None):
        self.progress = progress or (lambda _message: None)

    def _orchestrator(self, config_path: Path, mode: str) -> QuickSightBackupOrchestrator:
        if mode not in ("full", "users-only", "assets-only"):
            raise ValueError("unsupported backup mode")
        return QuickSightBackupOrchestrator(
            SimpleNamespace(config=str(config_path.resolve()), mode=mode)
        )

    def dry_run(self, config_path: Path, profile: str, mode: str) -> None:
        self.progress("Validating configuration, credentials, connectivity, and prerequisites...")
        handler = _ProgressHandler(self.progress)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            with _selected_profile(profile):
                self._orchestrator(config_path, mode).initialize()
        finally:
            root_logger.removeHandler(handler)
        self.progress("Backup dry-run passed. No AWS resources were changed.")

    def run(
        self,
        config_path: Path,
        profile: str,
        mode: str,
        output_directory: Path,
    ) -> BackupExecution:
        output_directory.mkdir(parents=True, exist_ok=True)
        handler = _ProgressHandler(self.progress)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            with _selected_profile(profile):
                orchestrator = self._orchestrator(config_path, mode)
                self.progress("Initializing backup services...")
                orchestrator.initialize()
                self.progress("Executing {0} backup...".format(mode))
                report = orchestrator.run_backup()
                timestamp = report.start_time.strftime("%Y%m%d_%H%M%S")
                manifest_path = output_directory / "backup_manifest_{0}.json".format(timestamp)
                report_path = output_directory / "backup_report_{0}.txt".format(timestamp)
                manifest = orchestrator.generate_backup_manifest(str(manifest_path))
                orchestrator.save_backup_report(str(report_path))
        finally:
            root_logger.removeHandler(handler)
        self.progress(
            "Backup completed with {0:.1f}% resource success.".format(report.resource_success_rate)
        )
        return BackupExecution(
            report=report,
            manifest=manifest,
            manifest_path=manifest_path,
            report_path=report_path,
            output_directory=output_directory,
        )
