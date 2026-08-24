"""Atomic, digest-verified persistence for restore reports."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import hmac
import json
import os
import re
import secrets
import tempfile

from ..json_safety import loads_strict_json
from ..limits import MAX_REPORT_BYTES
from ..local_paths import read_bounded_regular_file, reject_link_components
from ..models.contracts import RestoreReport
from ..models.errors import RestoreExecutionError

_MAX_RESERVATION_BYTES = 4096
_RESERVATION_VERSION = "1.0"


@dataclass
class _Reservation:
    token: str
    last_digest: Optional[str] = None


class RestoreReportService:
    """Persist and retrieve machine-readable reports by restore ID."""

    def __init__(self, report_directory: str):
        try:
            self.directory = reject_link_components(
                Path(report_directory).expanduser(),
                "restore report directory",
                allow_missing=True,
            )
        except (OSError, ValueError) as error:
            raise RestoreExecutionError(str(error))
        self._reservations: Dict[str, _Reservation] = {}

    def reserve(self, restore_id: str) -> Path:
        """Reserve a report ID before any target mutation."""

        destination = self.path_for(restore_id)
        reservation_path = self._reservation_path(restore_id)
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            reject_link_components(
                self.directory,
                "restore report directory",
                allow_missing=False,
            )
            if not self.directory.is_dir():
                raise ValueError("restore report directory is not a directory")
            if self._lexists(destination):
                raise RestoreExecutionError("Restore report already exists: {0}".format(restore_id))
            token = secrets.token_hex(32)
            encoded = (
                json.dumps(
                    {
                        "restore_id": restore_id,
                        "token": token,
                        "version": _RESERVATION_VERSION,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            descriptor = os.open(
                str(reservation_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            raise RestoreExecutionError(
                "Restore report ID is already reserved: {0}".format(restore_id)
            )
        except RestoreExecutionError:
            raise
        except (OSError, ValueError) as error:
            raise RestoreExecutionError(
                "Unable to reserve restore report {0}: {1}".format(restore_id, error)
            )

        record = _Reservation(token=token)
        self._reservations[restore_id] = record
        self._fsync_directory()
        if self._lexists(destination):
            self._remove_owned_reservation(restore_id, record)
            self._reservations.pop(restore_id, None)
            raise RestoreExecutionError(
                "Restore report appeared while its ID was being reserved: {0}".format(restore_id)
            )
        return destination

    def save(self, report: RestoreReport) -> Path:
        """Persist a complete one-shot report without overwriting audit history."""

        self.reserve(report.restore_id)
        try:
            return self.save_checkpoint(report, final=True)
        except Exception as error:
            record = self._reservations.get(report.restore_id)
            if record is not None and record.last_digest is None:
                try:
                    self.cancel_reservation(report.restore_id)
                except RestoreExecutionError as cleanup_error:
                    raise RestoreExecutionError(
                        "Unable to persist restore report and release its reservation: {0}; {1}".format(
                            error, cleanup_error
                        )
                    ) from error
            raise

    def save_checkpoint(self, report: RestoreReport, final: bool = False) -> Path:
        """Atomically write one owned running/final report checkpoint."""

        terminal = report.overall_status != "running"
        if final != terminal:
            raise RestoreExecutionError("Restore report final flag does not match overall_status")
        record = self._reservations.get(report.restore_id)
        if record is None:
            raise RestoreExecutionError(
                "Restore report ID is not reserved by this execution: {0}".format(report.restore_id)
            )
        destination = self.path_for(report.restore_id)
        try:
            report.seal()
            serialized = (
                json.dumps(
                    report.to_dict(),
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            )
            encoded = serialized.encode("utf-8")
        except (TypeError, ValueError) as error:
            raise RestoreExecutionError("Invalid restore report: {0}".format(error))
        if len(encoded) > MAX_REPORT_BYTES:
            raise RestoreExecutionError("Restore report exceeds the local size limit")

        self._validate_owned_reservation(report.restore_id, record)
        self._validate_checkpoint_destination(report.restore_id, record)
        descriptor, temporary = tempfile.mkstemp(
            prefix=destination.name + ".", suffix=".tmp", dir=str(self.directory)
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            if record.last_digest is None:
                self._validate_owned_reservation(report.restore_id, record)
                self._validate_checkpoint_destination(report.restore_id, record)
                os.link(temporary, destination)
            else:
                # The sidecar is a cooperative ownership protocol. Validation and
                # replacement are not one conditional filesystem transaction, so
                # every retry rechecks both ownership and the prior checkpoint.
                from time import sleep

                replace_attempts = 5
                retry_delay_seconds = 0.01
                max_retry_delay_seconds = 0.1
                retryable_winerrors = {5, 32, 33}
                for attempt in range(replace_attempts):
                    self._validate_owned_reservation(report.restore_id, record)
                    self._validate_checkpoint_destination(report.restore_id, record)
                    try:
                        os.replace(temporary, destination)
                        break
                    except OSError as error:
                        if (
                            getattr(error, "winerror", None) not in retryable_winerrors
                            or attempt == replace_attempts - 1
                        ):
                            raise
                        sleep(retry_delay_seconds)
                        retry_delay_seconds = min(
                            retry_delay_seconds * 2,
                            max_retry_delay_seconds,
                        )
            record.last_digest = report.report_digest
            self._fsync_directory()
        except Exception as error:
            if isinstance(error, RestoreExecutionError):
                raise
            raise RestoreExecutionError(
                "Unable to persist restore report {0}: {1}".format(report.restore_id, error)
            )
        finally:
            try:
                os.unlink(temporary)
            except OSError:
                pass

        if final:
            try:
                self._remove_owned_reservation(report.restore_id, record)
            except RestoreExecutionError as error:
                raise RestoreExecutionError(
                    "Restore report was committed but reservation cleanup failed: {0}".format(error)
                ) from error
            self._reservations.pop(report.restore_id, None)
            self._fsync_directory()
        return destination

    def cancel_reservation(self, restore_id: str) -> None:
        """Release an owned reservation only before its first checkpoint."""

        record = self._reservations.get(restore_id)
        if record is None:
            raise RestoreExecutionError(
                "Restore report ID is not reserved by this execution: {0}".format(restore_id)
            )
        if record.last_digest is not None or self._lexists(self.path_for(restore_id)):
            raise RestoreExecutionError(
                "Cannot cancel a reservation after a report checkpoint exists"
            )
        self._remove_owned_reservation(restore_id, record)
        self._reservations.pop(restore_id, None)
        self._fsync_directory()

    def load(self, restore_id: str) -> RestoreReport:
        path = self.path_for(restore_id)
        if not self._lexists(path):
            raise RestoreExecutionError("Restore report not found: {0}".format(restore_id))
        try:
            encoded = read_bounded_regular_file(path, MAX_REPORT_BYTES, "restore report")
            raw = loads_strict_json(encoded.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise RestoreExecutionError("Unable to read restore report: {0}".format(error))
        if not isinstance(raw, dict):
            raise RestoreExecutionError("Restore report root must be an object")
        try:
            report = RestoreReport.from_dict(raw)
        except (KeyError, TypeError, ValueError) as error:
            raise RestoreExecutionError("Invalid restore report: {0}".format(error))
        if report.restore_id != restore_id:
            raise RestoreExecutionError("Restore report ID does not match its file name")
        return report

    def path_for(self, restore_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", restore_id or ""):
            raise RestoreExecutionError("Invalid restore ID")
        return self.directory / "restore-{0}.json".format(restore_id)

    def _reservation_path(self, restore_id: str) -> Path:
        self.path_for(restore_id)
        return self.directory / ".restore-{0}.reserve".format(restore_id)

    def _validate_owned_reservation(self, restore_id: str, record: _Reservation) -> None:
        path = self._reservation_path(restore_id)
        try:
            encoded = read_bounded_regular_file(
                path, _MAX_RESERVATION_BYTES, "restore report reservation"
            )
            value = loads_strict_json(encoded.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise RestoreExecutionError(
                "Unable to verify restore report reservation: {0}".format(error)
            )
        if (
            not isinstance(value, dict)
            or value.get("version") != _RESERVATION_VERSION
            or value.get("restore_id") != restore_id
            or not isinstance(value.get("token"), str)
            or not hmac.compare_digest(value["token"], record.token)
        ):
            raise RestoreExecutionError("Restore report reservation is not owned by this execution")

    def _validate_checkpoint_destination(self, restore_id: str, record: _Reservation) -> None:
        destination = self.path_for(restore_id)
        if record.last_digest is None:
            if self._lexists(destination):
                raise RestoreExecutionError(
                    "Unexpected restore report exists before the first checkpoint"
                )
            return
        existing = self.load(restore_id)
        if not hmac.compare_digest(existing.report_digest, record.last_digest):
            raise RestoreExecutionError("Restore report checkpoint changed outside this execution")

    def _remove_owned_reservation(self, restore_id: str, record: _Reservation) -> None:
        self._validate_owned_reservation(restore_id, record)
        try:
            self._reservation_path(restore_id).unlink()
        except OSError as error:
            raise RestoreExecutionError(
                "Unable to remove owned restore report reservation: {0}".format(error)
            )

    @staticmethod
    def _lexists(path: Path) -> bool:
        try:
            path.lstat()
            return True
        except FileNotFoundError:
            return False

    def _fsync_directory(self) -> None:
        try:
            descriptor = os.open(str(self.directory), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)
