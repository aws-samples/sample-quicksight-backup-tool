"""Quick Sight asset bundle import execution with strict terminal accounting."""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
import copy
import random
import time

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ProxyConnectionError,
    ReadTimeoutError,
)

from ..limits import (
    IMPORT_ACTION,
    IMPORT_TRANSPORT_INLINE,
    INLINE_IMPORT_MAX_BYTES,
    MAX_API_ATTEMPTS,
    RETRY_BASE_SECONDS,
    RETRY_CAP_SECONDS,
)
from ..models.config import RestoreConfig
from ..models.contracts import (
    BundleInventory,
    ImportJobResult,
    PlannedBundle,
    RestorePlan,
)
from ..models.errors import ArchiveValidationError
from .catalog import LegacyBackupCatalog

_NONTERMINAL = {
    "QUEUED_FOR_IMMEDIATE_EXECUTION",
    "IN_PROGRESS",
    "FAILED_ROLLBACK_IN_PROGRESS",
}
_FAILURE = {"FAILED", "FAILED_ROLLBACK_COMPLETED", "FAILED_ROLLBACK_ERROR"}
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
_TRANSIENT_ERROR_CODES = {
    "InternalFailure",
    "InternalServerError",
    "InternalServerException",
    "PriorRequestNotComplete",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "ServiceUnavailableException",
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
}
_NOT_FOUND_ERROR_CODES = {"ResourceNotFoundException", "NotFoundException", "404"}
_RECONCILABLE_START_ERROR_CODES = {
    "ConflictException",
    "ResourceExistsException",
    "ResourceInUseException",
}
_TRANSIENT_BOTOCORE_ERRORS = (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ProxyConnectionError,
    ReadTimeoutError,
)


class _RetryExhausted(Exception):
    def __init__(self, error: BaseException):
        super().__init__(str(error))
        self.error = error


class _RemoteStateUncertain(Exception):
    pass


class AssetBundleRestoreService:
    """Replay verified source bundles byte-for-byte and poll import jobs."""

    def __init__(
        self,
        config: RestoreConfig,
        catalog: LegacyBackupCatalog,
        quicksight_client: Any,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Optional[Callable[[float], float]] = None,
    ):
        self.config = config
        self.catalog = catalog
        self.quicksight = quicksight_client
        self.sleep = sleep
        self.monotonic = monotonic
        self.jitter = jitter or (lambda maximum: random.uniform(0.0, maximum))

    def restore_bundle(
        self,
        plan: RestorePlan,
        bundle: PlannedBundle,
        restore_id: str,
        index: int,
    ) -> ImportJobResult:
        start_clock = self.monotonic()
        deadline = start_clock + self.config.restore.poll_timeout_seconds
        started_at = self._now()
        job_id = self._job_id(restore_id, index, bundle)
        attempts = {"start": 0, "describe": 0}
        try:
            if bundle.execution_action != IMPORT_ACTION:
                raise ArchiveValidationError(
                    "Only planned import actions may start a Quick Sight import job"
                )
            if bundle.import_transport != IMPORT_TRANSPORT_INLINE:
                raise ArchiveValidationError(
                    "Unsupported sealed import transport: {0}".format(bundle.import_transport)
                )
            if bundle.size < 0 or bundle.size > INLINE_IMPORT_MAX_BYTES:
                raise ArchiveValidationError(
                    "Bundle size exceeds the {0}-byte inline import limit".format(
                        INLINE_IMPORT_MAX_BYTES
                    )
                )
            manifest_bundle = self._manifest_bundle(plan, bundle)
            source_bytes = self.catalog.read_and_verify_bundle(manifest_bundle)
            body = self._materialize(source_bytes, bundle, manifest_bundle)
            if len(body) != bundle.size or len(body) > INLINE_IMPORT_MAX_BYTES:
                raise ArchiveValidationError(
                    "Materialized bundle size does not match the sealed inline transport"
                )
            request: Dict[str, Any] = {
                "AwsAccountId": self.config.target.aws_account_id,
                "AssetBundleImportJobId": job_id,
                "AssetBundleImportSource": {"Body": body},
                "FailureAction": plan.failure_action,
            }
            request.update(copy.deepcopy(bundle.import_overrides))
            initial_response = self._start_or_reconcile(request, job_id, attempts, deadline)
        except _RemoteStateUncertain as error:
            return self._result(
                bundle=bundle,
                job_id=job_id,
                status="timeout",
                terminal_status="REMOTE_STATE_UNCERTAIN",
                started_at=started_at,
                start_clock=start_clock,
                errors=[{"Message": str(error)}],
                rollback_errors=[],
                attempts=attempts,
                reason=str(error),
            )
        except _RetryExhausted as error:
            message = "Start import retry budget exhausted: {0}".format(error.error)
            return self._result(
                bundle=bundle,
                job_id=job_id,
                status="failed",
                terminal_status="START_RETRY_EXHAUSTED",
                started_at=started_at,
                start_clock=start_clock,
                errors=[{"Message": message}],
                rollback_errors=[],
                attempts=attempts,
                reason=message,
            )
        except (ArchiveValidationError, BotoCoreError, ClientError, ValueError) as error:
            return self._result(
                bundle=bundle,
                job_id=job_id,
                status="failed",
                terminal_status="START_FAILED",
                started_at=started_at,
                start_clock=start_clock,
                errors=[{"Message": str(error)}],
                rollback_errors=[],
                attempts=attempts,
                reason=str(error),
            )
        return self._poll(
            bundle,
            job_id,
            started_at,
            start_clock,
            deadline,
            attempts,
            initial_response,
        )

    def _start_or_reconcile(
        self,
        request: Dict[str, Any],
        job_id: str,
        attempts: Dict[str, int],
        deadline: float,
    ) -> Optional[Dict[str, Any]]:
        last_error: Optional[BaseException] = None
        for retry_number in range(1, MAX_API_ATTEMPTS + 1):
            attempts["start"] += 1
            try:
                self.quicksight.start_asset_bundle_import_job(**request)
                return None
            except (BotoCoreError, ClientError) as error:
                last_error = error
                should_reconcile = (
                    self._is_retryable(error)
                    or self._error_code(error) in _RECONCILABLE_START_ERROR_CODES
                )
                if not should_reconcile:
                    raise
                try:
                    response = self._describe_with_retries(
                        job_id,
                        attempts,
                        deadline,
                        allow_not_found=True,
                    )
                except _RetryExhausted as describe_error:
                    raise _RemoteStateUncertain(
                        "Start response was ambiguous and job reconciliation exhausted its retry budget: {0}".format(
                            describe_error.error
                        )
                    )
                except (BotoCoreError, ClientError) as describe_error:
                    raise _RemoteStateUncertain(
                        "Start response was ambiguous and job reconciliation failed: {0}".format(
                            describe_error
                        )
                    )
                if response is not None:
                    return response
                if self._error_code(error) in _RECONCILABLE_START_ERROR_CODES:
                    raise
                if retry_number >= MAX_API_ATTEMPTS or not self._sleep_for_retry(
                    retry_number, deadline
                ):
                    break
        if last_error is None:
            last_error = RuntimeError("import start failed without an exception")
        raise _RetryExhausted(last_error)

    def _poll(
        self,
        bundle: PlannedBundle,
        job_id: str,
        started_at: str,
        start_clock: float,
        deadline: float,
        attempts: Dict[str, int],
        initial_response: Optional[Dict[str, Any]],
    ) -> ImportJobResult:
        delay = 1.0
        response = initial_response
        last_terminal_status = ""
        while True:
            if response is None:
                try:
                    response = self._describe_with_retries(
                        job_id, attempts, deadline, allow_not_found=False
                    )
                except _RetryExhausted as error:
                    message = "Import job state is uncertain after transient describe failures: {0}".format(
                        error.error
                    )
                    return self._result(
                        bundle,
                        job_id,
                        "timeout",
                        "REMOTE_STATE_UNCERTAIN",
                        started_at,
                        start_clock,
                        [{"Message": message}],
                        [],
                        attempts,
                        message,
                    )
                except (BotoCoreError, ClientError) as error:
                    return self._result(
                        bundle,
                        job_id,
                        "failed",
                        "DESCRIBE_FAILED",
                        started_at,
                        start_clock,
                        [{"Message": str(error)}],
                        [],
                        attempts,
                        str(error),
                    )
            if response is None:
                message = "Describe import job unexpectedly returned no response"
                return self._result(
                    bundle,
                    job_id,
                    "failed",
                    "DESCRIBE_FAILED",
                    started_at,
                    start_clock,
                    [{"Message": message}],
                    [],
                    attempts,
                    message,
                )
            terminal_status = str(response.get("JobStatus") or "")
            last_terminal_status = terminal_status or last_terminal_status
            errors = self._json_records(response.get("Errors", []))
            rollback_errors = self._json_records(response.get("RollbackErrors", []))
            if terminal_status == "SUCCESSFUL":
                return self._result(
                    bundle,
                    job_id,
                    "success",
                    terminal_status,
                    started_at,
                    start_clock,
                    errors,
                    rollback_errors,
                    attempts,
                    "Quick Sight import job completed successfully",
                )
            if terminal_status in _FAILURE:
                reason = "Quick Sight import job ended in {0}".format(terminal_status)
                return self._result(
                    bundle,
                    job_id,
                    "failed",
                    terminal_status,
                    started_at,
                    start_clock,
                    errors,
                    rollback_errors,
                    attempts,
                    reason,
                )
            if terminal_status not in _NONTERMINAL:
                message = "Unknown import job status; failed closed"
                errors.append({"Message": message})
                return self._result(
                    bundle,
                    job_id,
                    "failed",
                    terminal_status or "MISSING_STATUS",
                    started_at,
                    start_clock,
                    errors,
                    rollback_errors,
                    attempts,
                    message,
                )
            now = self.monotonic()
            if now >= deadline:
                message = "Import polling timed out after {0} seconds".format(
                    self.config.restore.poll_timeout_seconds
                )
                errors.append({"Message": message})
                return self._result(
                    bundle,
                    job_id,
                    "timeout",
                    last_terminal_status or "REMOTE_STATE_UNCERTAIN",
                    started_at,
                    start_clock,
                    errors,
                    rollback_errors,
                    attempts,
                    message,
                )
            remaining = max(0.0, deadline - now)
            self.sleep(min(delay, remaining))
            delay = min(delay * 2.0, 5.0)
            response = None

    def _describe_with_retries(
        self,
        job_id: str,
        attempts: Dict[str, int],
        deadline: float,
        allow_not_found: bool,
    ) -> Optional[Dict[str, Any]]:
        last_error: Optional[BaseException] = None
        for retry_number in range(1, MAX_API_ATTEMPTS + 1):
            attempts["describe"] += 1
            try:
                return self.quicksight.describe_asset_bundle_import_job(
                    AwsAccountId=self.config.target.aws_account_id,
                    AssetBundleImportJobId=job_id,
                )
            except (BotoCoreError, ClientError) as error:
                if allow_not_found and self._is_not_found(error):
                    return None
                if not self._is_retryable(error):
                    raise
                last_error = error
                if retry_number >= MAX_API_ATTEMPTS or not self._sleep_for_retry(
                    retry_number, deadline
                ):
                    break
        if last_error is None:
            last_error = RuntimeError("describe failed without an exception")
        raise _RetryExhausted(last_error)

    def _sleep_for_retry(self, retry_number: int, deadline: float) -> bool:
        remaining = deadline - self.monotonic()
        if remaining <= 0:
            return False
        maximum = min(RETRY_BASE_SECONDS * (2 ** (retry_number - 1)), RETRY_CAP_SECONDS)
        jittered = self.jitter(maximum)
        delay = min(max(0.0, float(jittered)), maximum, remaining)
        self.sleep(delay)
        return self.monotonic() < deadline

    @classmethod
    def _is_retryable(cls, error: BaseException) -> bool:
        if isinstance(error, _TRANSIENT_BOTOCORE_ERRORS):
            return True
        if not isinstance(error, ClientError):
            return False
        code = cls._error_code(error)
        if code in _TRANSIENT_ERROR_CODES or code.lower().startswith("throttl"):
            return True
        metadata = error.response.get("ResponseMetadata", {})
        try:
            status = int(metadata.get("HTTPStatusCode", 0))
        except (TypeError, ValueError):
            status = 0
        return status in _TRANSIENT_STATUS_CODES

    @classmethod
    def _is_not_found(cls, error: BaseException) -> bool:
        return isinstance(error, ClientError) and cls._error_code(error) in _NOT_FOUND_ERROR_CODES

    @staticmethod
    def _error_code(error: BaseException) -> str:
        if not isinstance(error, ClientError):
            return ""
        return str(error.response.get("Error", {}).get("Code", ""))

    @staticmethod
    def _materialize(
        source: bytes,
        planned: PlannedBundle,
        manifest: BundleInventory,
    ) -> bytes:
        if planned.materialization_mode != "original":
            raise ArchiveValidationError("Only byte-for-byte original bundle replay is supported")
        expected_names = sorted(member.member_name for member in manifest.members)
        expected_resources = sorted(member.resource_key for member in manifest.members)
        if planned.omitted_member_names:
            raise ArchiveValidationError("Plan attempts unsupported member-level bundle rewriting")
        if sorted(planned.selected_member_names) != expected_names:
            raise ArchiveValidationError("Planned members do not cover the complete source archive")
        if sorted(planned.selected_resources) != expected_resources:
            raise ArchiveValidationError(
                "Planned resources do not match the complete source archive"
            )
        return source

    @staticmethod
    def _manifest_bundle(plan: RestorePlan, bundle: PlannedBundle) -> BundleInventory:
        matches = [item for item in plan.manifest.bundles if item.key == bundle.key]
        if len(matches) != 1:
            raise ArchiveValidationError(
                "Planned bundle does not resolve to exactly one manifest artifact: {0}".format(
                    bundle.key
                )
            )
        manifest = matches[0]
        expected = (
            bundle.bucket,
            bundle.version_id,
            bundle.size,
            bundle.sha256,
            bundle.root_type,
        )
        actual = (
            manifest.bucket,
            manifest.version_id,
            manifest.size,
            manifest.sha256,
            manifest.root_type,
        )
        if expected != actual:
            raise ArchiveValidationError("Planned bundle fingerprint differs from manifest")
        return manifest

    @staticmethod
    def _job_id(restore_id: str, index: int, bundle: PlannedBundle) -> str:
        safe_restore = "".join(
            character if character.isalnum() or character in "-_" else "-"
            for character in restore_id
        )
        return "{0}-{1:03d}-{2}".format(safe_restore[:420], index, bundle.sha256[:16])

    def _result(
        self,
        bundle: PlannedBundle,
        job_id: str,
        status: str,
        terminal_status: str,
        started_at: str,
        start_clock: float,
        errors: List[Dict[str, Any]],
        rollback_errors: List[Dict[str, Any]],
        attempts: Dict[str, int],
        reason: str,
    ) -> ImportJobResult:
        outcome = {
            "success": "imported",
            "timeout": "timed_out",
        }.get(status, "failed")
        return ImportJobResult(
            bundle_key=bundle.key,
            job_id=job_id,
            status=status,
            terminal_status=terminal_status,
            selected_member_count=len(bundle.selected_member_names),
            started_at=started_at,
            completed_at=self._now(),
            duration_seconds=max(0.0, self.monotonic() - start_clock),
            errors=errors,
            rollback_errors=rollback_errors,
            outcome=outcome,
            attempted=True,
            attempts=dict(attempts),
            prerequisite_bundle_keys=list(bundle.prerequisite_bundle_keys),
            reason=reason,
        )

    @classmethod
    def _json_records(cls, values: Any) -> List[Dict[str, Any]]:
        if not isinstance(values, list):
            return [{"Message": str(values)}]
        records: List[Dict[str, Any]] = []
        for value in values:
            if isinstance(value, dict):
                records.append(cls._json_safe(value))
            else:
                records.append({"Message": str(value)})
        return records

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): cls._json_safe(child) for key, child in value.items()}
        if isinstance(value, list):
            return [cls._json_safe(child) for child in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()
