"""Coordinate catalog, planning, identity restore, imports, and reporting."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence
import hashlib
import json
import time
import uuid

from .json_safety import loads_strict_json
from .local_paths import (
    read_bounded_regular_file,
    reject_link_components,
    resolve_under_root,
)
from .limits import (
    IMPORT_ACTION,
    IMPORT_TRANSPORT_INLINE,
    IMPORT_TRANSPORT_NONE,
    INLINE_IMPORT_MAX_BYTES,
    MAX_OVERRIDES_BYTES,
    SKIP_POLICY_ACTION,
)
from .models.config import RestoreConfig
from .models.contracts import (
    ImportJobResult,
    PlannedBundle,
    RestorePlan,
    RestoreReport,
    TargetFingerprint,
    canonical_json,
)
from .models.errors import PlanIntegrityError, RestoreExecutionError
from .services.asset_bundle import AssetBundleRestoreService
from .services.catalog import LegacyBackupCatalog
from .services.planner import QuickSightTargetInventory, RestorePlanner
from .services.report import RestoreReportService
from .services.identities import UserGroupRestoreService
from .session_factory import SessionFactory


class RestoreOrchestrator:
    """Execute the P0 restore workflow without changing legacy backup behavior."""

    def __init__(
        self,
        config: RestoreConfig,
        session_factory: Optional[SessionFactory] = None,
        source_s3_client: Optional[Any] = None,
        source_dynamodb_client: Optional[Any] = None,
        target_quicksight_client: Optional[Any] = None,
        target_iam_client: Optional[Any] = None,
        sleep: Any = time.sleep,
        monotonic: Any = time.monotonic,
        progress_callback: Optional[Callable[[str], None]] = None,
    ):
        self.config = config
        self.factory = session_factory or SessionFactory(
            config.source_backup.auth, config.target.auth
        )
        self.source_s3 = source_s3_client
        self.source_dynamodb = source_dynamodb_client
        self.target_quicksight = target_quicksight_client
        self.target_identity_quicksight = (
            target_quicksight_client
            if target_quicksight_client is not None
            and config.target.identity_region == config.target.asset_region
            else None
        )
        self.target_iam = target_iam_client
        self.catalog = (
            LegacyBackupCatalog(config.source_backup, self.source_s3, self.source_dynamodb)
            if self.source_s3 is not None and self.source_dynamodb is not None
            else None
        )
        self.report_service = RestoreReportService(config.restore.report_directory)
        self.sleep = sleep
        self.monotonic = monotonic
        self.progress_callback = progress_callback

    def _emit_progress(self, message: str) -> None:
        """Publish optional human-facing progress without affecting restore execution."""
        if self.progress_callback is None:
            return
        try:
            self.progress_callback(message)
        except Exception:
            # Progress display is advisory and must never change restore behavior.
            return

    def plan(
        self,
        output_path: str,
        backup_date: Optional[str] = None,
        bundle_keys: Optional[Sequence[str]] = None,
    ) -> RestorePlan:
        include_assets = self.config.restore.mode != "identities-only"
        include_identities = self.config.restore.restore_identities
        destination = self._resolve_local_operation_path(
            output_path,
            "restore plan output",
            must_exist=False,
            protect_inputs=True,
        )
        overrides = self._load_overrides()
        self._ensure_catalog(include_assets, include_identities)
        if include_assets:
            self._ensure_asset_quicksight()
        if self.config.restore.validate_target_principals and self.config.restore.target_principals:
            self._ensure_identity_quicksight()
        if self.catalog is None:
            raise RestoreExecutionError("Restore catalog clients were not initialized")
        manifest = self.catalog.build_manifest(
            backup_date=backup_date,
            explicit_keys=bundle_keys,
            include_assets=include_assets,
            include_identities=include_identities,
        )
        planner = RestorePlanner(
            self.config, self.target_quicksight, self.target_identity_quicksight
        )
        plan = planner.build_plan(manifest, overrides)
        planner.save_plan(plan, str(destination))
        return plan

    def run(self, plan_path: str) -> RestoreReport:
        resolved_plan_path = self._resolve_local_operation_path(
            plan_path,
            "restore plan input",
            must_exist=True,
            protect_inputs=False,
        )
        plan = RestorePlanner.load_plan(str(resolved_plan_path))
        self._verify_plan(plan)
        restore_id = "{0}-{1}".format(
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"), uuid.uuid4().hex[:12]
        )
        self._emit_progress("Starting restore {0}.".format(restore_id))
        start_clock = self.monotonic()
        started_at = datetime.now(timezone.utc).isoformat()
        identity_result = None
        jobs_by_key: Dict[str, ImportJobResult] = {}
        errors: List[str] = []
        warnings = list(plan.warnings)
        execution_stage = "artifact_preflight"
        execution_bundle_key: Optional[str] = None
        interrupted = False
        validation_results = [
            {
                "name": "source_artifact_preflight",
                "status": "not_run",
                "details": "Selected source artifacts have not been reverified",
            },
            {
                "name": "identity_source_preflight",
                "status": "not_run",
                "details": (
                    "Identity source verification has not run"
                    if plan.restore_identities
                    else "Identity restore was not selected"
                ),
            },
            {
                "name": "target_state_preflight",
                "status": "not_run",
                "details": "Target conflict observations have not been revalidated",
            },
            {
                "name": "post_restore_workload_validation",
                "status": "not_run",
                "details": "Representative workload validation is operator-owned and was not run by this tool",
            },
        ]

        def set_validation(name: str, status: str, details: str) -> None:
            for item in validation_results:
                if item["name"] == name:
                    item["status"] = status
                    item["details"] = details
                    return

        def checkpoint() -> None:
            running_jobs = [
                jobs_by_key.get(bundle.key)
                or self._synthetic_job_result(
                    bundle=bundle,
                    status="pending",
                    outcome="pending",
                    terminal_status="PENDING",
                    reason="Awaiting restore execution",
                    attempted=False,
                )
                for bundle in plan.bundles
            ]
            running = self._build_report(
                plan=plan,
                restore_id=restore_id,
                overall_status="running",
                started_at=started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                duration_seconds=max(0.0, self.monotonic() - start_clock),
                identity_result=identity_result,
                jobs=running_jobs,
                warnings=warnings,
                errors=errors,
                validation_results=validation_results,
            )
            self.report_service.save_checkpoint(running, final=False)

        # Report destination failures are detected before source checks or target mutation.
        self.report_service.reserve(restore_id)
        try:
            checkpoint()
        except Exception as error:
            try:
                self.report_service.cancel_reservation(restore_id)
            except RestoreExecutionError as cleanup_error:
                raise RestoreExecutionError(
                    "Initial report checkpoint failed and its reservation could not be released: {0}; {1}".format(
                        error, cleanup_error
                    )
                ) from error
            raise

        try:
            self._emit_progress(
                "Preflight 1/3: verifying {0} source bundle(s) and checksums...".format(
                    len(plan.bundles)
                )
            )
            self._ensure_catalog(bool(plan.bundles), plan.restore_identities)
            if self.catalog is None:
                raise RestoreExecutionError("Restore catalog clients were not initialized")
            manifest_by_key = {item.key: item for item in plan.manifest.bundles}
            for planned in plan.bundles:
                execution_bundle_key = planned.key
                manifest_bundle = manifest_by_key.get(planned.key)
                if manifest_bundle is None:
                    raise PlanIntegrityError(
                        "Plan references a bundle missing from its manifest: {0}".format(
                            planned.key
                        )
                    )
                self.catalog.read_and_verify_bundle(manifest_bundle)
            set_validation(
                "source_artifact_preflight",
                "passed",
                "Every selected version, size, checksum, and member inventory was reverified",
            )
            self._emit_progress("Preflight 1/3 passed: source artifacts verified.")

            identity_snapshot = None
            if plan.restore_identities:
                self._emit_progress(
                    "Preflight 2/3: verifying users, groups, and memberships snapshot..."
                )
                execution_stage = "identity_source_preflight"
                execution_bundle_key = None
                identity_snapshot = plan.manifest.identity_snapshot
                if identity_snapshot is None:
                    raise PlanIntegrityError(
                        "Identity restore plan does not contain a sealed source snapshot"
                    )
                self.catalog.verify_identity_snapshot(identity_snapshot)
                set_validation(
                    "identity_source_preflight",
                    "passed",
                    "The sealed identity table snapshot was reverified",
                )
                self._emit_progress("Preflight 2/3 passed: identity snapshot verified.")
            else:
                self._emit_progress("Preflight 2/3 skipped: identities were not selected.")

            self._emit_progress("Preflight 3/3: rechecking target conflicts and drift...")
            execution_stage = "target_state_preflight"
            if plan.conflict_decisions:
                self._ensure_asset_quicksight()
            self._verify_target_state(plan)
            set_validation(
                "target_state_preflight",
                "passed",
                "Target conflict observations still match the reviewed plan",
            )
            self._emit_progress("Preflight 3/3 passed: target state is unchanged.")

            for bundle in plan.bundles:
                if bundle.execution_action == SKIP_POLICY_ACTION:
                    jobs_by_key[bundle.key] = self._synthetic_job_result(
                        bundle=bundle,
                        status="skipped",
                        outcome="skipped_policy",
                        terminal_status="SKIPPED_POLICY",
                        reason=(
                            "Whole archive skipped because every contained resource already exists under the reviewed skip policy"
                        ),
                        attempted=False,
                    )
            checkpoint()

            if identity_snapshot is not None:
                self._emit_progress(
                    "Restoring identities: {0} user(s), {1} group(s), {2} membership(s)...".format(
                        identity_snapshot.users.item_count,
                        identity_snapshot.groups.item_count,
                        identity_snapshot.memberships.item_count,
                    )
                )
                execution_stage = "identity_restore"
                self._ensure_identity_quicksight()
                needs_iam = any(
                    str(item.get("identity_type", "")).upper() == "IAM"
                    for item in identity_snapshot.users.items
                )
                if needs_iam:
                    self._ensure_target_iam()
                identity_service = UserGroupRestoreService(
                    plan.target,
                    self.target_identity_quicksight,
                    self.target_iam,
                    plan.identity_mappings,
                )
                identity_result = identity_service.restore(identity_snapshot)
                errors.extend(identity_result.errors)
                self._emit_progress(
                    "Identity restore complete: {0} succeeded, {1} failed.".format(
                        identity_result.succeeded,
                        identity_result.failed,
                    )
                )
                if identity_result.failed and not identity_result.errors:
                    errors.append(
                        "Identity restore reported {0} failed result(s)".format(
                            identity_result.failed
                        )
                    )
                checkpoint()

            fail_fast_trigger: Optional[Dict[str, Optional[str]]] = None
            if errors and not plan.continue_on_error:
                fail_fast_trigger = {"bundle_key": None, "stage": "identity_restore"}

            asset_service = None
            import_bundle_count = sum(
                1 for bundle in plan.bundles if bundle.execution_action == IMPORT_ACTION
            )
            if import_bundle_count:
                self._ensure_asset_quicksight()
                service_options: Dict[str, Any] = {
                    "sleep": self.sleep,
                    "monotonic": self.monotonic,
                }
                if self.progress_callback is not None:
                    service_options["progress_callback"] = self.progress_callback
                asset_service = AssetBundleRestoreService(
                    self.config,
                    self.catalog,
                    self.target_quicksight,
                    **service_options,
                )
                self._emit_progress(
                    "Starting asset restore: {0} bundle(s) to import.".format(import_bundle_count)
                )
            execution_stage = "asset_import"
            for index, bundle in enumerate(plan.bundles, start=1):
                execution_bundle_key = bundle.key
                bundle_label = Path(bundle.key).name
                if bundle.execution_action == SKIP_POLICY_ACTION:
                    self._emit_progress(
                        "Bundle {0}/{1}: skipped {2} by reviewed conflict policy.".format(
                            index, len(plan.bundles), bundle_label
                        )
                    )
                    continue
                if fail_fast_trigger is not None:
                    reason = "Not attempted because fail-fast execution stopped during {0}".format(
                        fail_fast_trigger["stage"]
                    )
                    jobs_by_key[bundle.key] = self._synthetic_job_result(
                        bundle=bundle,
                        status="not_attempted",
                        outcome="not_attempted_fail_fast",
                        terminal_status="NOT_ATTEMPTED_FAIL_FAST",
                        reason=reason,
                        attempted=False,
                        trigger_bundle_key=fail_fast_trigger["bundle_key"],
                        trigger_stage=fail_fast_trigger["stage"],
                    )
                    self._emit_progress(
                        "Bundle {0}/{1}: not attempted ({2}).".format(
                            index, len(plan.bundles), reason
                        )
                    )
                    checkpoint()
                    continue

                blockers = [
                    prerequisite
                    for prerequisite in bundle.prerequisite_bundle_keys
                    if prerequisite not in jobs_by_key
                    or jobs_by_key[prerequisite].outcome not in ("imported", "skipped_policy")
                ]
                if blockers:
                    reason = "Blocked by unsuccessful prerequisite archive(s): {0}".format(
                        ", ".join(blockers)
                    )
                    jobs_by_key[bundle.key] = self._synthetic_job_result(
                        bundle=bundle,
                        status="blocked",
                        outcome="blocked_prerequisite",
                        terminal_status="BLOCKED_PREREQUISITE",
                        reason=reason,
                        attempted=False,
                        blocked_by=blockers,
                        trigger_bundle_key=blockers[0],
                        trigger_stage="prerequisite",
                    )
                    errors.append("{0}: {1}".format(bundle.key, reason))
                    self._emit_progress(
                        "Bundle {0}/{1}: blocked ({2}).".format(index, len(plan.bundles), reason)
                    )
                    checkpoint()
                    continue

                self._emit_progress(
                    "Bundle {0}/{1}: importing {2}...".format(
                        index, len(plan.bundles), bundle_label
                    )
                )
                try:
                    if asset_service is None:
                        raise RestoreExecutionError("Asset import service was not initialized")
                    result = asset_service.restore_bundle(plan, bundle, restore_id, index)
                except Exception as error:
                    reason = "Unexpected import execution error: {0}".format(error)
                    result = self._synthetic_job_result(
                        bundle=bundle,
                        status="failed",
                        outcome="failed",
                        terminal_status="EXECUTION_EXCEPTION",
                        reason=reason,
                        attempted=True,
                        job_id=AssetBundleRestoreService._job_id(restore_id, index, bundle),
                        trigger_bundle_key=bundle.key,
                        trigger_stage="asset_import",
                    )
                jobs_by_key[bundle.key] = result
                if result.outcome == "imported":
                    self._emit_progress(
                        "Bundle {0}/{1}: imported successfully ({2}).".format(
                            index, len(plan.bundles), bundle_label
                        )
                    )
                else:
                    self._emit_progress(
                        "Bundle {0}/{1}: ended as {2} ({3}).".format(
                            index,
                            len(plan.bundles),
                            result.outcome,
                            result.terminal_status,
                        )
                    )
                if result.outcome != "imported":
                    errors.append(
                        "Import job {0} for {1} ended as {2} ({3})".format(
                            result.job_id or "<not-started>",
                            result.bundle_key,
                            result.outcome,
                            result.terminal_status,
                        )
                    )
                    if not plan.continue_on_error:
                        fail_fast_trigger = {
                            "bundle_key": bundle.key,
                            "stage": "asset_import",
                        }
                checkpoint()
        except KeyboardInterrupt:
            interrupted = True
            errors.append("Restore execution was interrupted")
        except Exception as error:
            errors.append(str(error))
            validation_name = {
                "artifact_preflight": "source_artifact_preflight",
                "identity_source_preflight": "identity_source_preflight",
                "target_state_preflight": "target_state_preflight",
            }.get(execution_stage)
            if validation_name:
                set_validation(validation_name, "failed", str(error))

        if interrupted and execution_stage == "asset_import" and execution_bundle_key:
            active = next(
                (bundle for bundle in plan.bundles if bundle.key == execution_bundle_key),
                None,
            )
            if active is not None and active.key not in jobs_by_key:
                index = plan.bundles.index(active) + 1
                jobs_by_key[active.key] = self._synthetic_job_result(
                    bundle=active,
                    status="timeout",
                    outcome="timed_out",
                    terminal_status="INTERRUPTED_REMOTE_STATE_UNCERTAIN",
                    reason="Execution was interrupted while this bundle may have been active",
                    attempted=True,
                    job_id=AssetBundleRestoreService._job_id(restore_id, index, active),
                    trigger_bundle_key=active.key,
                    trigger_stage="asset_import",
                )

        if len(jobs_by_key) != len(plan.bundles):
            outcome = "not_attempted_interrupted" if interrupted else "not_attempted_precondition"
            terminal_status = (
                "NOT_ATTEMPTED_INTERRUPTED" if interrupted else "NOT_ATTEMPTED_PRECONDITION"
            )
            reason = "Not attempted because restore stopped during {0}".format(execution_stage)
            for bundle in plan.bundles:
                if bundle.key in jobs_by_key:
                    continue
                jobs_by_key[bundle.key] = self._synthetic_job_result(
                    bundle=bundle,
                    status="not_attempted",
                    outcome=outcome,
                    terminal_status=terminal_status,
                    reason=reason,
                    attempted=False,
                    trigger_bundle_key=execution_bundle_key,
                    trigger_stage=execution_stage,
                )

        jobs = [jobs_by_key[bundle.key] for bundle in plan.bundles]
        successful = sum(1 for job in jobs if job.outcome in ("imported", "skipped_policy"))
        failed = len(jobs) - successful
        if identity_result:
            successful += identity_result.succeeded
            failed += identity_result.failed
        if errors or failed:
            overall = "partial" if successful else "failed"
        else:
            overall = "success"
        completed_at = datetime.now(timezone.utc).isoformat()
        report = self._build_report(
            plan=plan,
            restore_id=restore_id,
            overall_status=overall,
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=max(0.0, self.monotonic() - start_clock),
            identity_result=identity_result,
            jobs=jobs,
            warnings=warnings,
            errors=errors,
            validation_results=validation_results,
        )
        self.report_service.save_checkpoint(report, final=True)
        self._emit_progress("Restore execution finished with status {0}.".format(overall.upper()))
        if interrupted:
            raise KeyboardInterrupt()
        return report

    def status(self, restore_id: str) -> RestoreReport:
        return self.report_service.load(restore_id)

    def _ensure_catalog(self, require_assets: bool, require_identities: bool) -> None:
        if self.catalog is not None:
            return
        if require_assets and self.source_s3 is None:
            self.source_s3 = self.factory.source_client("s3", self.config.source_backup.s3_region)
        if require_identities and self.source_dynamodb is None:
            self.source_dynamodb = self.factory.source_client(
                "dynamodb", self.config.source_backup.dynamodb_region
            )
        self.catalog = LegacyBackupCatalog(
            self.config.source_backup, self.source_s3, self.source_dynamodb
        )

    def _ensure_asset_quicksight(self) -> None:
        if self.target_quicksight is None:
            self.target_quicksight = self.factory.target_client(
                "quicksight", self.config.target.asset_region
            )
        if self.config.target.identity_region == self.config.target.asset_region:
            self.target_identity_quicksight = self.target_quicksight

    def _ensure_identity_quicksight(self) -> None:
        if self.target_identity_quicksight is not None:
            return
        if self.config.target.identity_region == self.config.target.asset_region:
            self._ensure_asset_quicksight()
            self.target_identity_quicksight = self.target_quicksight
        else:
            self.target_identity_quicksight = self.factory.target_client(
                "quicksight", self.config.target.identity_region
            )

    def _ensure_target_iam(self) -> None:
        if self.target_iam is None:
            self.target_iam = self.factory.target_client("iam", self.config.target.identity_region)

    def _resolve_local_operation_path(
        self,
        value: str,
        label: str,
        must_exist: bool,
        protect_inputs: bool,
    ) -> Path:
        if self.config.config_directory:
            protected = []
            if protect_inputs:
                if self.config.config_path:
                    protected.append(Path(self.config.config_path))
                if self.config.restore.overrides_file:
                    protected.append(Path(self.config.restore.overrides_file))
            return resolve_under_root(
                value,
                Path(self.config.config_directory),
                label,
                must_exist=must_exist,
                require_file=must_exist,
                protected_paths=protected,
            )
        try:
            path = reject_link_components(
                Path(value).expanduser(), label, allow_missing=not must_exist
            )
        except (OSError, ValueError) as error:
            raise RestoreExecutionError("{0}: {1}".format(label, error))
        if must_exist and not path.is_file():
            raise RestoreExecutionError("{0} must be a regular file".format(label))
        return path

    @staticmethod
    def _synthetic_job_result(
        bundle: PlannedBundle,
        status: str,
        outcome: str,
        terminal_status: str,
        reason: str,
        attempted: bool,
        job_id: str = "",
        blocked_by: Optional[List[str]] = None,
        trigger_bundle_key: Optional[str] = None,
        trigger_stage: Optional[str] = None,
    ) -> ImportJobResult:
        now = datetime.now(timezone.utc).isoformat()
        return ImportJobResult(
            bundle_key=bundle.key,
            job_id=job_id,
            status=status,
            terminal_status=terminal_status,
            selected_member_count=len(bundle.selected_member_names),
            started_at=now,
            completed_at=now,
            duration_seconds=0.0,
            errors=([] if outcome in ("skipped_policy", "pending") else [{"Message": reason}]),
            rollback_errors=[],
            outcome=outcome,
            attempted=attempted,
            attempts={},
            prerequisite_bundle_keys=list(bundle.prerequisite_bundle_keys),
            blocked_by=list(blocked_by or []),
            reason=reason,
            trigger_bundle_key=trigger_bundle_key,
            trigger_stage=trigger_stage,
        )

    @staticmethod
    def _build_report(
        plan: RestorePlan,
        restore_id: str,
        overall_status: str,
        started_at: str,
        completed_at: str,
        duration_seconds: float,
        identity_result: Any,
        jobs: List[ImportJobResult],
        warnings: List[str],
        errors: List[str],
        validation_results: List[Dict[str, Any]],
    ) -> RestoreReport:
        manifest_by_key = {item.key: item for item in plan.manifest.bundles}
        selected_bundles = []
        planned_evidence = []
        for bundle in plan.bundles:
            manifest = manifest_by_key[bundle.key]
            selected_bundles.append(
                {
                    "bucket": manifest.bucket,
                    "key": manifest.key,
                    "version_id": manifest.version_id,
                    "size": manifest.size,
                    "sha256": manifest.sha256,
                    "root_type": manifest.root_type,
                }
            )
            planned_evidence.append(
                {
                    "key": bundle.key,
                    "order": bundle.order,
                    "execution_action": bundle.execution_action,
                    "import_transport": bundle.import_transport,
                    "selected_member_names": list(bundle.selected_member_names),
                    "selected_resources": list(bundle.selected_resources),
                    "omitted_member_names": list(bundle.omitted_member_names),
                    "prerequisite_bundle_keys": list(bundle.prerequisite_bundle_keys),
                }
            )
        validation_status = (
            "failed"
            if any(item["status"] == "failed" for item in validation_results)
            else (
                "not_run"
                if any(item["status"] == "not_run" for item in validation_results)
                else "passed"
            )
        )
        return RestoreReport(
            restore_id=restore_id,
            plan_id=plan.plan_id,
            plan_digest=plan.plan_digest,
            overall_status=overall_status,
            target=plan.target,
            selected_backup={
                "backup_date": plan.manifest.backup_date,
                "source_bucket": plan.manifest.source_bucket,
                "source_s3_region": plan.manifest.source_s3_region,
                "source_dynamodb_region": plan.manifest.source_dynamodb_region,
                "bundles": selected_bundles,
                "identity_tables": plan.manifest.identity_tables.to_dict(),
                "identity_snapshot_sha256": (
                    plan.manifest.identity_snapshot.sha256
                    if plan.manifest.identity_snapshot
                    else None
                ),
                "identity_counts": (
                    {
                        "user": plan.manifest.identity_snapshot.users.item_count,
                        "group": plan.manifest.identity_snapshot.groups.item_count,
                        "membership": (plan.manifest.identity_snapshot.memberships.item_count),
                    }
                    if plan.restore_identities and plan.manifest.identity_snapshot
                    else {}
                ),
            },
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            identity_result=identity_result,
            import_jobs=jobs,
            warnings=list(warnings),
            errors=list(errors),
            plan_evidence={
                "plan_created_at": plan.created_at,
                "artifact_digest": plan.artifact_digest,
                "config_snapshot": plan.config_snapshot,
                "config_sha256": plan.config_sha256,
                "bundles": planned_evidence,
                "duplicate_decisions": [item.to_dict() for item in plan.duplicate_decisions],
                "conflict_decisions": [item.to_dict() for item in plan.conflict_decisions],
                "conflict_policy": plan.conflict_policy,
                "conflict_prefix": plan.conflict_prefix,
                "failure_action": plan.failure_action,
                "continue_on_error": plan.continue_on_error,
                "restore_identities": plan.restore_identities,
                "target_principals": list(plan.target_principals),
                "identity_mappings": list(plan.identity_mappings),
                "overrides_sha256": plan.overrides_sha256,
            },
            validation_results=[dict(item) for item in validation_results],
            validation_status=validation_status,
            config_sha256=plan.config_sha256,
        )

    def _verify_plan(self, plan: RestorePlan) -> None:
        if not plan.verify_digest():
            raise PlanIntegrityError("Restore plan digest verification failed")
        configured_overrides = self._load_overrides()
        configured_overrides_sha256 = (
            hashlib.sha256(canonical_json(configured_overrides)).hexdigest()
            if configured_overrides
            else None
        )
        if plan.overrides != configured_overrides:
            raise PlanIntegrityError(
                "Restore plan native overrides do not match the current reviewed configuration"
            )
        if plan.overrides_sha256 != configured_overrides_sha256:
            raise PlanIntegrityError(
                "Restore plan overrides digest does not match its native overrides"
            )
        expected_config_snapshot = self.config.audit_snapshot(configured_overrides_sha256)
        if plan.config_snapshot != expected_config_snapshot:
            raise PlanIntegrityError(
                "Restore plan sanitized configuration snapshot does not match configuration"
            )
        if plan.config_sha256 != self.config.audit_digest(configured_overrides_sha256):
            raise PlanIntegrityError(
                "Restore plan configuration digest does not match configuration"
            )
        if plan.target_principals != sorted(self.config.restore.target_principals):
            raise PlanIntegrityError("Restore plan target principals do not match configuration")
        expected = TargetFingerprint(
            aws_account_id=self.config.target.aws_account_id,
            asset_region=self.config.target.asset_region,
            identity_region=self.config.target.identity_region,
            namespace=self.config.target.namespace,
        )
        if plan.target.to_dict() != expected.to_dict():
            raise PlanIntegrityError("Restore plan target fingerprint does not match configuration")
        if (
            plan.manifest.source_bucket != self.config.source_backup.s3_bucket_name
            or plan.manifest.source_s3_region != self.config.source_backup.s3_region
            or plan.manifest.source_dynamodb_region != self.config.source_backup.dynamodb_region
        ):
            raise PlanIntegrityError("Restore plan source fingerprint does not match configuration")
        configured_backup_date = self.config.source_backup.backup_date
        if (
            configured_backup_date is not None
            and plan.manifest.backup_date != configured_backup_date
        ):
            raise PlanIntegrityError("Restore plan backup date does not match configuration")
        if self.config.source_backup.bundle_keys:
            planned_keys = sorted(bundle.key for bundle in plan.manifest.bundles)
            if planned_keys != sorted(self.config.source_backup.bundle_keys):
                raise PlanIntegrityError(
                    "Restore plan bundle keys do not match explicit configuration"
                )
        if self.config.restore.mode == "identities-only" and plan.bundles:
            raise PlanIntegrityError("Identities-only restore plan must not contain asset bundles")
        if self.config.restore.mode == "assets-only" and plan.restore_identities:
            raise PlanIntegrityError("Assets-only restore plan must not contain identity execution")
        if plan.failure_action != self.config.restore.failure_action:
            raise PlanIntegrityError("Restore plan failure_action does not match configuration")
        if plan.conflict_policy != self.config.restore.conflict_policy:
            raise PlanIntegrityError("Restore plan conflict policy does not match configuration")
        if plan.conflict_prefix != self.config.restore.conflict_prefix:
            raise PlanIntegrityError("Restore plan conflict prefix does not match configuration")
        if plan.continue_on_error != self.config.restore.continue_on_error:
            raise PlanIntegrityError("Restore plan continue_on_error does not match configuration")
        if [bundle.order for bundle in plan.bundles] != list(range(len(plan.bundles))):
            raise PlanIntegrityError(
                "Restore plan bundle order is not contiguous and deterministic"
            )
        positions = {bundle.key: index for index, bundle in enumerate(plan.bundles)}
        if len(positions) != len(plan.bundles):
            raise PlanIntegrityError("Restore plan contains duplicate bundle keys")
        manifest_keys = [bundle.key for bundle in plan.manifest.bundles]
        if len(set(manifest_keys)) != len(manifest_keys):
            raise PlanIntegrityError("Restore manifest contains duplicate bundle keys")
        if set(manifest_keys) != set(positions):
            raise PlanIntegrityError(
                "Restore plan must contain exactly one action for every manifest bundle"
            )
        manifest_by_key = {bundle.key: bundle for bundle in plan.manifest.bundles}
        for bundle in plan.bundles:
            manifest_bundle = manifest_by_key[bundle.key]
            planned_metadata = (
                bundle.bucket,
                bundle.key,
                bundle.version_id,
                bundle.size,
                bundle.sha256,
                bundle.root_type,
            )
            manifest_metadata = (
                manifest_bundle.bucket,
                manifest_bundle.key,
                manifest_bundle.version_id,
                manifest_bundle.size,
                manifest_bundle.sha256,
                manifest_bundle.root_type,
            )
            if planned_metadata != manifest_metadata:
                raise PlanIntegrityError(
                    "Restore planned bundle metadata does not match manifest: {0}".format(
                        bundle.key
                    )
                )
            manifest_members = sorted(manifest_bundle.members, key=lambda item: item.member_name)
            member_names = [item.member_name for item in manifest_members]
            member_resources = [item.resource_key for item in manifest_members]
            if len(member_names) != len(set(member_names)):
                raise PlanIntegrityError(
                    "Restore manifest contains duplicate member inventory: {0}".format(bundle.key)
                )
            if len(member_resources) != len(set(member_resources)):
                raise PlanIntegrityError(
                    "Restore manifest contains duplicate resource inventory: {0}".format(bundle.key)
                )
            if (
                bundle.selected_member_names != member_names
                or bundle.selected_resources != member_resources
            ):
                raise PlanIntegrityError(
                    "Restore planned bundle selection does not match complete manifest inventory: {0}".format(
                        bundle.key
                    )
                )
        from .models.errors import RestorePlanningError

        dependency_compiler = RestorePlanner(self.config)
        imported_manifest = {
            bundle.key: manifest_by_key[bundle.key]
            for bundle in plan.bundles
            if bundle.execution_action == IMPORT_ACTION
        }
        provided_resources = {
            member.resource_key
            for bundle in imported_manifest.values()
            for member in bundle.members
        }
        external_dependencies = {
            dependency
            for bundle in imported_manifest.values()
            for member in bundle.members
            for dependency in member.dependencies
            if dependency not in provided_resources
        }
        try:
            expected_import_keys, expected_prerequisites = dependency_compiler._dependency_order(
                imported_manifest,
                external_dependencies,
                plan.conflict_policy,
            )
        except RestorePlanningError as error:
            raise PlanIntegrityError(
                "Restore plan dependency graph cannot be reproduced: {0}".format(error)
            ) from error
        expected_bundle_keys = (
            sorted(
                bundle.key
                for bundle in plan.bundles
                if bundle.execution_action == SKIP_POLICY_ACTION
            )
            + expected_import_keys
        )
        actual_bundle_keys = [bundle.key for bundle in plan.bundles]
        if actual_bundle_keys != expected_bundle_keys:
            raise PlanIntegrityError(
                "Restore plan bundle order does not match its manifest dependency graph"
            )
        for bundle in plan.bundles:
            expected_bundle_prerequisites = (
                expected_prerequisites.get(bundle.key, [])
                if bundle.execution_action == IMPORT_ACTION
                else []
            )
            if bundle.prerequisite_bundle_keys != expected_bundle_prerequisites:
                raise PlanIntegrityError(
                    "Restore plan prerequisites do not match its manifest dependency graph: {0}".format(
                        bundle.key
                    )
                )

        selected_resources = {
            resource for bundle in plan.bundles for resource in bundle.selected_resources
        }
        decision_resources = [decision.resource_key for decision in plan.conflict_decisions]
        if (
            len(decision_resources) != len(set(decision_resources))
            or set(decision_resources) != selected_resources
        ):
            raise PlanIntegrityError(
                "Restore plan requires exactly one conflict decision per selected resource"
            )
        compiler = RestorePlanner(self.config)
        actions_by_resource: Dict[str, str] = {}
        for decision in plan.conflict_decisions:
            expected_destination = compiler._destination_resource_key(decision.resource_key)
            if (
                decision.policy != plan.conflict_policy
                or decision.destination_resource_key != expected_destination
            ):
                raise PlanIntegrityError(
                    "Restore plan conflict decision does not match configuration"
                )
            if plan.conflict_policy == "prefix":
                if decision.target_exists:
                    raise PlanIntegrityError(
                        "Prefixed restore plan contains an existing target conflict"
                    )
                expected_action = "prefix"
            elif decision.target_exists and plan.conflict_policy == "fail":
                raise PlanIntegrityError(
                    "Fail-policy restore plan contains an executable target conflict"
                )
            elif decision.target_exists and plan.conflict_policy == "skip":
                expected_action = "skip"
            elif decision.target_exists:
                expected_action = "update"
            else:
                expected_action = "create"
            if decision.action != expected_action:
                raise PlanIntegrityError(
                    "Restore plan conflict action does not match configuration"
                )
            actions_by_resource[decision.resource_key] = expected_action
        skipped_resources = {
            decision.resource_key
            for decision in plan.conflict_decisions
            if decision.action == "skip"
        }
        for bundle in plan.bundles:
            if bundle.materialization_mode != "original" or bundle.omitted_member_names:
                raise PlanIntegrityError(
                    "Restore plan attempts unsupported member-level bundle rewriting"
                )
            if not bundle.selected_member_names or not bundle.selected_resources:
                raise PlanIntegrityError(
                    "Restore plan bundle action contains no selected resources"
                )
            bundle_actions = {
                actions_by_resource[resource] for resource in bundle.selected_resources
            }
            if "skip" in bundle_actions and bundle_actions != {"skip"}:
                raise PlanIntegrityError(
                    "Restore plan skip policy would require member-level rewriting"
                )
            expected_execution_action = (
                SKIP_POLICY_ACTION if bundle_actions == {"skip"} else IMPORT_ACTION
            )
            if bundle.execution_action != expected_execution_action:
                raise PlanIntegrityError(
                    "Restore plan bundle action does not match its conflict decisions"
                )
            if bundle.execution_action == IMPORT_ACTION:
                expected_import_overrides = compiler._compile_bundle_overrides(
                    configured_overrides, set(bundle.selected_resources)
                )
                compiler._validate_final_import_request(expected_import_overrides, bundle.key)
                if bundle.import_overrides != expected_import_overrides:
                    raise PlanIntegrityError(
                        "Restore plan executable import overrides do not match configuration"
                    )
                if bundle.import_transport != IMPORT_TRANSPORT_INLINE:
                    raise PlanIntegrityError(
                        "Import action does not use the sealed inline transport"
                    )
                if bundle.size < 0 or bundle.size > INLINE_IMPORT_MAX_BYTES:
                    raise PlanIntegrityError(
                        "Import action exceeds the inline Quick Sight bundle size limit"
                    )
            elif bundle.execution_action == SKIP_POLICY_ACTION:
                if plan.conflict_policy != "skip":
                    raise PlanIntegrityError(
                        "Skip action is only valid under the reviewed skip policy"
                    )
                if bundle.import_transport != IMPORT_TRANSPORT_NONE:
                    raise PlanIntegrityError("Skip action contains an import transport")
                if bundle.import_overrides or bundle.prerequisite_bundle_keys:
                    raise PlanIntegrityError(
                        "Skip action contains import overrides or prerequisites"
                    )
                if not set(bundle.selected_resources).issubset(skipped_resources):
                    raise PlanIntegrityError(
                        "Skip action contains a resource not covered by a skip decision"
                    )
            else:
                raise PlanIntegrityError(
                    "Unknown restore plan bundle action: {0}".format(bundle.execution_action)
                )
            if len(set(bundle.prerequisite_bundle_keys)) != len(bundle.prerequisite_bundle_keys):
                raise PlanIntegrityError("Restore plan contains duplicate prerequisites")
            for prerequisite in bundle.prerequisite_bundle_keys:
                if (
                    prerequisite not in positions
                    or positions[prerequisite] >= positions[bundle.key]
                ):
                    raise PlanIntegrityError(
                        "Restore plan prerequisite order is invalid for {0}".format(bundle.key)
                    )
            prefix_value = (
                bundle.import_overrides.get("OverrideParameters", {})
                .get("ResourceIdOverrideConfiguration", {})
                .get("PrefixForAllResources")
            )
            if bundle.execution_action == IMPORT_ACTION and plan.conflict_policy == "prefix":
                if prefix_value != plan.conflict_prefix:
                    raise PlanIntegrityError("Sealed bundle prefix does not match the restore plan")
            elif prefix_value is not None:
                raise PlanIntegrityError("Non-prefix restore plan contains a resource ID prefix")
        if plan.restore_identities != self.config.restore.restore_identities:
            raise PlanIntegrityError("Restore plan identity setting does not match configuration")
        if plan.restore_identities:
            snapshot = plan.manifest.identity_snapshot
            if snapshot is None:
                raise PlanIntegrityError(
                    "Identity restore plan does not contain a sealed source snapshot"
                )
            if not snapshot.verify_digest():
                raise PlanIntegrityError("Identity snapshot digest verification failed")
            table_names = {
                "users": snapshot.users.table_name,
                "groups": snapshot.groups.table_name,
                "memberships": snapshot.memberships.table_name,
            }
            expected_names = {
                "users": plan.manifest.identity_tables.users,
                "groups": plan.manifest.identity_tables.groups,
                "memberships": plan.manifest.identity_tables.memberships,
            }
            if table_names != expected_names:
                raise PlanIntegrityError(
                    "Identity snapshot table names do not match the restore manifest"
                )
            configured_tables = self.config.source_backup.identity_tables
            configured_names = {
                "users": "{0}-{1}".format(plan.manifest.backup_date, configured_tables.users),
                "groups": "{0}-{1}".format(plan.manifest.backup_date, configured_tables.groups),
                "memberships": "{0}-{1}".format(
                    plan.manifest.backup_date, configured_tables.memberships
                ),
            }
            if expected_names != configured_names:
                raise PlanIntegrityError(
                    "Restore plan identity table names do not match configuration"
                )
            current_mappings = self._identity_mapping_payload()
            if plan.identity_mappings != current_mappings:
                raise PlanIntegrityError(
                    "Restore plan identity mappings do not match configuration"
                )

    def _verify_target_state(self, plan: RestorePlan) -> None:
        if not plan.conflict_decisions:
            return
        resource_types = {
            (decision.destination_resource_key or decision.resource_key).split("/", 1)[0]
            for decision in plan.conflict_decisions
        }
        inventory = QuickSightTargetInventory(self.target_quicksight, plan.target.aws_account_id)
        current = inventory.list_resources(resource_types)
        for decision in plan.conflict_decisions:
            destination = decision.destination_resource_key or decision.resource_key
            exists = destination in current
            if exists != decision.target_exists:
                raise PlanIntegrityError(
                    "Target state changed after planning for {0}: expected exists={1}, got exists={2}".format(
                        destination, decision.target_exists, exists
                    )
                )

    def _identity_mapping_payload(self) -> List[Dict[str, Any]]:
        return sorted(
            [
                {
                    "source_principal_arn": mapping.source_principal_arn,
                    "target_principal_arn": mapping.target_principal_arn,
                    "target_iam_arn": mapping.target_iam_arn,
                    "session_name": mapping.session_name,
                    "identity_center": mapping.identity_center,
                }
                for mapping in self.config.restore.identity_mappings
            ],
            key=lambda item: item["source_principal_arn"],
        )

    def _load_overrides(self) -> Dict[str, Any]:
        path = self.config.restore.overrides_file
        if not path:
            return {}
        try:
            encoded = read_bounded_regular_file(
                Path(path), MAX_OVERRIDES_BYTES, "restore overrides file"
            )
            value = loads_strict_json(encoded.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise RestoreExecutionError("Unable to load overrides file: {0}".format(error))
        if not isinstance(value, dict):
            raise RestoreExecutionError("Overrides file root must be an object")
        return value
