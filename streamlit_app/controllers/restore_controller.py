"""Controller that wraps the existing manifest-based restore workflow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from quicksight_restore.config.loader import RestoreConfigLoader
from quicksight_restore.models.contracts import RestorePlan, RestoreReport
from quicksight_restore.orchestrator import RestoreOrchestrator


@dataclass
class RestorePreview:
    plan_path: Path
    plan_id: str
    plan_digest: str
    target_account_id: str
    target_region: str
    bundle_count: int
    existing_conflicts: int
    action_counts: dict[str, int]
    resource_counts: dict[str, int]
    identity_counts: dict[str, int]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_path": str(self.plan_path),
            "plan_id": self.plan_id,
            "plan_digest": self.plan_digest,
            "target_account_id": self.target_account_id,
            "target_region": self.target_region,
            "bundle_count": self.bundle_count,
            "existing_conflicts": self.existing_conflicts,
            "action_counts": dict(self.action_counts),
            "resource_counts": dict(self.resource_counts),
            "identity_counts": dict(self.identity_counts),
            "warnings": list(self.warnings),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RestorePreview":
        return cls(
            plan_path=Path(value["plan_path"]),
            plan_id=value["plan_id"],
            plan_digest=value["plan_digest"],
            target_account_id=value["target_account_id"],
            target_region=value["target_region"],
            bundle_count=int(value["bundle_count"]),
            existing_conflicts=int(value["existing_conflicts"]),
            action_counts=dict(value["action_counts"]),
            resource_counts=dict(value["resource_counts"]),
            identity_counts=dict(value["identity_counts"]),
            warnings=list(value["warnings"]),
        )


@dataclass
class RestoreExecution:
    report: RestoreReport
    report_path: Path


class RestoreController:
    """Plan and execute through existing loader/orchestrator contracts."""

    def __init__(self, progress: Optional[Callable[[str], None]] = None):
        self.progress = progress or (lambda _message: None)

    @staticmethod
    def _resource_counts(plan: RestorePlan) -> dict[str, int]:
        unique = {resource for bundle in plan.bundles for resource in bundle.selected_resources}
        result: dict[str, int] = {}
        for resource in unique:
            kind = resource.split("/", 1)[0]
            result[kind] = result.get(kind, 0) + 1
        return dict(sorted(result.items()))

    @staticmethod
    def _identity_counts(plan: RestorePlan) -> dict[str, int]:
        snapshot = plan.manifest.identity_snapshot if plan.restore_identities else None
        if snapshot is None:
            return {}
        return {
            "user": snapshot.users.item_count,
            "group": snapshot.groups.item_count,
            "membership": snapshot.memberships.item_count,
        }

    def preview(
        self,
        config_path: Path,
        manifest_path: Path,
        plan_path: Path,
    ) -> RestorePreview:
        self.progress("Loading and validating restore configuration and backup manifest...")
        loader = RestoreConfigLoader()
        config = loader.load(str(config_path), backup_manifest=str(manifest_path))
        orchestrator = RestoreOrchestrator(config)
        self.progress("Discovering source artifacts and target state read-only...")
        plan = orchestrator.plan(
            output_path=str(plan_path),
            backup_date=config.source_backup.backup_date,
            bundle_keys=config.source_backup.bundle_keys,
        )
        actions: dict[str, int] = {}
        for decision in plan.conflict_decisions:
            actions[decision.action] = actions.get(decision.action, 0) + 1
        existing_conflicts = sum(
            1 for decision in plan.conflict_decisions if decision.target_exists
        )
        self.progress("Restore preview is ready. No target resources were changed.")
        return RestorePreview(
            plan_path=plan_path,
            plan_id=plan.plan_id,
            plan_digest=plan.plan_digest,
            target_account_id=plan.target.aws_account_id,
            target_region=plan.target.asset_region,
            bundle_count=len(plan.bundles),
            existing_conflicts=existing_conflicts,
            action_counts=dict(sorted(actions.items())),
            resource_counts=self._resource_counts(plan),
            identity_counts=self._identity_counts(plan),
            warnings=list(plan.warnings),
        )

    def execute(
        self,
        config_path: Path,
        manifest_path: Path,
        plan_path: Path,
    ) -> RestoreExecution:
        loader = RestoreConfigLoader()
        config = loader.load(str(config_path), backup_manifest=str(manifest_path))
        orchestrator = RestoreOrchestrator(config, progress_callback=self.progress)
        report = orchestrator.run(str(plan_path))
        report_path = Path(config.restore.report_directory) / "restore-{0}.json".format(
            report.restore_id
        )
        return RestoreExecution(report=report, report_path=report_path)
