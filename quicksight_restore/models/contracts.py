"""Versioned, JSON-serializable contracts for restore planning and reporting."""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
import copy
import hashlib
import hmac
import json
import math
import re

CONTRACT_VERSION = "2.0"


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _require_timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("{0} is required".format(label))
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("{0} must be an ISO-8601 timestamp".format(label))
    if parsed.tzinfo is None:
        raise ValueError("{0} must include a timezone".format(label))
    return parsed


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("{0} must be a lowercase SHA-256 digest".format(label))
    return value


class SerializableContract:
    """Small explicit serialization surface shared by restore contracts."""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ResolvedIdentityTables(SerializableContract):
    version: str = CONTRACT_VERSION
    users: str = ""
    groups: str = ""
    memberships: str = ""

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "ResolvedIdentityTables":
        return cls(**value)


@dataclass
class IdentityTableSnapshot(SerializableContract):
    """Canonical records and fingerprint for one legacy identity table."""

    table_name: str
    key_name: str
    items: List[Dict[str, Any]]
    item_count: int = 0
    sha256: str = ""
    version: str = CONTRACT_VERSION

    def digest_payload(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "key_name": self.key_name,
            "items": self.items,
            "version": self.version,
        }

    def calculate_digest(self) -> str:
        return sha256_json(self.digest_payload())

    def seal(self) -> "IdentityTableSnapshot":
        self.item_count = len(self.items)
        self.sha256 = self.calculate_digest()
        return self

    def verify_digest(self) -> bool:
        return (
            self.item_count == len(self.items)
            and bool(self.sha256)
            and self.sha256 == self.calculate_digest()
        )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "IdentityTableSnapshot":
        return cls(**value)


@dataclass
class IdentitySnapshot(SerializableContract):
    """Sealed point-in-time input for identity restoration."""

    users: IdentityTableSnapshot
    groups: IdentityTableSnapshot
    memberships: IdentityTableSnapshot
    sha256: str = ""
    version: str = CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "users": self.users.to_dict(),
            "groups": self.groups.to_dict(),
            "memberships": self.memberships.to_dict(),
            "sha256": self.sha256,
            "version": self.version,
        }

    def digest_payload(self) -> Dict[str, Any]:
        value = self.to_dict()
        value.pop("sha256", None)
        return value

    def calculate_digest(self) -> str:
        return sha256_json(self.digest_payload())

    def seal(self) -> "IdentitySnapshot":
        self.sha256 = self.calculate_digest()
        return self

    def verify_digest(self) -> bool:
        return (
            self.users.verify_digest()
            and self.groups.verify_digest()
            and self.memberships.verify_digest()
            and bool(self.sha256)
            and self.sha256 == self.calculate_digest()
        )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "IdentitySnapshot":
        data = dict(value)
        data["users"] = IdentityTableSnapshot.from_dict(value["users"])
        data["groups"] = IdentityTableSnapshot.from_dict(value["groups"])
        data["memberships"] = IdentityTableSnapshot.from_dict(value["memberships"])
        return cls(**data)


@dataclass
class BundleMemberInventory(SerializableContract):
    member_name: str
    resource_type: str
    resource_id: str
    member_size: int
    member_sha256: str
    canonical_json_sha256: str
    known_resource: bool
    dependencies: List[str] = field(default_factory=list)
    dependency_scopes: List[str] = field(default_factory=list)
    version: str = CONTRACT_VERSION

    @property
    def resource_key(self) -> str:
        return "{0}/{1}".format(self.resource_type, self.resource_id)

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "BundleMemberInventory":
        return cls(**value)


@dataclass
class BundleInventory(SerializableContract):
    bucket: str
    key: str
    version_id: str
    size: int
    sha256: str
    root_type: str
    members: List[BundleMemberInventory] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    version: str = CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["members"] = [member.to_dict() for member in self.members]
        return value

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "BundleInventory":
        data = dict(value)
        data["members"] = [
            BundleMemberInventory.from_dict(item) for item in value.get("members", [])
        ]
        return cls(**data)


@dataclass
class LegacyRestoreManifest(SerializableContract):
    backup_date: Optional[str]
    source_bucket: str
    source_s3_region: str
    source_dynamodb_region: str
    identity_tables: ResolvedIdentityTables
    bundles: List[BundleInventory]
    generated_at: str
    identity_snapshot: Optional[IdentitySnapshot] = None
    warnings: List[str] = field(default_factory=list)
    version: str = CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["identity_tables"] = self.identity_tables.to_dict()
        value["identity_snapshot"] = (
            self.identity_snapshot.to_dict() if self.identity_snapshot else None
        )
        value["bundles"] = [bundle.to_dict() for bundle in self.bundles]
        return value

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "LegacyRestoreManifest":
        data = dict(value)
        data["identity_tables"] = ResolvedIdentityTables.from_dict(value["identity_tables"])
        snapshot = value.get("identity_snapshot")
        data["identity_snapshot"] = IdentitySnapshot.from_dict(snapshot) if snapshot else None
        data["bundles"] = [BundleInventory.from_dict(item) for item in value.get("bundles", [])]
        return cls(**data)


@dataclass
class TargetFingerprint(SerializableContract):
    aws_account_id: str
    asset_region: str
    identity_region: str
    namespace: str
    version: str = CONTRACT_VERSION

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "TargetFingerprint":
        return cls(**value)


@dataclass
class DuplicateDecision(SerializableContract):
    resource_key: str
    canonical_json_sha256: str
    selected_bundle_key: str
    selected_member_name: str
    duplicate_bundle_keys: List[str]
    reason: str
    version: str = CONTRACT_VERSION

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "DuplicateDecision":
        return cls(**value)


@dataclass
class ConflictDecision(SerializableContract):
    resource_key: str
    policy: str
    action: str
    target_exists: bool
    destination_resource_key: Optional[str] = None
    version: str = CONTRACT_VERSION

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "ConflictDecision":
        return cls(**value)


@dataclass
class PlannedBundle(SerializableContract):
    bucket: str
    key: str
    version_id: str
    size: int
    sha256: str
    root_type: str
    selected_member_names: List[str]
    selected_resources: List[str]
    execution_action: str = "import"
    import_transport: str = "inline_body"
    import_overrides: Dict[str, Any] = field(default_factory=dict)
    omitted_member_names: List[str] = field(default_factory=list)
    prerequisite_bundle_keys: List[str] = field(default_factory=list)
    materialization_mode: str = "original"
    order: int = 0
    version: str = CONTRACT_VERSION

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "PlannedBundle":
        return cls(**value)


@dataclass
class RestorePlan(SerializableContract):
    manifest: LegacyRestoreManifest
    target: TargetFingerprint
    bundles: List[PlannedBundle]
    duplicate_decisions: List[DuplicateDecision]
    conflict_decisions: List[ConflictDecision]
    conflict_policy: str
    conflict_prefix: Optional[str]
    failure_action: str
    continue_on_error: bool
    restore_identities: bool
    target_principals: List[str]
    identity_mappings: List[Dict[str, Any]]
    overrides: Dict[str, Any]
    overrides_sha256: Optional[str]
    created_at: str
    config_snapshot: Dict[str, Any] = field(default_factory=dict)
    config_sha256: str = ""
    warnings: List[str] = field(default_factory=list)
    plan_id: str = ""
    plan_digest: str = ""
    artifact_digest: str = ""
    version: str = CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["manifest"] = self.manifest.to_dict()
        value["target"] = self.target.to_dict()
        value["bundles"] = [item.to_dict() for item in self.bundles]
        value["duplicate_decisions"] = [item.to_dict() for item in self.duplicate_decisions]
        value["conflict_decisions"] = [item.to_dict() for item in self.conflict_decisions]
        return value

    def digest_payload(self) -> Dict[str, Any]:
        value = self.to_dict()
        value.pop("plan_digest", None)
        value.pop("plan_id", None)
        value.pop("artifact_digest", None)
        value.pop("created_at", None)
        if isinstance(value.get("manifest"), dict):
            value["manifest"].pop("generated_at", None)
        return value

    def calculate_digest(self) -> str:
        return sha256_json(self.digest_payload())

    def artifact_payload(self) -> Dict[str, Any]:
        value = self.to_dict()
        value.pop("artifact_digest", None)
        return value

    def calculate_artifact_digest(self) -> str:
        return sha256_json(self.artifact_payload())

    def seal(self) -> "RestorePlan":
        self.artifact_digest = ""
        self.config_sha256 = sha256_json(self.config_snapshot)
        self.plan_digest = self.calculate_digest()
        self.plan_id = "plan-{0}".format(self.plan_digest[:20])
        self.artifact_digest = self.calculate_artifact_digest()
        return self

    def verify_digest(self) -> bool:
        return (
            bool(self.plan_digest)
            and bool(self.config_sha256)
            and hmac.compare_digest(self.config_sha256, sha256_json(self.config_snapshot))
            and hmac.compare_digest(self.plan_digest, self.calculate_digest())
            and bool(self.artifact_digest)
            and hmac.compare_digest(self.artifact_digest, self.calculate_artifact_digest())
        )

    def validate_seal(self) -> None:
        if self.version != CONTRACT_VERSION:
            raise ValueError("unsupported restore plan version")
        _require_sha256(self.config_sha256, "restore plan config_sha256")
        _require_sha256(self.plan_digest, "restore plan plan_digest")
        _require_sha256(self.artifact_digest, "restore plan artifact_digest")
        if self.plan_id != "plan-{0}".format(self.plan_digest[:20]):
            raise ValueError("restore plan ID does not match its digest")
        _require_timestamp(self.created_at, "restore plan created_at")
        if self.target.version != CONTRACT_VERSION:
            raise ValueError("unsupported target fingerprint version")
        if self.manifest.version != CONTRACT_VERSION:
            raise ValueError("unsupported restore manifest version")
        if self.manifest.identity_snapshot and not self.manifest.identity_snapshot.verify_digest():
            raise ValueError("identity snapshot digest verification failed")
        if not self.verify_digest():
            raise ValueError("restore plan digest verification failed")

    @staticmethod
    def _raw_digest_payload(value: Dict[str, Any]) -> Dict[str, Any]:
        payload = copy.deepcopy(value)
        payload.pop("plan_digest", None)
        payload.pop("plan_id", None)
        payload.pop("artifact_digest", None)
        payload.pop("created_at", None)
        manifest = payload.get("manifest")
        if isinstance(manifest, dict):
            manifest.pop("generated_at", None)
        return payload

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "RestorePlan":
        if not isinstance(value, dict) or value.get("version") != CONTRACT_VERSION:
            raise ValueError("unsupported restore plan version")
        if "config_snapshot" not in value or not value.get("config_sha256"):
            raise ValueError("restore plan is missing sanitized configuration evidence")
        config_digest = _require_sha256(value.get("config_sha256"), "restore plan config_sha256")
        plan_digest = _require_sha256(value.get("plan_digest"), "restore plan plan_digest")
        artifact_digest = _require_sha256(
            value.get("artifact_digest"), "restore plan artifact_digest"
        )
        raw_artifact_payload = copy.deepcopy(value)
        raw_artifact_payload.pop("artifact_digest", None)
        if not hmac.compare_digest(artifact_digest, sha256_json(raw_artifact_payload)):
            raise ValueError("restore plan artifact digest verification failed")
        if not hmac.compare_digest(config_digest, sha256_json(value.get("config_snapshot"))):
            raise ValueError("restore plan configuration digest verification failed")
        if value.get("plan_id") != "plan-{0}".format(plan_digest[:20]):
            raise ValueError("restore plan ID does not match its digest")
        raw_digest = sha256_json(cls._raw_digest_payload(value))
        if not hmac.compare_digest(plan_digest, raw_digest):
            raise ValueError("restore plan raw digest verification failed")

        data = dict(value)
        data["manifest"] = LegacyRestoreManifest.from_dict(value["manifest"])
        data["target"] = TargetFingerprint.from_dict(value["target"])
        data["bundles"] = [PlannedBundle.from_dict(item) for item in value.get("bundles", [])]
        data["duplicate_decisions"] = [
            DuplicateDecision.from_dict(item) for item in value.get("duplicate_decisions", [])
        ]
        data["conflict_decisions"] = [
            ConflictDecision.from_dict(item) for item in value.get("conflict_decisions", [])
        ]
        plan = cls(**data)
        plan.validate_seal()
        return plan


@dataclass
class ImportJobResult(SerializableContract):
    bundle_key: str
    job_id: str
    status: str
    terminal_status: str
    selected_member_count: int
    started_at: str
    completed_at: str
    duration_seconds: float
    errors: List[Dict[str, Any]] = field(default_factory=list)
    rollback_errors: List[Dict[str, Any]] = field(default_factory=list)
    outcome: str = ""
    attempted: bool = True
    attempts: Dict[str, int] = field(default_factory=dict)
    prerequisite_bundle_keys: List[str] = field(default_factory=list)
    blocked_by: List[str] = field(default_factory=list)
    reason: str = ""
    trigger_bundle_key: Optional[str] = None
    trigger_stage: Optional[str] = None
    version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        if not self.outcome:
            self.outcome = {
                "success": "imported",
                "timeout": "timed_out",
            }.get(self.status, "failed")

    def validate(self) -> None:
        if self.version != CONTRACT_VERSION:
            raise ValueError("unsupported import job result version")
        specifications = {
            "imported": ("success", True),
            "failed": ("failed", True),
            "timed_out": ("timeout", True),
            "skipped_policy": ("skipped", False),
            "blocked_prerequisite": ("blocked", False),
            "not_attempted_fail_fast": ("not_attempted", False),
            "not_attempted_precondition": ("not_attempted", False),
            "not_attempted_interrupted": ("not_attempted", False),
            "pending": ("pending", False),
        }
        if self.outcome not in specifications:
            raise ValueError("unknown import job outcome: {0}".format(self.outcome))
        expected_status, expected_attempted = specifications[self.outcome]
        if self.status != expected_status or self.attempted is not expected_attempted:
            raise ValueError("import job status/outcome/attempted fields are inconsistent")
        if not isinstance(self.bundle_key, str) or not self.bundle_key:
            raise ValueError("import job bundle_key is required")
        if (
            not isinstance(self.selected_member_count, int)
            or isinstance(self.selected_member_count, bool)
            or self.selected_member_count < 0
        ):
            raise ValueError("import job selected_member_count is invalid")
        if (
            not isinstance(self.duration_seconds, (int, float))
            or isinstance(self.duration_seconds, bool)
            or not math.isfinite(float(self.duration_seconds))
            or self.duration_seconds < 0
        ):
            raise ValueError("import job duration_seconds is invalid")
        _require_timestamp(self.started_at, "import job started_at")
        _require_timestamp(self.completed_at, "import job completed_at")
        if not isinstance(self.attempts, dict) or any(
            not isinstance(key, str)
            or isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            for key, value in self.attempts.items()
        ):
            raise ValueError("import job attempts are invalid")
        if self.outcome == "imported" and self.terminal_status != "SUCCESSFUL":
            raise ValueError("imported outcome requires SUCCESSFUL terminal status")
        if self.outcome == "blocked_prerequisite" and not self.blocked_by:
            raise ValueError("blocked prerequisite outcome requires blockers")

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "ImportJobResult":
        if not isinstance(value, dict) or not value.get("outcome"):
            raise ValueError("persisted import job requires an explicit outcome")
        result = cls(**value)
        result.validate()
        return result


@dataclass
class IdentityResult(SerializableContract):
    source_principal_arn: str
    target_principal_arn: Optional[str]
    identity_kind: str
    action: str
    status: str
    message: str = ""
    boundary: Optional[str] = None
    version: str = CONTRACT_VERSION

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "IdentityResult":
        return cls(**value)


@dataclass
class IdentityRestoreResult(SerializableContract):
    results: List[IdentityResult] = field(default_factory=list)
    principal_mapping: Dict[str, str] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    version: str = CONTRACT_VERSION

    @property
    def succeeded(self) -> int:
        return sum(1 for item in self.results if item.status == "success")

    @property
    def failed(self) -> int:
        return sum(1 for item in self.results if item.status == "failed")

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["results"] = [item.to_dict() for item in self.results]
        value["succeeded"] = self.succeeded
        value["failed"] = self.failed
        return value

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "IdentityRestoreResult":
        data = dict(value)
        data.pop("succeeded", None)
        data.pop("failed", None)
        data["results"] = [IdentityResult.from_dict(item) for item in value.get("results", [])]
        return cls(**data)


@dataclass
class RestoreReport(SerializableContract):
    restore_id: str
    plan_id: str
    plan_digest: str
    overall_status: str
    target: TargetFingerprint
    selected_backup: Dict[str, Any]
    started_at: str
    completed_at: str
    duration_seconds: float
    identity_result: Optional[IdentityRestoreResult]
    import_jobs: List[ImportJobResult]
    warnings: List[str]
    errors: List[str]
    plan_evidence: Dict[str, Any] = field(default_factory=dict)
    validation_results: List[Dict[str, Any]] = field(default_factory=list)
    validation_status: str = "not_run"
    summary: Dict[str, Any] = field(default_factory=dict)
    config_sha256: str = ""
    report_digest: str = ""
    rollback_scope: str = (
        "FailureAction applies to each import job only; this is not an atomic multi-bundle rollback."
    )
    version: str = CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["target"] = self.target.to_dict()
        value["identity_result"] = self.identity_result.to_dict() if self.identity_result else None
        value["import_jobs"] = [item.to_dict() for item in self.import_jobs]
        return value

    def digest_payload(self) -> Dict[str, Any]:
        value = self.to_dict()
        value.pop("report_digest", None)
        return value

    def calculate_digest(self) -> str:
        return sha256_json(self.digest_payload())

    def calculate_summary(self) -> Dict[str, Any]:
        outcomes: Dict[str, int] = {}
        for job in self.import_jobs:
            outcomes[job.outcome] = outcomes.get(job.outcome, 0) + 1
        return {
            "bundle_count": len(self.import_jobs),
            "bundle_outcomes": dict(sorted(outcomes.items())),
            "identity_succeeded": self.identity_result.succeeded if self.identity_result else 0,
            "identity_failed": self.identity_result.failed if self.identity_result else 0,
            "warning_count": len(self.warnings),
            "error_count": len(self.errors),
        }

    def seal(self) -> "RestoreReport":
        self.summary = self.calculate_summary()
        self.report_digest = ""
        self.validate(require_digest=False)
        self.report_digest = self.calculate_digest()
        return self

    def verify_digest(self) -> bool:
        return bool(self.report_digest) and hmac.compare_digest(
            self.report_digest, self.calculate_digest()
        )

    def validate(self, require_digest: bool = True) -> None:
        if self.version != CONTRACT_VERSION:
            raise ValueError("unsupported restore report version")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.restore_id or ""):
            raise ValueError("restore report ID is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", self.plan_digest or ""):
            raise ValueError("restore report plan_digest is invalid")
        if self.plan_id != "plan-{0}".format(self.plan_digest[:20]):
            raise ValueError("restore report plan_id does not match plan_digest")
        if not re.fullmatch(r"[0-9a-f]{64}", self.config_sha256 or ""):
            raise ValueError("restore report config_sha256 is invalid")
        if self.target.version != CONTRACT_VERSION:
            raise ValueError("unsupported target fingerprint version")
        if self.overall_status not in ("running", "success", "partial", "failed"):
            raise ValueError("restore report overall_status is invalid")
        started = _require_timestamp(self.started_at, "restore report started_at")
        completed = _require_timestamp(self.completed_at, "restore report completed_at")
        if completed < started:
            raise ValueError("restore report completed_at precedes started_at")
        if (
            not isinstance(self.duration_seconds, (int, float))
            or isinstance(self.duration_seconds, bool)
            or not math.isfinite(float(self.duration_seconds))
            or self.duration_seconds < 0
        ):
            raise ValueError("restore report duration_seconds is invalid")
        if not isinstance(self.selected_backup, dict):
            raise ValueError("restore report selected_backup is invalid")
        selected_bundles = self.selected_backup.get("bundles")
        if not isinstance(selected_bundles, list):
            raise ValueError("restore report selected_backup.bundles is required")
        selected_keys: List[str] = []
        for bundle in selected_bundles:
            if not isinstance(bundle, dict):
                raise ValueError("restore report selected bundle evidence is invalid")
            required = ("bucket", "key", "version_id", "size", "sha256", "root_type")
            if any(field_name not in bundle for field_name in required):
                raise ValueError("restore report selected bundle evidence is incomplete")
            if not isinstance(bundle["key"], str) or not bundle["key"]:
                raise ValueError("restore report selected bundle key is invalid")
            if (
                isinstance(bundle["size"], bool)
                or not isinstance(bundle["size"], int)
                or bundle["size"] < 0
            ):
                raise ValueError("restore report selected bundle size is invalid")
            if not re.fullmatch(r"[0-9a-f]{64}", str(bundle["sha256"])):
                raise ValueError("restore report selected bundle checksum is invalid")
            selected_keys.append(bundle["key"])
        if len(set(selected_keys)) != len(selected_keys):
            raise ValueError("restore report contains duplicate selected bundle keys")
        if not isinstance(self.plan_evidence, dict):
            raise ValueError("restore report plan_evidence is invalid")
        _require_sha256(
            self.plan_evidence.get("artifact_digest"),
            "restore report plan_evidence.artifact_digest",
        )
        plan_bundles = self.plan_evidence.get("bundles")
        if not isinstance(plan_bundles, list):
            raise ValueError("restore report plan_evidence.bundles is required")
        plan_keys = [item.get("key") for item in plan_bundles if isinstance(item, dict)]
        if len(plan_keys) != len(plan_bundles) or plan_keys != selected_keys:
            raise ValueError("restore report plan evidence does not match selected bundles")
        if self.plan_evidence.get("config_sha256") != self.config_sha256:
            raise ValueError("restore report plan/config evidence does not match")
        if not isinstance(self.import_jobs, list):
            raise ValueError("restore report import_jobs is invalid")
        for job in self.import_jobs:
            job.validate()
        job_keys = [job.bundle_key for job in self.import_jobs]
        if job_keys != selected_keys or len(set(job_keys)) != len(job_keys):
            raise ValueError(
                "restore report requires exactly one ordered outcome per selected bundle"
            )
        if self.validation_status not in ("not_run", "passed", "partial", "failed"):
            raise ValueError("restore report validation_status is invalid")
        if not isinstance(self.validation_results, list) or any(
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or item.get("status") not in ("not_run", "passed", "failed")
            for item in self.validation_results
        ):
            raise ValueError("restore report validation_results are invalid")
        expected_summary = self.calculate_summary()
        if self.summary != expected_summary:
            raise ValueError("restore report summary does not match its results")
        if self.overall_status != "running":
            if any(job.outcome == "pending" for job in self.import_jobs):
                raise ValueError("final restore report contains pending bundle outcomes")
            successful = sum(
                1 for job in self.import_jobs if job.outcome in ("imported", "skipped_policy")
            )
            failed = len(self.import_jobs) - successful
            if self.identity_result:
                successful += self.identity_result.succeeded
                failed += self.identity_result.failed
            expected_status = (
                "partial"
                if (self.errors or failed) and successful
                else "failed" if self.errors or failed else "success"
            )
            if self.overall_status != expected_status:
                raise ValueError("restore report overall_status does not match its results")
        if require_digest and not self.verify_digest():
            raise ValueError("restore report digest verification failed")

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "RestoreReport":
        if not isinstance(value, dict) or "import_jobs" not in value:
            raise ValueError("persisted restore report requires import_jobs")
        report_digest = _require_sha256(value.get("report_digest"), "restore report report_digest")
        raw_payload = copy.deepcopy(value)
        raw_payload.pop("report_digest", None)
        if not hmac.compare_digest(report_digest, sha256_json(raw_payload)):
            raise ValueError("restore report raw digest verification failed")

        data = dict(value)
        data["target"] = TargetFingerprint.from_dict(value["target"])
        identity = value.get("identity_result")
        data["identity_result"] = IdentityRestoreResult.from_dict(identity) if identity else None
        data["import_jobs"] = [ImportJobResult.from_dict(item) for item in value["import_jobs"]]
        report = cls(**data)
        report.validate()
        return report
