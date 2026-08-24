"""Validated configuration models for restore operations."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional
import hashlib
import json
import math
import re

from .errors import RestoreConfigurationError

_REGION_RE = re.compile(r"^[a-z]{2}(?:-gov)?-[a-z0-9-]+-\d+$")
_ACCOUNT_RE = re.compile(r"^\d{12}$")
_ROLE_ARN_RE = re.compile(
    r"^arn:(?P<partition>aws|aws-us-gov|aws-cn):iam::(?P<account>\d{12}):role/[A-Za-z0-9+=,.@_\-/]+$"
)
_IAM_ARN_RE = re.compile(
    r"^arn:(?P<partition>aws|aws-us-gov|aws-cn):iam::(?P<account>\d{12}):(?P<kind>role|user)/[A-Za-z0-9+=,.@_\-/]+$"
)
_PRINCIPAL_ARN_RE = re.compile(
    r"^arn:(?P<partition>aws|aws-us-gov|aws-cn):quicksight:(?P<region>[a-z0-9-]+):"
    r"(?P<account>\d{12}):(?P<kind>user|group)/(?P<namespace>[A-Za-z0-9._-]+)/(?P<name>.+)$"
)
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{2,254}$")


@dataclass
class AuthConfig:
    """Non-secret AWS authentication selection.

    A profile can be used as the source credentials for an AssumeRole call. When
    neither field is supplied, boto3's default credential provider chain is used.
    """

    profile: Optional[str] = None
    role_arn: Optional[str] = None
    external_id: Optional[str] = field(default=None, repr=False)
    role_session_name: str = "quicksight-restore"
    sts_region: Optional[str] = None

    def validate(self, label: str) -> List[str]:
        errors: List[str] = []
        if self.profile is not None and (not isinstance(self.profile, str) or not self.profile):
            errors.append("{0}.profile must be a non-empty string".format(label))
        if self.role_arn is not None and (
            not isinstance(self.role_arn, str) or not _ROLE_ARN_RE.match(self.role_arn)
        ):
            errors.append("{0}.role_arn must be an IAM role ARN".format(label))
        if self.external_id is not None and (
            not isinstance(self.external_id, str) or not self.external_id
        ):
            errors.append("{0}.external_id must be a non-empty string".format(label))
        if self.external_id and not self.role_arn:
            errors.append("{0}.external_id requires role_arn".format(label))
        if not isinstance(self.role_session_name, str) or not re.match(
            r"^[\w+=,.@-]{2,64}$", self.role_session_name or ""
        ):
            errors.append("{0}.role_session_name is invalid".format(label))
        if self.sts_region is not None and (
            not isinstance(self.sts_region, str) or not _REGION_RE.match(self.sts_region)
        ):
            errors.append("{0}.sts_region is invalid".format(label))
        return errors

    def audit_snapshot(self) -> Dict[str, Any]:
        """Return non-secret authentication provenance for plan sealing."""

        return {
            "profile": self.profile,
            "role_arn": self.role_arn,
            "role_session_name": self.role_session_name,
            "sts_region": self.sts_region,
            "external_id_configured": bool(self.external_id),
        }


@dataclass
class IdentityTableNames:
    """Base names used to resolve the three legacy date-prefixed tables."""

    users: str = "quicksight-users-backup"
    groups: str = "quicksight-groups-backup"
    memberships: str = "quicksight-users-groups-backup"

    def validate(self) -> List[str]:
        errors: List[str] = []
        values = [self.users, self.groups, self.memberships]
        for name, value in zip(("users", "groups", "memberships"), values):
            if not isinstance(value, str) or not _TABLE_RE.match(value):
                errors.append("source_backup.identity_tables.{0} is invalid".format(name))
        if all(isinstance(value, str) for value in values) and len(set(values)) != 3:
            errors.append("source_backup identity table base names must be distinct")
        return errors


@dataclass
class SourceBackupConfig:
    """Location and credentials for immutable Part 1 backup artifacts."""

    s3_bucket_name: str
    s3_region: str
    dynamodb_region: str
    s3_prefix: str = "quicksight-backups"
    backup_date: Optional[str] = None
    bundle_keys: List[str] = field(default_factory=list)
    date_prefix_format: str = "YYYY/MM/DD"
    identity_tables: IdentityTableNames = field(default_factory=IdentityTableNames)
    auth: AuthConfig = field(default_factory=AuthConfig)
    max_zip_entries: int = 10000
    max_uncompressed_bytes: int = 2 * 1024 * 1024 * 1024
    max_compression_ratio: float = 100.0

    def validate(self) -> List[str]:
        errors: List[str] = []
        if not isinstance(self.s3_bucket_name, str) or not re.match(
            r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", self.s3_bucket_name
        ):
            errors.append("source_backup.s3_bucket_name is invalid")
        if not isinstance(self.s3_prefix, str) or not self.s3_prefix:
            errors.append("source_backup.s3_prefix must be a non-empty relative prefix")
        elif self.s3_prefix.startswith("/") or self.s3_prefix.endswith("/"):
            errors.append("source_backup.s3_prefix must be a non-empty relative prefix")
        elif "//" in self.s3_prefix or ".." in PurePosixPath(self.s3_prefix).parts:
            errors.append("source_backup.s3_prefix is unsafe")
        for name, region in (
            ("s3_region", self.s3_region),
            ("dynamodb_region", self.dynamodb_region),
        ):
            if not isinstance(region, str) or not _REGION_RE.match(region):
                errors.append("source_backup.{0} is invalid".format(name))
        if not isinstance(self.date_prefix_format, str) or self.date_prefix_format not in (
            "YYYY/MM/DD",
            "YYYY-MM-DD",
            "YYYYMMDD",
        ):
            errors.append(
                "source_backup.date_prefix_format must be YYYY/MM/DD, YYYY-MM-DD, or YYYYMMDD"
            )
        if self.backup_date is not None and not isinstance(self.backup_date, str):
            errors.append("source_backup.backup_date must use YYYY-MM-DD")
        elif self.backup_date:
            try:
                datetime.strptime(self.backup_date, "%Y-%m-%d")
            except ValueError:
                errors.append("source_backup.backup_date must use YYYY-MM-DD")
        if not isinstance(self.bundle_keys, list) or any(
            not isinstance(key, str) for key in self.bundle_keys
        ):
            errors.append("source_backup.bundle_keys must be a list of strings")
            safe_bundle_keys: List[str] = []
        else:
            safe_bundle_keys = self.bundle_keys
        if not self.backup_date and not safe_bundle_keys:
            errors.append("source_backup requires an explicit backup_date or explicit bundle_keys")
        for key in safe_bundle_keys:
            path = PurePosixPath(key)
            if (
                not key
                or key.startswith("/")
                or "\\" in key
                or ".." in path.parts
                or path.is_absolute()
            ):
                errors.append("source_backup.bundle_keys contains an unsafe key: {0}".format(key))
        if len(set(safe_bundle_keys)) != len(safe_bundle_keys):
            errors.append("source_backup.bundle_keys must not contain duplicates")
        if (
            isinstance(self.max_zip_entries, bool)
            or not isinstance(self.max_zip_entries, int)
            or self.max_zip_entries < 1
        ):
            errors.append("source_backup.max_zip_entries must be a positive integer")
        if (
            isinstance(self.max_uncompressed_bytes, bool)
            or not isinstance(self.max_uncompressed_bytes, int)
            or self.max_uncompressed_bytes < 1
        ):
            errors.append("source_backup.max_uncompressed_bytes must be a positive integer")
        if (
            isinstance(self.max_compression_ratio, bool)
            or not isinstance(self.max_compression_ratio, (int, float))
            or not math.isfinite(float(self.max_compression_ratio))
            or self.max_compression_ratio < 1
        ):
            errors.append("source_backup.max_compression_ratio must be a finite number at least 1")
        errors.extend(self.identity_tables.validate())
        errors.extend(self.auth.validate("source_backup.auth"))
        role_match = (
            _ROLE_ARN_RE.match(self.auth.role_arn) if isinstance(self.auth.role_arn, str) else None
        )
        if role_match:
            expected_partitions = {
                partition_for_region(region)
                for region in (self.s3_region, self.dynamodb_region)
                if isinstance(region, str) and _REGION_RE.match(region)
            }
            if expected_partitions and role_match.group("partition") not in expected_partitions:
                errors.append("source_backup.auth.role_arn must use the source AWS partition")
        return errors

    def audit_snapshot(self) -> Dict[str, Any]:
        return {
            "s3_bucket_name": self.s3_bucket_name,
            "s3_prefix": self.s3_prefix,
            "backup_date": self.backup_date,
            "bundle_keys": list(self.bundle_keys),
            "date_prefix_format": self.date_prefix_format,
            "s3_region": self.s3_region,
            "dynamodb_region": self.dynamodb_region,
            "identity_tables": {
                "users": self.identity_tables.users,
                "groups": self.identity_tables.groups,
                "memberships": self.identity_tables.memberships,
            },
            "archive_limits": {
                "max_zip_entries": self.max_zip_entries,
                "max_uncompressed_bytes": self.max_uncompressed_bytes,
                "max_compression_ratio": self.max_compression_ratio,
            },
            "auth": self.auth.audit_snapshot(),
        }

    def date_prefix(self, backup_date: Optional[str] = None) -> str:
        date_value = backup_date or self.backup_date
        if not date_value:
            raise RestoreConfigurationError("A backup date is required to derive the legacy prefix")
        parsed = datetime.strptime(date_value, "%Y-%m-%d")
        rendered = {
            "YYYY/MM/DD": parsed.strftime("%Y/%m/%d"),
            "YYYY-MM-DD": parsed.strftime("%Y-%m-%d"),
            "YYYYMMDD": parsed.strftime("%Y%m%d"),
        }[self.date_prefix_format]
        return "{0}/{1}/".format(self.s3_prefix.rstrip("/"), rendered)


@dataclass
class TargetConfig:
    """Target account with independent asset and identity Regions."""

    aws_account_id: str
    asset_region: str
    identity_region: str
    namespace: str = "default"
    auth: AuthConfig = field(default_factory=AuthConfig)

    def validate(self) -> List[str]:
        errors: List[str] = []
        if not isinstance(self.aws_account_id, str) or not _ACCOUNT_RE.match(self.aws_account_id):
            errors.append("target.aws_account_id must be a 12-digit account ID")
        if not isinstance(self.asset_region, str) or not _REGION_RE.match(self.asset_region):
            errors.append("target.asset_region is invalid")
        if not isinstance(self.identity_region, str) or not _REGION_RE.match(self.identity_region):
            errors.append("target.identity_region is invalid")
        if not isinstance(self.namespace, str) or not re.match(
            r"^[A-Za-z0-9._-]{1,64}$", self.namespace
        ):
            errors.append("target.namespace is invalid")
        errors.extend(self.auth.validate("target.auth"))
        role_match = (
            _ROLE_ARN_RE.match(self.auth.role_arn) if isinstance(self.auth.role_arn, str) else None
        )
        if role_match:
            if (
                isinstance(self.aws_account_id, str)
                and role_match.group("account") != self.aws_account_id
            ):
                errors.append("target.auth.role_arn must belong to the target account")
            if isinstance(self.identity_region, str) and role_match.group(
                "partition"
            ) != partition_for_region(self.identity_region):
                errors.append("target.auth.role_arn must use the target AWS partition")
        return errors

    def audit_snapshot(self) -> Dict[str, Any]:
        return {
            "aws_account_id": self.aws_account_id,
            "asset_region": self.asset_region,
            "identity_region": self.identity_region,
            "namespace": self.namespace,
            "auth": self.auth.audit_snapshot(),
        }


@dataclass
class IdentityMapping:
    """Reviewed mapping for a source Quick Sight principal."""

    source_principal_arn: str
    target_principal_arn: Optional[str] = None
    target_iam_arn: Optional[str] = None
    session_name: Optional[str] = None
    identity_center: bool = False

    def validate(self, target: TargetConfig) -> List[str]:
        errors: List[str] = []
        source_match = (
            _PRINCIPAL_ARN_RE.match(self.source_principal_arn)
            if isinstance(self.source_principal_arn, str)
            else None
        )
        if not source_match:
            errors.append(
                "identity mapping source_principal_arn must be a Quick Sight user or group ARN"
            )
        if self.target_principal_arn is not None:
            if not isinstance(self.target_principal_arn, str):
                errors.append("identity mapping target_principal_arn must be a string")
            else:
                errors.extend(validate_target_principal(self.target_principal_arn, target))
        if self.target_iam_arn is not None:
            match = (
                _IAM_ARN_RE.match(self.target_iam_arn)
                if isinstance(self.target_iam_arn, str)
                else None
            )
            if not match:
                errors.append("identity mapping target_iam_arn must be an IAM user or role ARN")
            elif match.group("account") != target.aws_account_id:
                errors.append("identity mapping target_iam_arn must belong to the target account")
            elif match.group("partition") != partition_for_region(target.identity_region):
                errors.append("identity mapping target_iam_arn must use the target partition")
            elif match.group("kind") == "role" and not self.session_name:
                errors.append("identity mapping for an IAM role requires session_name")
        if self.session_name is not None and (
            not isinstance(self.session_name, str)
            or not re.match(r"^[\w+=,.@-]{2,64}$", self.session_name)
        ):
            errors.append("identity mapping session_name is invalid")
        if not isinstance(self.identity_center, bool):
            errors.append("identity mapping identity_center must be a boolean")
        if self.identity_center and not self.target_principal_arn:
            errors.append("IAM Identity Center mappings require a target_principal_arn to verify")
        return errors


@dataclass
class RestoreOptions:
    """Operator-reviewed restore behavior."""

    mode: str = "full"
    conflict_policy: str = "update"
    conflict_prefix: Optional[str] = None
    failure_action: str = "ROLLBACK"
    continue_on_error: bool = False
    restore_identities: bool = True
    target_principals: List[str] = field(default_factory=list)
    identity_mappings: List[IdentityMapping] = field(default_factory=list)
    overrides_file: Optional[str] = None
    report_directory: str = "./restore-reports"
    poll_timeout_seconds: int = 1200
    validate_target_principals: bool = True

    def validate(self, source: SourceBackupConfig, target: TargetConfig) -> List[str]:
        errors: List[str] = []
        if not isinstance(self.mode, str) or self.mode not in (
            "full",
            "assets-only",
            "identities-only",
        ):
            errors.append("restore.mode must be full, assets-only, or identities-only")
        if not isinstance(self.conflict_policy, str) or self.conflict_policy not in (
            "update",
            "skip",
            "fail",
            "prefix",
        ):
            errors.append("restore.conflict_policy must be update, skip, fail, or prefix")
        if self.conflict_policy == "prefix":
            if not isinstance(self.conflict_prefix, str) or not re.match(
                r"^[A-Za-z0-9_-]{1,64}$", self.conflict_prefix
            ):
                errors.append(
                    "restore.conflict_prefix is required and must be a safe string for prefix policy"
                )
        elif self.conflict_prefix is not None and not isinstance(self.conflict_prefix, str):
            errors.append("restore.conflict_prefix must be a string")
        if not isinstance(self.failure_action, str) or self.failure_action not in (
            "ROLLBACK",
            "DO_NOTHING",
        ):
            errors.append("restore.failure_action must be ROLLBACK or DO_NOTHING")
        if not isinstance(self.continue_on_error, bool):
            errors.append("restore.continue_on_error must be a boolean")
        if not isinstance(self.restore_identities, bool):
            errors.append("restore.restore_identities must be a boolean")
        if self.mode == "assets-only" and self.restore_identities:
            errors.append("restore.restore_identities must be false when mode is assets-only")
        if self.mode == "identities-only" and not self.restore_identities:
            errors.append("restore.restore_identities must be true when mode is identities-only")
        if self.restore_identities and not source.backup_date:
            errors.append("identity restore requires an explicit source_backup.backup_date")
        if (
            isinstance(self.poll_timeout_seconds, bool)
            or not isinstance(self.poll_timeout_seconds, int)
            or not 1 <= self.poll_timeout_seconds <= 86400
        ):
            errors.append("restore.poll_timeout_seconds must be between 1 and 86400")
        if not isinstance(self.report_directory, str) or not self.report_directory:
            errors.append("restore.report_directory is required")
        if self.overrides_file is not None and (
            not isinstance(self.overrides_file, str)
            or not self.overrides_file.lower().endswith(".json")
        ):
            errors.append("restore.overrides_file must be a JSON file")
        if not isinstance(self.validate_target_principals, bool):
            errors.append("restore.validate_target_principals must be a boolean")
        if not isinstance(self.target_principals, list) or any(
            not isinstance(principal, str) for principal in self.target_principals
        ):
            errors.append("restore.target_principals must be a list of strings")
            principals: List[str] = []
        else:
            principals = self.target_principals
        for principal in principals:
            errors.extend(validate_target_principal(principal, target))
        if len(set(principals)) != len(principals):
            errors.append("restore.target_principals must not contain duplicates")
        if not isinstance(self.identity_mappings, list) or any(
            not isinstance(mapping, IdentityMapping) for mapping in self.identity_mappings
        ):
            errors.append("restore.identity_mappings must contain identity mappings")
            mappings: List[IdentityMapping] = []
        else:
            mappings = self.identity_mappings
        seen_sources = set()
        for mapping in mappings:
            if mapping.source_principal_arn in seen_sources:
                errors.append("restore.identity_mappings contains duplicate source principal ARNs")
            seen_sources.add(mapping.source_principal_arn)
            errors.extend(mapping.validate(target))
        return errors

    def audit_snapshot(
        self, root: Optional[str], overrides_sha256: Optional[str]
    ) -> Dict[str, Any]:
        def relative_path(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            if not root:
                return value
            try:
                return str(Path(value).resolve().relative_to(Path(root).resolve()))
            except ValueError:
                return value

        return {
            "mode": self.mode,
            "conflict_policy": self.conflict_policy,
            "conflict_prefix": self.conflict_prefix,
            "failure_action": self.failure_action,
            "continue_on_error": self.continue_on_error,
            "restore_identities": self.restore_identities,
            "target_principals": sorted(self.target_principals),
            "identity_mappings": sorted(
                [
                    {
                        "source_principal_arn": mapping.source_principal_arn,
                        "target_principal_arn": mapping.target_principal_arn,
                        "target_iam_arn": mapping.target_iam_arn,
                        "session_name": mapping.session_name,
                        "identity_center": mapping.identity_center,
                    }
                    for mapping in self.identity_mappings
                ],
                key=lambda item: item["source_principal_arn"],
            ),
            "overrides_file": relative_path(self.overrides_file),
            "overrides_sha256": overrides_sha256,
            "report_directory": relative_path(self.report_directory),
            "poll_timeout_seconds": self.poll_timeout_seconds,
            "validate_target_principals": self.validate_target_principals,
        }

    def mapping_by_source(self) -> Dict[str, IdentityMapping]:
        return {mapping.source_principal_arn: mapping for mapping in self.identity_mappings}


@dataclass
class RestoreConfig:
    """Complete backward-compatible restore configuration."""

    source_backup: SourceBackupConfig
    target: TargetConfig
    restore: RestoreOptions
    config_directory: str = ""
    config_path: str = ""

    def validate(self) -> None:
        errors = self.source_backup.validate()
        errors.extend(self.target.validate())
        errors.extend(self.restore.validate(self.source_backup, self.target))
        if self.config_directory and not Path(self.config_directory).is_absolute():
            errors.append("config_directory must be an absolute path")
        if self.config_path and not Path(self.config_path).is_absolute():
            errors.append("config_path must be an absolute path")
        if errors:
            raise RestoreConfigurationError(
                "Restore configuration validation failed:\n"
                + "\n".join("- " + error for error in errors)
            )

    def audit_snapshot(self, overrides_sha256: Optional[str]) -> Dict[str, Any]:
        return {
            "source_backup": self.source_backup.audit_snapshot(),
            "target": self.target.audit_snapshot(),
            "restore": self.restore.audit_snapshot(self.config_directory or None, overrides_sha256),
            "config_file": (Path(self.config_path).name if self.config_path else None),
            "version": "1.0",
        }

    def audit_digest(self, overrides_sha256: Optional[str]) -> str:
        encoded = json.dumps(
            self.audit_snapshot(overrides_sha256),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def partition_for_region(region: str) -> str:
    """Return the ARN partition implied by an AWS Region."""

    if region.startswith("cn-"):
        return "aws-cn"
    if region.startswith("us-gov-"):
        return "aws-us-gov"
    return "aws"


def parse_target_principal(arn: str) -> Optional[Dict[str, str]]:
    if not isinstance(arn, str):
        return None
    match = _PRINCIPAL_ARN_RE.match(arn)
    if not match:
        return None
    return match.groupdict()


def validate_target_principal(arn: str, target: TargetConfig) -> List[str]:
    """Validate that an ARN names a target Quick Sight user or group."""

    parsed = parse_target_principal(arn)
    if not parsed:
        return ["target principal must be a Quick Sight user or group ARN: {0}".format(arn)]
    errors: List[str] = []
    if parsed["partition"] != partition_for_region(target.identity_region):
        errors.append("target principal must use the target AWS partition: {0}".format(arn))
    if parsed["account"] != target.aws_account_id:
        errors.append("target principal belongs to a different AWS account: {0}".format(arn))
    if parsed["region"] != target.identity_region:
        errors.append("target principal must use target.identity_region: {0}".format(arn))
    if parsed["namespace"] != target.namespace:
        errors.append("target principal must use target.namespace: {0}".format(arn))
    return errors
