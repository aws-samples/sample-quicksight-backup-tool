"""Strict YAML/JSON loader for the separate restore configuration contract."""

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
import json
import math
import re

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode

from ..json_safety import loads_strict_json
from ..limits import MAX_CONFIG_BYTES, MAX_OVERRIDES_BYTES
from ..local_paths import (
    read_bounded_regular_file,
    reject_link_components,
    resolve_under_root,
)
from ..models.config import (
    AuthConfig,
    IdentityMapping,
    IdentityTableNames,
    RestoreConfig,
    RestoreOptions,
    SourceBackupConfig,
    TargetConfig,
)
from ..models.errors import RestoreConfigurationError

_ROOT_KEYS = {"source_backup", "target", "restore"}
_AUTH_KEYS = {"profile", "role_arn", "external_id", "role_session_name", "sts_region"}
_SOURCE_KEYS = {
    "s3_bucket_name",
    "s3_region",
    "storage_region",
    "dynamodb_region",
    "s3_prefix",
    "backup_date",
    "bundle_keys",
    "date_prefix_format",
    "identity_tables",
    "auth",
    "max_zip_entries",
    "max_uncompressed_bytes",
    "max_compression_ratio",
} | _AUTH_KEYS
_TARGET_KEYS = {
    "aws_account_id",
    "asset_region",
    "identity_region",
    "namespace",
    "auth",
} | _AUTH_KEYS
_RESTORE_KEYS = {
    "mode",
    "conflict_policy",
    "conflict_prefix",
    "failure_action",
    "continue_on_error",
    "restore_identities",
    "target_principals",
    "identity_mappings",
    "overrides_file",
    "report_directory",
    "poll_timeout_seconds",
    "validate_target_principals",
}
_TABLE_KEYS = {"users", "groups", "memberships"}
_MANIFEST_SOURCE_KEYS = {
    "schema_version",
    "backup_id",
    "complete",
    "mode",
    "s3_bucket_name",
    "s3_prefix",
    "backup_date",
    "date_prefix_format",
    "s3_region",
    "dynamodb_region",
    "identity_tables",
    "resolved_identity_tables",
    "bundle_keys",
}
_MANIFEST_CONFIG_KEYS = {
    "s3_bucket_name",
    "s3_prefix",
    "backup_date",
    "date_prefix_format",
    "s3_region",
    "dynamodb_region",
    "identity_tables",
    "bundle_keys",
}
_MAPPING_KEYS = {
    "source_principal_arn",
    "target_principal_arn",
    "target_iam_arn",
    "session_name",
    "identity_center",
}
_FORBIDDEN_KEYS = {
    "accesskeyid",
    "awsaccesskeyid",
    "secretaccesskey",
    "awssecretaccesskey",
    "sessiontoken",
    "awssessiontoken",
    "password",
    "credentialpair",
}


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader, node: MappingNode, deep: bool = False
) -> Dict[Any, Any]:
    if not isinstance(node, MappingNode):
        raise ConstructorError(None, None, "expected a mapping node", node.start_mark)
    loader.flatten_mapping(node)
    mapping: Dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable key",
                key_node.start_mark,
            )
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found duplicate key: {0}".format(key),
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


class RestoreConfigLoader:
    """Load and validate restore configuration without contacting AWS."""

    def load(
        self,
        path: str,
        backup_date: Optional[str] = None,
        bundle_keys: Optional[List[str]] = None,
        backup_manifest: Optional[str] = None,
    ) -> RestoreConfig:
        raw_path = Path(path).expanduser()
        if raw_path.suffix.lower() not in (".yaml", ".yml", ".json"):
            raise RestoreConfigurationError("Restore configuration must be YAML or JSON")
        try:
            config_path = reject_link_components(
                raw_path, "restore configuration file", allow_missing=False
            )
            encoded = read_bounded_regular_file(
                config_path, MAX_CONFIG_BYTES, "restore configuration file"
            )
            text = encoded.decode("utf-8")
            if config_path.suffix.lower() in (".yaml", ".yml"):
                loader = _UniqueKeySafeLoader(text)
                try:
                    raw = loader.get_single_data()
                finally:
                    loader.dispose()
            else:
                raw = loads_strict_json(text)
        except RestoreConfigurationError:
            raise
        except (
            OSError,
            UnicodeDecodeError,
            yaml.YAMLError,
            json.JSONDecodeError,
            ValueError,
        ) as error:
            raise RestoreConfigurationError(
                "Unable to read restore configuration: {0}".format(error)
            )
        if not isinstance(raw, Mapping):
            raise RestoreConfigurationError("Restore configuration root must be an object")
        self._reject_plaintext_credentials(raw)
        self._reject_unknown_keys(raw, _ROOT_KEYS, "config")
        manifest_source = self._load_backup_manifest(backup_manifest) if backup_manifest else None
        source_value = raw.get("source_backup")
        if source_value is None and manifest_source is not None:
            source_value = {}
        source_data = dict(self._mapping(source_value, "source_backup"))
        target_data = self._mapping(raw.get("target"), "target")
        restore_data = self._mapping(raw.get("restore"), "restore")
        self._reject_unknown_keys(source_data, _SOURCE_KEYS, "source_backup")
        self._reject_unknown_keys(target_data, _TARGET_KEYS, "target")
        self._reject_unknown_keys(restore_data, _RESTORE_KEYS, "restore")

        if manifest_source is not None:
            for key in _MANIFEST_CONFIG_KEYS:
                if key not in manifest_source:
                    continue
                manifest_value = manifest_source[key]
                existing_value = source_data.get(key)
                if existing_value not in (None, "", [], {}) and existing_value != manifest_value:
                    raise RestoreConfigurationError(
                        "source_backup.{0} conflicts with the backup manifest".format(key)
                    )
                source_data[key] = manifest_value

        source_region = self._aliased_string(
            source_data,
            "s3_region",
            "storage_region",
            "source_backup",
            required=True,
        )
        source = SourceBackupConfig(
            s3_bucket_name=self._required_string(source_data, "s3_bucket_name", "source_backup"),
            s3_region=source_region,
            dynamodb_region=self._required_string(source_data, "dynamodb_region", "source_backup"),
            s3_prefix=self._default_string(
                source_data, "s3_prefix", "quicksight-backups", "source_backup"
            ),
            backup_date=self._optional_string(
                source_data.get("backup_date"), "source_backup.backup_date"
            ),
            bundle_keys=self._string_list(
                source_data.get("bundle_keys", []), "source_backup.bundle_keys"
            ),
            date_prefix_format=self._default_string(
                source_data,
                "date_prefix_format",
                "YYYY/MM/DD",
                "source_backup",
            ),
            identity_tables=self._load_table_names(source_data.get("identity_tables")),
            auth=self._load_auth(source_data, "source_backup"),
            max_zip_entries=self._integer(
                source_data.get("max_zip_entries", 10000),
                "source_backup.max_zip_entries",
            ),
            max_uncompressed_bytes=self._integer(
                source_data.get("max_uncompressed_bytes", 2 * 1024 * 1024 * 1024),
                "source_backup.max_uncompressed_bytes",
            ),
            max_compression_ratio=self._number(
                source_data.get("max_compression_ratio", 100.0),
                "source_backup.max_compression_ratio",
            ),
        )
        if backup_date is not None:
            if not isinstance(backup_date, str):
                raise RestoreConfigurationError("backup_date override must be a string")
            source.backup_date = backup_date
        if bundle_keys:
            source.bundle_keys = self._string_list(bundle_keys, "bundle_keys override")

        target = TargetConfig(
            aws_account_id=self._required_string(target_data, "aws_account_id", "target"),
            asset_region=self._required_string(target_data, "asset_region", "target"),
            identity_region=self._required_string(target_data, "identity_region", "target"),
            namespace=self._default_string(target_data, "namespace", "default", "target"),
            auth=self._load_auth(target_data, "target"),
        )

        root = config_path.parent.resolve()
        overrides_file = self._resolve_optional_path(
            restore_data.get("overrides_file"), root, "restore.overrides_file"
        )
        report_directory = str(
            resolve_under_root(
                self._default_string(
                    restore_data,
                    "report_directory",
                    "./restore-reports",
                    "restore",
                ),
                root,
                "restore.report_directory",
            )
        )
        restore = RestoreOptions(
            mode=self._default_string(restore_data, "mode", "full", "restore"),
            conflict_policy=self._default_string(
                restore_data, "conflict_policy", "update", "restore"
            ),
            conflict_prefix=self._optional_string(
                restore_data.get("conflict_prefix"), "restore.conflict_prefix"
            ),
            failure_action=self._default_string(
                restore_data, "failure_action", "ROLLBACK", "restore"
            ),
            continue_on_error=self._boolean(
                restore_data.get("continue_on_error", False),
                "restore.continue_on_error",
            ),
            restore_identities=self._boolean(
                restore_data.get("restore_identities", True),
                "restore.restore_identities",
            ),
            target_principals=self._string_list(
                restore_data.get("target_principals", []),
                "restore.target_principals",
            ),
            identity_mappings=self._load_identity_mappings(
                restore_data.get("identity_mappings", [])
            ),
            overrides_file=overrides_file,
            report_directory=report_directory,
            poll_timeout_seconds=self._integer(
                restore_data.get("poll_timeout_seconds", 1200),
                "restore.poll_timeout_seconds",
            ),
            validate_target_principals=self._boolean(
                restore_data.get("validate_target_principals", True),
                "restore.validate_target_principals",
            ),
        )
        if manifest_source is not None:
            backup_mode = manifest_source["mode"]
            compatible_modes = {
                "full": {"full"},
                "assets-only": {"full", "assets-only"},
                "identities-only": {"full", "users-only"},
            }
            if backup_mode not in compatible_modes.get(restore.mode, set()):
                raise RestoreConfigurationError(
                    "restore mode {0} is incompatible with manifest backup mode {1}".format(
                        restore.mode, backup_mode
                    )
                )

        config = RestoreConfig(
            source_backup=source,
            target=target,
            restore=restore,
            config_directory=str(root),
            config_path=str(config_path),
        )
        if restore.overrides_file:
            try:
                read_bounded_regular_file(
                    Path(restore.overrides_file),
                    MAX_OVERRIDES_BYTES,
                    "restore overrides file",
                )
            except (OSError, ValueError) as error:
                raise RestoreConfigurationError(
                    "Restore overrides file not found or not regular: {0}".format(error)
                )
        config.validate()
        return config

    def _load_backup_manifest(self, path: str) -> Dict[str, Any]:
        """Load the authoritative restore-source section from a backup manifest."""
        raw_path = Path(path).expanduser()
        if raw_path.suffix.lower() != ".json":
            raise RestoreConfigurationError("Backup manifest must be JSON")
        try:
            manifest_path = reject_link_components(
                raw_path, "backup manifest file", allow_missing=False
            )
            encoded = read_bounded_regular_file(
                manifest_path, MAX_OVERRIDES_BYTES, "backup manifest file"
            )
            raw = loads_strict_json(encoded.decode("utf-8"))
        except RestoreConfigurationError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise RestoreConfigurationError("Unable to read backup manifest: {0}".format(error))
        if not isinstance(raw, Mapping):
            raise RestoreConfigurationError("Backup manifest root must be an object")
        self._reject_plaintext_credentials(raw, "backup_manifest")
        source = self._mapping(raw.get("restore_source"), "backup_manifest.restore_source")
        self._reject_unknown_keys(source, _MANIFEST_SOURCE_KEYS, "backup_manifest.restore_source")
        if source.get("schema_version") != "1.0":
            raise RestoreConfigurationError("Unsupported backup manifest restore schema")
        if source.get("complete") is not True:
            raise RestoreConfigurationError(
                "Backup manifest is incomplete; restore requires a backup without failures"
            )
        backup_id = self._required_string(source, "backup_id", "backup_manifest.restore_source")
        if not re.match(r"^backup-[A-Za-z0-9._-]+$", backup_id):
            raise RestoreConfigurationError("Backup manifest backup_id is invalid")
        mode = self._required_string(source, "mode", "backup_manifest.restore_source")
        if mode not in ("full", "assets-only", "users-only"):
            raise RestoreConfigurationError("Backup manifest mode is invalid")

        tables = self._mapping(
            source.get("identity_tables"),
            "backup_manifest.restore_source.identity_tables",
        )
        resolved_tables = self._mapping(
            source.get("resolved_identity_tables"),
            "backup_manifest.restore_source.resolved_identity_tables",
        )
        self._reject_unknown_keys(tables, _TABLE_KEYS, "backup manifest identity tables")
        self._reject_unknown_keys(
            resolved_tables, _TABLE_KEYS, "backup manifest resolved identity tables"
        )
        backup_date = self._required_string(source, "backup_date", "backup_manifest.restore_source")
        for name in _TABLE_KEYS:
            base_name = self._required_string(tables, name, "backup manifest identity tables")
            resolved_name = self._required_string(
                resolved_tables, name, "backup manifest resolved identity tables"
            )
            if resolved_name != "{0}-{1}".format(backup_date, base_name):
                raise RestoreConfigurationError(
                    "Backup manifest resolved identity table does not match its date and base"
                )

        bundle_keys = self._string_list(
            source.get("bundle_keys", []),
            "backup_manifest.restore_source.bundle_keys",
        )
        if len(set(bundle_keys)) != len(bundle_keys):
            raise RestoreConfigurationError("Backup manifest bundle keys must be unique")
        if mode in ("full", "assets-only") and not bundle_keys:
            raise RestoreConfigurationError(
                "Backup manifest does not contain any successful asset bundle keys"
            )
        return dict(source)

    def _load_auth(self, section: Mapping[str, Any], label: str) -> AuthConfig:
        nested_value = section.get("auth", {})
        if nested_value is None:
            nested_value = {}
        nested = self._mapping(nested_value, "{0}.auth".format(label))
        self._reject_unknown_keys(nested, _AUTH_KEYS, "{0}.auth".format(label))
        values: Dict[str, Any] = {}
        for key in _AUTH_KEYS:
            nested_present = key in nested
            direct_present = key in section
            if nested_present and direct_present and nested[key] != section[key]:
                raise RestoreConfigurationError(
                    "Conflicting {0}.{1} and {0}.auth.{1} values".format(label, key)
                )
            if nested_present:
                values[key] = nested[key]
            elif direct_present:
                values[key] = section[key]
        return AuthConfig(
            profile=self._optional_string(values.get("profile"), "{0}.auth.profile".format(label)),
            role_arn=self._optional_string(
                values.get("role_arn"), "{0}.auth.role_arn".format(label)
            ),
            external_id=self._optional_string(
                values.get("external_id"), "{0}.auth.external_id".format(label)
            ),
            role_session_name=self._default_value_string(
                values.get("role_session_name"),
                "quicksight-restore",
                "{0}.auth.role_session_name".format(label),
            ),
            sts_region=self._optional_string(
                values.get("sts_region"), "{0}.auth.sts_region".format(label)
            ),
        )

    def _load_table_names(self, value: Any) -> IdentityTableNames:
        if value is None:
            return IdentityTableNames()
        data = self._mapping(value, "source_backup.identity_tables")
        self._reject_unknown_keys(data, _TABLE_KEYS, "source_backup.identity_tables")
        return IdentityTableNames(
            users=self._default_string(
                data, "users", "quicksight-users-backup", "source_backup.identity_tables"
            ),
            groups=self._default_string(
                data, "groups", "quicksight-groups-backup", "source_backup.identity_tables"
            ),
            memberships=self._default_string(
                data,
                "memberships",
                "quicksight-users-groups-backup",
                "source_backup.identity_tables",
            ),
        )

    def _load_identity_mappings(self, value: Any) -> List[IdentityMapping]:
        mappings: List[IdentityMapping] = []
        if isinstance(value, Mapping):
            iterable = []
            for source, destination in value.items():
                if not isinstance(source, str):
                    raise RestoreConfigurationError("identity mapping source keys must be strings")
                if isinstance(destination, str):
                    item = {
                        "source_principal_arn": source,
                        "target_principal_arn": destination,
                    }
                elif isinstance(destination, Mapping):
                    item = dict(destination)
                    if "source_principal_arn" in item and item["source_principal_arn"] != source:
                        raise RestoreConfigurationError(
                            "Conflicting identity mapping source principal"
                        )
                    item["source_principal_arn"] = source
                else:
                    raise RestoreConfigurationError(
                        "identity mapping values must be strings or objects"
                    )
                iterable.append(item)
        elif isinstance(value, list):
            iterable = value
        else:
            raise RestoreConfigurationError("restore.identity_mappings must be a list or object")
        for index, raw in enumerate(iterable):
            label = "restore.identity_mappings[{0}]".format(index)
            data = self._mapping(raw, label)
            self._reject_unknown_keys(data, _MAPPING_KEYS, label)
            mappings.append(
                IdentityMapping(
                    source_principal_arn=self._required_string(data, "source_principal_arn", label),
                    target_principal_arn=self._optional_string(
                        data.get("target_principal_arn"),
                        "{0}.target_principal_arn".format(label),
                    ),
                    target_iam_arn=self._optional_string(
                        data.get("target_iam_arn"),
                        "{0}.target_iam_arn".format(label),
                    ),
                    session_name=self._optional_string(
                        data.get("session_name"),
                        "{0}.session_name".format(label),
                    ),
                    identity_center=self._boolean(
                        data.get("identity_center", False),
                        "{0}.identity_center".format(label),
                    ),
                )
            )
        return sorted(mappings, key=lambda item: item.source_principal_arn)

    def _reject_plaintext_credentials(self, value: Any, path: str = "config") -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
                if normalized in _FORBIDDEN_KEYS:
                    raise RestoreConfigurationError(
                        "Plaintext credentials are not allowed in restore configuration ({0}.{1})".format(
                            path, key
                        )
                    )
                self._reject_plaintext_credentials(child, "{0}.{1}".format(path, key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                self._reject_plaintext_credentials(child, "{0}[{1}]".format(path, index))

    @staticmethod
    def _reject_unknown_keys(data: Mapping[str, Any], allowed: set, label: str) -> None:
        for key in data:
            if not isinstance(key, str) or key not in allowed:
                raise RestoreConfigurationError(
                    "Unknown restore configuration key: {0}.{1}".format(label, key)
                )

    @staticmethod
    def _mapping(value: Any, label: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise RestoreConfigurationError("{0} must be an object".format(label))
        return value

    @staticmethod
    def _required_string(data: Mapping[str, Any], key: str, label: str) -> str:
        value = data.get(key)
        if not isinstance(value, str) or not value:
            raise RestoreConfigurationError(
                "{0}.{1} is required and must be a string".format(label, key)
            )
        return value

    @classmethod
    def _default_string(cls, data: Mapping[str, Any], key: str, default: str, label: str) -> str:
        return cls._default_value_string(data.get(key), default, "{0}.{1}".format(label, key))

    @staticmethod
    def _default_value_string(value: Any, default: str, label: str) -> str:
        if value is None:
            return default
        if not isinstance(value, str):
            raise RestoreConfigurationError("{0} must be a string".format(label))
        return value

    @staticmethod
    def _optional_string(value: Any, label: str) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            raise RestoreConfigurationError("{0} must be a string".format(label))
        return value

    @staticmethod
    def _string_list(value: Any, label: str) -> List[str]:
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise RestoreConfigurationError("{0} must be a list of strings".format(label))
        return list(value)

    @staticmethod
    def _boolean(value: Any, label: str) -> bool:
        if not isinstance(value, bool):
            raise RestoreConfigurationError("{0} must be a boolean".format(label))
        return value

    @staticmethod
    def _integer(value: Any, label: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise RestoreConfigurationError("{0} must be an integer".format(label))
        return value

    @staticmethod
    def _number(value: Any, label: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise RestoreConfigurationError("{0} must be a finite number".format(label))
        return float(value)

    @staticmethod
    def _aliased_string(
        data: Mapping[str, Any],
        canonical: str,
        alias: str,
        label: str,
        required: bool,
    ) -> str:
        if canonical in data and alias in data and data[canonical] != data[alias]:
            raise RestoreConfigurationError(
                "Conflicting {0}.{1} and {0}.{2} values".format(label, canonical, alias)
            )
        value = data.get(canonical, data.get(alias))
        if not isinstance(value, str) or (required and not value):
            raise RestoreConfigurationError(
                "{0}.{1} is required and must be a string".format(label, canonical)
            )
        return value

    @staticmethod
    def _resolve_optional_path(value: Any, root: Path, label: str) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise RestoreConfigurationError("{0} must be a non-empty path string".format(label))
        return str(resolve_under_root(value, root, label))
