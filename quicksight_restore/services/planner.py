"""Read-only deterministic planning for Quick Sight restores."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple
import copy
import hashlib
import json
import os
import re
import tempfile

from botocore.exceptions import ClientError, ParamValidationError
from botocore.session import Session
from botocore.validate import validate_parameters

from ..json_safety import loads_strict_json
from ..limits import (
    IMPORT_ACTION,
    IMPORT_TRANSPORT_INLINE,
    IMPORT_TRANSPORT_NONE,
    INLINE_IMPORT_MAX_BYTES,
    MAX_PLAN_BYTES,
    SKIP_POLICY_ACTION,
)
from ..local_paths import read_bounded_regular_file, reject_link_components
from ..models.config import RestoreConfig, parse_target_principal, validate_target_principal
from ..models.contracts import (
    BundleInventory,
    BundleMemberInventory,
    ConflictDecision,
    DuplicateDecision,
    LegacyRestoreManifest,
    PlannedBundle,
    RestorePlan,
    TargetFingerprint,
    canonical_json,
)
from ..models.errors import RestorePlanningError
from ..permissions import PERMISSION_OVERRIDE_SPECS, build_override_permissions

_TIER = {
    "datasources": 0,
    "themes": 0,
    "folders": 0,
    "datasets": 1,
    "analyses": 2,
    "dashboards": 3,
}
_RESOURCE_ROOT = {
    "datasource": "datasources",
    "dataset": "datasets",
    "analysis": "analyses",
    "dashboard": "dashboards",
    "theme": "themes",
    "folder": "folders",
    "topic": "topics",
    "vpcconnection": "datasources",
    "refreshschedule": "datasets",
}
_ALLOWED_OVERRIDE_KEYS = {
    "OverrideParameters",
    "OverridePermissions",
    "OverrideTags",
    "OverrideValidationStrategy",
}
_FORBIDDEN_OVERRIDE_FIELDS = {
    "password",
    "credentialpair",
    "secretaccesskey",
    "awssecretaccesskey",
}
# top-level override -> section -> (resource type, identifier field, identifier is a list)
_OVERRIDE_ENTRY_SPECS = {
    "OverrideParameters": {
        "DataSources": ("datasource", "DataSourceId", False),
        "DataSets": ("dataset", "DataSetId", False),
        "Themes": ("theme", "ThemeId", False),
        "Analyses": ("analysis", "AnalysisId", False),
        "Dashboards": ("dashboard", "DashboardId", False),
        "Folders": ("folder", "FolderId", False),
        "VPCConnections": ("vpcconnection", "VPCConnectionId", False),
        # Refresh schedules are scoped by their parent data set and schedule ID.
        "RefreshSchedules": ("dataset", "DataSetId", False),
    },
    "OverridePermissions": {
        "DataSources": ("datasource", "DataSourceIds", True),
        "DataSets": ("dataset", "DataSetIds", True),
        "Themes": ("theme", "ThemeIds", True),
        "Analyses": ("analysis", "AnalysisIds", True),
        "Dashboards": ("dashboard", "DashboardIds", True),
        "Folders": ("folder", "FolderIds", True),
    },
    "OverrideTags": {
        "DataSources": ("datasource", "DataSourceIds", True),
        "DataSets": ("dataset", "DataSetIds", True),
        "Themes": ("theme", "ThemeIds", True),
        "Analyses": ("analysis", "AnalysisIds", True),
        "Dashboards": ("dashboard", "DashboardIds", True),
        "Folders": ("folder", "FolderIds", True),
        "VPCConnections": ("vpcconnection", "VPCConnectionIds", True),
    },
}
_RESOURCE_ID_CONFIGURATION = "ResourceIdOverrideConfiguration"
_NON_PERMISSION_RESOURCE_TYPES = {"vpcconnection", "refreshschedule"}
_RESOURCE_ID_LIMITS = {
    "datasource": 512,
    "dataset": 512,
    "analysis": 512,
    "dashboard": 512,
    "theme": 512,
    "folder": 2048,
    "topic": 256,
    "vpcconnection": 1000,
}
_STANDARD_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_TOPIC_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9_.+\\-]+$")


class QuickSightTargetInventory:
    """Read-only target inventory backed by list APIs."""

    _OPERATIONS = {
        "datasource": ("list_data_sources", "DataSources", "DataSourceId"),
        "dataset": ("list_data_sets", "DataSetSummaries", "DataSetId"),
        "analysis": ("list_analyses", "AnalysisSummaryList", "AnalysisId"),
        "dashboard": ("list_dashboards", "DashboardSummaryList", "DashboardId"),
        "theme": ("list_themes", "ThemeSummaryList", "ThemeId"),
        "folder": ("list_folders", "FolderSummaryList", "FolderId"),
        "topic": ("list_topics", "TopicsSummaries", "TopicId"),
        "vpcconnection": (
            "list_vpc_connections",
            "VPCConnectionSummaries",
            "VPCConnectionId",
        ),
    }

    def __init__(self, client: Any, account_id: str):
        self.client = client
        self.account_id = account_id

    def list_resources(self, resource_types: Iterable[str]) -> Set[str]:
        requested = set(resource_types)
        resources: Set[str] = set()
        dataset_ids: Optional[List[str]] = None
        for resource_type in sorted(requested - {"refreshschedule"}):
            resources.update(self._list_standard_resources(resource_type))
        if "refreshschedule" in requested:
            dataset_ids = sorted(
                item.split("/", 1)[1] for item in self._list_standard_resources("dataset")
            )
            resources.update("dataset/{0}".format(item) for item in dataset_ids)
            for data_set_id in dataset_ids:
                response = self.client.list_refresh_schedules(
                    AwsAccountId=self.account_id,
                    DataSetId=data_set_id,
                )
                for item in response.get("RefreshSchedules", []):
                    schedule_id = item.get("ScheduleId")
                    if schedule_id:
                        resources.add("refreshschedule/{0}/{1}".format(data_set_id, schedule_id))
        return resources

    def _list_standard_resources(self, resource_type: str) -> Set[str]:
        operation_spec = self._OPERATIONS.get(resource_type)
        if not operation_spec:
            return set()
        operation_name, response_key, id_key = operation_spec
        operation = getattr(self.client, operation_name)
        resources: Set[str] = set()
        token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"AwsAccountId": self.account_id}
            if token:
                request["NextToken"] = token
            response = operation(**request)
            for item in response.get(response_key, []):
                resource_id = item.get(id_key)
                if resource_id:
                    resources.add("{0}/{1}".format(resource_type, resource_id))
            token = response.get("NextToken")
            if not token:
                break
        return resources


class RestorePlanner:
    """Select one owner for every member and resolve target conflict behavior."""

    def __init__(
        self,
        config: RestoreConfig,
        target_quicksight_client: Optional[Any] = None,
        target_identity_quicksight_client: Optional[Any] = None,
        target_inventory: Optional[Any] = None,
    ):
        self.config = config
        self.target_client = target_quicksight_client
        self.identity_client = target_identity_quicksight_client or target_quicksight_client
        self.target_inventory = target_inventory or (
            QuickSightTargetInventory(target_quicksight_client, config.target.aws_account_id)
            if target_quicksight_client is not None
            else None
        )
        service_model = Session().get_service_model("quicksight")
        self.import_request_shape = service_model.operation_model(
            "StartAssetBundleImportJob"
        ).input_shape

    def build_plan(
        self,
        manifest: LegacyRestoreManifest,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> RestorePlan:
        native_overrides = copy.deepcopy(overrides or {})
        self._validate_overrides_shape(native_overrides)
        self._validate_target_principals()

        bundle_by_key = {bundle.key: bundle for bundle in manifest.bundles}
        if len(bundle_by_key) != len(manifest.bundles):
            raise RestorePlanningError("Restore manifest contains duplicate bundle keys")
        for bundle in manifest.bundles:
            if bundle.size < 0 or bundle.size > INLINE_IMPORT_MAX_BYTES:
                raise RestorePlanningError(
                    "Bundle {0} is {1} bytes; inline Quick Sight imports are limited to {2} bytes and target-owned S3 staging is not implemented".format(
                        bundle.key, bundle.size, INLINE_IMPORT_MAX_BYTES
                    )
                )

        grouped: Dict[str, List[Tuple[BundleInventory, BundleMemberInventory]]] = {}
        for bundle in sorted(manifest.bundles, key=lambda item: item.key):
            if not bundle.members:
                raise RestorePlanningError(
                    "Restore bundle contains no cataloged resources: {0}".format(bundle.key)
                )
            for member in bundle.members:
                if not member.known_resource:
                    raise RestorePlanningError(
                        "Unsupported ancillary member reached planning: {0}:{1}".format(
                            bundle.key, member.member_name
                        )
                    )
                grouped.setdefault(member.resource_key, []).append((bundle, member))

        duplicate_decisions: List[DuplicateDecision] = []
        warnings = list(manifest.warnings)
        dependency_scopes = {
            scope
            for candidates in grouped.values()
            for _, member in candidates
            for scope in member.dependency_scopes
        }
        if len(dependency_scopes) > 1:
            raise RestorePlanningError(
                "Selected archives contain Quick Sight dependencies from multiple source scopes: {0}".format(
                    ", ".join(sorted(dependency_scopes))
                )
            )
        for resource_key in sorted(grouped):
            candidates = grouped[resource_key]
            hashes = {member.canonical_json_sha256 for _, member in candidates}
            if len(hashes) > 1:
                details = ", ".join(
                    "{0}:{1}".format(bundle.key, member.canonical_json_sha256)
                    for bundle, member in candidates
                )
                raise RestorePlanningError(
                    "Conflicting duplicate definitions for {0}: {1}".format(resource_key, details)
                )
            if len(candidates) > 1:
                canonical = sorted(candidates, key=self._ownership_score)[0]
                duplicate_keys = sorted(
                    bundle.key for bundle, _ in candidates if bundle.key != canonical[0].key
                )
                duplicate_decisions.append(
                    DuplicateDecision(
                        resource_key=resource_key,
                        canonical_json_sha256=canonical[1].canonical_json_sha256,
                        selected_bundle_key=canonical[0].key,
                        selected_member_name=canonical[1].member_name,
                        duplicate_bundle_keys=duplicate_keys,
                        reason=(
                            "matching definitions verified; every original archive is replayed atomically"
                        ),
                    )
                )
                warnings.append(
                    "Verified matching duplicate {0}; preserved all original archives".format(
                        resource_key
                    )
                )

        source_resources = set(grouped)
        dependency_resources = {
            dependency
            for candidates in grouped.values()
            for _, member in candidates
            for dependency in member.dependencies
        }
        resource_types = {item.split("/", 1)[0] for item in source_resources | dependency_resources}
        target_resources: Set[str] = set()
        if self.target_inventory is not None:
            try:
                target_resources = set(self.target_inventory.list_resources(resource_types))
            except ClientError as error:
                raise RestorePlanningError(
                    "Unable to inventory target Quick Sight assets: {0}".format(error)
                )

        destinations: Dict[str, str] = {}
        reverse_destinations: Dict[str, Set[str]] = {}
        for resource_key in sorted(source_resources):
            destination = self._destination_resource_key(resource_key)
            destinations[resource_key] = destination
            reverse_destinations.setdefault(destination, set()).add(resource_key)
        collisions = {
            destination: sources
            for destination, sources in reverse_destinations.items()
            if len(sources) > 1
        }
        if collisions:
            details = "; ".join(
                "{0} <- {1}".format(destination, ", ".join(sorted(sources)))
                for destination, sources in sorted(collisions.items())
            )
            raise RestorePlanningError(
                "Multiple source resources resolve to the same target ID: {0}".format(details)
            )

        conflict_decisions: List[ConflictDecision] = []
        actions: Dict[str, str] = {}
        for resource_key in sorted(source_resources):
            destination = destinations[resource_key]
            exists = destination in target_resources
            policy = self.config.restore.conflict_policy
            if policy == "prefix":
                if exists:
                    raise RestorePlanningError(
                        "Prefixed target conflict for {0} at {1}".format(resource_key, destination)
                    )
                action = "prefix"
            elif exists and policy == "fail":
                raise RestorePlanningError(
                    "Target conflict for {0} under fail policy".format(resource_key)
                )
            elif exists and policy == "skip":
                action = "skip"
            elif exists:
                action = "update"
            else:
                action = "create"
            actions[resource_key] = action
            conflict_decisions.append(
                ConflictDecision(
                    resource_key=resource_key,
                    destination_resource_key=destination,
                    policy=policy,
                    action=action,
                    target_exists=exists,
                )
            )

        selected_bundles: Dict[str, BundleInventory] = {}
        skipped_bundles: Dict[str, BundleInventory] = {}
        for bundle in sorted(manifest.bundles, key=lambda item: item.key):
            bundle_resources = {member.resource_key for member in bundle.members}
            skipped = {resource for resource in bundle_resources if actions[resource] == "skip"}
            if skipped:
                if skipped != bundle_resources:
                    raise RestorePlanningError(
                        "Skip policy would require unsupported member-level rewriting of {0}; skipped={1}, retained={2}".format(
                            bundle.key,
                            ", ".join(sorted(skipped)),
                            ", ".join(sorted(bundle_resources - skipped)),
                        )
                    )
                warnings.append(
                    "Skipped whole archive because every contained resource exists: {0}".format(
                        bundle.key
                    )
                )
                skipped_bundles[bundle.key] = bundle
                continue
            selected_bundles[bundle.key] = bundle

        selected_resource_keys = {
            member.resource_key for bundle in selected_bundles.values() for member in bundle.members
        }
        self._validate_override_references(native_overrides, selected_resource_keys)
        self._validate_permission_generation_support(selected_resource_keys)
        ordered_keys, prerequisites = self._dependency_order(
            selected_bundles, target_resources, self.config.restore.conflict_policy
        )

        planned: List[PlannedBundle] = []
        for key in sorted(skipped_bundles):
            bundle = skipped_bundles[key]
            selected = sorted(bundle.members, key=lambda item: item.member_name)
            planned.append(
                PlannedBundle(
                    bucket=bundle.bucket,
                    key=bundle.key,
                    version_id=bundle.version_id,
                    size=bundle.size,
                    sha256=bundle.sha256,
                    root_type=bundle.root_type,
                    selected_member_names=[item.member_name for item in selected],
                    selected_resources=[item.resource_key for item in selected],
                    execution_action=SKIP_POLICY_ACTION,
                    import_transport=IMPORT_TRANSPORT_NONE,
                    import_overrides={},
                    omitted_member_names=[],
                    prerequisite_bundle_keys=[],
                    materialization_mode="original",
                    order=len(planned),
                )
            )

        for key in ordered_keys:
            bundle = selected_bundles[key]
            selected = sorted(bundle.members, key=lambda item: item.member_name)
            selected_names = [item.member_name for item in selected]
            selected_resources = [item.resource_key for item in selected]
            import_overrides = self._compile_bundle_overrides(
                native_overrides, set(selected_resources)
            )
            self._validate_final_import_request(import_overrides, bundle.key)
            planned.append(
                PlannedBundle(
                    bucket=bundle.bucket,
                    key=bundle.key,
                    version_id=bundle.version_id,
                    size=bundle.size,
                    sha256=bundle.sha256,
                    root_type=bundle.root_type,
                    selected_member_names=selected_names,
                    selected_resources=selected_resources,
                    execution_action=IMPORT_ACTION,
                    import_transport=IMPORT_TRANSPORT_INLINE,
                    import_overrides=import_overrides,
                    omitted_member_names=[],
                    prerequisite_bundle_keys=prerequisites[key],
                    materialization_mode="original",
                    order=len(planned),
                )
            )

        mapping_payload = sorted(
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
        overrides_digest = (
            hashlib.sha256(canonical_json(native_overrides)).hexdigest()
            if native_overrides
            else None
        )
        plan = RestorePlan(
            manifest=manifest,
            target=TargetFingerprint(
                aws_account_id=self.config.target.aws_account_id,
                asset_region=self.config.target.asset_region,
                identity_region=self.config.target.identity_region,
                namespace=self.config.target.namespace,
            ),
            bundles=planned,
            duplicate_decisions=duplicate_decisions,
            conflict_decisions=conflict_decisions,
            conflict_policy=self.config.restore.conflict_policy,
            conflict_prefix=self.config.restore.conflict_prefix,
            failure_action=self.config.restore.failure_action,
            continue_on_error=self.config.restore.continue_on_error,
            restore_identities=self.config.restore.restore_identities,
            target_principals=sorted(self.config.restore.target_principals),
            identity_mappings=mapping_payload,
            overrides=native_overrides,
            overrides_sha256=overrides_digest,
            created_at=datetime.now(timezone.utc).isoformat(),
            config_snapshot=self.config.audit_snapshot(overrides_digest),
            warnings=warnings,
        )
        return plan.seal()

    @staticmethod
    def save_plan(plan: RestorePlan, path: str) -> None:
        try:
            plan.validate_seal()
            serialized = (
                json.dumps(
                    plan.to_dict(),
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            )
            encoded = serialized.encode("utf-8")
            if len(encoded) > MAX_PLAN_BYTES:
                raise ValueError("restore plan exceeds the local size limit")
            destination = reject_link_components(
                Path(path).expanduser(),
                "restore plan output",
                allow_missing=True,
            )
            if destination.exists():
                raise FileExistsError(str(destination))
            destination.parent.mkdir(parents=True, exist_ok=True)
            reject_link_components(
                destination.parent,
                "restore plan output directory",
                allow_missing=False,
            )
        except FileExistsError:
            raise RestorePlanningError(
                "Restore plan output already exists; choose a new reviewed path"
            )
        except (OSError, TypeError, ValueError) as error:
            raise RestorePlanningError("Unable to prepare restore plan: {0}".format(error))

        descriptor, temporary = tempfile.mkstemp(
            prefix=destination.name + ".", suffix=".tmp", dir=str(destination.parent)
        )
        published = False
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, destination)
            published = True
            RestorePlanner._fsync_directory(destination.parent)
        except FileExistsError:
            raise RestorePlanningError(
                "Restore plan output already exists; choose a new reviewed path"
            )
        except OSError as error:
            raise RestorePlanningError("Unable to persist restore plan: {0}".format(error))
        finally:
            try:
                os.unlink(temporary)
            except OSError:
                pass
        if not published:
            raise RestorePlanningError("Restore plan was not published")

    @staticmethod
    def load_plan(path: str) -> RestorePlan:
        try:
            encoded = read_bounded_regular_file(
                Path(path).expanduser(), MAX_PLAN_BYTES, "restore plan input"
            )
            raw = loads_strict_json(encoded.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise RestorePlanningError("Unable to read restore plan: {0}".format(error))
        if not isinstance(raw, dict):
            raise RestorePlanningError("Restore plan root must be an object")
        try:
            return RestorePlan.from_dict(raw)
        except (KeyError, TypeError, ValueError) as error:
            raise RestorePlanningError("Invalid restore plan structure: {0}".format(error))

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        try:
            descriptor = os.open(str(directory), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)

    def _validate_target_principals(self) -> None:
        for arn in self.config.restore.target_principals:
            errors = validate_target_principal(arn, self.config.target)
            if errors:
                raise RestorePlanningError("; ".join(errors))
            if not self.config.restore.validate_target_principals:
                continue
            if self.identity_client is None:
                raise RestorePlanningError(
                    "Target-principal validation requires an identity-region Quick Sight client"
                )
            parsed = parse_target_principal(arn)
            if parsed is None:
                raise RestorePlanningError("Invalid target principal ARN: {0}".format(arn))
            request = {
                "AwsAccountId": self.config.target.aws_account_id,
                "Namespace": self.config.target.namespace,
            }
            try:
                if parsed["kind"] == "user":
                    request["UserName"] = parsed["name"]
                    self.identity_client.describe_user(**request)
                else:
                    request["GroupName"] = parsed["name"]
                    self.identity_client.describe_group(**request)
            except ClientError as error:
                raise RestorePlanningError(
                    "Unable to verify target principal {0}: {1}".format(arn, error)
                )

    def _validate_overrides_shape(self, overrides: Dict[str, Any]) -> None:
        if not isinstance(overrides, dict):
            raise RestorePlanningError("Overrides JSON root must be an object")
        unknown = sorted(set(overrides) - _ALLOWED_OVERRIDE_KEYS)
        if unknown:
            raise RestorePlanningError(
                "Unsupported import override keys: {0}".format(", ".join(unknown))
            )
        self._reject_plaintext_override_fields(overrides)
        for key in overrides:
            if not isinstance(overrides[key], dict):
                raise RestorePlanningError("{0} must be a JSON object".format(key))
        explicit_permissions = overrides.get("OverridePermissions")
        if self.config.restore.target_principals and explicit_permissions:
            raise RestorePlanningError(
                "Use either restore.target_principals or explicit OverridePermissions, not both"
            )
        for top_key, specs in _OVERRIDE_ENTRY_SPECS.items():
            container = overrides.get(top_key, {})
            if not isinstance(container, dict):
                continue
            allowed_sections = set(specs)
            if top_key == "OverrideParameters":
                allowed_sections.add(_RESOURCE_ID_CONFIGURATION)
            unknown_sections = sorted(set(container) - allowed_sections)
            if unknown_sections:
                raise RestorePlanningError(
                    "Unsupported {0} sections: {1}".format(top_key, ", ".join(unknown_sections))
                )
        self._validate_resource_id_configuration(overrides)

    def _validate_resource_id_configuration(self, overrides: Dict[str, Any]) -> None:
        parameters = overrides.get("OverrideParameters", {})
        resource_ids = parameters.get(_RESOURCE_ID_CONFIGURATION, {})
        if resource_ids is None:
            resource_ids = {}
        if not isinstance(resource_ids, dict):
            raise RestorePlanningError(
                "OverrideParameters.ResourceIdOverrideConfiguration must be an object"
            )
        configured_prefix = resource_ids.get("PrefixForAllResources")
        if configured_prefix is not None and not isinstance(configured_prefix, str):
            raise RestorePlanningError("PrefixForAllResources must be a string")
        if self.config.restore.conflict_policy != "prefix" and configured_prefix is not None:
            raise RestorePlanningError(
                "PrefixForAllResources requires restore.conflict_policy=prefix"
            )
        if (
            self.config.restore.conflict_policy == "prefix"
            and configured_prefix is not None
            and configured_prefix != self.config.restore.conflict_prefix
        ):
            raise RestorePlanningError(
                "PrefixForAllResources conflicts with restore.conflict_prefix"
            )

    def _reject_plaintext_override_fields(self, value: Any, path: str = "overrides") -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                normalized = "".join(
                    character for character in str(key).lower() if character.isalnum()
                )
                if normalized in _FORBIDDEN_OVERRIDE_FIELDS:
                    raise RestorePlanningError(
                        "Plaintext credential field is not allowed in {0}.{1}".format(path, key)
                    )
                self._reject_plaintext_override_fields(child, "{0}.{1}".format(path, key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                self._reject_plaintext_override_fields(child, "{0}[{1}]".format(path, index))

    def _validate_override_references(
        self, overrides: Dict[str, Any], selected_resources: Set[str]
    ) -> None:
        for top_key, specs in _OVERRIDE_ENTRY_SPECS.items():
            container = overrides.get(top_key, {})
            if not isinstance(container, dict):
                continue
            for section, (resource_type, identifier_field, is_list) in specs.items():
                entries = container.get(section, [])
                if entries is None:
                    continue
                if not isinstance(entries, list):
                    raise RestorePlanningError("{0}.{1} must be a list".format(top_key, section))
                for entry in entries:
                    if not isinstance(entry, dict):
                        raise RestorePlanningError(
                            "{0}.{1} entries must be objects".format(top_key, section)
                        )
                    if identifier_field not in entry:
                        raise RestorePlanningError(
                            "{0}.{1} entries require {2}".format(top_key, section, identifier_field)
                        )
                    raw_identifiers = entry[identifier_field]
                    if is_list:
                        if (
                            not isinstance(raw_identifiers, list)
                            or not raw_identifiers
                            or any(
                                not isinstance(item, str) or not item for item in raw_identifiers
                            )
                        ):
                            raise RestorePlanningError(
                                "{0}.{1}.{2} must be a non-empty list of IDs".format(
                                    top_key, section, identifier_field
                                )
                            )
                        identifiers = list(raw_identifiers)
                    else:
                        if not isinstance(raw_identifiers, str) or not raw_identifiers:
                            raise RestorePlanningError(
                                "{0}.{1}.{2} must be a non-empty ID".format(
                                    top_key, section, identifier_field
                                )
                            )
                        identifiers = [raw_identifiers]
                    if section == "RefreshSchedules":
                        schedule_id = entry.get("ScheduleId")
                        if not isinstance(schedule_id, str) or not schedule_id:
                            raise RestorePlanningError(
                                "OverrideParameters.RefreshSchedules entries require ScheduleId"
                            )
                    available_ids = self._resource_ids(selected_resources, resource_type)
                    for identifier in identifiers:
                        if identifier == "*":
                            if not is_list:
                                raise RestorePlanningError(
                                    "{0}.{1}.{2} does not support wildcard selectors".format(
                                        top_key, section, identifier_field
                                    )
                                )
                            if not available_ids:
                                raise RestorePlanningError(
                                    "Override wildcard has no selected {0} resources".format(
                                        resource_type
                                    )
                                )
                            continue
                        if identifier not in available_ids:
                            raise RestorePlanningError(
                                "Override references an unselected resource: {0}/{1}".format(
                                    resource_type, identifier
                                )
                            )
                        if section == "RefreshSchedules":
                            schedule_key = "refreshschedule/{0}/{1}".format(identifier, schedule_id)
                            if schedule_key not in selected_resources:
                                raise RestorePlanningError(
                                    "Override references an unselected resource: {0}".format(
                                        schedule_key
                                    )
                                )
        self._validate_override_principal_arns(overrides)

    def _validate_override_principal_arns(self, value: Any) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if str(key).lower() in ("principal", "principals"):
                    principals = [child] if isinstance(child, str) else child
                    if not isinstance(principals, list) or any(
                        not isinstance(item, str) for item in principals
                    ):
                        raise RestorePlanningError(
                            "Override permission principals must be ARN strings"
                        )
                    for principal in principals:
                        errors = validate_target_principal(principal, self.config.target)
                        if errors:
                            raise RestorePlanningError("; ".join(errors))
                self._validate_override_principal_arns(child)
        elif isinstance(value, list):
            for child in value:
                self._validate_override_principal_arns(child)

    def _validate_permission_generation_support(self, selected_resources: Set[str]) -> None:
        if not self.config.restore.target_principals:
            return
        selected_types = {item.split("/", 1)[0] for item in selected_resources}
        unsupported = (
            selected_types - set(PERMISSION_OVERRIDE_SPECS) - _NON_PERMISSION_RESOURCE_TYPES
        )
        if unsupported:
            raise RestorePlanningError(
                "Target-principal permission overrides are unsupported for selected resource types: {0}".format(
                    ", ".join(sorted(unsupported))
                )
            )

    def _compile_bundle_overrides(
        self, overrides: Dict[str, Any], bundle_resources: Set[str]
    ) -> Dict[str, Any]:
        compiled: Dict[str, Any] = {}
        for top_key, specs in _OVERRIDE_ENTRY_SPECS.items():
            container = overrides.get(top_key, {})
            if not isinstance(container, dict):
                continue
            compiled_container: Dict[str, Any] = {}
            for section, (resource_type, identifier_field, is_list) in specs.items():
                entries = container.get(section, [])
                if not isinstance(entries, list):
                    continue
                bundle_ids = self._resource_ids(bundle_resources, resource_type)
                selected_entries: List[Dict[str, Any]] = []
                for entry in entries:
                    value = entry.get(identifier_field)
                    identifiers = value if is_list else [value]
                    if "*" in identifiers:
                        matching_ids = sorted(bundle_ids)
                    else:
                        matching_ids = sorted(set(identifiers) & bundle_ids)
                    if not matching_ids:
                        continue
                    effective_ids = [
                        self._destination_resource_key(
                            "{0}/{1}".format(resource_type, resource_id)
                        ).split("/", 1)[1]
                        for resource_id in matching_ids
                    ]
                    selected_entry = copy.deepcopy(entry)
                    selected_entry[identifier_field] = (
                        effective_ids if is_list else effective_ids[0]
                    )
                    selected_entries.append(selected_entry)
                if selected_entries:
                    compiled_container[section] = selected_entries
            if top_key == "OverrideParameters":
                configured = container.get(_RESOURCE_ID_CONFIGURATION, {})
                if configured:
                    compiled_container[_RESOURCE_ID_CONFIGURATION] = copy.deepcopy(configured)
            if compiled_container:
                compiled[top_key] = compiled_container

        if self.config.restore.target_principals:
            generated = build_override_permissions(
                bundle_resources, self.config.restore.target_principals
            )
            if generated:
                compiled["OverridePermissions"] = generated

        if self.config.restore.conflict_policy == "prefix":
            parameters = compiled.setdefault("OverrideParameters", {})
            resource_ids = parameters.setdefault(_RESOURCE_ID_CONFIGURATION, {})
            resource_ids["PrefixForAllResources"] = self.config.restore.conflict_prefix

        validation = overrides.get("OverrideValidationStrategy")
        selected_types = {item.split("/", 1)[0] for item in bundle_resources}
        if validation and selected_types.intersection({"analysis", "dashboard"}):
            compiled["OverrideValidationStrategy"] = copy.deepcopy(validation)
        return compiled

    def _validate_final_import_request(
        self, import_overrides: Dict[str, Any], bundle_key: str
    ) -> None:
        request: Dict[str, Any] = {
            "AwsAccountId": self.config.target.aws_account_id,
            "AssetBundleImportJobId": "restore-plan-validation",
            "AssetBundleImportSource": {"Body": b"x"},
            "FailureAction": self.config.restore.failure_action,
        }
        request.update(copy.deepcopy(import_overrides))
        try:
            validate_parameters(request, self.import_request_shape)
        except ParamValidationError as error:
            raise RestorePlanningError(
                "Invalid import overrides for bundle {0}: {1}".format(bundle_key, error)
            )

    def _destination_resource_key(self, resource_key: str) -> str:
        if self.config.restore.conflict_policy != "prefix":
            return resource_key
        resource_type, resource_id = resource_key.split("/", 1)
        if resource_type == "refreshschedule":
            raise RestorePlanningError(
                "Prefix restore for refresh schedules is unsupported until schedule ID transformation is verified"
            )
        prefix = self.config.restore.conflict_prefix or ""
        destination_id = prefix + resource_id
        limit = _RESOURCE_ID_LIMITS.get(resource_type)
        pattern = _TOPIC_RESOURCE_ID_RE if resource_type == "topic" else _STANDARD_RESOURCE_ID_RE
        if limit is None or len(destination_id) > limit or not pattern.fullmatch(destination_id):
            raise RestorePlanningError(
                "Prefixed resource ID is invalid for {0}: {1}".format(resource_type, destination_id)
            )
        return "{0}/{1}".format(resource_type, destination_id)

    def _dependency_order(
        self,
        bundles: Mapping[str, BundleInventory],
        target_resources: Set[str],
        conflict_policy: str,
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        resources_by_bundle = {
            key: {member.resource_key for member in bundle.members}
            for key, bundle in bundles.items()
        }
        providers: Dict[str, List[str]] = {}
        for key, resources in resources_by_bundle.items():
            for resource in resources:
                providers.setdefault(resource, []).append(key)
        for resource in providers:
            providers[resource].sort(key=lambda key: self._bundle_sort_key(bundles[key]))

        edges: Dict[str, Set[str]] = {key: set() for key in bundles}
        indegree: Dict[str, int] = {key: 0 for key in bundles}
        for dependent_key, bundle in bundles.items():
            dependencies = {
                dependency for member in bundle.members for dependency in member.dependencies
            }
            for dependency in sorted(dependencies):
                if dependency in resources_by_bundle[dependent_key]:
                    continue
                candidates = providers.get(dependency, [])
                if candidates:
                    provider_key = candidates[0]
                    if dependent_key not in edges[provider_key]:
                        edges[provider_key].add(dependent_key)
                        indegree[dependent_key] += 1
                    continue
                if conflict_policy != "prefix" and dependency in target_resources:
                    continue
                raise RestorePlanningError(
                    "Missing dependency {0} required by archive {1}".format(
                        dependency, dependent_key
                    )
                )

        ready = sorted(
            (key for key, count in indegree.items() if count == 0),
            key=lambda key: self._bundle_sort_key(bundles[key]),
        )
        ordered: List[str] = []
        while ready:
            key = ready.pop(0)
            ordered.append(key)
            for dependent in sorted(
                edges[key], key=lambda item: self._bundle_sort_key(bundles[item])
            ):
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    ready.append(dependent)
                    ready.sort(key=lambda item: self._bundle_sort_key(bundles[item]))
        if len(ordered) != len(bundles):
            cycle = sorted(key for key, count in indegree.items() if count > 0)
            raise RestorePlanningError(
                "Dependency cycle between restore archives: {0}".format(", ".join(cycle))
            )
        prerequisites = {
            key: sorted(provider for provider, dependents in edges.items() if key in dependents)
            for key in bundles
        }
        return ordered, prerequisites

    @staticmethod
    def _bundle_sort_key(bundle: BundleInventory) -> Tuple[int, str]:
        return (_TIER.get(bundle.root_type, 99), bundle.key)

    @staticmethod
    def _resource_ids(resources: Set[str], resource_type: str) -> Set[str]:
        prefix = resource_type + "/"
        return {item[len(prefix) :] for item in resources if item.startswith(prefix)}

    @staticmethod
    def _ownership_score(
        candidate: Tuple[BundleInventory, BundleMemberInventory],
    ) -> Tuple[int, int, str, str]:
        bundle, member = candidate
        dedicated = _RESOURCE_ROOT.get(member.resource_type) == bundle.root_type
        return (
            0 if dedicated else 1,
            _TIER.get(bundle.root_type, 99),
            bundle.key,
            member.member_name,
        )
