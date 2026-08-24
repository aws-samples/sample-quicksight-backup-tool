"""Discovery and validation for legacy Part 1 backup artifacts."""

from datetime import datetime, timezone
from decimal import Decimal
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import base64
import hashlib
import json
import re
import zipfile

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from ..limits import INLINE_IMPORT_MAX_BYTES
from ..models.config import SourceBackupConfig
from ..models.contracts import (
    BundleInventory,
    BundleMemberInventory,
    IdentitySnapshot,
    IdentityTableSnapshot,
    LegacyRestoreManifest,
    ResolvedIdentityTables,
    canonical_json,
)
from ..models.errors import (
    ArchiveValidationError,
    CatalogAccessDeniedError,
    CatalogAmbiguityError,
    CatalogNotFoundError,
    PlanIntegrityError,
    RestoreCatalogError,
)

_ROOT_TYPES = ("datasources", "datasets", "analyses", "dashboards")
_ROOT_RESOURCE_TYPES = {
    "datasources": "datasource",
    "datasets": "dataset",
    "analyses": "analysis",
    "dashboards": "dashboard",
}
_FILE_RE = re.compile(
    r"^(?P<type>datasources|datasets|analyses|dashboards)"
    r"(?:_bundle_(?P<index>\d+))?-(?P<time>\d{6})\.zip$"
)
_MEMBER_TYPES = {
    "datasource": ("datasource", "dataSourceId"),
    "dataset": ("dataset", "dataSetId"),
    "analysis": ("analysis", "analysisId"),
    "dashboard": ("dashboard", "dashboardId"),
    "theme": ("theme", "themeId"),
    "folder": ("folder", "folderId"),
    "topic": ("topic", "topicId"),
    "vpcConnection": ("vpcconnection", "vpcConnectionId"),
}
_RESOURCE_TYPE_NAMES = {
    "datasource": "datasource",
    "dataset": "dataset",
    "analysis": "analysis",
    "dashboard": "dashboard",
    "theme": "theme",
    "folder": "folder",
    "topic": "topic",
    "vpcconnection": "vpcconnection",
}
_ASSET_ARN_RE = re.compile(
    r"^arn:(?P<partition>aws|aws-us-gov|aws-cn):quicksight:"
    r"(?P<region>[a-z0-9-]+):(?P<account>\d{12}):"
    r"(?P<type>datasource|dataset|analysis|dashboard|theme|folder|topic|vpcConnection)/"
    r"(?P<id>[A-Za-z0-9_.+@-]+)$"
)
_REFERENCE_FIELDS = {
    "datasource": {"vpcConnectionArn"},
    "dataset": {"dataSourceArn"},
    "analysis": {"dataSetArn", "themeArn"},
    "dashboard": {"dataSetArn", "themeArn"},
    "folder": {"parentFolderArn", "memberArn"},
    "topic": {"dataSetArn", "datasetArn"},
    "theme": set(),
    "vpcconnection": set(),
    "refreshschedule": set(),
}
_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9_.+@-]+$")
_ACCESS_DENIED = {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "403"}
_NOT_FOUND = {"NoSuchBucket", "NoSuchKey", "NotFound", "ResourceNotFoundException", "404"}


class LegacyBackupCatalog:
    """Resolve, inspect, and verify date-oriented Part 1 artifacts."""

    def __init__(self, config: SourceBackupConfig, s3_client: Any, dynamodb_client: Any):
        self.config = config
        self.s3 = s3_client
        self.dynamodb = dynamodb_client
        self.deserializer = TypeDeserializer()

    def build_manifest(
        self,
        backup_date: Optional[str] = None,
        explicit_keys: Optional[Sequence[str]] = None,
        include_assets: bool = True,
        include_identities: bool = True,
    ) -> LegacyRestoreManifest:
        if include_assets and self.s3 is None:
            raise RestoreCatalogError("Asset catalog requires an S3 client")
        if include_identities and self.dynamodb is None:
            raise RestoreCatalogError("Identity catalog requires a DynamoDB client")
        date_value = backup_date or self.config.backup_date
        keys = list(explicit_keys if explicit_keys is not None else self.config.bundle_keys)
        explicit_selection = bool(keys)
        bundles: List[BundleInventory] = []
        warnings: List[str] = []
        if include_assets:
            if not keys:
                if not date_value:
                    raise RestoreCatalogError("Asset discovery requires an explicit backup date")
                keys = self.list_bundle_keys(date_value)
                self._validate_automatic_selection(keys)
            else:
                keys = self._validate_explicit_keys(keys, date_value)
            bundles = [self.inspect_bundle(key) for key in sorted(keys)]
        tables = ResolvedIdentityTables()
        identity_snapshot: Optional[IdentitySnapshot] = None
        if include_identities:
            if not date_value:
                raise RestoreCatalogError(
                    "Identity table discovery requires an explicit backup date"
                )
            tables = self.resolve_identity_tables(date_value)
            identity_snapshot = self.snapshot_identity_tables(tables, validate_tables=False)
        if explicit_selection:
            warnings.append(
                "Bundle keys were selected explicitly; same-day run grouping was operator-reviewed."
            )
        return LegacyRestoreManifest(
            backup_date=date_value,
            source_bucket=self.config.s3_bucket_name,
            source_s3_region=self.config.s3_region,
            source_dynamodb_region=self.config.dynamodb_region,
            identity_tables=tables,
            bundles=bundles,
            generated_at=datetime.now(timezone.utc).isoformat(),
            identity_snapshot=identity_snapshot,
            warnings=warnings,
        )

    def list_bundle_keys(self, backup_date: str) -> List[str]:
        prefix = self.config.date_prefix(backup_date)
        keys: List[str] = []
        token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {
                "Bucket": self.config.s3_bucket_name,
                "Prefix": prefix,
            }
            if token:
                request["ContinuationToken"] = token
            try:
                response = self.s3.list_objects_v2(**request)
            except ClientError as error:
                self._raise_catalog_client_error(error, "list S3 prefix {0}".format(prefix))
            for item in response.get("Contents", []):
                key = item.get("Key")
                if isinstance(key, str) and key.lower().endswith(".zip"):
                    keys.append(key)
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
            if not token:
                raise RestoreCatalogError("S3 pagination ended without a continuation token")
        if not keys:
            raise CatalogNotFoundError(
                "No legacy bundle ZIP files found under s3://{0}/{1}".format(
                    self.config.s3_bucket_name, prefix
                )
            )
        return sorted(set(keys))

    def resolve_identity_tables(self, backup_date: str) -> ResolvedIdentityTables:
        names = ResolvedIdentityTables(
            users="{0}-{1}".format(backup_date, self.config.identity_tables.users),
            groups="{0}-{1}".format(backup_date, self.config.identity_tables.groups),
            memberships="{0}-{1}".format(backup_date, self.config.identity_tables.memberships),
        )
        for table_name, expected_key in self._identity_table_specs(names):
            self._validate_identity_table(table_name, expected_key)
        return names

    def snapshot_identity_tables(
        self,
        tables: ResolvedIdentityTables,
        validate_tables: bool = True,
    ) -> IdentitySnapshot:
        """Capture canonical, JSON-safe records from all identity tables."""

        snapshots: Dict[str, IdentityTableSnapshot] = {}
        for label, (table_name, key_name) in zip(
            ("users", "groups", "memberships"), self._identity_table_specs(tables)
        ):
            if not table_name:
                raise RestoreCatalogError("Identity snapshot requires all three table names")
            if validate_tables:
                self._validate_identity_table(table_name, key_name)
            items = self._scan_identity_table(table_name, key_name)
            snapshots[label] = IdentityTableSnapshot(
                table_name=table_name,
                key_name=key_name,
                items=items,
            ).seal()
        return IdentitySnapshot(
            users=snapshots["users"],
            groups=snapshots["groups"],
            memberships=snapshots["memberships"],
        ).seal()

    def verify_identity_snapshot(self, expected: IdentitySnapshot) -> None:
        """Fail when identity tables no longer match the reviewed plan input."""

        if not expected.verify_digest():
            raise PlanIntegrityError("Identity snapshot digest verification failed")
        tables = ResolvedIdentityTables(
            users=expected.users.table_name,
            groups=expected.groups.table_name,
            memberships=expected.memberships.table_name,
        )
        actual = self.snapshot_identity_tables(tables)
        if actual.to_dict() != expected.to_dict():
            changed = [
                label
                for label in ("users", "groups", "memberships")
                if getattr(actual, label).to_dict() != getattr(expected, label).to_dict()
            ]
            raise PlanIntegrityError(
                "Identity source changed after planning: {0}".format(", ".join(changed))
            )

    @staticmethod
    def _identity_table_specs(
        names: ResolvedIdentityTables,
    ) -> List[Tuple[str, str]]:
        return [
            (names.users, "user_name"),
            (names.groups, "group_name"),
            (names.memberships, "membership_id"),
        ]

    def _validate_identity_table(self, table_name: str, expected_key: str) -> None:
        try:
            response = self.dynamodb.describe_table(TableName=table_name)
        except ClientError as error:
            self._raise_catalog_client_error(
                error, "describe DynamoDB table {0}".format(table_name)
            )
        table = response.get("Table", {})
        if table.get("TableName") != table_name:
            raise RestoreCatalogError(
                "DynamoDB returned an unexpected table for {0}".format(table_name)
            )
        hash_keys = [
            item.get("AttributeName")
            for item in table.get("KeySchema", [])
            if item.get("KeyType") == "HASH"
        ]
        if hash_keys != [expected_key]:
            raise RestoreCatalogError(
                "DynamoDB table {0} has an unexpected partition key".format(table_name)
            )
        if table.get("TableStatus") != "ACTIVE":
            raise RestoreCatalogError("DynamoDB table {0} is not ACTIVE".format(table_name))

    def _scan_identity_table(self, table_name: str, key_name: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen_keys = set()
        start_key: Optional[Dict[str, Any]] = None
        while True:
            request: Dict[str, Any] = {
                "TableName": table_name,
                "ConsistentRead": True,
            }
            if start_key:
                request["ExclusiveStartKey"] = start_key
            try:
                response = self.dynamodb.scan(**request)
            except ClientError as error:
                self._raise_catalog_client_error(
                    error, "scan DynamoDB table {0}".format(table_name)
                )
            for raw in response.get("Items", []):
                if not isinstance(raw, Mapping):
                    raise RestoreCatalogError(
                        "DynamoDB table {0} returned a non-object item".format(table_name)
                    )
                item = self._deserialize_identity_item(raw)
                item_key = item.get(key_name)
                if not isinstance(item_key, str) or not item_key:
                    raise RestoreCatalogError(
                        "DynamoDB table {0} item is missing string key {1}".format(
                            table_name, key_name
                        )
                    )
                if item_key in seen_keys:
                    raise RestoreCatalogError(
                        "DynamoDB table {0} returned duplicate key {1}".format(table_name, item_key)
                    )
                seen_keys.add(item_key)
                items.append(item)
            next_key = response.get("LastEvaluatedKey")
            if not next_key:
                break
            if next_key == start_key:
                raise RestoreCatalogError(
                    "DynamoDB pagination did not advance for table {0}".format(table_name)
                )
            start_key = next_key
        return sorted(items, key=lambda item: (str(item[key_name]), canonical_json(item)))

    def _deserialize_identity_item(self, item: Mapping[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        attribute_types = {"S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"}
        for key, value in item.items():
            if isinstance(value, dict) and len(value) == 1 and next(iter(value)) in attribute_types:
                value = self.deserializer.deserialize(value)
            result[str(key)] = self._json_safe(value)
        return dict(sorted(result.items()))

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if isinstance(value, Decimal):
            return {"__dynamodb_number__": str(value)}
        if isinstance(value, (bytes, bytearray)):
            return {"__dynamodb_binary__": base64.b64encode(bytes(value)).decode("ascii")}
        if isinstance(value, Mapping):
            return {
                str(key): cls._json_safe(child)
                for key, child in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if isinstance(value, (set, frozenset)):
            normalized = [cls._json_safe(child) for child in value]
            return sorted(normalized, key=canonical_json)
        if isinstance(value, (list, tuple)):
            return [cls._json_safe(child) for child in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise RestoreCatalogError(
            "Identity record contains an unsupported value type: {0}".format(type(value).__name__)
        )

    def inspect_bundle(self, key: str) -> BundleInventory:
        root_type = self._root_type(key)
        head = self._head(key)
        size = int(head.get("ContentLength", -1))
        if size < 0:
            raise RestoreCatalogError("S3 HeadObject omitted ContentLength for {0}".format(key))
        version_id = str(head.get("VersionId") or "null")
        data = self._get(key, version_id, size)
        members = self.inspect_archive(data, key)
        expected_root_type = _ROOT_RESOURCE_TYPES[root_type]
        if not any(member.resource_type == expected_root_type for member in members):
            raise ArchiveValidationError(
                "Bundle key root type does not match any contained resource: {0}".format(key)
            )
        return BundleInventory(
            bucket=self.config.s3_bucket_name,
            key=key,
            version_id=version_id,
            size=size,
            sha256=hashlib.sha256(data).hexdigest(),
            root_type=root_type,
            members=members,
        )

    def read_and_verify_bundle(self, bundle: BundleInventory) -> bytes:
        """Read one exact source object and re-verify it against the manifest."""

        if bundle.bucket != self.config.s3_bucket_name:
            raise ArchiveValidationError(
                "Planned bundle bucket does not match restore configuration"
            )
        head = self._head(bundle.key, bundle.version_id)
        actual_version = str(head.get("VersionId") or "null")
        if bundle.version_id != actual_version:
            raise ArchiveValidationError(
                "S3 object version changed for {0}: expected {1}, got {2}".format(
                    bundle.key, bundle.version_id, actual_version
                )
            )
        size = int(head.get("ContentLength", -1))
        if size != bundle.size:
            raise ArchiveValidationError(
                "S3 object size changed for {0}: expected {1}, got {2}".format(
                    bundle.key, bundle.size, size
                )
            )
        data = self._get(bundle.key, bundle.version_id, size)
        digest = hashlib.sha256(data).hexdigest()
        if digest != bundle.sha256:
            raise ArchiveValidationError(
                "S3 object checksum changed for {0}: expected {1}, got {2}".format(
                    bundle.key, bundle.sha256, digest
                )
            )
        current_members = self.inspect_archive(data, bundle.key)
        expected = [item.to_dict() for item in bundle.members]
        actual = [item.to_dict() for item in current_members]
        if actual != expected:
            raise ArchiveValidationError("ZIP member inventory changed for {0}".format(bundle.key))
        return data

    def inspect_archive(self, data: bytes, key: str) -> List[BundleMemberInventory]:
        if not zipfile.is_zipfile(BytesIO(data)):
            raise ArchiveValidationError("Object is not a valid ZIP archive: {0}".format(key))
        members: List[BundleMemberInventory] = []
        seen_names = set()
        total_uncompressed = 0
        try:
            with zipfile.ZipFile(BytesIO(data), "r") as archive:
                all_infos = archive.infolist()
                if len(all_infos) > self.config.max_zip_entries:
                    raise ArchiveValidationError(
                        "ZIP entry count exceeds configured limit for {0}".format(key)
                    )
                infos = [item for item in all_infos if not item.is_dir()]
                for info in infos:
                    self._validate_member_info(info, seen_names, key)
                    seen_names.add(info.filename)
                    total_uncompressed += info.file_size
                    if total_uncompressed > self.config.max_uncompressed_bytes:
                        raise ArchiveValidationError(
                            "ZIP uncompressed size exceeds configured limit for {0}".format(key)
                        )
                    ratio = float(info.file_size) / float(max(info.compress_size, 1))
                    if ratio > self.config.max_compression_ratio:
                        raise ArchiveValidationError(
                            "ZIP compression ratio exceeds configured limit for {0}:{1}".format(
                                key, info.filename
                            )
                        )
                    raw = archive.read(info)
                    if len(raw) != info.file_size:
                        raise ArchiveValidationError(
                            "ZIP member size mismatch for {0}:{1}".format(key, info.filename)
                        )
                    if not info.filename.endswith(".json"):
                        raise ArchiveValidationError(
                            "Unsupported ancillary ZIP member in {0}: {1}".format(
                                key, info.filename
                            )
                        )
                    try:
                        document = self._load_strict_json(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                        raise ArchiveValidationError(
                            "Malformed JSON member {0}:{1}: {2}".format(key, info.filename, error)
                        )
                    if not isinstance(document, dict):
                        raise ArchiveValidationError(
                            "Quick Sight resource member must contain a JSON object: {0}:{1}".format(
                                key, info.filename
                            )
                        )
                    if "AWSTemplateFormatVersion" in document:
                        raise ArchiveValidationError(
                            "CLOUDFORMATION_JSON bundles are not supported: {0}".format(key)
                        )
                    resource_type, resource_id = self._classify_member(info.filename, document)
                    resource_key = "{0}/{1}".format(resource_type, resource_id)
                    dependencies, dependency_scopes = self._extract_dependencies(
                        document, resource_key, resource_type
                    )
                    fingerprint_document: Mapping[str, Any] = document
                    permissions = document.get("permissions")
                    if permissions is not None:
                        if not isinstance(permissions, list):
                            raise ArchiveValidationError(
                                "Quick Sight permissions must be a list in {0}".format(resource_key)
                            )
                        normalized_permissions: List[Dict[str, Any]] = []
                        for permission in permissions:
                            if not isinstance(permission, Mapping):
                                raise ArchiveValidationError(
                                    "Quick Sight permission must be an object in {0}".format(
                                        resource_key
                                    )
                                )
                            actions = permission.get("actions")
                            if not isinstance(actions, list) or any(
                                not isinstance(action, str) for action in actions
                            ):
                                raise ArchiveValidationError(
                                    "Quick Sight permission actions must be strings in {0}".format(
                                        resource_key
                                    )
                                )
                            normalized_permission = dict(permission)
                            normalized_permission["actions"] = sorted(actions)
                            normalized_permissions.append(normalized_permission)
                        normalized_document = dict(document)
                        normalized_document["permissions"] = normalized_permissions
                        fingerprint_document = normalized_document
                    members.append(
                        BundleMemberInventory(
                            member_name=info.filename,
                            resource_type=resource_type,
                            resource_id=resource_id,
                            member_size=len(raw),
                            member_sha256=hashlib.sha256(raw).hexdigest(),
                            canonical_json_sha256=hashlib.sha256(
                                canonical_json(fingerprint_document)
                            ).hexdigest(),
                            known_resource=True,
                            dependencies=dependencies,
                            dependency_scopes=dependency_scopes,
                        )
                    )
        except zipfile.BadZipFile as error:
            raise ArchiveValidationError("Invalid ZIP structure for {0}: {1}".format(key, error))
        if not members:
            raise ArchiveValidationError("ZIP archive contains no files: {0}".format(key))
        return sorted(members, key=lambda item: item.member_name)

    def _validate_explicit_keys(self, keys: Sequence[str], backup_date: Optional[str]) -> List[str]:
        if len(set(keys)) != len(keys):
            raise RestoreCatalogError("Explicit bundle keys contain duplicates")
        base_prefix = self.config.s3_prefix.rstrip("/") + "/"
        validated: List[str] = []
        for key in keys:
            path = PurePosixPath(key)
            if (
                not key.startswith(base_prefix)
                or key.startswith("/")
                or "\\" in key
                or ".." in path.parts
                or not key.endswith(".zip")
            ):
                raise RestoreCatalogError(
                    "Explicit bundle key is outside the configured safe prefix: {0}".format(key)
                )
            self._parse_bundle_key(key)
            validated.append(key)
        self._validate_run_shape(validated)
        return sorted(validated)

    def _validate_automatic_selection(self, keys: Sequence[str]) -> None:
        self._validate_run_shape(keys)
        if len(keys) != 1:
            raise CatalogAmbiguityError(
                "Legacy bundle objects do not contain a shared run identifier; select every exact --bundle-key explicitly"
            )

    def _validate_run_shape(self, keys: Sequence[str]) -> None:
        slots: Dict[Tuple[str, str], str] = {}
        forms: Dict[str, set] = {}
        batch_indexes: Dict[str, List[int]] = {}
        for key in keys:
            root_type, index = self._parse_bundle_key(key)
            slot = (root_type, index or "singleton")
            if slot in slots:
                raise CatalogAmbiguityError(
                    "Selected keys contain more than one legacy run artifact for {0}/{1}: {2}, {3}; select exact --bundle-key values".format(
                        root_type, slot[1], slots[slot], key
                    )
                )
            slots[slot] = key
            forms.setdefault(root_type, set()).add("batch" if index else "singleton")
            if index is not None:
                batch_indexes.setdefault(root_type, []).append(int(index))
        mixed = sorted(root for root, values in forms.items() if len(values) > 1)
        if mixed:
            raise CatalogAmbiguityError(
                "Selected keys mix singleton and batched artifacts for: {0}".format(
                    ", ".join(mixed)
                )
            )
        for root_type, indexes in sorted(batch_indexes.items()):
            expected = list(range(1, max(indexes) + 1))
            if sorted(indexes) != expected:
                raise CatalogAmbiguityError(
                    "Selected {0} batch indexes are incomplete: expected {1}, got {2}".format(
                        root_type, expected, sorted(indexes)
                    )
                )

    def _parse_bundle_key(self, key: str) -> Tuple[str, Optional[str]]:
        root_type = self._root_type(key)
        match = _FILE_RE.fullmatch(PurePosixPath(key).name)
        if not match or match.group("type") != root_type:
            raise CatalogAmbiguityError(
                "Legacy bundle name is not attributable to the Part 1 writer: {0}".format(key)
            )
        index = match.group("index")
        if index is not None and int(index) < 1:
            raise CatalogAmbiguityError("Legacy bundle index must be positive: {0}".format(key))
        return root_type, index

    def _root_type(self, key: str) -> str:
        parts = PurePosixPath(key).parts
        if len(parts) < 2 or parts[-2].lower() not in _ROOT_TYPES:
            raise RestoreCatalogError(
                "Cannot infer a supported Part 1 root type from S3 key: {0}".format(key)
            )
        return parts[-2].lower()

    def _head(self, key: str, version_id: Optional[str] = None) -> Dict[str, Any]:
        request: Dict[str, Any] = {"Bucket": self.config.s3_bucket_name, "Key": key}
        if version_id and version_id != "null":
            request["VersionId"] = version_id
        try:
            return self.s3.head_object(**request)
        except ClientError as error:
            self._raise_catalog_client_error(error, "head S3 object {0}".format(key))
        raise AssertionError("unreachable")

    def _get(self, key: str, version_id: str, expected_size: int) -> bytes:
        if expected_size < 0 or expected_size > INLINE_IMPORT_MAX_BYTES:
            raise ArchiveValidationError(
                "Compressed bundle size for {0} is {1} bytes; inline Quick Sight imports are limited to {2} bytes".format(
                    key, expected_size, INLINE_IMPORT_MAX_BYTES
                )
            )
        request: Dict[str, Any] = {"Bucket": self.config.s3_bucket_name, "Key": key}
        if version_id != "null":
            request["VersionId"] = version_id
        try:
            response = self.s3.get_object(**request)
        except ClientError as error:
            self._raise_catalog_client_error(error, "get S3 object {0}".format(key))
        response_length = response.get("ContentLength")
        if response_length is not None and int(response_length) != expected_size:
            raise ArchiveValidationError("HeadObject/GetObject size mismatch for {0}".format(key))
        if response_length is not None and int(response_length) > INLINE_IMPORT_MAX_BYTES:
            raise ArchiveValidationError(
                "GetObject content length exceeds the inline import limit for {0}".format(key)
            )
        body = response.get("Body")
        if body is None:
            raise RestoreCatalogError("S3 GetObject returned no body for {0}".format(key))
        data = bytearray()
        chunks = (
            body.iter_chunks(chunk_size=1024 * 1024)
            if hasattr(body, "iter_chunks")
            else (body.read(),)
        )
        for chunk in chunks:
            if not chunk:
                continue
            if len(data) + len(chunk) > INLINE_IMPORT_MAX_BYTES:
                raise ArchiveValidationError(
                    "S3 object body exceeds the inline import limit for {0}".format(key)
                )
            data.extend(chunk)
        if len(data) != expected_size:
            raise ArchiveValidationError(
                "S3 object read size mismatch for {0}: expected {1}, got {2}".format(
                    key, expected_size, len(data)
                )
            )
        return bytes(data)

    def _validate_member_info(self, info: zipfile.ZipInfo, seen_names: set, key: str) -> None:
        name = info.filename
        path = PurePosixPath(name)
        if (
            not name
            or "\x00" in name
            or "\\" in name
            or name.startswith("/")
            or path.is_absolute()
            or any(part in ("", ".", "..") for part in path.parts)
            or (path.parts and ":" in path.parts[0])
            or str(path) != name
        ):
            raise ArchiveValidationError("Unsafe ZIP member path in {0}: {1}".format(key, name))
        if name in seen_names:
            raise ArchiveValidationError("Duplicate ZIP member name in {0}: {1}".format(key, name))
        if info.flag_bits & 0x1:
            raise ArchiveValidationError(
                "Encrypted ZIP members are not supported: {0}".format(name)
            )
        if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            raise ArchiveValidationError("Unsupported ZIP compression for member: {0}".format(name))

    @staticmethod
    def _classify_member(name: str, document: Mapping[str, Any]) -> Tuple[str, str]:
        path = PurePosixPath(name)
        if path.suffix != ".json":
            raise ArchiveValidationError(
                "Unsupported Quick Sight bundle member path: {0}".format(name)
            )
        if len(path.parts) == 2 and path.parts[0] == "refreshSchedule":
            if document.get("resourceType") != "refreshSchedule":
                raise ArchiveValidationError(
                    "Refresh schedule resourceType does not match its member path: {0}".format(name)
                )
            data_set_id = document.get("dataSetId")
            schedule = document.get("schedule")
            schedule_id = schedule.get("scheduleId") if isinstance(schedule, Mapping) else None
            if (
                not isinstance(data_set_id, str)
                or not isinstance(schedule_id, str)
                or not _RESOURCE_ID_RE.fullmatch(data_set_id)
                or not _RESOURCE_ID_RE.fullmatch(schedule_id)
            ):
                raise ArchiveValidationError(
                    "Invalid refresh schedule member identity: {0}".format(name)
                )
            expected_stem = "{0}--refresh-schedule--{1}".format(data_set_id, schedule_id)
            if path.stem != expected_stem:
                raise ArchiveValidationError(
                    "Refresh schedule IDs do not match its member path: {0}".format(name)
                )
            return "refreshschedule", "{0}/{1}".format(data_set_id, schedule_id)
        if len(path.parts) != 2 or path.parts[0] not in _MEMBER_TYPES:
            raise ArchiveValidationError(
                "Unsupported Quick Sight bundle member path: {0}".format(name)
            )
        directory = path.parts[0]
        resource_type, identifier_field = _MEMBER_TYPES[directory]
        resource_id = path.stem
        if not _RESOURCE_ID_RE.fullmatch(resource_id):
            raise ArchiveValidationError(
                "Invalid Quick Sight resource ID in member path: {0}".format(name)
            )
        if document.get("resourceType") != directory:
            raise ArchiveValidationError(
                "Quick Sight resourceType does not match its member path: {0}".format(name)
            )
        if document.get(identifier_field) != resource_id:
            raise ArchiveValidationError(
                "Quick Sight resource ID does not match its member path: {0}".format(name)
            )
        return resource_type, resource_id

    @staticmethod
    def _load_strict_json(raw: bytes) -> Any:
        def reject_duplicate_keys(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
            result: Dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate JSON object key: {0}".format(key))
                result[key] = value
            return result

        def reject_constant(value: str) -> None:
            raise ValueError("non-finite JSON number: {0}".format(value))

        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )

    @classmethod
    def _extract_dependencies(
        cls,
        document: Mapping[str, Any],
        resource_key: str,
        resource_type: str,
    ) -> Tuple[List[str], List[str]]:
        dependencies = set()
        scopes = set()
        allowed_fields = _REFERENCE_FIELDS[resource_type]

        def capture(value: Any) -> None:
            if isinstance(value, list):
                for child in value:
                    capture(child)
                return
            if not isinstance(value, str):
                return
            match = _ASSET_ARN_RE.fullmatch(value)
            if not match:
                if value.startswith("arn:") and ":quicksight:" in value:
                    raise ArchiveValidationError(
                        "Unsupported Quick Sight dependency ARN in {0}: {1}".format(
                            resource_key, value
                        )
                    )
                return
            raw_type = match.group("type").lower()
            dependency_type = _RESOURCE_TYPE_NAMES.get(raw_type)
            if not dependency_type:
                return
            dependency = "{0}/{1}".format(dependency_type, match.group("id"))
            if dependency != resource_key:
                dependencies.add(dependency)
                scopes.add(
                    "{0}:{1}:{2}".format(
                        match.group("partition"),
                        match.group("region"),
                        match.group("account"),
                    )
                )

        def visit(value: Any, parent_key: Optional[str] = None) -> None:
            if isinstance(value, Mapping):
                for key, child in value.items():
                    if key in allowed_fields or (
                        resource_type == "dataset"
                        and key == "arn"
                        and parent_key == "rowLevelPermissionDataSet"
                    ):
                        capture(child)
                    visit(child, str(key))
            elif isinstance(value, list):
                for child in value:
                    visit(child, parent_key)

        visit(document)
        if resource_key.startswith("refreshschedule/"):
            _, data_set_id, _ = resource_key.split("/", 2)
            dependencies.add("dataset/{0}".format(data_set_id))
        return sorted(dependencies), sorted(scopes)

    @staticmethod
    def _raise_catalog_client_error(error: ClientError, operation: str) -> None:
        code = str(error.response.get("Error", {}).get("Code", "Unknown"))
        if code in _ACCESS_DENIED:
            raise CatalogAccessDeniedError(
                "Access denied while attempting to {0}".format(operation)
            )
        if code in _NOT_FOUND:
            raise CatalogNotFoundError(
                "Required artifact not found while attempting to {0}".format(operation)
            )
        raise RestoreCatalogError("AWS error {0} while attempting to {1}".format(code, operation))
