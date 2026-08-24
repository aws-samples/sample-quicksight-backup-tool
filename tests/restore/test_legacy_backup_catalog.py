from io import BytesIO
import copy

import pytest
from botocore.exceptions import ClientError

from quicksight_restore.models.errors import (
    ArchiveValidationError,
    CatalogAccessDeniedError,
    CatalogAmbiguityError,
    CatalogNotFoundError,
    PlanIntegrityError,
)
from quicksight_restore.services.catalog import LegacyBackupCatalog
from conftest import make_zip


class Body:
    def __init__(self, value):
        self.value = value

    def iter_chunks(self, chunk_size):
        midpoint = len(self.value) // 2
        yield self.value[:midpoint]
        yield self.value[midpoint:]


class FakeS3:
    def __init__(self, objects=None, pages=None, list_error=None):
        self.objects = objects or {}
        self.pages = pages or []
        self.list_error = list_error
        self.list_calls = []

    def list_objects_v2(self, **request):
        self.list_calls.append(request)
        if self.list_error:
            raise self.list_error
        index = 1 if request.get("ContinuationToken") else 0
        return self.pages[index]

    def head_object(self, **request):
        if request["Key"] not in self.objects:
            raise client_error("NoSuchKey", "HeadObject")
        value = self.objects[request["Key"]]
        return {"ContentLength": len(value), "VersionId": "v1"}

    def get_object(self, **request):
        if request["Key"] not in self.objects:
            raise client_error("NoSuchKey", "GetObject")
        value = self.objects[request["Key"]]
        return {"ContentLength": len(value), "Body": Body(value), "VersionId": "v1"}


class FakeDynamoDB:
    def __init__(self, pages=None):
        self.pages = pages or {}
        self.scan_calls = []

    def describe_table(self, TableName):
        key = (
            "membership_id"
            if "users-groups" in TableName
            else ("group_name" if "groups" in TableName else "user_name")
        )
        return {
            "Table": {
                "TableName": TableName,
                "TableStatus": "ACTIVE",
                "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}],
            }
        }

    def scan(self, **request):
        self.scan_calls.append(request)
        pages = self.pages.get(request["TableName"], [{"Items": []}])
        index = 1 if request.get("ExclusiveStartKey") else 0
        return pages[index]


def client_error(code, operation):
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def test_s3_discovery_paginates_and_catalogs_exact_fingerprints(restore_config):
    key1 = "quicksight-backups/2026/08/17/datasources/datasources-010101.zip"
    key2 = "quicksight-backups/2026/08/17/datasets/datasets-010102.zip"
    objects = {
        key1: make_zip(
            {
                "datasource/source.json": {
                    "resourceType": "datasource",
                    "dataSourceId": "source",
                }
            }
        ),
        key2: make_zip(
            {
                "dataset/set.json": {
                    "resourceType": "dataset",
                    "dataSetId": "set",
                }
            }
        ),
    }
    pages = [
        {
            "Contents": [{"Key": key1}],
            "IsTruncated": True,
            "NextContinuationToken": "next",
        },
        {"Contents": [{"Key": key2}], "IsTruncated": False},
    ]
    s3 = FakeS3(objects, pages)
    catalog = LegacyBackupCatalog(restore_config.source_backup, s3, FakeDynamoDB())
    assert catalog.list_bundle_keys("2026-08-17") == sorted([key1, key2])
    manifest = catalog.build_manifest(explicit_keys=[key1, key2], include_identities=True)
    assert [item.key for item in manifest.bundles] == sorted([key1, key2])
    assert all(item.version_id == "v1" and len(item.sha256) == 64 for item in manifest.bundles)
    assert s3.list_calls[1]["ContinuationToken"] == "next"
    assert manifest.identity_tables.memberships.startswith("2026-08-17-")
    assert manifest.identity_snapshot is not None
    assert manifest.identity_snapshot.verify_digest()
    assert manifest.identity_snapshot.users.item_count == 0


@pytest.mark.parametrize(
    "code, error_type",
    [("AccessDenied", CatalogAccessDeniedError), ("NoSuchBucket", CatalogNotFoundError)],
)
def test_s3_discovery_distinguishes_access_denied_from_not_found(restore_config, code, error_type):
    s3 = FakeS3(list_error=client_error(code, "ListObjectsV2"))
    catalog = LegacyBackupCatalog(restore_config.source_backup, s3, FakeDynamoDB())
    with pytest.raises(error_type):
        catalog.list_bundle_keys("2026-08-17")


def test_obvious_same_day_ambiguity_requires_explicit_keys(restore_config):
    keys = [
        "quicksight-backups/2026/08/17/datasets/datasets-010101.zip",
        "quicksight-backups/2026/08/17/datasets/datasets-020202.zip",
    ]
    s3 = FakeS3(pages=[{"Contents": [{"Key": key} for key in keys], "IsTruncated": False}])
    catalog = LegacyBackupCatalog(restore_config.source_backup, s3, FakeDynamoDB())
    with pytest.raises(CatalogAmbiguityError, match="--bundle-key"):
        catalog.build_manifest(include_identities=False)


def test_unsafe_and_cloudformation_archives_fail_closed(restore_config):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())
    unsafe = make_zip({"../assets/dataSources/bad.json": {"id": "bad"}})
    with pytest.raises(ArchiveValidationError, match="Unsafe ZIP member"):
        catalog.inspect_archive(unsafe, "unsafe.zip")
    cloudformation = make_zip(
        {"template.json": {"AWSTemplateFormatVersion": "2010-09-09", "Resources": {}}}
    )
    with pytest.raises(ArchiveValidationError, match="CLOUDFORMATION_JSON"):
        catalog.inspect_archive(cloudformation, "template.zip")


def test_tampered_object_is_rejected_before_import(restore_config):
    key = "quicksight-backups/2026/08/17/datasources/datasources-010101.zip"
    original = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    s3 = FakeS3({key: original})
    catalog = LegacyBackupCatalog(restore_config.source_backup, s3, FakeDynamoDB())
    inventory = catalog.inspect_bundle(key)
    tampered = bytearray(original)
    tampered[-1] ^= 1
    s3.objects[key] = bytes(tampered)
    with pytest.raises(ArchiveValidationError, match="checksum changed"):
        catalog.read_and_verify_bundle(inventory)


def test_identity_snapshot_paginates_sorts_and_detects_drift(restore_config):
    users = "2026-08-17-quicksight-users-backup"
    pages = {
        users: [
            {
                "Items": [
                    {
                        "user_name": {"S": "user-b"},
                        "arn": {
                            "S": "arn:aws:quicksight:us-west-2:111111111111:user/default/user-b"
                        },
                    }
                ],
                "LastEvaluatedKey": {"user_name": {"S": "user-b"}},
            },
            {
                "Items": [
                    {
                        "user_name": {"S": "user-a"},
                        "arn": {
                            "S": "arn:aws:quicksight:us-west-2:111111111111:user/default/user-a"
                        },
                    }
                ]
            },
        ]
    }
    dynamodb = FakeDynamoDB(pages)
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), dynamodb)
    tables = catalog.resolve_identity_tables("2026-08-17")
    snapshot = catalog.snapshot_identity_tables(tables)

    assert [item["user_name"] for item in snapshot.users.items] == ["user-a", "user-b"]
    assert snapshot.users.item_count == 2
    assert snapshot.verify_digest()
    user_scans = [call for call in dynamodb.scan_calls if call["TableName"] == users]
    assert user_scans[0]["ConsistentRead"] is True
    assert "ExclusiveStartKey" in user_scans[1]

    pages[users][1]["Items"][0]["arn"]["S"] += "-changed"
    with pytest.raises(PlanIntegrityError, match="users"):
        catalog.verify_identity_snapshot(snapshot)


def test_automatic_discovery_rejects_complementary_same_day_artifacts(restore_config):
    keys = [
        "quicksight-backups/2026/08/17/datasources/datasources-010101.zip",
        "quicksight-backups/2026/08/17/datasets/datasets-020202.zip",
    ]
    s3 = FakeS3(pages=[{"Contents": [{"Key": key} for key in keys], "IsTruncated": False}])
    catalog = LegacyBackupCatalog(restore_config.source_backup, s3, FakeDynamoDB())

    with pytest.raises(CatalogAmbiguityError, match="shared run identifier"):
        catalog.build_manifest(include_identities=False)


def test_explicit_keys_can_cover_a_midnight_crossing_run(restore_config):
    first = "quicksight-backups/2026/08/17/datasources/datasources-235959.zip"
    second = "quicksight-backups/2026/08/18/datasets/datasets-000101.zip"
    objects = {
        first: make_zip(
            {"datasource/source.json": {"resourceType": "datasource", "dataSourceId": "source"}}
        ),
        second: make_zip({"dataset/set.json": {"resourceType": "dataset", "dataSetId": "set"}}),
    }
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(objects), FakeDynamoDB())

    manifest = catalog.build_manifest(explicit_keys=[first, second], include_identities=False)

    assert [bundle.key for bundle in manifest.bundles] == [first, second]
    assert "operator-reviewed" in manifest.warnings[0]


def test_native_member_identity_and_dependencies_are_cataloged(restore_config):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())
    datasource_arn = "arn:aws:quicksight:us-west-2:111111111111:datasource/source"
    archive = make_zip(
        {
            "dataset/set.json": {
                "resourceType": "dataset",
                "dataSetId": "set",
                "physicalTableMap": {
                    "table": {"relationalTable": {"dataSourceArn": datasource_arn}}
                },
            }
        }
    )

    members = catalog.inspect_archive(archive, "native.zip")

    assert members[0].resource_key == "dataset/set"
    assert members[0].dependencies == ["datasource/source"]
    assert members[0].dependency_scopes == ["aws:us-west-2:111111111111"]


@pytest.mark.parametrize(
    "entries, expected",
    [
        (
            {
                "datasource/source.json": {
                    "resourceType": "datasource",
                    "dataSourceId": "different",
                }
            },
            "does not match",
        ),
        (
            {
                "assets/datasource/source.json": {
                    "resourceType": "datasource",
                    "dataSourceId": "source",
                }
            },
            "Unsupported Quick Sight bundle member path",
        ),
        (
            {
                "datasource/source.json": {
                    "resourceType": "datasource",
                    "dataSourceId": "source",
                },
                "README.md": b"operator sidecar",
            },
            "Unsupported ancillary ZIP member",
        ),
    ],
)
def test_unknown_ancillary_and_ambiguous_member_semantics_fail_closed(
    restore_config, entries, expected
):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())

    with pytest.raises(ArchiveValidationError, match=expected):
        catalog.inspect_archive(make_zip(entries), "unsupported.zip")


def test_bundle_key_root_type_must_match_contained_root_resource(restore_config):
    key = "quicksight-backups/2026/08/17/datasets/datasets-010101.zip"
    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3({key: body}), FakeDynamoDB())

    with pytest.raises(ArchiveValidationError, match="root type"):
        catalog.inspect_bundle(key)


def test_dependency_extraction_ignores_arn_shaped_free_text(restore_config):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())
    archive = make_zip(
        {
            "dashboard/board.json": {
                "resourceType": "dashboard",
                "dashboardId": "board",
                "description": (
                    "arn:aws:quicksight:us-west-2:111111111111:dataset/not-a-reference"
                ),
            }
        }
    )

    members = catalog.inspect_archive(archive, "free-text.zip")

    assert members[0].dependencies == []
    assert members[0].dependency_scopes == []


@pytest.mark.parametrize(
    "raw, expected",
    [
        (
            b'{"resourceType":"dataset","dataSetId":"set","dataSetId":"other"}',
            "duplicate JSON object key",
        ),
        (
            b'{"resourceType":"dataset","dataSetId":"set","value":NaN}',
            "non-finite JSON number",
        ),
    ],
)
def test_ambiguous_json_is_rejected_before_byte_replay(restore_config, raw, expected):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())

    with pytest.raises(ArchiveValidationError, match=expected):
        catalog.inspect_archive(make_zip({"dataset/set.json": raw}), "ambiguous.zip")


def test_explicit_batched_selection_requires_contiguous_indexes(restore_config):
    keys = [
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_1-010101.zip",
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_3-010103.zip",
    ]
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())

    with pytest.raises(CatalogAmbiguityError, match="batch indexes are incomplete"):
        catalog.build_manifest(explicit_keys=keys, include_identities=False)


def test_compressed_size_limit_blocks_get_object_before_download(restore_config):
    from quicksight_restore.limits import INLINE_IMPORT_MAX_BYTES

    class GuardS3:
        get_calls = 0

        def get_object(self, **request):
            self.get_calls += 1
            raise AssertionError("oversized object must not be downloaded")

    s3 = GuardS3()
    catalog = LegacyBackupCatalog(restore_config.source_backup, s3, FakeDynamoDB())

    with pytest.raises(ArchiveValidationError, match="inline Quick Sight imports"):
        catalog._get("oversized.zip", "v1", INLINE_IMPORT_MAX_BYTES + 1)

    assert s3.get_calls == 0


def test_zip_entry_limit_counts_directory_entries(restore_config):
    import zipfile

    restore_config.source_backup.max_zip_entries = 2
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("first/", b"")
        archive.writestr("second/", b"")
        archive.writestr(
            "dataset/set.json",
            b'{"resourceType":"dataset","dataSetId":"set"}',
        )
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())

    with pytest.raises(ArchiveValidationError, match="ZIP entry count"):
        catalog.inspect_archive(output.getvalue(), "directory-heavy.zip")


def test_native_flattened_refresh_schedule_uses_nested_schedule_identity(
    restore_config,
):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())
    member_name = "refreshSchedule/set--refresh-schedule--daily.json"
    document = {
        "resourceType": "refreshSchedule",
        "dataSetId": "set",
        "scheduleId": "ignored-top-level-value",
        "schedule": {
            "scheduleId": "daily",
            "scheduleFrequency": {"interval": "DAILY"},
        },
    }

    members = catalog.inspect_archive(
        make_zip({member_name: document}), "native-refresh-schedule.zip"
    )

    assert len(members) == 1
    assert members[0].member_name == member_name
    assert members[0].resource_key == "refreshschedule/set/daily"
    assert members[0].dependencies == ["dataset/set"]

    document["schedule"]["scheduleId"] = "weekly"
    with pytest.raises(ArchiveValidationError, match="IDs do not match"):
        catalog.inspect_archive(
            make_zip({member_name: document}), "mismatched-refresh-schedule.zip"
        )


def test_logical_fingerprint_normalizes_only_permission_action_order(
    restore_config,
):
    catalog = LegacyBackupCatalog(restore_config.source_backup, FakeS3(), FakeDynamoDB())
    member_name = "dataset/set.json"
    first = {
        "resourceType": "dataset",
        "dataSetId": "set",
        "permissions": [
            {
                "principal": "arn:aws:quicksight:us-east-1:111111111111:user/default/user",
                "actions": ["quicksight:UpdateDataSet", "quicksight:DescribeDataSet"],
            }
        ],
        "orderedValues": ["first", "second"],
    }
    reordered_actions = copy.deepcopy(first)
    reordered_actions["permissions"][0]["actions"].reverse()
    reordered_other_list = copy.deepcopy(first)
    reordered_other_list["orderedValues"].reverse()

    first_member = catalog.inspect_archive(make_zip({member_name: first}), "first.zip")[0]
    action_member = catalog.inspect_archive(
        make_zip({member_name: reordered_actions}), "actions.zip"
    )[0]
    other_member = catalog.inspect_archive(
        make_zip({member_name: reordered_other_list}), "other-list.zip"
    )[0]

    assert first_member.member_sha256 != action_member.member_sha256
    assert first_member.canonical_json_sha256 == action_member.canonical_json_sha256
    assert first_member.canonical_json_sha256 != other_member.canonical_json_sha256
