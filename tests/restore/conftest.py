from io import BytesIO
import hashlib
import json
import zipfile

import pytest

from quicksight_restore.models.config import (
    AuthConfig,
    RestoreConfig,
    RestoreOptions,
    SourceBackupConfig,
    TargetConfig,
)
from quicksight_restore.models.contracts import (
    BundleInventory,
    BundleMemberInventory,
    IdentitySnapshot,
    IdentityTableSnapshot,
    LegacyRestoreManifest,
    ResolvedIdentityTables,
    canonical_json,
)


@pytest.fixture
def restore_config(tmp_path):
    source = SourceBackupConfig(
        s3_bucket_name="backup-bucket",
        s3_prefix="quicksight-backups",
        backup_date="2026-08-17",
        date_prefix_format="YYYY/MM/DD",
        s3_region="us-east-1",
        dynamodb_region="us-west-2",
        auth=AuthConfig(profile="source"),
    )
    target = TargetConfig(
        aws_account_id="222222222222",
        asset_region="us-east-2",
        identity_region="us-east-1",
        namespace="default",
        auth=AuthConfig(profile="target"),
    )
    options = RestoreOptions(
        mode="assets-only",
        restore_identities=False,
        report_directory=str(tmp_path / "reports"),
        validate_target_principals=False,
        poll_timeout_seconds=10,
    )
    config = RestoreConfig(source_backup=source, target=target, restore=options)
    config.validate()
    return config


def make_zip(entries):
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in entries.items():
            if isinstance(value, bytes):
                body = value
            else:
                body = json.dumps(value).encode("utf-8")
            archive.writestr(name, body)
    return output.getvalue()


def make_member(
    name,
    resource_type,
    resource_id,
    document=None,
    known=True,
    dependencies=None,
    dependency_scopes=None,
):
    document = document if document is not None else {"id": resource_id}
    raw = json.dumps(document).encode("utf-8")
    return BundleMemberInventory(
        member_name=name,
        resource_type=resource_type,
        resource_id=resource_id,
        member_size=len(raw),
        member_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_json_sha256=hashlib.sha256(canonical_json(document)).hexdigest(),
        known_resource=known,
        dependencies=sorted(dependencies or []),
        dependency_scopes=sorted(dependency_scopes or []),
    )


def make_bundle(key, root_type, members, body=b"bundle", version_id="v1"):
    return BundleInventory(
        bucket="backup-bucket",
        key=key,
        version_id=version_id,
        size=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
        root_type=root_type,
        members=list(members),
    )


def make_identity_snapshot(
    users=None,
    groups=None,
    memberships=None,
    table_names=None,
):
    names = table_names or ResolvedIdentityTables(
        users="2026-08-17-quicksight-users-backup",
        groups="2026-08-17-quicksight-groups-backup",
        memberships="2026-08-17-quicksight-users-groups-backup",
    )

    def table(table_name, key_name, items):
        return IdentityTableSnapshot(
            table_name=table_name,
            key_name=key_name,
            items=sorted(list(items or []), key=lambda item: str(item[key_name])),
        ).seal()

    return IdentitySnapshot(
        users=table(names.users, "user_name", users),
        groups=table(names.groups, "group_name", groups),
        memberships=table(names.memberships, "membership_id", memberships),
    ).seal()


def make_manifest(bundles, with_tables=False):
    tables = ResolvedIdentityTables(
        users="2026-08-17-quicksight-users-backup" if with_tables else "",
        groups="2026-08-17-quicksight-groups-backup" if with_tables else "",
        memberships="2026-08-17-quicksight-users-groups-backup" if with_tables else "",
    )
    return LegacyRestoreManifest(
        backup_date="2026-08-17",
        source_bucket="backup-bucket",
        source_s3_region="us-east-1",
        source_dynamodb_region="us-west-2",
        identity_tables=tables,
        bundles=list(bundles),
        generated_at="2026-08-17T00:00:00+00:00",
        identity_snapshot=make_identity_snapshot(table_names=tables) if with_tables else None,
    )
