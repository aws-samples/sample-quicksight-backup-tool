import pytest
from botocore.exceptions import ClientError

from quicksight_restore.models.config import IdentityMapping
from quicksight_restore.models.contracts import TargetFingerprint
from quicksight_restore.services.identities import UserGroupRestoreService
from conftest import make_identity_snapshot


SOURCE_USER = "arn:aws:quicksight:us-west-2:111111111111:user/default/source-user"
SOURCE_GROUP = "arn:aws:quicksight:us-west-2:111111111111:group/default/source-group"
TARGET_USER = "arn:aws:quicksight:us-east-1:222222222222:user/default/idc-user"
TARGET_GROUP = "arn:aws:quicksight:us-east-1:222222222222:group/default/source-group"


def not_found(operation):
    return ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}}, operation
    )


def target_from(config):
    return TargetFingerprint(
        aws_account_id=config.target.aws_account_id,
        asset_region=config.target.asset_region,
        identity_region=config.target.identity_region,
        namespace=config.target.namespace,
    )


def group_record():
    return {
        "group_name": "source-group",
        "arn": SOURCE_GROUP,
        "members": ["source-user"],
    }


def user_record(identity_type="QUICKSIGHT", **overrides):
    value = {
        "user_name": "source-user",
        "arn": SOURCE_USER,
        "email": "user@example.com",
        "role": "AUTHOR",
        "identity_type": identity_type,
        "active": True,
    }
    value.update(overrides)
    return value


def membership_record():
    return {
        "membership_id": "source-user#source-group",
        "user_name": "source-user",
        "group_name": "source-group",
        "user_arn": SOURCE_USER,
        "group_arn": SOURCE_GROUP,
    }


def group_mapping():
    return IdentityMapping(
        source_principal_arn=SOURCE_GROUP,
        target_principal_arn=TARGET_GROUP,
    )


class IdentityQuickSight:
    def __init__(
        self,
        existing_users=None,
        existing_groups=None,
        register_user=None,
        existing_memberships=None,
    ):
        self.existing_users = existing_users or {}
        self.existing_groups = existing_groups or {}
        self.registered_user = register_user
        self.existing_memberships = set(existing_memberships or [])
        self.register_requests = []
        self.group_requests = []
        self.membership_requests = []

    def describe_group(self, **request):
        group = self.existing_groups.get(request["GroupName"])
        if group:
            return {"Group": group}
        raise not_found("DescribeGroup")

    def create_group(self, **request):
        self.group_requests.append(request)
        return {
            "Group": {
                "Arn": TARGET_GROUP,
                "GroupName": request["GroupName"],
            }
        }

    def describe_user(self, **request):
        user = self.existing_users.get(request["UserName"])
        if user:
            return {"User": user}
        raise not_found("DescribeUser")

    def register_user(self, **request):
        self.register_requests.append(request)
        if self.registered_user:
            return {"User": self.registered_user}
        name = request.get("UserName", "source-user")
        return {
            "User": {
                "Arn": "arn:aws:quicksight:us-east-1:222222222222:user/default/" + name,
                "UserName": name,
            }
        }

    def list_group_memberships(self, **request):
        pair = (request["GroupName"], "idc-user")
        members = [{"MemberName": pair[1]}] if pair in self.existing_memberships else []
        return {"GroupMemberList": members}

    def create_group_membership(self, **request):
        self.membership_requests.append(request)
        return {}


class IAM:
    def __init__(self, arn=None):
        self.arn = arn
        self.calls = []

    def get_role(self, RoleName):
        self.calls.append(("role", RoleName))
        return {"Role": {"Arn": self.arn}}

    def get_user(self, UserName):
        self.calls.append(("user", UserName))
        return {"User": {"Arn": self.arn}}


def service(config, quicksight, iam, mappings):
    return UserGroupRestoreService(target_from(config), quicksight, iam, mappings)


def test_identity_center_is_verify_only_and_membership_uses_mapped_name(restore_config):
    mappings = [
        group_mapping(),
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=TARGET_USER,
            identity_center=True,
        ),
    ]
    snapshot = make_identity_snapshot(
        users=[user_record("IAM_IDENTITY_CENTER", role="READER")],
        groups=[group_record()],
        memberships=[membership_record()],
    )
    quicksight = IdentityQuickSight(
        existing_users={
            "idc-user": {
                "Arn": TARGET_USER,
                "UserName": "idc-user",
                "IdentityType": "IAM_IDENTITY_CENTER",
                "Active": True,
                "Role": "READER",
            }
        },
        existing_memberships={("source-group", "idc-user")},
    )

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 0
    assert result.principal_mapping[SOURCE_USER] == TARGET_USER
    restored_user = [item for item in result.results if item.identity_kind == "user"][0]
    assert restored_user.action == "verified-only"
    assert "authoritative identity source" in restored_user.boundary
    assert quicksight.register_requests == []
    restored_membership = [item for item in result.results if item.identity_kind == "membership"][0]
    assert restored_membership.action == "verified-only"
    assert "authoritative identity source" in restored_membership.boundary
    assert quicksight.membership_requests == []


def test_quicksight_managed_register_user_uses_required_api_shape(restore_config):
    target_user = "arn:aws:quicksight:us-east-1:222222222222:user/default/managed-user"
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=target_user,
        )
    ]
    snapshot = make_identity_snapshot(
        users=[
            user_record(
                "QUICKSIGHT",
                user_name="managed-user",
                email="managed@example.com",
                role="AUTHOR",
            )
        ]
    )
    quicksight = IdentityQuickSight(register_user={"Arn": target_user, "UserName": "managed-user"})

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 0
    request = quicksight.register_requests[0]
    assert request == {
        "AwsAccountId": "222222222222",
        "Namespace": "default",
        "IdentityType": "QUICKSIGHT",
        "Email": "managed@example.com",
        "UserRole": "AUTHOR",
        "UserName": "managed-user",
    }


def test_iam_user_requires_reviewed_mappings_and_verifies_role(restore_config):
    target_iam = "arn:aws:iam::222222222222:role/QuickSightAuthor"
    target_user = "arn:aws:quicksight:us-east-1:222222222222:user/default/restored-session"
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=target_user,
            target_iam_arn=target_iam,
            session_name="restored-session",
        )
    ]
    snapshot = make_identity_snapshot(users=[user_record("IAM")])
    quicksight = IdentityQuickSight(
        register_user={"Arn": target_user, "UserName": "restored-session"}
    )
    iam = IAM(target_iam)

    result = service(restore_config, quicksight, iam, mappings).restore(snapshot)

    assert result.failed == 0
    request = quicksight.register_requests[0]
    assert request["IdentityType"] == "IAM"
    assert request["IamArn"] == target_iam
    assert request["SessionName"] == "restored-session"
    assert iam.calls == [("role", "QuickSightAuthor")]


@pytest.mark.parametrize(
    "change, expected",
    [
        ({"identity_type": None}, "identity_type"),
        ({"identity_type": "UNKNOWN"}, "unsupported source identity_type"),
        ({"active": False}, "inactive"),
        ({"role": None}, "role"),
    ],
)
def test_invalid_source_users_fail_preflight_without_mutation(restore_config, change, expected):
    target_user = "arn:aws:quicksight:us-east-1:222222222222:user/default/source-user"
    record = user_record()
    record.update(change)
    snapshot = make_identity_snapshot(users=[record])
    quicksight = IdentityQuickSight()
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=target_user,
        )
    ]

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 1
    assert expected in result.errors[0]
    assert quicksight.register_requests == []
    assert quicksight.group_requests == []


def test_existing_iam_user_is_not_accepted_without_reviewed_iam_mapping(restore_config):
    target_user = "arn:aws:quicksight:us-east-1:222222222222:user/default/source-user"
    snapshot = make_identity_snapshot(users=[user_record("IAM")])
    quicksight = IdentityQuickSight(
        existing_users={
            "source-user": {
                "Arn": target_user,
                "UserName": "source-user",
                "IdentityType": "IAM",
                "Active": True,
                "Role": "AUTHOR",
            }
        }
    )
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=target_user,
        )
    ]

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 1
    assert "target_iam_arn" in result.errors[0]
    assert quicksight.register_requests == []


def test_identity_center_mapping_rejects_existing_wrong_identity_type(restore_config):
    snapshot = make_identity_snapshot(users=[user_record("IAM_IDENTITY_CENTER", role="READER")])
    quicksight = IdentityQuickSight(
        existing_users={
            "idc-user": {
                "Arn": TARGET_USER,
                "UserName": "idc-user",
                "IdentityType": "IAM",
                "Active": True,
                "Role": "READER",
            }
        }
    )
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=TARGET_USER,
            identity_center=True,
        )
    ]

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 1
    assert "IdentityType" in result.errors[0]
    assert quicksight.register_requests == []


@pytest.mark.parametrize(
    "target_arn, expected",
    [
        (
            "arn:aws:quicksight:us-west-2:222222222222:user/default/source-user",
            "identity Region",
        ),
        (
            "arn:aws-cn:quicksight:us-east-1:222222222222:user/default/source-user",
            "partition",
        ),
    ],
)
def test_target_mapping_rejects_wrong_region_or_partition(restore_config, target_arn, expected):
    snapshot = make_identity_snapshot(users=[user_record()])
    quicksight = IdentityQuickSight()
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=target_arn,
        )
    ]

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 1
    assert expected in result.errors[0]
    assert quicksight.register_requests == []


@pytest.mark.parametrize(
    "field, value, expected",
    [
        ("Active", False, "not active"),
        ("Role", "READER", "role"),
    ],
)
def test_existing_target_user_must_match_active_state_and_role(
    restore_config, field, value, expected
):
    target_user = "arn:aws:quicksight:us-east-1:222222222222:user/default/source-user"
    existing = {
        "Arn": target_user,
        "UserName": "source-user",
        "IdentityType": "QUICKSIGHT",
        "Active": True,
        "Role": "AUTHOR",
    }
    existing[field] = value
    snapshot = make_identity_snapshot(users=[user_record()])
    quicksight = IdentityQuickSight(existing_users={"source-user": existing})
    mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=target_user,
        )
    ]

    result = service(restore_config, quicksight, IAM(), mappings).restore(snapshot)

    assert result.failed == 1
    assert expected in result.errors[0]
    assert quicksight.register_requests == []
