"""Restore or verify identities exclusively from sealed plan snapshots."""

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from botocore.exceptions import ClientError

from ..models.config import IdentityMapping, parse_target_principal, partition_for_region
from ..models.contracts import (
    IdentityRestoreResult,
    IdentityResult,
    IdentitySnapshot,
    TargetFingerprint,
)

_NOT_FOUND = {"ResourceNotFoundException", "ResourceNotFound"}
_ALREADY_EXISTS = {"ResourceExistsException", "ResourceExists"}
_ALLOWED_IDENTITY_TYPES = {"QUICKSIGHT", "IAM", "IAM_IDENTITY_CENTER"}
_ALLOWED_USER_ROLES = {
    "ADMIN",
    "AUTHOR",
    "READER",
    "ADMIN_PRO",
    "AUTHOR_PRO",
    "READER_PRO",
    "RESTRICTED_AUTHOR",
    "RESTRICTED_READER",
}


class UserGroupRestoreService:
    """Restore identities from reviewed, immutable plan data only."""

    def __init__(
        self,
        target: TargetFingerprint,
        quicksight_client: Any,
        iam_client: Any,
        identity_mappings: Sequence[Union[IdentityMapping, Mapping[str, Any]]],
    ):
        self.target = target
        self.quicksight = quicksight_client
        self.iam = iam_client
        self.mappings: Dict[str, IdentityMapping] = {}
        for raw in identity_mappings:
            mapping = raw if isinstance(raw, IdentityMapping) else IdentityMapping(**dict(raw))
            if mapping.source_principal_arn in self.mappings:
                raise ValueError("sealed identity mappings contain a duplicate source principal")
            self.mappings[mapping.source_principal_arn] = mapping

    def restore(self, snapshot: IdentitySnapshot) -> IdentityRestoreResult:
        if not snapshot.verify_digest():
            raise ValueError("sealed identity snapshot digest verification failed")
        expected_keys = {
            "users": "user_name",
            "groups": "group_name",
            "memberships": "membership_id",
        }
        for label, expected_key in expected_keys.items():
            if getattr(snapshot, label).key_name != expected_key:
                raise ValueError("sealed {0} snapshot has an unexpected key".format(label))

        result = IdentityRestoreResult()
        users = list(snapshot.users.items)
        groups = list(snapshot.groups.items)
        memberships = list(snapshot.memberships.items)
        user_names_by_source: Dict[str, str] = {}
        group_names_by_source: Dict[str, str] = {}

        # Validate every source record and reviewed mapping before the first target mutation.
        preflight_errors = self._preflight(groups, users, memberships)
        if preflight_errors:
            result.errors.extend(preflight_errors)
            for message in preflight_errors:
                result.results.append(
                    IdentityResult(
                        source_principal_arn="",
                        target_principal_arn=None,
                        identity_kind="preflight",
                        action="validate",
                        status="failed",
                        message=message,
                    )
                )
            return result

        for group in groups:
            source_arn = str(group["arn"])
            try:
                target_arn, target_name, action = self._ensure_group(group)
                result.principal_mapping[source_arn] = target_arn
                group_names_by_source[source_arn] = target_name
                result.results.append(
                    IdentityResult(
                        source_principal_arn=source_arn,
                        target_principal_arn=target_arn,
                        identity_kind="group",
                        action=action,
                        status="success",
                    )
                )
            except Exception as error:
                message = "Group {0}: {1}".format(group.get("group_name", ""), error)
                result.errors.append(message)
                result.results.append(
                    IdentityResult(
                        source_principal_arn=source_arn,
                        target_principal_arn=None,
                        identity_kind="group",
                        action="create-or-verify",
                        status="failed",
                        message=message,
                    )
                )

        for user in users:
            source_arn = str(user["arn"])
            try:
                target_arn, target_name, action, boundary = self._ensure_user(user)
                result.principal_mapping[source_arn] = target_arn
                user_names_by_source[source_arn] = target_name
                result.results.append(
                    IdentityResult(
                        source_principal_arn=source_arn,
                        target_principal_arn=target_arn,
                        identity_kind="user",
                        action=action,
                        status="success",
                        boundary=boundary,
                    )
                )
            except Exception as error:
                message = "User {0}: {1}".format(user.get("user_name", ""), error)
                result.errors.append(message)
                result.results.append(
                    IdentityResult(
                        source_principal_arn=source_arn,
                        target_principal_arn=None,
                        identity_kind="user",
                        action="register-or-verify",
                        status="failed",
                        message=message,
                    )
                )

        for membership in memberships:
            user_source = str(membership["user_arn"])
            group_source = str(membership["group_arn"])
            membership_source = "{0}#{1}".format(user_source, group_source)
            try:
                user_name = user_names_by_source.get(user_source)
                group_name = group_names_by_source.get(group_source)
                if not user_name or not group_name:
                    raise ValueError(
                        "source user or group did not resolve to a verified target principal"
                    )
                action = self._ensure_membership(user_name, group_name)
                result.results.append(
                    IdentityResult(
                        source_principal_arn=membership_source,
                        target_principal_arn=None,
                        identity_kind="membership",
                        action=action,
                        status="success",
                    )
                )
            except Exception as error:
                message = "Membership {0}: {1}".format(
                    membership.get("membership_id", membership_source), error
                )
                result.errors.append(message)
                result.results.append(
                    IdentityResult(
                        source_principal_arn=membership_source,
                        target_principal_arn=None,
                        identity_kind="membership",
                        action="create-or-verify",
                        status="failed",
                        message=message,
                    )
                )
        return result

    def _preflight(
        self,
        groups: Sequence[Mapping[str, Any]],
        users: Sequence[Mapping[str, Any]],
        memberships: Sequence[Mapping[str, Any]],
    ) -> List[str]:
        errors: List[str] = []
        user_arns = set()
        group_arns = set()
        for group in groups:
            try:
                source_arn = self._required(group, "arn")
                self._required(group, "group_name")
                self._mapping_for_source(source_arn, "group")
                if source_arn in group_arns:
                    raise ValueError("duplicate source group ARN")
                group_arns.add(source_arn)
            except Exception as error:
                errors.append("Group {0}: {1}".format(group.get("group_name", ""), error))
        for user in users:
            try:
                source_arn = self._required(user, "arn")
                self._required(user, "user_name")
                identity_type, _ = self._source_user_settings(user)
                mapping = self._mapping_for_source(source_arn, "user")
                self._validate_mapping_for_identity_type(mapping, identity_type)
                if source_arn in user_arns:
                    raise ValueError("duplicate source user ARN")
                user_arns.add(source_arn)
            except Exception as error:
                errors.append("User {0}: {1}".format(user.get("user_name", ""), error))
        for membership in memberships:
            try:
                self._required(membership, "membership_id")
                user_arn = self._required(membership, "user_arn")
                group_arn = self._required(membership, "group_arn")
                self._required(membership, "user_name")
                self._required(membership, "group_name")
                if user_arn not in user_arns or group_arn not in group_arns:
                    raise ValueError("membership references an unknown source user or group")
            except Exception as error:
                errors.append(
                    "Membership {0}: {1}".format(membership.get("membership_id", ""), error)
                )
        return errors

    def _ensure_group(self, group: Mapping[str, Any]) -> Tuple[str, str, str]:
        source_arn = self._required(group, "arn")
        source_name = self._required(group, "group_name")
        mapping = self._mapping_for_source(source_arn, "group")
        target_name = self._mapped_name(mapping, "group") or source_name
        existing = self._describe_group(target_name)
        if existing:
            target_arn = self._required(existing, "Arn")
            self._verify_mapped_principal(mapping, target_arn, "group")
            self._verify_returned_name(existing, "GroupName", target_name)
            return target_arn, target_name, "verified"
        request: Dict[str, Any] = {
            "AwsAccountId": self.target.aws_account_id,
            "Namespace": self.target.namespace,
            "GroupName": target_name,
        }
        description = group.get("description")
        if description:
            request["Description"] = str(description)
        try:
            response = self.quicksight.create_group(**request)
        except ClientError as error:
            if self._code(error) not in _ALREADY_EXISTS:
                raise
            existing = self._describe_group(target_name)
            if not existing:
                raise
            response = {"Group": existing}
        target = response.get("Group", {})
        target_arn = self._required(target, "Arn")
        self._verify_mapped_principal(mapping, target_arn, "group")
        self._verify_returned_name(target, "GroupName", target_name)
        return target_arn, target_name, "created"

    def _ensure_user(self, user: Mapping[str, Any]) -> Tuple[str, str, str, Optional[str]]:
        source_arn = self._required(user, "arn")
        source_name = self._required(user, "user_name")
        identity_type, user_role = self._source_user_settings(user)
        mapping = self._mapping_for_source(source_arn, "user")
        self._validate_mapping_for_identity_type(mapping, identity_type)
        target_name = self._mapped_name(mapping, "user") or source_name

        if identity_type == "IAM":
            assert mapping is not None and mapping.target_iam_arn is not None
            self._verify_iam_principal(mapping.target_iam_arn)

        existing = self._describe_user(target_name)
        if existing:
            self._verify_existing_user(existing, target_name, identity_type, user_role, mapping)
            boundary = (
                "IAM Identity Center provisioning and assignments remain in the authoritative identity source."
                if identity_type == "IAM_IDENTITY_CENTER"
                else None
            )
            return (
                self._required(existing, "Arn"),
                target_name,
                "verified-only" if boundary else "verified",
                boundary,
            )

        if identity_type == "IAM_IDENTITY_CENTER":
            raise ValueError("mapped IAM Identity Center user does not exist in Quick Sight")

        request: Dict[str, Any] = {
            "AwsAccountId": self.target.aws_account_id,
            "Namespace": self.target.namespace,
            "IdentityType": identity_type,
            "Email": self._required(user, "email"),
            "UserRole": user_role,
        }
        if identity_type == "IAM":
            assert mapping is not None and mapping.target_iam_arn is not None
            request["IamArn"] = mapping.target_iam_arn
            if ":role/" in mapping.target_iam_arn:
                if not mapping.session_name:
                    raise ValueError("IAM role mappings require session_name")
                request["SessionName"] = mapping.session_name
        else:
            request["UserName"] = target_name
            custom_permissions = user.get("custom_permissions_name")
            if custom_permissions:
                request["CustomPermissionsName"] = str(custom_permissions)
        try:
            response = self.quicksight.register_user(**request)
        except ClientError as error:
            if self._code(error) not in _ALREADY_EXISTS:
                raise
            existing = self._describe_user(target_name)
            if not existing:
                raise
            self._verify_existing_user(existing, target_name, identity_type, user_role, mapping)
            response = {"User": existing}
        target = response.get("User", {})
        target_arn = self._required(target, "Arn")
        self._verify_mapped_principal(mapping, target_arn, "user")
        returned_name = str(target.get("UserName") or target_name)
        if returned_name != target_name:
            raise ValueError("Quick Sight returned a different user name than planned")
        return target_arn, target_name, "registered", None

    def _source_user_settings(self, user: Mapping[str, Any]) -> Tuple[str, str]:
        identity_type = self._required(user, "identity_type").upper()
        if identity_type not in _ALLOWED_IDENTITY_TYPES:
            raise ValueError("unsupported source identity_type: {0}".format(identity_type))
        if user.get("active") is not True:
            raise ValueError("source user is inactive or missing an explicit active=true state")
        user_role = self._required(user, "role").upper()
        if user_role not in _ALLOWED_USER_ROLES:
            raise ValueError("unsupported source Quick Sight role: {0}".format(user_role))
        return identity_type, user_role

    def _validate_mapping_for_identity_type(
        self, mapping: Optional[IdentityMapping], identity_type: str
    ) -> None:
        if identity_type == "IAM_IDENTITY_CENTER":
            if not mapping or not mapping.target_principal_arn or not mapping.identity_center:
                raise ValueError(
                    "IAM Identity Center users require an explicit reviewed identity_center target mapping"
                )
            if mapping.target_iam_arn or mapping.session_name:
                raise ValueError(
                    "IAM Identity Center mappings must not include IAM registration fields"
                )
            return
        if mapping and mapping.identity_center:
            raise ValueError("identity_center mapping flag does not match the source identity type")
        if identity_type == "IAM":
            if not mapping or not mapping.target_iam_arn:
                raise ValueError("IAM users require a reviewed target_iam_arn mapping")
            if ":role/" in mapping.target_iam_arn and not mapping.session_name:
                raise ValueError("IAM role mappings require session_name")
        elif mapping and (mapping.target_iam_arn or mapping.session_name):
            raise ValueError("QUICKSIGHT user mappings must not include IAM registration fields")

    def _mapping_for_source(self, source_arn: str, kind: str) -> Optional[IdentityMapping]:
        parsed = parse_target_principal(source_arn)
        if not parsed or parsed["kind"] != kind:
            raise ValueError("source identity ARN is invalid or has the wrong principal kind")
        mapping = self.mappings.get(source_arn)
        if parsed["account"] != self.target.aws_account_id and (
            not mapping or not mapping.target_principal_arn
        ):
            raise ValueError(
                "cross-account identities require an explicit reviewed target principal mapping"
            )
        if mapping and mapping.target_principal_arn:
            self._verify_target_arn(mapping.target_principal_arn, kind)
        return mapping

    def _verify_existing_user(
        self,
        existing: Mapping[str, Any],
        target_name: str,
        expected_identity_type: str,
        expected_role: str,
        mapping: Optional[IdentityMapping],
    ) -> None:
        target_arn = self._required(existing, "Arn")
        self._verify_mapped_principal(mapping, target_arn, "user")
        self._verify_returned_name(existing, "UserName", target_name)
        actual_type = self._required(existing, "IdentityType").upper()
        if actual_type != expected_identity_type:
            raise ValueError("existing target user IdentityType does not match the source identity")
        if existing.get("Active") is not True:
            raise ValueError("existing target user is not active")
        actual_role = self._required(existing, "Role").upper()
        if actual_role != expected_role:
            raise ValueError("existing target user role does not match the source role")

    def _ensure_membership(self, user_name: str, group_name: str) -> str:
        try:
            self.quicksight.create_group_membership(
                AwsAccountId=self.target.aws_account_id,
                Namespace=self.target.namespace,
                GroupName=group_name,
                MemberName=user_name,
            )
            return "created"
        except ClientError as error:
            if self._code(error) in _ALREADY_EXISTS:
                return "verified"
            raise

    def _describe_group(self, group_name: str) -> Optional[Dict[str, Any]]:
        try:
            return self.quicksight.describe_group(
                AwsAccountId=self.target.aws_account_id,
                Namespace=self.target.namespace,
                GroupName=group_name,
            ).get("Group")
        except ClientError as error:
            if self._code(error) in _NOT_FOUND:
                return None
            raise

    def _describe_user(self, user_name: str) -> Optional[Dict[str, Any]]:
        try:
            return self.quicksight.describe_user(
                AwsAccountId=self.target.aws_account_id,
                Namespace=self.target.namespace,
                UserName=user_name,
            ).get("User")
        except ClientError as error:
            if self._code(error) in _NOT_FOUND:
                return None
            raise

    def _verify_iam_principal(self, arn: str) -> None:
        parts = arn.split(":", 5)
        expected_partition = partition_for_region(self.target.identity_region)
        if (
            len(parts) != 6
            or parts[0] != "arn"
            or parts[1] != expected_partition
            or parts[2] != "iam"
            or parts[3] != ""
            or parts[4] != self.target.aws_account_id
        ):
            raise ValueError("target_iam_arn is outside the target account or partition")
        resource = parts[5]
        if "/" not in resource:
            raise ValueError("target_iam_arn must identify an IAM role or user")
        kind, resource_name = resource.split("/", 1)
        name = resource_name.rsplit("/", 1)[-1]
        if kind == "role":
            response = self.iam.get_role(RoleName=name)
            actual = response.get("Role", {}).get("Arn")
        elif kind == "user":
            response = self.iam.get_user(UserName=name)
            actual = response.get("User", {}).get("Arn")
        else:
            raise ValueError("target_iam_arn must identify an IAM role or user")
        if actual != arn:
            raise ValueError("verified IAM principal ARN does not match the reviewed mapping")

    def _verify_target_arn(self, arn: str, expected_kind: str) -> None:
        parsed = parse_target_principal(arn)
        if not parsed or parsed["kind"] != expected_kind:
            raise ValueError("Quick Sight principal has the wrong kind")
        if parsed["partition"] != partition_for_region(self.target.identity_region):
            raise ValueError("Quick Sight principal is outside the target AWS partition")
        if parsed["account"] != self.target.aws_account_id:
            raise ValueError("Quick Sight principal is outside the target account")
        if parsed["region"] != self.target.identity_region:
            raise ValueError("Quick Sight principal is outside the target identity Region")
        if parsed["namespace"] != self.target.namespace:
            raise ValueError("Quick Sight principal is outside the target namespace")

    def _verify_mapped_principal(
        self,
        mapping: Optional[IdentityMapping],
        actual_arn: str,
        expected_kind: str,
    ) -> None:
        self._verify_target_arn(actual_arn, expected_kind)
        if mapping and mapping.target_principal_arn != actual_arn:
            raise ValueError("verified target principal does not match the reviewed mapping")

    @staticmethod
    def _mapped_name(mapping: Optional[IdentityMapping], kind: str) -> Optional[str]:
        if not mapping or not mapping.target_principal_arn:
            return None
        parsed = parse_target_principal(mapping.target_principal_arn)
        if not parsed or parsed["kind"] != kind:
            raise ValueError(
                "identity mapping target principal kind does not match source identity"
            )
        return parsed["name"]

    @staticmethod
    def _verify_returned_name(value: Mapping[str, Any], key: str, expected_name: str) -> None:
        actual = value.get(key)
        if actual is not None and str(actual) != expected_name:
            raise ValueError("Quick Sight returned a different principal name than planned")

    @staticmethod
    def _required(value: Mapping[str, Any], key: str) -> str:
        item = value.get(key)
        if item is None or str(item) == "":
            raise ValueError("required identity field is missing: {0}".format(key))
        return str(item)

    @staticmethod
    def _code(error: ClientError) -> str:
        return str(error.response.get("Error", {}).get("Code", "Unknown"))
