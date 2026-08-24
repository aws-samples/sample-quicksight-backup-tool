import copy

import pytest

from quicksight_restore.models.contracts import (
    PlannedBundle,
    RestorePlan,
    TargetFingerprint,
)
from quicksight_restore.services.asset_bundle import AssetBundleRestoreService
from conftest import make_bundle, make_manifest, make_member, make_zip


class Catalog:
    def __init__(self, body):
        self.body = body
        self.calls = []

    def read_and_verify_bundle(self, bundle):
        self.calls.append(bundle.key)
        return self.body


class QuickSight:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.started = []
        self.described = []

    def start_asset_bundle_import_job(self, **request):
        self.started.append(request)
        return {"AssetBundleImportJobId": request["AssetBundleImportJobId"]}

    def describe_asset_bundle_import_job(self, **request):
        self.described.append(request)
        value = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if isinstance(value, dict):
            return value
        return {"JobStatus": value}


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


def make_plan(restore_config, body, audit_overrides=None, import_overrides=None, prefix=False):
    member = make_member("datasource/source.json", "datasource", "source")
    inventory = make_bundle(
        "quicksight-backups/2026/08/17/datasources/datasources-010101.zip",
        "datasources",
        [member],
        body=body,
    )
    manifest = make_manifest([inventory])
    sealed_overrides = copy.deepcopy(import_overrides or {})
    if prefix:
        parameters = sealed_overrides.setdefault("OverrideParameters", {})
        resource_ids = parameters.setdefault("ResourceIdOverrideConfiguration", {})
        resource_ids["PrefixForAllResources"] = "restored-"
    planned = PlannedBundle(
        bucket=inventory.bucket,
        key=inventory.key,
        version_id=inventory.version_id,
        size=inventory.size,
        sha256=inventory.sha256,
        root_type=inventory.root_type,
        selected_member_names=[member.member_name],
        selected_resources=[member.resource_key],
        import_overrides=sealed_overrides,
    )
    return RestorePlan(
        manifest=manifest,
        target=TargetFingerprint(
            aws_account_id=restore_config.target.aws_account_id,
            asset_region=restore_config.target.asset_region,
            identity_region=restore_config.target.identity_region,
            namespace=restore_config.target.namespace,
        ),
        bundles=[planned],
        duplicate_decisions=[],
        conflict_decisions=[],
        conflict_policy="prefix" if prefix else "update",
        conflict_prefix="restored-" if prefix else None,
        failure_action="ROLLBACK",
        continue_on_error=False,
        restore_identities=False,
        target_principals=[],
        identity_mappings=[],
        overrides=copy.deepcopy(audit_overrides or {}),
        overrides_sha256=None,
        created_at="2026-08-17T00:00:00+00:00",
    ).seal()


@pytest.mark.parametrize(
    "terminal, expected",
    [
        ("SUCCESSFUL", "success"),
        ("FAILED", "failed"),
        ("FAILED_ROLLBACK_COMPLETED", "failed"),
        ("FAILED_ROLLBACK_ERROR", "failed"),
        ("SOMETHING_NEW", "failed"),
    ],
)
def test_import_terminal_states_fail_closed(restore_config, terminal, expected):
    body = make_zip(
        {"datasource/source.json": {"resourceType": "datasource", "dataSourceId": "source"}}
    )
    plan = make_plan(restore_config, body)
    quicksight = QuickSight([terminal])
    service = AssetBundleRestoreService(
        restore_config, Catalog(body), quicksight, sleep=lambda _: None
    )
    result = service.restore_bundle(plan, plan.bundles[0], "restore-id", 1)
    assert result.status == expected
    assert result.terminal_status == terminal
    assert result.selected_member_count == 1


def test_timeout_and_rollback_errors_are_reported_as_failures(restore_config):
    restore_config.restore.poll_timeout_seconds = 2
    body = make_zip(
        {"datasource/source.json": {"resourceType": "datasource", "dataSourceId": "source"}}
    )
    plan = make_plan(restore_config, body)
    clock = Clock()
    service = AssetBundleRestoreService(
        restore_config,
        Catalog(body),
        QuickSight(["IN_PROGRESS"]),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    result = service.restore_bundle(plan, plan.bundles[0], "restore-id", 1)
    assert result.status == "timeout"
    assert any("timed out" in item["Message"] for item in result.errors)

    response = {
        "JobStatus": "FAILED_ROLLBACK_ERROR",
        "Errors": [{"Arn": "asset", "Message": "import failed"}],
        "RollbackErrors": [{"Arn": "asset", "Message": "rollback failed"}],
    }
    result = AssetBundleRestoreService(
        restore_config, Catalog(body), QuickSight([response]), sleep=lambda _: None
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)
    assert result.status == "failed"
    assert result.rollback_errors[0]["Message"] == "rollback failed"


def test_execution_uses_only_sealed_bundle_overrides(restore_config):
    body = make_zip(
        {"datasource/source.json": {"resourceType": "datasource", "dataSourceId": "source"}}
    )
    principal = "arn:aws:quicksight:us-east-1:222222222222:group/default/reviewers"
    import_overrides = {
        "OverrideParameters": {"DataSources": [{"DataSourceId": "source"}]},
        "OverridePermissions": {
            "DataSources": [
                {
                    "DataSourceIds": ["*"],
                    "Permissions": {
                        "Principals": [principal],
                        "Actions": ["quicksight:DescribeDataSource"],
                    },
                }
            ]
        },
        "OverrideTags": {
            "DataSources": [
                {
                    "DataSourceIds": ["source"],
                    "Tags": [{"Key": "restore", "Value": "tested"}],
                }
            ]
        },
    }
    audit_only = {"OverrideParameters": {"DataSources": [{"DataSourceId": "must-not-be-sent"}]}}
    plan = make_plan(
        restore_config,
        body,
        audit_overrides=audit_only,
        import_overrides=import_overrides,
        prefix=True,
    )
    quicksight = QuickSight(["SUCCESSFUL"])
    result = AssetBundleRestoreService(
        restore_config, Catalog(body), quicksight, sleep=lambda _: None
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)
    assert result.status == "success"
    request = quicksight.started[0]
    assert request["FailureAction"] == "ROLLBACK"
    assert request["OverridePermissions"] == import_overrides["OverridePermissions"]
    assert request["OverrideTags"] == import_overrides["OverrideTags"]
    assert request["OverrideParameters"]["DataSources"][0]["DataSourceId"] == "source"
    assert (
        request["OverrideParameters"]["ResourceIdOverrideConfiguration"]["PrefixForAllResources"]
        == "restored-"
    )
    assert "must-not-be-sent" not in repr(request)
    assert isinstance(request["AssetBundleImportSource"]["Body"], bytes)
    assert request["AssetBundleImportSource"]["Body"] == body


def test_execution_rejects_any_plan_that_would_rewrite_archive_members(restore_config):
    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    plan.bundles[0].selected_member_names = []
    plan.bundles[0].omitted_member_names = ["datasource/source.json"]
    quicksight = QuickSight(["SUCCESSFUL"])

    result = AssetBundleRestoreService(
        restore_config, Catalog(body), quicksight, sleep=lambda _: None
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.status == "failed"
    assert result.terminal_status == "START_FAILED"
    assert "member-level" in result.errors[0]["Message"]
    assert quicksight.started == []


class ScriptedQuickSight:
    def __init__(self, starts=None, describes=None):
        self.starts = list(starts or [{}])
        self.describes = list(describes or ["SUCCESSFUL"])
        self.started = []
        self.described = []

    @staticmethod
    def _resolve(action, request, default):
        if isinstance(action, BaseException):
            raise action
        if callable(action):
            return action(request)
        if isinstance(action, str):
            return {"JobStatus": action}
        return action if action is not None else default

    def start_asset_bundle_import_job(self, **request):
        self.started.append(request)
        action = self.starts.pop(0) if self.starts else {}
        return self._resolve(action, request, {})

    def describe_asset_bundle_import_job(self, **request):
        self.described.append(request)
        action = self.describes.pop(0) if self.describes else "SUCCESSFUL"
        return self._resolve(action, request, {"JobStatus": "SUCCESSFUL"})


def scripted_client_error(code, status=400):
    from botocore.exceptions import ClientError

    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "QuickSightOperation",
    )


def test_transient_start_is_reconciled_by_deterministic_job_id(restore_config):
    from botocore.exceptions import EndpointConnectionError

    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    quicksight = ScriptedQuickSight(
        starts=[EndpointConnectionError(endpoint_url="https://quicksight")],
        describes=["IN_PROGRESS", "SUCCESSFUL"],
    )

    result = AssetBundleRestoreService(
        restore_config,
        Catalog(body),
        quicksight,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.outcome == "imported"
    assert result.attempts == {"start": 1, "describe": 2}
    assert len(quicksight.started) == 1
    assert {call["AssetBundleImportJobId"] for call in quicksight.described} == {
        quicksight.started[0]["AssetBundleImportJobId"]
    }


def test_transient_start_retries_only_after_reconciliation_reports_not_found(
    restore_config,
):
    from botocore.exceptions import EndpointConnectionError

    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    quicksight = ScriptedQuickSight(
        starts=[EndpointConnectionError(endpoint_url="https://quicksight"), {}],
        describes=[scripted_client_error("ResourceNotFoundException", 404), "SUCCESSFUL"],
    )

    result = AssetBundleRestoreService(
        restore_config,
        Catalog(body),
        quicksight,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.outcome == "imported"
    assert result.attempts == {"start": 2, "describe": 2}
    assert len(quicksight.started) == 2
    assert (
        quicksight.started[0]["AssetBundleImportJobId"]
        == quicksight.started[1]["AssetBundleImportJobId"]
    )


def test_transient_describe_errors_use_bounded_exponential_retries(restore_config):
    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    quicksight = ScriptedQuickSight(
        describes=[
            scripted_client_error("ThrottlingException", 400),
            scripted_client_error("ServiceUnavailableException", 503),
            "SUCCESSFUL",
        ]
    )
    clock = Clock()
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock.sleep(seconds)

    result = AssetBundleRestoreService(
        restore_config,
        Catalog(body),
        quicksight,
        sleep=sleep,
        monotonic=clock.monotonic,
        jitter=lambda maximum: maximum,
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.outcome == "imported"
    assert result.attempts == {"start": 1, "describe": 3}
    assert sleeps == [1.0, 2.0]


def test_permanent_start_error_is_not_retried(restore_config):
    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    quicksight = ScriptedQuickSight(starts=[scripted_client_error("AccessDeniedException", 403)])

    result = AssetBundleRestoreService(
        restore_config,
        Catalog(body),
        quicksight,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.outcome == "failed"
    assert result.terminal_status == "START_FAILED"
    assert result.attempts == {"start": 1, "describe": 0}


def test_exhausted_transient_describe_retries_report_remote_uncertainty(
    restore_config,
):
    from botocore.exceptions import EndpointConnectionError

    restore_config.restore.poll_timeout_seconds = 100
    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    quicksight = ScriptedQuickSight(
        describes=[EndpointConnectionError(endpoint_url="https://quicksight") for _ in range(5)]
    )
    clock = Clock()

    result = AssetBundleRestoreService(
        restore_config,
        Catalog(body),
        quicksight,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        jitter=lambda maximum: maximum,
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.outcome == "timed_out"
    assert result.terminal_status == "REMOTE_STATE_UNCERTAIN"
    assert result.attempts == {"start": 1, "describe": 5}
    assert "uncertain" in result.reason


def test_execution_rejects_oversized_inline_plan_before_read_or_start(restore_config):
    from quicksight_restore.limits import INLINE_IMPORT_MAX_BYTES

    body = make_zip(
        {
            "datasource/source.json": {
                "resourceType": "datasource",
                "dataSourceId": "source",
            }
        }
    )
    plan = make_plan(restore_config, body)
    plan.bundles[0].size = INLINE_IMPORT_MAX_BYTES + 1
    catalog = Catalog(body)
    quicksight = ScriptedQuickSight()

    result = AssetBundleRestoreService(
        restore_config, catalog, quicksight, sleep=lambda _: None
    ).restore_bundle(plan, plan.bundles[0], "restore-id", 1)

    assert result.outcome == "failed"
    assert "inline import limit" in result.reason
    assert catalog.calls == []
    assert quicksight.started == []
