from pathlib import Path

import pytest

from quicksight_restore.models.config import IdentityMapping
from quicksight_restore.models.contracts import (
    ConflictDecision,
    ImportJobResult,
    PlannedBundle,
    RestorePlan,
    TargetFingerprint,
)
from quicksight_restore.models.errors import PlanIntegrityError
from quicksight_restore.orchestrator import RestoreOrchestrator
from quicksight_restore.services.planner import RestorePlanner
from conftest import make_bundle, make_identity_snapshot, make_manifest, make_member


class Dummy:
    pass


class Catalog:
    def __init__(self):
        self.keys = []

    def read_and_verify_bundle(self, bundle):
        self.keys.append(bundle.key)
        return b"verified"


class FakeAssetService:
    calls = []

    def __init__(self, *args, **kwargs):
        pass

    def restore_bundle(self, plan, bundle, restore_id, index):
        self.__class__.calls.append(bundle.key)
        success = index == 2
        return ImportJobResult(
            bundle_key=bundle.key,
            job_id="job-{0}".format(index),
            status="success" if success else "failed",
            terminal_status="SUCCESSFUL" if success else "FAILED_ROLLBACK_COMPLETED",
            selected_member_count=1,
            started_at="2026-08-17T00:00:00+00:00",
            completed_at="2026-08-17T00:00:01+00:00",
            duration_seconds=1.0,
            errors=[] if success else [{"Message": "failed"}],
            rollback_errors=[],
        )


def create_conflict_decisions(planned):
    resources = sorted({resource for bundle in planned for resource in bundle.selected_resources})
    return [
        ConflictDecision(
            resource_key=resource,
            destination_resource_key=resource,
            policy="update",
            action="create",
            target_exists=False,
        )
        for resource in resources
    ]


def build_plan(config):
    first_member = make_member("assets/dataSources/one.json", "datasource", "one")
    second_member = make_member("assets/dataSets/two.json", "dataset", "two")
    first = make_bundle("key-one.zip", "datasources", [first_member], body=b"one")
    second = make_bundle("key-two.zip", "datasets", [second_member], body=b"two")
    planned = [
        PlannedBundle(
            bucket=item.bucket,
            key=item.key,
            version_id=item.version_id,
            size=item.size,
            sha256=item.sha256,
            root_type=item.root_type,
            selected_member_names=[item.members[0].member_name],
            selected_resources=[item.members[0].resource_key],
            order=index,
        )
        for index, item in enumerate((first, second))
    ]
    return RestorePlan(
        manifest=make_manifest([first, second]),
        target=TargetFingerprint(
            aws_account_id=config.target.aws_account_id,
            asset_region=config.target.asset_region,
            identity_region=config.target.identity_region,
            namespace=config.target.namespace,
        ),
        bundles=planned,
        duplicate_decisions=[],
        conflict_decisions=create_conflict_decisions(planned),
        conflict_policy="update",
        conflict_prefix=None,
        failure_action="ROLLBACK",
        continue_on_error=True,
        restore_identities=False,
        target_principals=[],
        identity_mappings=[],
        overrides={},
        overrides_sha256=None,
        config_snapshot=config.audit_snapshot(None),
        created_at="2026-08-17T00:00:00+00:00",
    ).seal()


def test_continue_on_error_runs_later_bundles_and_reports_partial(
    restore_config, tmp_path, monkeypatch
):
    restore_config.restore.continue_on_error = True
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.report_directory = str(tmp_path / "reports")
    plan = build_plan(restore_config)
    path = tmp_path / "plan.json"
    RestorePlanner.save_plan(plan, str(path))
    FakeAssetService.calls = []
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.AssetBundleRestoreService", FakeAssetService
    )
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=EmptyQuickSightInventory(),
        target_iam_client=Dummy(),
    )
    catalog = Catalog()
    orchestrator.catalog = catalog
    report = orchestrator.run(str(path))
    assert report.overall_status == "partial"
    assert FakeAssetService.calls == ["key-one.zip", "key-two.zip"]
    assert catalog.keys == ["key-one.zip", "key-two.zip"]
    assert len(report.import_jobs) == 2
    assert report.plan_evidence["artifact_digest"] == plan.artifact_digest
    assert orchestrator.status(report.restore_id).overall_status == "partial"


SOURCE_USER = "arn:aws:quicksight:us-west-2:111111111111:user/default/source-user"


def build_identity_plan(config, mapping):
    manifest = make_manifest([], with_tables=True)
    manifest.identity_snapshot = make_identity_snapshot(
        users=[
            {
                "user_name": "source-user",
                "arn": SOURCE_USER,
                "email": "user@example.com",
                "role": "READER",
                "identity_type": "QUICKSIGHT",
                "active": True,
            }
        ],
        table_names=manifest.identity_tables,
    )
    mapping_payload = [
        {
            "source_principal_arn": mapping.source_principal_arn,
            "target_principal_arn": mapping.target_principal_arn,
            "target_iam_arn": mapping.target_iam_arn,
            "session_name": mapping.session_name,
            "identity_center": mapping.identity_center,
        }
    ]
    return RestorePlan(
        manifest=manifest,
        target=TargetFingerprint(
            aws_account_id=config.target.aws_account_id,
            asset_region=config.target.asset_region,
            identity_region=config.target.identity_region,
            namespace=config.target.namespace,
        ),
        bundles=[],
        duplicate_decisions=[],
        conflict_decisions=[],
        conflict_policy=config.restore.conflict_policy,
        conflict_prefix=config.restore.conflict_prefix,
        failure_action=config.restore.failure_action,
        continue_on_error=config.restore.continue_on_error,
        restore_identities=True,
        target_principals=[],
        identity_mappings=mapping_payload,
        overrides={},
        overrides_sha256=None,
        config_snapshot=config.audit_snapshot(None),
        created_at="2026-08-17T00:00:00+00:00",
    ).seal()


class ChangedIdentityCatalog(Catalog):
    def verify_identity_snapshot(self, snapshot):
        raise PlanIntegrityError("Identity source changed after planning: users")


class ForbiddenIdentityService:
    calls = 0

    def __init__(self, *args, **kwargs):
        self.__class__.calls += 1

    def restore(self, snapshot):
        raise AssertionError("identity mutation path must not run")


def test_identity_snapshot_drift_blocks_all_target_mutation(restore_config, tmp_path, monkeypatch):
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.mode = "identities-only"
    restore_config.restore.restore_identities = True
    restore_config.restore.report_directory = str(tmp_path / "reports")
    mapping = IdentityMapping(
        source_principal_arn=SOURCE_USER,
        target_principal_arn=("arn:aws:quicksight:us-east-2:222222222222:user/default/source-user"),
    )
    restore_config.restore.identity_mappings = [mapping]
    plan = build_identity_plan(restore_config, mapping)
    path = tmp_path / "identity-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    ForbiddenIdentityService.calls = 0
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.UserGroupRestoreService",
        ForbiddenIdentityService,
    )
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=Dummy(),
        target_iam_client=Dummy(),
    )
    orchestrator.catalog = ChangedIdentityCatalog()

    report = orchestrator.run(str(path))

    assert report.overall_status == "failed"
    assert report.import_jobs == []
    assert ForbiddenIdentityService.calls == 0
    assert "Identity source changed after planning" in report.errors[0]


def test_identity_mapping_drift_rejects_plan_before_source_or_target_calls(
    restore_config, tmp_path
):
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.mode = "identities-only"
    restore_config.restore.restore_identities = True
    restore_config.restore.report_directory = str(tmp_path / "reports")
    original = IdentityMapping(
        source_principal_arn=SOURCE_USER,
        target_principal_arn=("arn:aws:quicksight:us-east-2:222222222222:user/default/source-user"),
    )
    restore_config.restore.identity_mappings = [original]
    plan = build_identity_plan(restore_config, original)
    path = tmp_path / "identity-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    restore_config.restore.identity_mappings = [
        IdentityMapping(
            source_principal_arn=SOURCE_USER,
            target_principal_arn=(
                "arn:aws:quicksight:us-east-2:222222222222:user/default/different-user"
            ),
        )
    ]
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=Dummy(),
        target_iam_client=Dummy(),
    )

    with pytest.raises(
        PlanIntegrityError,
        match=("Restore plan sanitized configuration snapshot does not match configuration"),
    ):
        orchestrator.run(str(path))


def test_resealed_principal_redirection_is_rejected_before_any_calls_or_report(
    restore_config, tmp_path
):
    reviewed_principal = "arn:aws:quicksight:us-east-1:222222222222:user/default/reviewer"
    redirected_principal = "arn:aws:quicksight:us-east-1:222222222222:user/default/redirected"
    restore_config.restore.target_principals = [reviewed_principal]
    restore_config.restore.report_directory = str(tmp_path / "resealed-reports")
    member = make_member("dataset/one.json", "dataset", "one")
    bundle = make_bundle("principal.zip", "datasets", [member], body=b"principal")

    class EmptyPlanInventory:
        @staticmethod
        def list_resources(resource_types):
            return set()

    plan = RestorePlanner(restore_config, target_inventory=EmptyPlanInventory()).build_plan(
        make_manifest([bundle])
    )
    path = tmp_path / "reviewed-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    reloaded = RestorePlanner.load_plan(str(path))

    reloaded.target_principals = [redirected_principal]
    changed_permissions = 0
    for planned_bundle in reloaded.bundles:
        permission_sections = planned_bundle.import_overrides.get("OverridePermissions", {})
        for entries in permission_sections.values():
            for entry in entries:
                entry["Permissions"]["Principals"] = [redirected_principal]
                changed_permissions += 1
    assert changed_permissions > 0
    reloaded.seal()
    assert reloaded.verify_digest()

    class ForbiddenClient:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            self.calls.append(name)
            raise AssertionError("unexpected client call: {0}".format(name))

    source_s3 = ForbiddenClient()
    source_dynamodb = ForbiddenClient()
    target_quicksight = ForbiddenClient()
    target_iam = ForbiddenClient()
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=source_s3,
        source_dynamodb_client=source_dynamodb,
        target_quicksight_client=target_quicksight,
        target_iam_client=target_iam,
    )

    with pytest.raises(
        PlanIntegrityError,
        match="Restore plan target principals do not match configuration",
    ):
        orchestrator._verify_plan(reloaded)

    assert source_s3.calls == []
    assert source_dynamodb.calls == []
    assert target_quicksight.calls == []
    assert target_iam.calls == []
    assert not Path(restore_config.restore.report_directory).exists()


class TargetWithNewDataSource:
    def list_data_sources(self, **request):
        return {"DataSources": [{"DataSourceId": "one"}]}

    def list_data_sets(self, **request):
        return {"DataSetSummaries": []}


def test_target_conflict_drift_is_rechecked_before_import(restore_config, tmp_path, monkeypatch):
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.continue_on_error = True
    restore_config.restore.report_directory = str(tmp_path / "reports")
    plan = build_plan(restore_config)
    path = tmp_path / "target-drift-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    FakeAssetService.calls = []
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.AssetBundleRestoreService", FakeAssetService
    )
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=TargetWithNewDataSource(),
        target_iam_client=Dummy(),
    )
    orchestrator.catalog = Catalog()

    report = orchestrator.run(str(path))

    assert report.overall_status == "failed"
    assert FakeAssetService.calls == []
    assert "Target state changed after planning" in report.errors[0]


class GraphAssetService:
    calls = []

    def __init__(self, *args, **kwargs):
        pass

    def restore_bundle(self, plan, bundle, restore_id, index):
        self.__class__.calls.append(bundle.key)
        failed = bundle.key == "provider.zip"
        return ImportJobResult(
            bundle_key=bundle.key,
            job_id="job-{0}".format(index),
            status="failed" if failed else "success",
            terminal_status="FAILED" if failed else "SUCCESSFUL",
            selected_member_count=1,
            started_at="2026-08-17T00:00:00+00:00",
            completed_at="2026-08-17T00:00:01+00:00",
            duration_seconds=1.0,
            errors=[{"Message": "provider failed"}] if failed else [],
            rollback_errors=[],
        )


def build_graph_plan(config, continue_on_error):
    provider = make_bundle(
        "provider.zip",
        "datasources",
        [make_member("datasource/provider.json", "datasource", "provider")],
        body=b"provider",
    )
    dependent = make_bundle(
        "dependent.zip",
        "datasets",
        [
            make_member(
                "dataset/dependent.json",
                "dataset",
                "dependent",
                dependencies=["datasource/provider"],
            )
        ],
        body=b"dependent",
    )
    independent = make_bundle(
        "independent.zip",
        "datasets",
        [make_member("dataset/independent.json", "dataset", "independent")],
        body=b"independent",
    )
    planned = []
    for index, item in enumerate((provider, dependent, independent)):
        planned.append(
            PlannedBundle(
                bucket=item.bucket,
                key=item.key,
                version_id=item.version_id,
                size=item.size,
                sha256=item.sha256,
                root_type=item.root_type,
                selected_member_names=[item.members[0].member_name],
                selected_resources=[item.members[0].resource_key],
                prerequisite_bundle_keys=[provider.key] if item is dependent else [],
                order=index,
            )
        )
    return RestorePlan(
        manifest=make_manifest([provider, dependent, independent]),
        target=TargetFingerprint(
            aws_account_id=config.target.aws_account_id,
            asset_region=config.target.asset_region,
            identity_region=config.target.identity_region,
            namespace=config.target.namespace,
        ),
        bundles=planned,
        duplicate_decisions=[],
        conflict_decisions=create_conflict_decisions(planned),
        conflict_policy="update",
        conflict_prefix=None,
        failure_action="ROLLBACK",
        continue_on_error=continue_on_error,
        restore_identities=False,
        target_principals=[],
        identity_mappings=[],
        overrides={},
        overrides_sha256=None,
        config_snapshot=config.audit_snapshot(None),
        created_at="2026-08-17T00:00:00+00:00",
    ).seal()


def run_graph_restore(restore_config, tmp_path, monkeypatch, continue_on_error):
    restore_config.restore.continue_on_error = continue_on_error
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.report_directory = str(tmp_path / "reports")
    plan = build_graph_plan(restore_config, continue_on_error)
    path = tmp_path / "graph-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    GraphAssetService.calls = []
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.AssetBundleRestoreService",
        GraphAssetService,
    )
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=EmptyQuickSightInventory(),
        target_iam_client=Dummy(),
    )
    orchestrator.catalog = Catalog()
    return orchestrator.run(str(path))


def test_continue_on_error_blocks_dependents_but_runs_independent_branches(
    restore_config, tmp_path, monkeypatch
):
    report = run_graph_restore(restore_config, tmp_path, monkeypatch, continue_on_error=True)

    assert GraphAssetService.calls == ["provider.zip", "independent.zip"]
    outcomes = {job.bundle_key: job for job in report.import_jobs}
    assert outcomes["provider.zip"].outcome == "failed"
    assert outcomes["dependent.zip"].outcome == "blocked_prerequisite"
    assert outcomes["dependent.zip"].blocked_by == ["provider.zip"]
    assert outcomes["independent.zip"].outcome == "imported"
    assert report.overall_status == "partial"


def test_fail_fast_accounts_for_every_unattempted_bundle(restore_config, tmp_path, monkeypatch):
    report = run_graph_restore(restore_config, tmp_path, monkeypatch, continue_on_error=False)

    assert GraphAssetService.calls == ["provider.zip"]
    assert [job.outcome for job in report.import_jobs] == [
        "failed",
        "not_attempted_fail_fast",
        "not_attempted_fail_fast",
    ]
    assert all(job.trigger_bundle_key == "provider.zip" for job in report.import_jobs[1:])
    assert report.overall_status == "failed"


class SecondArtifactFailsCatalog(Catalog):
    def read_and_verify_bundle(self, bundle):
        self.keys.append(bundle.key)
        if len(self.keys) == 2:
            raise PlanIntegrityError("artifact changed during preflight")
        return b"verified"


def test_preflight_abort_emits_an_outcome_for_every_bundle(restore_config, tmp_path, monkeypatch):
    restore_config.restore.continue_on_error = True
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.report_directory = str(tmp_path / "reports")
    plan = build_graph_plan(restore_config, continue_on_error=True)
    path = tmp_path / "preflight-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    GraphAssetService.calls = []
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.AssetBundleRestoreService",
        GraphAssetService,
    )
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=EmptyQuickSightInventory(),
        target_iam_client=Dummy(),
    )
    orchestrator.catalog = SecondArtifactFailsCatalog()

    report = orchestrator.run(str(path))

    assert GraphAssetService.calls == []
    assert len(report.import_jobs) == 3
    assert {job.outcome for job in report.import_jobs} == {"not_attempted_precondition"}
    assert all(job.trigger_stage == "artifact_preflight" for job in report.import_jobs)
    assert "artifact changed during preflight" in report.errors[0]


def test_whole_archive_policy_skip_is_reported_as_successful_outcome(
    restore_config, tmp_path, monkeypatch
):
    restore_config.restore.conflict_policy = "skip"
    restore_config.target.identity_region = restore_config.target.asset_region
    restore_config.restore.report_directory = str(tmp_path / "reports")
    member = make_member("datasource/one.json", "datasource", "one")
    bundle = make_bundle("skip.zip", "datasources", [member], body=b"skip")
    planned = PlannedBundle(
        bucket=bundle.bucket,
        key=bundle.key,
        version_id=bundle.version_id,
        size=bundle.size,
        sha256=bundle.sha256,
        root_type=bundle.root_type,
        selected_member_names=[member.member_name],
        selected_resources=[member.resource_key],
        execution_action="skip_policy",
        import_transport="none",
    )
    plan = RestorePlan(
        manifest=make_manifest([bundle]),
        target=TargetFingerprint(
            aws_account_id=restore_config.target.aws_account_id,
            asset_region=restore_config.target.asset_region,
            identity_region=restore_config.target.identity_region,
            namespace=restore_config.target.namespace,
        ),
        bundles=[planned],
        duplicate_decisions=[],
        conflict_decisions=[
            ConflictDecision(
                resource_key="datasource/one",
                destination_resource_key="datasource/one",
                policy="skip",
                action="skip",
                target_exists=True,
            )
        ],
        conflict_policy="skip",
        conflict_prefix=None,
        failure_action="ROLLBACK",
        continue_on_error=False,
        restore_identities=False,
        target_principals=[],
        identity_mappings=[],
        overrides={},
        overrides_sha256=None,
        config_snapshot=restore_config.audit_snapshot(None),
        created_at="2026-08-17T00:00:00+00:00",
    ).seal()
    path = tmp_path / "skip-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    GraphAssetService.calls = []
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.AssetBundleRestoreService",
        GraphAssetService,
    )
    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=TargetWithNewDataSource(),
        target_iam_client=Dummy(),
    )
    orchestrator.catalog = Catalog()

    report = orchestrator.run(str(path))

    assert GraphAssetService.calls == []
    assert report.overall_status == "success"
    assert len(report.import_jobs) == 1
    assert report.import_jobs[0].outcome == "skipped_policy"
    assert report.import_jobs[0].attempted is False


class EmptyQuickSightInventory:
    def list_data_sources(self, **request):
        return {"DataSources": []}

    def list_data_sets(self, **request):
        return {"DataSetSummaries": []}


class RecordingFactory:
    def __init__(self):
        self.calls = []

    def source_client(self, service, region):
        self.calls.append(("source", service, region))
        return Dummy()

    def target_client(self, service, region):
        self.calls.append(("target", service, region))
        if service == "quicksight":
            return EmptyQuickSightInventory()
        return Dummy()


class PlanningCatalog:
    manifest = None
    clients = None

    def __init__(self, config, s3_client, dynamodb_client):
        self.__class__.clients = (s3_client, dynamodb_client)

    def build_manifest(self, **kwargs):
        return self.__class__.manifest


def test_asset_only_plan_initializes_only_s3_and_asset_quicksight(
    restore_config, tmp_path, monkeypatch
):
    member = make_member("datasource/source.json", "datasource", "source")
    bundle = make_bundle("source.zip", "datasources", [member], body=b"source")
    PlanningCatalog.manifest = make_manifest([bundle])
    PlanningCatalog.clients = None
    monkeypatch.setattr("quicksight_restore.orchestrator.LegacyBackupCatalog", PlanningCatalog)
    factory = RecordingFactory()
    orchestrator = RestoreOrchestrator(restore_config, session_factory=factory)

    plan = orchestrator.plan(str(tmp_path / "asset-plan.json"))

    assert plan.bundles
    assert factory.calls == [
        ("source", "s3", restore_config.source_backup.s3_region),
        ("target", "quicksight", restore_config.target.asset_region),
    ]
    assert PlanningCatalog.clients[0] is not None
    assert PlanningCatalog.clients[1] is None


def test_asset_only_run_never_initializes_dynamodb_identity_quicksight_or_iam(
    restore_config, tmp_path, monkeypatch
):
    restore_config.restore.continue_on_error = True
    restore_config.restore.report_directory = str(tmp_path / "reports")
    plan = build_plan(restore_config)
    path = tmp_path / "asset-run-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    FakeAssetService.calls = []
    monkeypatch.setattr(
        "quicksight_restore.orchestrator.AssetBundleRestoreService", FakeAssetService
    )
    factory = RecordingFactory()
    orchestrator = RestoreOrchestrator(restore_config, session_factory=factory)
    orchestrator.catalog = Catalog()

    orchestrator.run(str(path))

    assert factory.calls == [("target", "quicksight", restore_config.target.asset_region)]


def test_identity_source_drift_fails_before_any_target_client_is_created(restore_config, tmp_path):
    restore_config.restore.mode = "identities-only"
    restore_config.restore.restore_identities = True
    restore_config.restore.report_directory = str(tmp_path / "reports")
    mapping = IdentityMapping(
        source_principal_arn=SOURCE_USER,
        target_principal_arn=("arn:aws:quicksight:us-east-2:222222222222:user/default/source-user"),
    )
    restore_config.restore.identity_mappings = [mapping]
    plan = build_identity_plan(restore_config, mapping)
    path = tmp_path / "identity-drift-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    factory = RecordingFactory()
    orchestrator = RestoreOrchestrator(restore_config, session_factory=factory)
    orchestrator.catalog = ChangedIdentityCatalog()

    report = orchestrator.run(str(path))

    assert report.overall_status == "failed"
    assert factory.calls == []


class FailingInitialReportService:
    def __init__(self):
        self.reserved = []
        self.cancelled = []

    def reserve(self, restore_id):
        self.reserved.append(restore_id)

    def save_checkpoint(self, report, final=False):
        raise PlanIntegrityError("simulated initial checkpoint failure")

    def cancel_reservation(self, restore_id):
        self.cancelled.append(restore_id)


def test_initial_checkpoint_failure_releases_reservation_before_aws_clients(
    restore_config, tmp_path
):
    restore_config.restore.continue_on_error = True
    plan = build_plan(restore_config)
    path = tmp_path / "initial-checkpoint-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    factory = RecordingFactory()
    orchestrator = RestoreOrchestrator(restore_config, session_factory=factory)
    report_service = FailingInitialReportService()
    orchestrator.report_service = report_service

    with pytest.raises(PlanIntegrityError, match="initial checkpoint failure"):
        orchestrator.run(str(path))

    assert len(report_service.reserved) == 1
    assert report_service.cancelled == report_service.reserved
    assert factory.calls == []


def test_resealed_dependency_gate_removal_is_rejected_before_report_or_clients(
    restore_config, tmp_path
):
    restore_config.restore.continue_on_error = True
    restore_config.restore.report_directory = str(tmp_path / "dependency-reports")
    plan = build_graph_plan(restore_config, continue_on_error=True)
    dependent = next(bundle for bundle in plan.bundles if bundle.key == "dependent.zip")
    assert dependent.prerequisite_bundle_keys == ["provider.zip"]
    dependent.prerequisite_bundle_keys = []
    plan.seal()
    path = tmp_path / "resealed-dependency-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    reloaded = RestorePlanner.load_plan(str(path))

    orchestrator = RestoreOrchestrator(
        restore_config,
        source_s3_client=Dummy(),
        source_dynamodb_client=Dummy(),
        target_quicksight_client=Dummy(),
        target_iam_client=Dummy(),
    )

    with pytest.raises(
        PlanIntegrityError,
        match="prerequisites do not match.*dependency graph",
    ):
        orchestrator._verify_plan(reloaded)

    assert not Path(restore_config.restore.report_directory).exists()
