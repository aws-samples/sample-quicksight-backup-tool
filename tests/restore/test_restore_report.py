import json

import pytest

from quicksight_restore.models.contracts import (
    ImportJobResult,
    RestoreReport,
    TargetFingerprint,
    sha256_json,
)
from quicksight_restore.models.errors import RestoreExecutionError
from quicksight_restore.services.report import RestoreReportService


def report():
    plan_digest = "a" * 64
    config_snapshot = {}
    config_sha256 = sha256_json(config_snapshot)
    return RestoreReport(
        restore_id="20260817T010101Z-abcdef123456",
        plan_id="plan-{0}".format(plan_digest[:20]),
        plan_digest=plan_digest,
        overall_status="success",
        target=TargetFingerprint(
            aws_account_id="222222222222",
            asset_region="us-east-2",
            identity_region="us-east-1",
            namespace="default",
        ),
        selected_backup={
            "bundles": [
                {
                    "bucket": "source-bucket",
                    "key": "one.zip",
                    "version_id": "version-1",
                    "size": 128,
                    "sha256": "b" * 64,
                    "root_type": "dashboard",
                }
            ]
        },
        started_at="2026-08-17T01:01:01+00:00",
        completed_at="2026-08-17T01:01:02+00:00",
        duration_seconds=1.0,
        identity_result=None,
        import_jobs=[
            ImportJobResult(
                bundle_key="one.zip",
                job_id="job-one",
                status="success",
                terminal_status="SUCCESSFUL",
                selected_member_count=1,
                started_at="2026-08-17T01:01:01+00:00",
                completed_at="2026-08-17T01:01:02+00:00",
                duration_seconds=1.0,
                outcome="imported",
                attempted=True,
            )
        ],
        warnings=[],
        errors=[],
        plan_evidence={
            "artifact_digest": "c" * 64,
            "config_snapshot": config_snapshot,
            "config_sha256": config_sha256,
            "bundles": [
                {
                    "key": "one.zip",
                    "order": 0,
                    "execution_action": "import",
                    "import_transport": "inline_body",
                    "selected_member_names": ["assets/dashboard/dashboard-1.json"],
                    "selected_resources": ["dashboard/dashboard-1"],
                    "omitted_member_names": [],
                    "prerequisite_bundle_keys": [],
                }
            ],
        },
        validation_results=[
            {
                "name": "source_artifact_preflight",
                "status": "passed",
                "details": (
                    "Every selected version, size, checksum, and member inventory " "was reverified"
                ),
            },
            {
                "name": "identity_source_preflight",
                "status": "not_run",
                "details": "Identity restore was not selected",
            },
            {
                "name": "target_state_preflight",
                "status": "passed",
                "details": ("Target conflict observations still match the reviewed plan"),
            },
            {
                "name": "post_restore_workload_validation",
                "status": "not_run",
                "details": (
                    "Representative workload validation is operator-owned and was not "
                    "run by this tool"
                ),
            },
        ],
        validation_status="not_run",
        config_sha256=config_sha256,
    ).seal()


def test_report_requires_plan_id_derived_from_plan_digest():
    invalid = report()
    invalid.plan_id = "plan-" + "0" * 20

    with pytest.raises(ValueError, match="plan_id does not match plan_digest"):
        invalid.seal()


@pytest.mark.parametrize("artifact_digest", [None, "not-a-digest", "C" * 64])
def test_report_requires_valid_artifact_digest_evidence(artifact_digest):
    invalid = report()
    if artifact_digest is None:
        invalid.plan_evidence.pop("artifact_digest")
    else:
        invalid.plan_evidence["artifact_digest"] = artifact_digest

    with pytest.raises(ValueError, match="plan_evidence.artifact_digest"):
        invalid.seal()


def test_report_is_persisted_atomically_and_loaded_by_restore_id(tmp_path):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    path = service.save(expected)
    assert path.name == "restore-20260817T010101Z-abcdef123456.json"
    assert service.load(expected.restore_id).to_dict() == expected.to_dict()
    assert not list(tmp_path.glob("*.tmp"))


def test_report_rejects_path_traversal_and_missing_id(tmp_path):
    service = RestoreReportService(str(tmp_path))
    with pytest.raises(RestoreExecutionError, match="Invalid restore ID"):
        service.load("../escape")
    with pytest.raises(RestoreExecutionError, match="not found"):
        service.load("missing")


def test_report_digest_is_verified_before_default_normalization(tmp_path):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    path = service.save(expected)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.pop("rollback_scope")
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(RestoreExecutionError, match="raw digest verification failed"):
        service.load(expected.restore_id)


def test_report_digest_allows_only_serialization_reformatting(tmp_path):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    path = service.save(expected)
    raw = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps(raw, separators=(",", ":")), encoding="utf-8")

    assert service.load(expected.restore_id).report_digest == expected.report_digest


def test_report_file_name_must_match_embedded_restore_id(tmp_path):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    source = service.save(expected)
    other_id = "20260817T010101Z-fedcba654321"
    service.path_for(other_id).write_bytes(source.read_bytes())

    with pytest.raises(RestoreExecutionError, match="does not match its file name"):
        service.load(other_id)


def test_report_checkpoint_state_must_match_final_flag(tmp_path):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    service.reserve(expected.restore_id)

    with pytest.raises(RestoreExecutionError, match="final flag"):
        service.save_checkpoint(expected, final=False)
    service.cancel_reservation(expected.restore_id)
    assert not service._reservation_path(expected.restore_id).exists()


def test_report_reservation_token_is_verified_before_writing(tmp_path):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    service.reserve(expected.restore_id)
    reservation = service._reservation_path(expected.restore_id)
    raw = json.loads(reservation.read_text(encoding="utf-8"))
    raw["token"] = "0" * 64
    reservation.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(RestoreExecutionError, match="not owned"):
        service.save_checkpoint(expected, final=True)
    assert not service.path_for(expected.restore_id).exists()


def test_running_checkpoint_transitions_to_terminal_and_releases_owner(tmp_path):
    service = RestoreReportService(str(tmp_path))
    running = report()
    running.overall_status = "running"
    running.report_digest = ""
    service.reserve(running.restore_id)
    path = service.save_checkpoint(running, final=False)
    assert service.load(running.restore_id).overall_status == "running"
    assert service._reservation_path(running.restore_id).exists()

    terminal = report()
    service.save_checkpoint(terminal, final=True)
    assert path == service.path_for(terminal.restore_id)
    assert service.load(terminal.restore_id).overall_status == "success"
    assert not service._reservation_path(terminal.restore_id).exists()


def test_one_shot_validation_failure_releases_unused_reservation(tmp_path):
    service = RestoreReportService(str(tmp_path))
    invalid = report()
    invalid.duration_seconds = float("nan")

    with pytest.raises(RestoreExecutionError, match="Invalid restore report"):
        service.save(invalid)
    assert not service.path_for(invalid.restore_id).exists()
    assert not service._reservation_path(invalid.restore_id).exists()


def test_existing_final_report_is_never_overwritten(tmp_path):
    first = RestoreReportService(str(tmp_path))
    expected = report()
    path = first.save(expected)
    original = path.read_bytes()

    with pytest.raises(RestoreExecutionError, match="already exists"):
        RestoreReportService(str(tmp_path)).save(report())
    assert path.read_bytes() == original


def test_reservation_rechecks_destination_after_winning_sidecar(tmp_path, monkeypatch):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    destination = service.path_for(expected.restore_id)
    original_lexists = service._lexists
    checks = {"destination": 0}

    def destination_appears(path):
        if path == destination:
            checks["destination"] += 1
            if checks["destination"] == 2:
                destination.write_bytes(b"foreign-report")
                return True
        return original_lexists(path)

    monkeypatch.setattr(service, "_lexists", destination_appears)
    with pytest.raises(RestoreExecutionError, match="appeared while"):
        service.reserve(expected.restore_id)
    assert destination.read_bytes() == b"foreign-report"
    assert not service._reservation_path(expected.restore_id).exists()


def test_terminal_report_survives_visible_reservation_cleanup_failure(tmp_path, monkeypatch):
    service = RestoreReportService(str(tmp_path))
    expected = report()
    service.reserve(expected.restore_id)

    def fail_cleanup(restore_id, record):
        raise RestoreExecutionError("simulated cleanup failure")

    monkeypatch.setattr(service, "_remove_owned_reservation", fail_cleanup)
    with pytest.raises(RestoreExecutionError, match="committed.*cleanup failed"):
        service.save_checkpoint(expected, final=True)

    assert service.load(expected.restore_id).overall_status == "success"
    assert expected.restore_id in service._reservations


def _sharing_violation(winerror=32):
    error = PermissionError(13, "simulated Windows sharing violation")
    error.winerror = winerror
    return error


def _save_running_checkpoint(service):
    running = report()
    running.overall_status = "running"
    running.report_digest = ""
    service.reserve(running.restore_id)
    service.save_checkpoint(running, final=False)
    return running


def test_checkpoint_replace_retries_windows_sharing_violations(tmp_path, monkeypatch):
    import time
    import quicksight_restore.services.report as report_module

    service = RestoreReportService(str(tmp_path))
    _save_running_checkpoint(service)
    original_replace = report_module.os.replace
    attempts = {"count": 0}

    def flaky_replace(source, destination):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise _sharing_violation()
        return original_replace(source, destination)

    monkeypatch.setattr(report_module.os, "replace", flaky_replace)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    terminal = report()
    service.save_checkpoint(terminal, final=True)

    assert attempts["count"] == 3
    assert service.load(terminal.restore_id).overall_status == "success"


def test_checkpoint_replace_stops_after_bounded_sharing_retries(tmp_path, monkeypatch):
    import time
    import quicksight_restore.services.report as report_module

    service = RestoreReportService(str(tmp_path))
    running = _save_running_checkpoint(service)
    attempts = {"count": 0}

    def always_blocked(source, destination):
        attempts["count"] += 1
        raise _sharing_violation(5)

    monkeypatch.setattr(report_module.os, "replace", always_blocked)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)

    with pytest.raises(RestoreExecutionError, match="Unable to persist"):
        service.save_checkpoint(report(), final=True)

    assert attempts["count"] == 5
    assert service.load(running.restore_id).overall_status == "running"


def test_checkpoint_retry_revalidates_prior_digest_before_replacing(tmp_path, monkeypatch):
    import time
    import quicksight_restore.services.report as report_module

    service = RestoreReportService(str(tmp_path))
    running = _save_running_checkpoint(service)
    destination = service.path_for(running.restore_id)
    foreign = report()
    foreign.overall_status = "running"
    foreign.warnings = ["foreign checkpoint"]
    foreign.report_digest = ""
    foreign.seal()
    foreign_bytes = (
        json.dumps(
            foreign.to_dict(),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    attempts = {"count": 0}

    def collide_once(source, target):
        attempts["count"] += 1
        destination.write_bytes(foreign_bytes)
        raise _sharing_violation(33)

    monkeypatch.setattr(report_module.os, "replace", collide_once)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)

    with pytest.raises(RestoreExecutionError, match="checkpoint changed outside"):
        service.save_checkpoint(report(), final=True)

    assert attempts["count"] == 1
    assert destination.read_bytes() == foreign_bytes
