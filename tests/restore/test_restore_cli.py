from types import SimpleNamespace

import pytest

from quicksight_restore import cli as restore_cli
from quicksight_restore.models.errors import (
    RestoreConfigurationError,
    RestoreExecutionError,
)


class Loader:
    config = SimpleNamespace(restore=SimpleNamespace(report_directory="configured-reports"))
    error = None
    calls = []

    def load(self, *args, **kwargs):
        self.__class__.calls.append((args, kwargs))
        if self.error:
            raise self.error
        return self.config


class Orchestrator:
    status_value = "success"
    instances = 0

    def __init__(self, config):
        self.config = config
        self.__class__.instances += 1

    def plan(self, **kwargs):
        return SimpleNamespace(plan_id="plan-id", plan_digest="d" * 64, bundles=[object()])

    def run(self, path):
        value = self.status_value
        return SimpleNamespace(
            overall_status=value,
            to_dict=lambda: {"overall_status": value},
        )


class ReportService:
    status_value = "success"
    error = None
    directories = []

    def __init__(self, directory):
        self.directory = directory
        self.__class__.directories.append(directory)

    def load(self, restore_id):
        if self.error:
            raise self.error
        value = self.status_value
        return SimpleNamespace(
            overall_status=value,
            to_dict=lambda: {
                "restore_id": restore_id,
                "overall_status": value,
            },
        )


def patch_cli(monkeypatch):
    monkeypatch.setattr(restore_cli, "RestoreConfigLoader", Loader)
    monkeypatch.setattr(restore_cli, "RestoreOrchestrator", Orchestrator)
    monkeypatch.setattr(restore_cli, "RestoreReportService", ReportService)
    Loader.error = None
    Loader.calls = []
    Orchestrator.instances = 0
    Orchestrator.status_value = "success"
    ReportService.error = None
    ReportService.directories = []
    ReportService.status_value = "success"


def test_cli_plan_and_config_backed_status_exit_zero(monkeypatch, capsys):
    patch_cli(monkeypatch)
    assert (
        restore_cli.main(["plan", "--config", "restore.yaml", "--backup-date", "2026-08-17"]) == 0
    )
    assert '"read_only": true' in capsys.readouterr().out
    assert (
        restore_cli.main(
            [
                "status",
                "--config",
                "restore.yaml",
                "--restore-id",
                "restore-1",
            ]
        )
        == 0
    )
    assert '"restore_id": "restore-1"' in capsys.readouterr().out
    assert ReportService.directories[-1] == "configured-reports"


def test_direct_report_directory_status_is_local_only(monkeypatch, capsys):
    patch_cli(monkeypatch)
    Loader.error = AssertionError("configuration must not be loaded")

    assert (
        restore_cli.main(
            [
                "status",
                "--report-directory",
                "local-reports",
                "--restore-id",
                "restore-1",
            ]
        )
        == 0
    )
    assert Loader.calls == []
    assert Orchestrator.instances == 0
    assert ReportService.directories == ["local-reports"]
    assert '"overall_status": "success"' in capsys.readouterr().out


@pytest.mark.parametrize(
    "status, code",
    [("success", 0), ("partial", 2), ("failed", 1)],
)
def test_cli_run_exit_codes(monkeypatch, status, code):
    patch_cli(monkeypatch)
    Orchestrator.status_value = status
    assert restore_cli.main(["run", "--config", "restore.yaml", "--plan", "plan.json"]) == code


@pytest.mark.parametrize(
    "status, code",
    [("success", 0), ("failed", 1), ("partial", 2), ("running", 3)],
)
def test_cli_status_exit_codes(monkeypatch, status, code):
    patch_cli(monkeypatch)
    ReportService.status_value = status

    assert (
        restore_cli.main(
            [
                "status",
                "--report-directory",
                "reports",
                "--restore-id",
                "restore-1",
            ]
        )
        == code
    )


def test_cli_status_read_failure_has_distinct_exit_code(monkeypatch, capsys):
    patch_cli(monkeypatch)
    ReportService.error = RestoreExecutionError("report digest is invalid")

    assert (
        restore_cli.main(
            [
                "status",
                "--report-directory",
                "reports",
                "--restore-id",
                "restore-1",
            ]
        )
        == 4
    )
    assert "report digest is invalid" in capsys.readouterr().err


def test_status_requires_exactly_one_local_source():
    parser = restore_cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["status", "--restore-id", "restore-1"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "status",
                "--config",
                "restore.yaml",
                "--report-directory",
                "reports",
                "--restore-id",
                "restore-1",
            ]
        )


def test_cli_validation_failure_is_nonzero(monkeypatch, capsys):
    patch_cli(monkeypatch)
    Loader.error = RestoreConfigurationError("bad restore config")
    assert restore_cli.main(["plan", "--config", "bad.yaml"]) == 2
    assert "bad restore config" in capsys.readouterr().err


def test_cli_run_replays_plan_selection_overrides(monkeypatch):
    patch_cli(monkeypatch)
    selection = [
        "quicksight-backups/2026/08/17/datasources/datasources_bundle_1-010101.zip",
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_1-010102.zip",
    ]
    plan_arguments = [
        "plan",
        "--config",
        "restore.yaml",
        "--backup-date",
        "2026-08-17",
        "--output",
        "plan.json",
    ]
    run_arguments = [
        "run",
        "--config",
        "restore.yaml",
        "--plan",
        "plan.json",
        "--backup-date",
        "2026-08-17",
    ]
    for key in selection:
        plan_arguments.extend(["--bundle-key", key])
        run_arguments.extend(["--bundle-key", key])

    assert restore_cli.main(plan_arguments) == 0
    assert restore_cli.main(run_arguments) == 0

    expected = {
        "backup_date": "2026-08-17",
        "bundle_keys": selection,
    }
    assert Loader.calls == [
        (("restore.yaml",), expected),
        (("restore.yaml",), expected),
    ]
