import pytest
import yaml

from quicksight_restore.config.loader import RestoreConfigLoader
from quicksight_restore.models.errors import RestoreConfigurationError


def base_config(tmp_path):
    return {
        "source_backup": {
            "s3_bucket_name": "backup-bucket",
            "s3_prefix": "quicksight-backups",
            "backup_date": "2026-08-17",
            "s3_region": "us-east-1",
            "dynamodb_region": "us-west-2",
            "auth": {"profile": "source", "role_arn": "arn:aws:iam::111111111111:role/ReadBackup"},
        },
        "target": {
            "aws_account_id": "222222222222",
            "asset_region": "us-east-2",
            "identity_region": "us-east-1",
            "namespace": "default",
            "auth": {"profile": "target", "role_arn": "arn:aws:iam::222222222222:role/Restore"},
        },
        "restore": {
            "mode": "assets-only",
            "restore_identities": False,
            "failure_action": "ROLLBACK",
            "conflict_policy": "update",
            "continue_on_error": False,
            "poll_timeout_seconds": 30,
            "report_directory": str(tmp_path / "reports"),
            "target_principals": ["arn:aws:quicksight:us-east-1:222222222222:group/default/admins"],
        },
    }


def write_config(tmp_path, value):
    path = tmp_path / "restore.yaml"
    path.write_text(yaml.safe_dump(value), encoding="utf-8")
    return path


def test_loader_models_regions_and_non_secret_auth_separately(tmp_path):
    path = write_config(tmp_path, base_config(tmp_path))
    config = RestoreConfigLoader().load(str(path))
    assert config.source_backup.s3_region == "us-east-1"
    assert config.source_backup.dynamodb_region == "us-west-2"
    assert config.target.asset_region == "us-east-2"
    assert config.target.identity_region == "us-east-1"
    assert config.source_backup.auth.profile == "source"
    assert config.target.auth.role_arn.endswith(":role/Restore")


@pytest.mark.parametrize(
    "mutator, expected",
    [
        (lambda value: value["target"].update(aws_account_id="123"), "12-digit"),
        (lambda value: value["source_backup"].update(backup_date="08/17/2026"), "YYYY-MM-DD"),
        (lambda value: value["restore"].update(failure_action="IGNORE"), "failure_action"),
        (lambda value: value["restore"].update(conflict_policy="overwrite"), "conflict_policy"),
        (lambda value: value["restore"].update(poll_timeout_seconds=0), "poll_timeout_seconds"),
        (
            lambda value: value["restore"].update(
                target_principals=[
                    "arn:aws:quicksight:us-east-1:111111111111:user/default/source-user"
                ]
            ),
            "different AWS account",
        ),
    ],
)
def test_validation_rejects_invalid_restore_values(tmp_path, mutator, expected):
    value = base_config(tmp_path)
    mutator(value)
    with pytest.raises(RestoreConfigurationError, match=expected):
        RestoreConfigLoader().load(str(write_config(tmp_path, value)))


def test_loader_requires_explicit_selection_but_accepts_cli_override(tmp_path):
    value = base_config(tmp_path)
    value["source_backup"].pop("backup_date")
    path = write_config(tmp_path, value)
    with pytest.raises(RestoreConfigurationError, match="backup_date or explicit bundle_keys"):
        RestoreConfigLoader().load(str(path))
    loaded = RestoreConfigLoader().load(str(path), backup_date="2026-08-17")
    assert loaded.source_backup.backup_date == "2026-08-17"


def test_loader_rejects_plaintext_credentials(tmp_path):
    value = base_config(tmp_path)
    value["target"]["auth"]["password"] = "do-not-store-this"
    with pytest.raises(RestoreConfigurationError, match="Plaintext credentials"):
        RestoreConfigLoader().load(str(write_config(tmp_path, value)))


def test_prefix_policy_requires_safe_string(tmp_path):
    value = base_config(tmp_path)
    value["restore"].update(conflict_policy="prefix", conflict_prefix="../bad")
    with pytest.raises(RestoreConfigurationError, match="conflict_prefix"):
        RestoreConfigLoader().load(str(write_config(tmp_path, value)))


def test_loader_rejects_direct_configuration_symlink(tmp_path):
    target = write_config(tmp_path, base_config(tmp_path))
    link = tmp_path / "linked-restore.yaml"
    try:
        link.symlink_to(target)
    except OSError as error:
        pytest.skip("symbolic links are unavailable: {0}".format(error))

    with pytest.raises(RestoreConfigurationError, match="symbolic links|reparse"):
        RestoreConfigLoader().load(str(link))


def test_loader_enforces_config_size_on_the_open_descriptor(tmp_path, monkeypatch):
    import quicksight_restore.config.loader as loader_module

    monkeypatch.setattr(loader_module, "MAX_CONFIG_BYTES", 64)
    path = tmp_path / "oversized.yaml"
    path.write_bytes(b"x" * 65)

    with pytest.raises(RestoreConfigurationError, match="size limit"):
        RestoreConfigLoader().load(str(path))


def test_loader_enforces_overrides_size_before_planning(tmp_path, monkeypatch):
    import quicksight_restore.config.loader as loader_module

    monkeypatch.setattr(loader_module, "MAX_OVERRIDES_BYTES", 32)
    overrides = tmp_path / "overrides.json"
    overrides.write_text('{"OverrideTags":{"value":"' + "x" * 40 + '"}}', encoding="utf-8")
    value = base_config(tmp_path)
    value["restore"]["overrides_file"] = str(overrides)

    with pytest.raises(RestoreConfigurationError, match="size limit"):
        RestoreConfigLoader().load(str(write_config(tmp_path, value)))
