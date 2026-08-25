# Amazon Quick Sight Backup Tool - Project Structure

This document describes the current backup, restore, and local Streamlit UI architecture.

## Directory structure

```text
sample-quicksight-backup-tool/
├── quicksight_backup/
│   ├── cli.py                         # Backup CLI entry point
│   ├── orchestrator.py                # Identity + asset backup coordination
│   ├── config/                        # YAML/JSON loading and validation
│   ├── models/                        # Configuration, inventory, result, and report models
│   └── services/
│       ├── asset_bundle_backup.py     # Asset discovery, export polling, ZIP transfer to S3
│       ├── user_group_backup.py       # User/group/membership snapshots to DynamoDB
│       ├── base.py                    # Shared AWS client/service behavior
│       ├── error_handler.py           # Backup error classification
│       └── logging.py                 # Logging setup
├── quicksight_restore/
│   ├── cli.py                         # Restore plan/run/status CLI
│   ├── orchestrator.py                # Reviewed-plan execution coordination
│   ├── config/                        # Strict restore config/manifest loading
│   ├── models/                        # Plan, report, catalog, and identity models
│   ├── services/
│   │   ├── catalog.py                 # S3 bundle cataloging and integrity evidence
│   │   ├── planner.py                 # Dependency/conflict planning
│   │   ├── asset_bundle.py            # Quick Sight asset bundle imports
│   │   ├── identities.py              # Identity verification/restoration boundaries
│   │   └── report.py                  # Durable restore reports
│   ├── json_safety.py                 # Bounded JSON reading
│   ├── limits.py                      # Restore/archive safety limits
│   ├── local_paths.py                 # Local path and symlink protections
│   ├── permissions.py                 # Import permission overrides
│   └── session_factory.py             # Profile/assume-role AWS sessions
├── streamlit_app/
│   ├── app.py                         # Workspace-first local UI and operation state
│   ├── controllers/
│   │   ├── backup_controller.py       # Adapter over the existing backup orchestrator
│   │   └── restore_controller.py      # Adapter over restore preview/execution
│   └── state/
│       └── session_workspace.py       # Safe persistent/temp workspace storage
├── tests/restore/                      # Restore unit/integration-style tests
├── scripts/                            # Fixture/provisioning helpers; runtime evidence is ignored
├── docs/backup_process_flow.md         # Backup process diagrams
├── README.md                           # Installation, usage, UI, backup, and restore guide
├── pyproject.toml                      # Packaging, dependencies, and tool configuration
└── quicksight-backup.py                # Legacy backup launcher
```

## Entry points

### Backup CLI

`quicksight_backup.cli` loads a backup configuration, selects `full`, `users-only`, or `assets-only`, runs `QuickSightBackupOrchestrator`, and writes the backup manifest/report.

### Restore CLI

`quicksight_restore.cli` supports reviewed planning and execution. Restore execution consumes a sealed plan and produces a durable JSON report with import-job, identity, validation, warning, and rollback evidence.

### Streamlit UI

`streamlit_app.app` is a local-only interface bound to `127.0.0.1`. It uses the same controllers/orchestrators as the CLI rather than implementing separate backup or restore behavior.

The UI is workspace-first:

1. Open or create a local workspace.
2. Validate/run a backup or preview/execute a restore.
3. Review manifests and reports in Workspace History.
4. Reopen the persistent workspace after a new browser session.

Long operations are queued through a session-state operation guard. Interactive controls are hidden while a controller runs, progress is retained, and completion/failure results are reconstructed after normal widget reruns.

## Profile and credential boundaries

- Backup uses the named profile selected in the Backup tab.
- Restore source access uses `source_backup.auth.profile` from the restore configuration.
- Restore target access uses `target.auth.profile` from the restore configuration.
- Profiles are resolved by Boto3 from standard AWS configuration/SSO state.
- Workspaces reject credential, password, token, and invitation-URL fields.

## Workspace responsibilities

`SessionWorkspace` provides:

- persistent named workspace creation/open/rename;
- empty-only removal;
- bounded YAML/JSON/text uploads;
- sensitive-field rejection;
- safe ZIP import/export;
- browser-directory import with relative-path validation;
- workspace file classification for backup config, restore config, manifest, and overrides;
- local manifest/report discovery for History.

Internal plans and temporary files are not treated as portable user artifacts.

## Test layout

The current automated suite is concentrated under `tests/restore/` and covers configuration, path safety, catalog integrity, planning, import execution, identity behavior, reports, CLI behavior, and session creation. Streamlit behavior is additionally exercised with targeted `streamlit.testing.v1.AppTest` smoke/interaction checks during development.

## Related documentation

- [README](README.md)
- [Backup process flow](docs/backup_process_flow.md)
- [Contributing](CONTRIBUTING.md)
