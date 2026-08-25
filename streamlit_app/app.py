"""Local-only Streamlit UI for Quick Sight backup and restore workflows."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any
import json
import uuid

import boto3
import streamlit as st

from streamlit_app.controllers.backup_controller import BackupController
from streamlit_app.controllers.restore_controller import RestoreController, RestorePreview
from streamlit_app.state.session_workspace import SessionWorkspace

st.set_page_config(
    page_title="Quick Sight Backup & Restore",
    page_icon="💾",
    layout="wide",
)


def _workspace() -> SessionWorkspace:
    if "ui_session_id" not in st.session_state:
        st.session_state.ui_session_id = uuid.uuid4().hex
    return SessionWorkspace(st.session_state.ui_session_id)


def _profiles() -> list[str]:
    return sorted(boto3.Session().available_profiles)


def _save_uploads(workspace: SessionWorkspace, *uploads: Any) -> list[Path]:
    paths: list[Path] = []
    for upload in uploads:
        if upload is not None:
            paths.append(workspace.save_upload(upload.name, upload.getvalue()))
    return paths


def _selection_digest(*uploads: Any) -> str:
    digest = sha256()
    for upload in uploads:
        if upload is None:
            digest.update(b"<none>")
        else:
            digest.update(upload.name.encode("utf-8"))
            digest.update(upload.getvalue())
    return digest.hexdigest()


def _backup_rows(counts: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    return [
        {
            "Type": kind.replace("_", " ").title(),
            "Successful": values.get("successful", 0),
            "Failed": values.get("failed", 0),
            "Skipped": values.get("skipped", 0),
        }
        for kind, values in counts.items()
    ]


def _restore_rows(counts: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    return [
        {
            "Type": kind.replace("_", " ").title(),
            "Successful": values.get("successful", 0),
            "Failed": values.get("failed", 0),
            "Skipped": values.get("skipped", 0),
            "Not attempted": values.get("not_attempted", 0),
            "Pending": values.get("pending", 0),
        }
        for kind, values in counts.items()
    ]


def _preview_rows(preview: RestorePreview) -> list[dict[str, Any]]:
    combined = dict(preview.resource_counts)
    for kind, count in preview.identity_counts.items():
        combined[kind] = count
    return [
        {"Type": kind.replace("_", " ").title(), "Selected": count}
        for kind, count in combined.items()
    ]


def _download(path: Path, label: str, key: str) -> None:
    if path.exists():
        mime = "application/json" if path.suffix.lower() == ".json" else "text/plain"
        st.download_button(
            label,
            data=path.read_bytes(),
            file_name=path.name,
            mime=mime,
            key=key,
        )


def _run_status(label: str):
    status = st.status(label, expanded=True)

    def progress(message: str) -> None:
        status.write(message)

    return status, progress


workspace = _workspace()
profiles = _profiles()

st.title("Quick Sight Backup & Restore")
st.caption(
    "Local-only administrative UI. Credentials come from named AWS profiles; access keys, "
    "passwords, and native-user invitation URLs are never accepted or displayed."
)
st.info(
    "Run the app on `127.0.0.1`. Restore execution requires a successful read-only preview "
    "and explicit typed confirmation."
)

backup_tab, restore_tab, history_tab = st.tabs(["Backup", "Restore", "History"])

with backup_tab:
    st.subheader("Create backup")
    if not profiles:
        st.error("No named AWS profiles are available on this machine.")
    backup_config = st.file_uploader(
        "Backup YAML or JSON configuration",
        type=["yaml", "yml", "json"],
        key="backup_config_upload",
    )
    col_profile, col_mode = st.columns(2)
    with col_profile:
        backup_profile = st.selectbox(
            "AWS profile",
            options=profiles,
            index=None,
            placeholder="Select a named profile",
            key="backup_profile",
        )
    with col_mode:
        backup_mode = st.selectbox(
            "Backup mode",
            options=["full", "users-only", "assets-only"],
            key="backup_mode",
        )
    dry_col, run_col = st.columns(2)
    backup_ready = backup_config is not None and bool(backup_profile)
    with dry_col:
        backup_dry_run = st.button(
            "Validate backup",
            disabled=not backup_ready,
            use_container_width=True,
        )
    with run_col:
        backup_execute = st.button(
            "Run backup",
            type="primary",
            disabled=not backup_ready,
            use_container_width=True,
        )

    if backup_dry_run or backup_execute:
        status = None
        try:
            config_path = workspace.save_upload(backup_config.name, backup_config.getvalue())
            status, progress = _run_status(
                "Validating backup" if backup_dry_run else "Running backup"
            )
            controller = BackupController(progress)
            if backup_dry_run:
                controller.dry_run(config_path, backup_profile, backup_mode)
                status.update(label="Backup validation passed", state="complete")
                st.success("Configuration, credentials, connectivity, and prerequisites passed.")
            else:
                output_directory = workspace.new_backup_directory()
                execution = controller.run(
                    config_path,
                    backup_profile,
                    backup_mode,
                    output_directory,
                )
                status.update(
                    label="Backup completed",
                    state="complete" if not execution.report.has_failures else "error",
                )
                st.dataframe(
                    _backup_rows(execution.report.resource_counts),
                    use_container_width=True,
                    hide_index=True,
                )
                totals = execution.report.resource_totals
                st.metric("Successful resources", totals.get("successful", 0))
                if execution.report.has_failures:
                    st.error("Backup completed with failures. Review the report before restore.")
                else:
                    st.success("Backup completed without failures.")
                _download(execution.manifest_path, "Download restore manifest", "backup-manifest")
                _download(execution.report_path, "Download backup report", "backup-report")
                st.session_state.latest_backup_manifest = str(execution.manifest_path)
        except Exception as error:
            if status is not None:
                status.update(label="Backup failed", state="error")
            st.exception(error)

with restore_tab:
    st.subheader("Restore from manifest")
    manifest_upload = st.file_uploader(
        "Backup manifest",
        type=["json"],
        key="restore_manifest_upload",
    )
    restore_config = st.file_uploader(
        "Target restore YAML or JSON configuration",
        type=["yaml", "yml", "json"],
        key="restore_config_upload",
    )
    overrides_upload = st.file_uploader(
        "Optional overrides JSON referenced by the configuration",
        type=["json"],
        key="restore_overrides_upload",
    )
    selection = _selection_digest(manifest_upload, restore_config, overrides_upload)
    if st.session_state.get("restore_selection") != selection:
        st.session_state.pop("restore_preview", None)
        st.session_state.pop("restore_paths", None)
        st.session_state.pop("restore_report", None)
        st.session_state.restore_selection = selection

    restore_ready = manifest_upload is not None and restore_config is not None
    if st.button(
        "Preview restore (read-only)",
        disabled=not restore_ready,
        use_container_width=True,
    ):
        status = None
        try:
            uploads = [
                upload
                for upload in (manifest_upload, restore_config, overrides_upload)
                if upload is not None
            ]
            names = [upload.name for upload in uploads]
            if len(names) != len(set(names)):
                raise ValueError(
                    "manifest, config, and overrides uploads must have unique filenames"
                )
            _save_uploads(workspace, overrides_upload)
            manifest_path = workspace.save_upload(manifest_upload.name, manifest_upload.getvalue())
            config_path = workspace.save_upload(restore_config.name, restore_config.getvalue())
            plan_path = workspace.new_plan_path(config_path)
            status, progress = _run_status("Building read-only restore preview")
            preview = RestoreController(progress).preview(
                config_path,
                manifest_path,
                plan_path,
            )
            status.update(label="Restore preview ready", state="complete")
            st.session_state.restore_preview = preview.to_dict()
            st.session_state.restore_paths = {
                "config": str(config_path),
                "manifest": str(manifest_path),
                "plan": str(plan_path),
            }
        except Exception as error:
            if status is not None:
                status.update(label="Restore preview failed", state="error")
            st.exception(error)

    preview_value = st.session_state.get("restore_preview")
    if preview_value:
        preview = RestorePreview.from_dict(preview_value)
        st.markdown("#### Reviewed plan")
        metrics = st.columns(4)
        metrics[0].metric("Target account", preview.target_account_id)
        metrics[1].metric("Target Region", preview.target_region)
        metrics[2].metric("Bundles", preview.bundle_count)
        metrics[3].metric("Existing conflicts", preview.existing_conflicts)
        if preview.action_counts:
            st.write(
                "Target actions: "
                + ", ".join(
                    "{0}: {1}".format(action.replace("_", " ").title(), count)
                    for action, count in preview.action_counts.items()
                )
            )
        st.dataframe(_preview_rows(preview), use_container_width=True, hide_index=True)
        if preview.warnings:
            with st.expander("Reviewed warnings", expanded=True):
                for warning in preview.warnings:
                    st.write("- " + warning)

        confirmation = st.text_input(
            "Type RESTORE to execute this reviewed plan",
            key="restore_confirmation",
        )
        execute_restore = st.button(
            "Execute restore",
            type="primary",
            disabled=confirmation != "RESTORE",
            use_container_width=True,
        )
        if execute_restore:
            status = None
            try:
                paths = st.session_state.restore_paths
                status, progress = _run_status("Executing restore")
                execution = RestoreController(progress).execute(
                    Path(paths["config"]),
                    Path(paths["manifest"]),
                    Path(paths["plan"]),
                )
                success = execution.report.overall_status == "success"
                status.update(
                    label="Restore {0}".format(execution.report.overall_status),
                    state="complete" if success else "error",
                )
                st.dataframe(
                    _restore_rows(execution.report.summary.get("resource_counts", {})),
                    use_container_width=True,
                    hide_index=True,
                )
                if success:
                    st.success("Restore completed successfully.")
                else:
                    st.error(
                        "Restore ended as {0}. Review the report before retrying.".format(
                            execution.report.overall_status
                        )
                    )
                if execution.report.identity_result:
                    registered = sum(
                        1
                        for item in execution.report.identity_result.results
                        if item.identity_kind == "user" and item.action == "registered"
                    )
                    if registered:
                        st.warning(
                            "{0} user(s) were registered. Quick-native users remain inactive "
                            "until an administrator uses Manage Quick > Manage users > Resend "
                            "invitation and each user signs in once.".format(registered)
                        )
                _download(execution.report_path, "Download restore report", "restore-report")
                st.session_state.restore_report = str(execution.report_path)
            except Exception as error:
                if status is not None:
                    status.update(label="Restore failed", state="error")
                st.exception(error)

with history_tab:
    st.subheader("Local session history")
    st.caption("This tab reads local session files only and does not initialize AWS clients.")
    artifacts = workspace.artifacts()
    if not artifacts:
        st.info("No manifests or reports have been generated in this UI session.")
    for index, artifact in enumerate(artifacts[:50]):
        with st.expander("{0}: {1}".format(artifact.kind, artifact.label)):
            st.write("Size: {0:,} bytes".format(artifact.size))
            if artifact.path.suffix.lower() == ".json":
                try:
                    value = json.loads(artifact.path.read_text(encoding="utf-8"))
                    summary = value.get("summary") or value.get("resource_totals")
                    if summary:
                        st.json(summary)
                except (OSError, ValueError):
                    st.caption("JSON preview unavailable.")
            _download(
                artifact.path,
                "Download {0}".format(artifact.path.name),
                "history-{0}".format(index),
            )

st.divider()
st.caption("Session workspace: {0}".format(workspace.root))
