"""Safe, session-scoped local storage for uploaded and generated UI artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Optional
import json
import re
import tempfile
import uuid
import zipfile

import yaml

_SESSION_RE = re.compile(r"^[a-f0-9]{32}$")
_SAFE_SUFFIXES = {".json", ".yaml", ".yml", ".txt"}
_MAX_UPLOAD_BYTES = 16 * 1024 * 1024
_MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
_MAX_ARCHIVE_ENTRIES = 500
_MAX_ARCHIVE_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
_WORKSPACE_SCHEMA_VERSION = "1.0"
_FORBIDDEN_UPLOAD_KEYS = {
    "accesskeyid",
    "awsaccesskeyid",
    "secretaccesskey",
    "awssecretaccesskey",
    "sessiontoken",
    "awssessiontoken",
    "password",
    "credentialpair",
    "userinvitationurl",
}


@dataclass(frozen=True)
class LocalArtifact:
    """A generated file that can be displayed or downloaded by the UI."""

    path: Path
    label: str
    kind: str
    size: int


class SessionWorkspace:
    """Own files beneath one random directory in the operating-system temp area."""

    def __init__(self, session_id: str, base_directory: Optional[Path] = None):
        if not _SESSION_RE.fullmatch(session_id):
            raise ValueError("session_id must be a 32-character lowercase hexadecimal value")
        base = base_directory or Path(tempfile.gettempdir()) / "quicksight-backup-tool-ui"
        self.root = (base / session_id).resolve()
        self.inputs = self.root / "inputs"
        self.backups = self.root / "backup-runs"
        for directory in (self.inputs, self.backups):
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def create(cls) -> "SessionWorkspace":
        return cls(uuid.uuid4().hex)

    @property
    def session_id(self) -> str:
        return self.root.name

    @staticmethod
    def _canonical_key(value: Any) -> str:
        return re.sub(r"[^a-z0-9]", "", str(value).lower())

    @classmethod
    def _reject_sensitive_upload(cls, name: str, content: bytes) -> None:
        suffix = Path(name).suffix.lower()
        if suffix not in (".json", ".yaml", ".yml"):
            return
        try:
            text = content.decode("utf-8")
            value = json.loads(text) if suffix == ".json" else yaml.safe_load(text)
        except (UnicodeDecodeError, ValueError, yaml.YAMLError):
            # The backend's strict loader will report malformed content. A conservative
            # text scan still prevents obvious credential fields from reaching disk.
            rendered = content.decode("utf-8", errors="ignore")
            for line in rendered.splitlines():
                key = re.split(r"[:=]", line, maxsplit=1)[0]
                if cls._canonical_key(key) in _FORBIDDEN_UPLOAD_KEYS:
                    raise ValueError("uploaded files must not contain credentials or passwords")
            return

        def visit(item: Any) -> None:
            if isinstance(item, Mapping):
                for key, child in item.items():
                    if cls._canonical_key(key) in _FORBIDDEN_UPLOAD_KEYS:
                        raise ValueError(
                            "uploaded files must not contain credentials, passwords, or invitation URLs"
                        )
                    visit(child)
            elif isinstance(item, list):
                for child in item:
                    visit(child)

        visit(value)

    @classmethod
    def editable_json_text(cls, original_name: str, content: bytes) -> str:
        """Safely normalize an uploaded YAML/JSON object for inline JSON editing."""
        name = Path(original_name).name
        suffix = Path(name).suffix.lower()
        if suffix not in (".json", ".yaml", ".yml"):
            raise ValueError("only YAML or JSON files can be edited inline")
        if len(content) > _MAX_UPLOAD_BYTES:
            raise ValueError("uploaded file exceeds the 16 MiB UI limit")
        cls._reject_sensitive_upload(name, content)
        try:
            text = content.decode("utf-8")
            value = json.loads(text) if suffix == ".json" else yaml.safe_load(text)
        except (UnicodeDecodeError, ValueError, yaml.YAMLError) as error:
            raise ValueError("unable to parse uploaded configuration: {0}".format(error)) from error
        if not isinstance(value, Mapping):
            raise ValueError("inline-edited configuration must have a JSON object root")
        return json.dumps(value, indent=2, default=str)

    def save_upload(self, original_name: str, content: bytes) -> Path:
        """Persist one bounded config/manifest upload using only its basename."""
        name = Path(original_name).name
        if not name or name in (".", "..") or Path(name).suffix.lower() not in _SAFE_SUFFIXES:
            raise ValueError("uploaded file must be YAML, JSON, or text")
        if len(content) > _MAX_UPLOAD_BYTES:
            raise ValueError("uploaded file exceeds the 16 MiB UI limit")
        self._reject_sensitive_upload(name, content)
        destination = (self.inputs / name).resolve()
        if destination.parent != self.inputs.resolve():
            raise ValueError("uploaded filename escapes the session workspace")
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_bytes(content)
        temporary.replace(destination)
        return destination

    def new_backup_directory(self) -> Path:
        directory = self.backups / uuid.uuid4().hex
        directory.mkdir(parents=True, exist_ok=False)
        return directory

    def new_plan_path(self, config_path: Path) -> Path:
        """Return a unique plan path under the config directory required by the loader."""
        directory = config_path.resolve().parent / "plans"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "restore-plan-{0}.json".format(uuid.uuid4().hex)

    @staticmethod
    def _structured_value(path: Path) -> Any:
        try:
            text = path.read_text(encoding="utf-8")
            return json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
        except (OSError, UnicodeDecodeError, ValueError, yaml.YAMLError):
            return None

    def files_for(self, role: str) -> list[Path]:
        """Return restored/session files matching one UI input role."""
        if role not in ("backup_config", "manifest", "restore_config", "overrides"):
            raise ValueError("unsupported workspace file role")
        matches: list[Path] = []
        for path in self.root.rglob("*"):
            if not path.is_file() or "plans" in path.parts:
                continue
            if path.suffix.lower() not in (".json", ".yaml", ".yml"):
                continue
            value = self._structured_value(path)
            if not isinstance(value, Mapping):
                continue
            keys = set(value)
            if role == "backup_config" and {"aws", "s3"}.issubset(keys):
                matches.append(path)
            elif role == "manifest" and {"manifest_version", "restore_source"}.issubset(keys):
                matches.append(path)
            elif role == "restore_config" and {"target", "restore"}.issubset(keys):
                matches.append(path)
            elif (
                role == "overrides"
                and keys
                and all(str(key).startswith("Override") for key in keys)
            ):
                matches.append(path)
        return sorted(matches, key=lambda item: item.stat().st_mtime, reverse=True)

    def export_archive(self) -> Path:
        """Create a bounded portable archive, excluding plans, temp files, and old archives."""
        destination = self.root / "workspace-{0}.zip".format(self.session_id)
        temporary = destination.with_suffix(".zip.tmp")
        files: list[Path] = []
        total = 0
        for path in self.root.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(self.root)
            if "plans" in relative.parts or path.suffix.lower() in (".tmp", ".zip"):
                continue
            total += path.stat().st_size
            if total > _MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                raise ValueError("workspace exceeds the 256 MiB uncompressed archive limit")
            files.append(path)
        metadata = json.dumps(
            {
                "schema_version": _WORKSPACE_SCHEMA_VERSION,
                "source_session_id": self.session_id,
                "file_count": len(files),
            },
            sort_keys=True,
        ).encode("utf-8")
        with zipfile.ZipFile(
            temporary,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            archive.writestr("workspace.json", metadata)
            for path in files:
                archive.write(path, path.relative_to(self.root).as_posix())
        if temporary.stat().st_size > _MAX_ARCHIVE_BYTES:
            temporary.unlink(missing_ok=True)
            raise ValueError("workspace archive exceeds the 64 MiB compressed limit")
        temporary.replace(destination)
        return destination

    @classmethod
    def restore_archive(
        cls,
        content: bytes,
        base_directory: Optional[Path] = None,
    ) -> "SessionWorkspace":
        """Validate and extract an archive into a new isolated session workspace."""
        if len(content) > _MAX_ARCHIVE_BYTES:
            raise ValueError("workspace archive exceeds the 64 MiB compressed limit")
        try:
            archive = zipfile.ZipFile(BytesIO(content), mode="r")
        except zipfile.BadZipFile as error:
            raise ValueError("workspace archive is not a valid ZIP file") from error
        with archive:
            infos = archive.infolist()
            if len(infos) > _MAX_ARCHIVE_ENTRIES:
                raise ValueError("workspace archive contains too many files")
            names: set[str] = set()
            total = 0
            payloads: list[tuple[PurePosixPath, bytes]] = []
            metadata: Optional[dict[str, Any]] = None
            for info in infos:
                if info.is_dir():
                    continue
                name = info.filename
                path = PurePosixPath(name)
                if (
                    not name
                    or name in names
                    or name.startswith("/")
                    or "\\" in name
                    or path.is_absolute()
                    or ".." in path.parts
                    or "plans" in path.parts
                ):
                    raise ValueError("workspace archive contains an unsafe or duplicate path")
                names.add(name)
                mode = (info.external_attr >> 16) & 0o170000
                if mode == 0o120000:
                    raise ValueError("workspace archive contains a symbolic link")
                total += info.file_size
                if total > _MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                    raise ValueError("workspace archive exceeds the uncompressed size limit")
                data = archive.read(info)
                if name == "workspace.json":
                    try:
                        value = json.loads(data.decode("utf-8"))
                    except (UnicodeDecodeError, ValueError) as error:
                        raise ValueError("workspace metadata is invalid") from error
                    if not isinstance(value, dict):
                        raise ValueError("workspace metadata must be an object")
                    metadata = value
                    continue
                if path.suffix.lower() not in _SAFE_SUFFIXES:
                    raise ValueError("workspace archive contains an unsupported file type")
                if len(data) > _MAX_UPLOAD_BYTES:
                    raise ValueError("workspace archive contains an oversized file")
                cls._reject_sensitive_upload(path.name, data)
                payloads.append((path, data))
            if metadata is None or metadata.get("schema_version") != _WORKSPACE_SCHEMA_VERSION:
                raise ValueError("workspace archive metadata is missing or unsupported")
            if int(metadata.get("file_count", -1)) != len(payloads):
                raise ValueError("workspace archive file count does not match metadata")

        workspace = (
            cls.create() if base_directory is None else cls(uuid.uuid4().hex, base_directory)
        )
        for relative, data in payloads:
            destination = (workspace.root / Path(*relative.parts)).resolve()
            if workspace.root not in destination.parents:
                raise ValueError("workspace archive path escapes the new session")
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_suffix(destination.suffix + ".tmp")
            temporary.write_bytes(data)
            temporary.replace(destination)
        return workspace

    def artifacts(self) -> list[LocalArtifact]:
        """List user-facing manifests and reports while excluding internal plans/uploads."""
        values: list[LocalArtifact] = []
        patterns = (
            ("backup_manifest_*.json", "Backup manifest"),
            ("backup_report_*.txt", "Backup report"),
            ("restore-*.json", "Restore report"),
        )
        for pattern, kind in patterns:
            for path in sorted(
                self.root.rglob(pattern), key=lambda item: item.stat().st_mtime, reverse=True
            ):
                if "plans" in path.parts:
                    continue
                values.append(
                    LocalArtifact(
                        path=path,
                        label=str(path.relative_to(self.root)),
                        kind=kind,
                        size=path.stat().st_size,
                    )
                )
        return values
