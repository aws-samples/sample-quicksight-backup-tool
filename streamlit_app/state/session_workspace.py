"""Safe, session-scoped local storage for uploaded and generated UI artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Optional
import json
import os
import re
import tempfile
import uuid
import zipfile

import yaml

_SESSION_RE = re.compile(r"^[a-f0-9]{32}$")
_WORKSPACE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SAFE_SUFFIXES = {".json", ".yaml", ".yml", ".txt"}
_MAX_UPLOAD_BYTES = 16 * 1024 * 1024
_MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
_MAX_ARCHIVE_ENTRIES = 500
_MAX_ARCHIVE_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
_WORKSPACE_SCHEMA_VERSION = "1.0"
_WORKSPACE_MARKER = ".quicksight-workspace.json"
_WINDOWS_REPARSE_ATTRIBUTE = 0x400
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
    """Own files beneath one validated temporary or persistent local directory."""

    def __init__(
        self,
        session_id: str,
        base_directory: Optional[Path] = None,
        root_directory: Optional[Path] = None,
        require_marker: bool = False,
    ):
        if not _SESSION_RE.fullmatch(session_id):
            raise ValueError("session_id must be a 32-character lowercase hexadecimal value")
        if root_directory is None:
            base = base_directory or Path(tempfile.gettempdir()) / "quicksight-backup-tool-ui"
            raw_root = base / session_id
        else:
            raw_root = root_directory
        self._reject_reparse_components(raw_root)
        self.root = raw_root.expanduser().absolute()
        if self.root.exists() and not self.root.is_dir():
            raise ValueError("workspace path must be a directory")
        self.root.mkdir(parents=True, exist_ok=True)
        self._session_id = session_id
        self.inputs = self.root / "inputs"
        self.backups = self.root / "backup-runs"
        for directory in (self.inputs, self.backups):
            directory.mkdir(parents=True, exist_ok=True)
        marker = self.root / _WORKSPACE_MARKER
        if marker.exists():
            self._validate_marker(marker, expected_session_id=session_id)
        elif require_marker:
            raise ValueError("folder is not a Quick Sight backup UI workspace")
        else:
            self._write_marker(marker)
        self._validate_workspace_tree()

    def _validate_workspace_tree(self) -> None:
        files = 0
        total = 0
        for path in self.root.rglob("*"):
            stat = path.lstat()
            attributes = int(getattr(stat, "st_file_attributes", 0))
            if path.is_symlink() or attributes & _WINDOWS_REPARSE_ATTRIBUTE:
                raise ValueError("workspace contents must not include symlinks or reparse points")
            if not path.is_file():
                continue
            files += 1
            total += stat.st_size
            if files > _MAX_ARCHIVE_ENTRIES:
                raise ValueError("workspace contains too many files")
            if total > _MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                raise ValueError("workspace exceeds the 256 MiB size limit")
            if path.name == _WORKSPACE_MARKER:
                continue
            if path.suffix.lower() not in _SAFE_SUFFIXES | {".zip"}:
                raise ValueError("workspace contains an unsupported file type")

    @staticmethod
    def _reject_reparse_components(path: Path) -> None:
        raw = path.expanduser().absolute()
        current = Path(raw.anchor) if raw.anchor else Path()
        for part in raw.parts[1:] if raw.anchor else raw.parts:
            current = current / part
            if not current.exists():
                continue
            stat = current.lstat()
            attributes = int(getattr(stat, "st_file_attributes", 0))
            if current.is_symlink() or attributes & _WINDOWS_REPARSE_ATTRIBUTE:
                raise ValueError("workspace path must not contain symlinks or reparse points")

    @staticmethod
    def _read_marker(marker: Path) -> dict[str, Any]:
        if not marker.is_file() or marker.is_symlink():
            raise ValueError("workspace metadata must be a regular file")
        try:
            value = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as error:
            raise ValueError("workspace metadata is invalid") from error
        if not isinstance(value, dict) or value.get("schema_version") != _WORKSPACE_SCHEMA_VERSION:
            raise ValueError("workspace metadata schema is missing or unsupported")
        workspace_id = value.get("workspace_id")
        if not isinstance(workspace_id, str) or not _SESSION_RE.fullmatch(workspace_id):
            raise ValueError("workspace metadata ID is invalid")
        return value

    @classmethod
    def _validate_marker(cls, marker: Path, expected_session_id: str) -> None:
        value = cls._read_marker(marker)
        if value["workspace_id"] != expected_session_id:
            raise ValueError("workspace metadata ID does not match the selected session")

    def _write_marker(self, marker: Path) -> None:
        value = {
            "schema_version": _WORKSPACE_SCHEMA_VERSION,
            "workspace_id": self._session_id,
        }
        temporary = marker.with_suffix(marker.suffix + ".tmp")
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        temporary.replace(marker)

    @classmethod
    def create(cls) -> "SessionWorkspace":
        return cls(uuid.uuid4().hex)

    @staticmethod
    def default_home() -> Path:
        configured = os.environ.get("QUICKSIGHT_WORKSPACE_HOME")
        return (
            Path(configured).expanduser().absolute()
            if configured
            else (Path.home() / "QuickSightWorkspaces").absolute()
        )

    @staticmethod
    def _validate_workspace_name(name: str) -> str:
        if not _WORKSPACE_NAME_RE.fullmatch(name):
            raise ValueError(
                "workspace name must start with a letter or number and contain only "
                "letters, numbers, dots, underscores, or hyphens (maximum 64 characters)"
            )
        return name

    @classmethod
    def _library_child(cls, home: Path, child: Path) -> tuple[Path, Path]:
        cls._reject_reparse_components(home)
        root = home.expanduser().absolute()
        source = child.expanduser()
        if not source.is_absolute():
            source = root / source
        source = source.absolute()
        if source.parent != root:
            raise ValueError("workspace must be a direct child of the library")
        cls._reject_reparse_components(source)
        return root, source

    @classmethod
    def discover_folders(cls, home: Path) -> list[Path]:
        """List direct child folders containing valid workspace metadata."""
        cls._reject_reparse_components(home)
        root = home.expanduser().absolute()
        if not root.exists():
            return []
        if not root.is_dir():
            raise ValueError("workspace library path must be a directory")
        values: list[Path] = []
        for child in sorted(root.iterdir(), key=lambda item: item.name.lower()):
            if not child.is_dir() or child.is_symlink():
                continue
            attributes = int(getattr(child.lstat(), "st_file_attributes", 0))
            if attributes & _WINDOWS_REPARSE_ATTRIBUTE:
                continue
            marker = child / _WORKSPACE_MARKER
            if not marker.exists():
                continue
            try:
                cls._read_marker(marker)
            except ValueError:
                continue
            values.append(child.absolute())
            if len(values) >= 200:
                break
        return values

    @classmethod
    def create_named(cls, home: Path, name: str) -> "SessionWorkspace":
        """Create or reopen one safely named direct child of a workspace library."""
        validated_name = cls._validate_workspace_name(name)
        cls._reject_reparse_components(home)
        root = home.expanduser().absolute()
        root.mkdir(parents=True, exist_ok=True)
        return cls.create_folder(root / validated_name)

    @classmethod
    def create_folder(cls, path: Path) -> "SessionWorkspace":
        """Create a persistent workspace in a new or empty explicit folder."""
        cls._reject_reparse_components(path)
        root = path.expanduser().absolute()
        if root.exists() and not root.is_dir():
            raise ValueError("workspace path must be a directory")
        if root.exists():
            entries = list(root.iterdir())
            marker = root / _WORKSPACE_MARKER
            if entries and not marker.exists():
                raise ValueError(
                    "refusing to create a workspace in a nonempty folder that is not a workspace"
                )
            if marker.exists():
                value = cls._read_marker(marker)
                return cls(value["workspace_id"], root_directory=root, require_marker=True)
        return cls(uuid.uuid4().hex, root_directory=root)

    @classmethod
    def open_folder(cls, path: Path) -> "SessionWorkspace":
        """Open an existing persistent workspace identified by its marker."""
        cls._reject_reparse_components(path)
        root = path.expanduser().absolute()
        if not root.is_dir():
            raise ValueError("workspace folder does not exist")
        value = cls._read_marker(root / _WORKSPACE_MARKER)
        return cls(value["workspace_id"], root_directory=root, require_marker=True)

    def is_empty(self) -> bool:
        """Return whether the workspace has only its marker and empty standard folders."""
        self._validate_workspace_tree()
        marker = self.root / _WORKSPACE_MARKER
        for entry in self.root.iterdir():
            if entry == marker:
                continue
            if entry in (self.inputs, self.backups) and entry.is_dir() and not any(entry.iterdir()):
                continue
            return False
        return True

    @classmethod
    def rename_named(cls, home: Path, child: Path, new_name: str) -> "SessionWorkspace":
        """Rename a valid direct-child library workspace."""
        root, source = cls._library_child(home, child)
        destination = root / cls._validate_workspace_name(new_name)
        if os.path.normcase(str(source)) == os.path.normcase(str(destination)):
            raise ValueError("new workspace name must be different")
        workspace = cls.open_folder(source)
        if destination.exists():
            raise ValueError("a workspace with that name already exists")
        workspace._validate_workspace_tree()
        source.rename(destination)
        return cls.open_folder(destination)

    @classmethod
    def remove_empty(cls, path: Path) -> None:
        """Remove a valid workspace only when it contains no user files."""
        workspace = cls.open_folder(path)
        if not workspace.is_empty():
            raise ValueError("workspace is not empty")
        workspace.inputs.rmdir()
        workspace.backups.rmdir()
        (workspace.root / _WORKSPACE_MARKER).unlink()
        workspace.root.rmdir()

    @property
    def session_id(self) -> str:
        return self._session_id

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
            if (
                relative.as_posix() == _WORKSPACE_MARKER
                or "plans" in relative.parts
                or path.suffix.lower() in (".tmp", ".zip")
            ):
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
        workspace._validate_workspace_tree()
        return workspace

    @classmethod
    def restore_directory_files(
        cls,
        files: list[tuple[str, bytes]],
        base_directory: Optional[Path] = None,
    ) -> "SessionWorkspace":
        """Load browser-selected directory files into a new isolated workspace."""
        if not files:
            raise ValueError("selected workspace folder is empty")
        if len(files) > _MAX_ARCHIVE_ENTRIES:
            raise ValueError("selected workspace folder contains too many files")
        normalized: list[tuple[PurePosixPath, bytes]] = []
        total = 0
        for name, data in files:
            path = PurePosixPath(name)
            if (
                not name
                or name.startswith("/")
                or "\\" in name
                or path.is_absolute()
                or ".." in path.parts
                or any(
                    "\x00" in part or ":" in part or part.endswith(" ") or part.endswith(".")
                    for part in path.parts
                )
            ):
                raise ValueError("selected workspace folder contains an unsafe path")
            total += len(data)
            if total > _MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                raise ValueError("selected workspace folder exceeds the 256 MiB size limit")
            normalized.append((path, data))

        markers = [(path, data) for path, data in normalized if path.name == _WORKSPACE_MARKER]
        marker_aliases = [
            path
            for path, _data in normalized
            if path.name.casefold() == _WORKSPACE_MARKER.casefold()
        ]
        if len(markers) != 1 or len(marker_aliases) != 1:
            raise ValueError("selected folder is not a valid workspace")
        marker_path, marker_data = markers[0]
        if len(marker_data) > _MAX_UPLOAD_BYTES:
            raise ValueError("workspace metadata exceeds the 16 MiB file limit")
        try:
            marker_value = json.loads(marker_data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as error:
            raise ValueError("workspace metadata is invalid") from error
        if (
            not isinstance(marker_value, dict)
            or marker_value.get("schema_version") != _WORKSPACE_SCHEMA_VERSION
            or not _SESSION_RE.fullmatch(str(marker_value.get("workspace_id", "")))
        ):
            raise ValueError("workspace format or ID is invalid")

        prefix = marker_path.parent.parts
        payloads: list[tuple[PurePosixPath, bytes]] = []
        names: set[str] = set()
        for path, data in normalized:
            if path.parts[: len(prefix)] != prefix:
                raise ValueError("selected files do not belong to one workspace")
            relative = PurePosixPath(*path.parts[len(prefix) :])
            if relative.name == _WORKSPACE_MARKER:
                continue
            relative_key = relative.as_posix().casefold()
            if not relative.parts or relative_key in names:
                raise ValueError("selected workspace folder contains duplicate paths")
            names.add(relative_key)
            suffix = relative.suffix.lower()
            if any(part.casefold() == "plans" for part in relative.parts) or suffix == ".tmp":
                continue
            if suffix == ".zip" and relative.name.casefold().startswith("workspace-"):
                continue
            if suffix not in _SAFE_SUFFIXES | {".zip"}:
                raise ValueError("selected workspace folder contains an unsupported file type")
            if len(data) > _MAX_UPLOAD_BYTES:
                raise ValueError("selected workspace folder contains an oversized file")
            if suffix != ".zip":
                cls._reject_sensitive_upload(relative.name, data)
            payloads.append((relative, data))

        workspace = (
            cls.create() if base_directory is None else cls(uuid.uuid4().hex, base_directory)
        )
        for relative, data in payloads:
            destination = (workspace.root / Path(*relative.parts)).absolute()
            if workspace.root not in destination.parents:
                raise ValueError("selected workspace path escapes the new session")
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_suffix(destination.suffix + ".tmp")
            temporary.write_bytes(data)
            temporary.replace(destination)
        workspace._validate_workspace_tree()
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
