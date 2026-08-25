"""Safe, session-scoped local storage for uploaded and generated UI artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import json
import re
import tempfile
import uuid

import yaml

_SESSION_RE = re.compile(r"^[a-f0-9]{32}$")
_SAFE_SUFFIXES = {".json", ".yaml", ".yml", ".txt"}
_MAX_UPLOAD_BYTES = 16 * 1024 * 1024
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
