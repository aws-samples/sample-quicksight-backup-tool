"""Fail-closed local path resolution and bounded reads for restore-controlled files."""

from pathlib import Path
from typing import Iterable, Optional
import errno
import ntpath
import os
import stat

from .models.errors import RestoreConfigurationError

_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
_READ_CHUNK_BYTES = 64 * 1024
_WINDOWS_FINAL_PATH_INITIAL_CHARS = 512
_WINDOWS_FINAL_PATH_MAX_CHARS = 32 * 1024
_WINDOWS_FINAL_PATH_MAX_ATTEMPTS = 8

if os.name == "nt":
    import ctypes
    import msvcrt
    from ctypes import wintypes

    _GENERIC_READ = 0x80000000
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _OPEN_EXISTING = 3
    _FILE_ATTRIBUTE_NORMAL = 0x00000080
    _FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
    _INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _CreateFileW = _kernel32.CreateFileW
    _CreateFileW.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    _CreateFileW.restype = wintypes.HANDLE

    _GetFinalPathNameByHandleW = _kernel32.GetFinalPathNameByHandleW
    _GetFinalPathNameByHandleW.argtypes = (
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    _GetFinalPathNameByHandleW.restype = wintypes.DWORD

    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = (wintypes.HANDLE,)
    _CloseHandle.restype = wintypes.BOOL


def _is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((str(path), str(root))) == str(root)
    except ValueError:
        return False


def _absolute_without_resolving(path: Path) -> Path:
    if not path.is_absolute():
        path = Path.cwd() / path
    return Path(os.path.abspath(str(path)))


def _is_link_or_reparse(metadata: os.stat_result) -> bool:
    return stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
    )


def _strip_windows_extended_prefix(value: str) -> str:
    folded = value.casefold()
    if folded.startswith("\\\\?\\unc\\"):
        return "\\\\" + value[8:]
    if folded.startswith("\\\\?\\"):
        return value[4:]
    return value


def _canonical_path_observation(path: Path) -> str:
    value = str(path)
    looks_windows = os.name == "nt" or value.startswith("\\\\") or bool(ntpath.splitdrive(value)[0])
    if looks_windows:
        value = _strip_windows_extended_prefix(value)
        if not ntpath.isabs(value):
            value = ntpath.abspath(value)
        return ntpath.normcase(ntpath.normpath(value))
    return os.path.normcase(os.path.normpath(os.path.abspath(value)))


def _paths_match_canonically(*paths: Path) -> bool:
    return len({_canonical_path_observation(path) for path in paths}) == 1


def _open_read_descriptor(path: Path) -> int:
    if os.name != "nt":
        flags = os.O_RDONLY
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        return os.open(str(path), flags)

    native_handle = _CreateFileW(
        str(path),
        _GENERIC_READ,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        None,
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    if native_handle == _INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    descriptor_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    descriptor_flags |= getattr(os, "O_NOINHERIT", 0)
    try:
        descriptor = msvcrt.open_osfhandle(native_handle, descriptor_flags)
        if descriptor == -1:
            raise OSError(
                errno.EBADF,
                "msvcrt.open_osfhandle returned an invalid descriptor",
            )
    except BaseException:
        _CloseHandle(native_handle)
        raise
    return descriptor


def _descriptor_final_path(descriptor: int, non_windows_observation: Path) -> Path:
    """Return the normalized DOS path for an open descriptor on Windows."""
    if os.name != "nt":
        return non_windows_observation

    native_handle = msvcrt.get_osfhandle(descriptor)
    if native_handle == -1:
        raise OSError(errno.EBADF, "descriptor has no valid Windows handle")

    buffer_size = _WINDOWS_FINAL_PATH_INITIAL_CHARS
    for _ in range(_WINDOWS_FINAL_PATH_MAX_ATTEMPTS):
        buffer = ctypes.create_unicode_buffer(buffer_size)
        length = _GetFinalPathNameByHandleW(
            native_handle,
            buffer,
            buffer_size,
            0,  # FILE_NAME_NORMALIZED | VOLUME_NAME_DOS
        )
        if length == 0:
            raise ctypes.WinError(ctypes.get_last_error())
        if length < buffer_size:
            return Path(buffer.value)

        next_size = max(buffer_size + 1, int(length) + 1)
        if next_size > _WINDOWS_FINAL_PATH_MAX_CHARS:
            raise OSError(
                errno.ENAMETOOLONG,
                "final descriptor path exceeds the verification limit",
            )
        buffer_size = next_size

    raise OSError(
        errno.EAGAIN,
        "final descriptor path did not stabilize during verification",
    )


def reject_link_components(path: Path, label: str, allow_missing: bool = False) -> Path:
    """Reject symbolic-link/reparse components without resolving the supplied path."""
    candidate = _absolute_without_resolving(path.expanduser())
    anchor = Path(candidate.anchor)
    current = anchor
    parts = candidate.parts[1:] if candidate.anchor else candidate.parts
    for index, part in enumerate(parts):
        current = current / part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if allow_missing:
                break
            raise ValueError("{0} does not exist".format(label))
        if _is_link_or_reparse(metadata):
            raise ValueError("{0} must not contain symbolic links or reparse points".format(label))
        if index < len(parts) - 1 and not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("{0} has a non-directory parent".format(label))
    return candidate


def read_bounded_regular_file(path: Path, max_bytes: int, label: str) -> bytes:
    """Read one stable regular-file snapshot through a single bounded descriptor."""
    candidate = reject_link_components(path, label, allow_missing=False)
    before_path = candidate.resolve(strict=True)
    descriptor = _open_read_descriptor(candidate)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("{0} must be a regular file".format(label))
        if before.st_size > max_bytes:
            raise ValueError("{0} exceeds the local size limit".format(label))

        path_metadata = os.stat(str(candidate), follow_symlinks=False)
        if _is_link_or_reparse(path_metadata) or not os.path.samestat(before, path_metadata):
            raise ValueError("{0} changed while it was opened".format(label))
        try:
            opened_descriptor_path = _descriptor_final_path(descriptor, before_path)
        except OSError as error:
            raise ValueError("{0} changed while it was opened".format(label)) from error

        # This is a cooperative fail-closed identity check within a trusted directory
        # boundary. It does not lock a hostile local namespace against mutation.
        if not _paths_match_canonically(candidate, before_path, opened_descriptor_path):
            raise ValueError("{0} changed while it was opened".format(label))

        chunks = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(
                descriptor,
                min(_READ_CHUNK_BYTES, max_bytes + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > max_bytes:
            raise ValueError("{0} exceeds the local size limit".format(label))

        after = os.fstat(descriptor)
        try:
            after_path = candidate.resolve(strict=True)
            final_path_metadata = os.stat(str(candidate), follow_symlinks=False)
            final_descriptor_path = _descriptor_final_path(descriptor, after_path)
        except (OSError, RuntimeError) as error:
            raise ValueError("{0} changed while it was read".format(label)) from error

        stable_fields = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            getattr(before, "st_mtime_ns", None),
        )
        final_fields = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            getattr(after, "st_mtime_ns", None),
        )
        if (
            stable_fields != final_fields
            or _is_link_or_reparse(final_path_metadata)
            or not os.path.samestat(after, final_path_metadata)
            or not _paths_match_canonically(
                candidate,
                before_path,
                opened_descriptor_path,
                after_path,
                final_descriptor_path,
            )
            or total != after.st_size
        ):
            raise ValueError("{0} changed while it was read".format(label))
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def resolve_under_root(
    value: str,
    root: Path,
    label: str,
    must_exist: bool = False,
    require_file: bool = False,
    protected_paths: Optional[Iterable[Path]] = None,
) -> Path:
    """Resolve a reviewed path under ``root`` and reject escape/collision cases."""
    if not isinstance(value, str) or not value:
        raise RestoreConfigurationError("{0} must be a non-empty path string".format(label))
    try:
        raw_root = reject_link_components(root, "restore configuration directory")
        safe_root = raw_root.resolve(strict=True)
        raw = Path(value).expanduser()
        candidate = raw if raw.is_absolute() else safe_root / raw
        candidate = reject_link_components(candidate, label, allow_missing=not must_exist)
        resolved = candidate.resolve(strict=must_exist)
    except (OSError, ValueError) as error:
        raise RestoreConfigurationError("{0}: {1}".format(label, error))
    if not _is_within(resolved, safe_root):
        raise RestoreConfigurationError(
            "{0} must resolve within the restore configuration directory".format(label)
        )
    for protected in protected_paths or ():
        if resolved == protected.expanduser().resolve():
            raise RestoreConfigurationError(
                "{0} collides with a protected restore input".format(label)
            )
    if must_exist and not resolved.exists():
        raise RestoreConfigurationError("{0} does not exist".format(label))
    if require_file:
        try:
            metadata = resolved.lstat()
        except OSError as error:
            raise RestoreConfigurationError("{0}: {1}".format(label, error))
        if _is_link_or_reparse(metadata) or not stat.S_ISREG(metadata.st_mode):
            raise RestoreConfigurationError("{0} must be a regular file".format(label))
    return resolved
