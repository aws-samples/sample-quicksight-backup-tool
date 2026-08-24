import pytest

from quicksight_restore.local_paths import (
    read_bounded_regular_file,
    resolve_under_root,
)
from quicksight_restore.models.errors import RestoreConfigurationError


def test_bounded_reader_rejects_more_than_limit(tmp_path):
    path = tmp_path / "large.json"
    path.write_bytes(b"x" * 17)

    with pytest.raises(ValueError, match="size limit"):
        read_bounded_regular_file(path, 16, "test input")


def test_bounded_reader_detects_concurrent_growth(tmp_path, monkeypatch):
    import quicksight_restore.local_paths as local_paths

    path = tmp_path / "changing.json"
    path.write_bytes(b"small")
    original_read = local_paths.os.read
    changed = {"value": False}

    def grow_after_first_read(descriptor, size):
        value = original_read(descriptor, size)
        if not changed["value"]:
            changed["value"] = True
            with path.open("ab") as handle:
                handle.write(b"!")
                handle.flush()
        return value

    monkeypatch.setattr(local_paths.os, "read", grow_after_first_read)
    with pytest.raises(ValueError, match="changed while it was read"):
        read_bounded_regular_file(path, 64, "test input")


def test_bounded_reader_rejects_direct_symlink(tmp_path):
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "linked.json"
    try:
        link.symlink_to(target)
    except OSError as error:
        pytest.skip("symbolic links are unavailable: {0}".format(error))

    with pytest.raises(ValueError, match="symbolic links|reparse"):
        read_bounded_regular_file(link, 64, "test input")


def test_root_resolver_rejects_parent_symlink_even_when_target_stays_inside_root(
    tmp_path,
):
    real = tmp_path / "real"
    real.mkdir()
    (real / "input.json").write_text("{}", encoding="utf-8")
    link = tmp_path / "linked-parent"
    try:
        link.symlink_to(real, target_is_directory=True)
    except OSError as error:
        pytest.skip("symbolic links are unavailable: {0}".format(error))

    with pytest.raises(RestoreConfigurationError, match="symbolic links|reparse"):
        resolve_under_root(
            "linked-parent/input.json",
            tmp_path,
            "test input",
            must_exist=True,
            require_file=True,
        )


def test_bounded_reader_rejects_descriptor_final_path_mismatch(tmp_path, monkeypatch):
    import quicksight_restore.local_paths as local_paths

    path = tmp_path / "reviewed.json"
    path.write_text("{}", encoding="utf-8")
    outside = tmp_path / "redirected.json"
    outside.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        local_paths,
        "_descriptor_final_path",
        lambda descriptor, observation: outside,
    )

    with pytest.raises(ValueError, match="changed while it was opened"):
        read_bounded_regular_file(path, 64, "test input")


def test_windows_reader_descriptor_allows_delete_sharing(tmp_path):
    import os
    import quicksight_restore.local_paths as local_paths

    if os.name != "nt":
        pytest.skip("Windows delete-sharing behavior")

    path = tmp_path / "current.json"
    path.write_bytes(b"old")
    descriptor = local_paths._open_read_descriptor(path)
    try:
        os.unlink(path)
        assert not path.exists()
        assert os.read(descriptor, 3) == b"old"
    finally:
        os.close(descriptor)
