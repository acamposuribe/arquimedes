from __future__ import annotations

import builtins
import errno
import hashlib
import io

import pytest

from arquimedes.models import compute_file_hash, compute_material_id


def test_compute_hash_retries_transient_deadlock(tmp_path, monkeypatch):
    path = tmp_path / "doc.pdf"
    payload = b"retry me"
    path.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    attempts = {"count": 0}
    original_open = builtins.open

    class DeadlockOnce(io.BytesIO):
        def __init__(self, data: bytes):
            super().__init__(data)
            self._failed = False

        def read(self, *args, **kwargs):
            if not self._failed:
                self._failed = True
                raise OSError(errno.EDEADLK, "Resource deadlock avoided")
            return super().read(*args, **kwargs)

    def flaky_open(file, mode="r", *args, **kwargs):
        if file == path and "rb" in mode:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return DeadlockOnce(payload)
        return original_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", flaky_open)

    assert compute_file_hash(path) == expected
    assert compute_material_id(path) == expected[:12]
    assert attempts["count"] >= 1


def test_compute_hash_raises_after_retry_budget(tmp_path, monkeypatch):
    path = tmp_path / "doc.pdf"
    path.write_bytes(b"still blocked")
    original_open = builtins.open

    def deadlocking_open(file, mode="r", *args, **kwargs):
        if file == path and "rb" in mode:
            return _AlwaysDeadlock()
        return original_open(file, mode, *args, **kwargs)

    class _AlwaysDeadlock(io.BytesIO):
        def read(self, *args, **kwargs):
            raise OSError(errno.EDEADLK, "Resource deadlock avoided")

    monkeypatch.setattr(builtins, "open", deadlocking_open)

    with pytest.raises(OSError, match="Resource deadlock avoided"):
        compute_file_hash(path)
