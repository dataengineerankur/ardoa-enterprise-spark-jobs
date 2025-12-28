from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass


@dataclass(frozen=True)
class AtomicWriteResult:
    ok: bool
    path: str
    bytes_written: int


def atomic_write_text(path: str, text: str) -> AtomicWriteResult:
    """
    Write text atomically (temp file + fsync + rename).
    Intended to prevent partial outputs when a job crashes mid-write.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d, text=True)
    bytes_written = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            if text and not text.endswith("\n"):
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        bytes_written = len((text or "").encode("utf-8"))
        os.replace(tmp, path)
        return AtomicWriteResult(ok=True, path=path, bytes_written=bytes_written)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return AtomicWriteResult(ok=False, path=path, bytes_written=bytes_written)


