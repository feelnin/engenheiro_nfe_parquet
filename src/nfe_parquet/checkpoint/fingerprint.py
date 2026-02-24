from __future__ import annotations

from pathlib import Path


def fingerprint_size_mtime(path: Path) -> str:
    st = path.stat()
    # mtime_ns é mais estável/preciso que mtime (float)
    return f"{st.st_size}|{st.st_mtime_ns}"