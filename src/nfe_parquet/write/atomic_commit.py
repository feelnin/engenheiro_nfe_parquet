from __future__ import annotations

from pathlib import Path
import os
import shutil


def atomic_replace(src: Path, dst: Path) -> None:
    """Move/replace atômico quando possível (mesmo filesystem)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(src, dst)
    except OSError:
        # fallback: copy + replace
        tmp = dst.with_suffix(dst.suffix + ".copytmp")
        shutil.copy2(src, tmp)
        os.replace(tmp, dst)
        src.unlink(missing_ok=True)