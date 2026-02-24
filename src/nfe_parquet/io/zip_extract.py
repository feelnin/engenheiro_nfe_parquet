from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import shutil
import zipfile


@contextmanager
def extract_zip_to_temp(zip_path: Path, tmp_root: Path) -> Iterator[Path]:
    """Extrai ZIP para um diretório temporário e garante cleanup ao final."""
    tmp_dir = tmp_root / f"_zip_{zip_path.stem}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_dir)
        yield tmp_dir
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)