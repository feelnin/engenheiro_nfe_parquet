from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import shutil
import zipfile


def _safe_extract_zip(zf: zipfile.ZipFile, dest: Path) -> None:
    """
    Extrai membros do ZIP validando contra Zip Slip (path traversal).

    Cada membro é verificado com Path.relative_to() para garantir que o caminho
    resolvido pertence ao diretório de destino. Levanta ValueError se detectar
    um membro com caminho malicioso (ex: '../../etc/cron.d/evil').
    """
    dest_resolved = dest.resolve()
    for member in zf.infolist():
        target = (dest_resolved / member.filename).resolve()
        try:
            target.relative_to(dest_resolved)
        except ValueError:
            raise ValueError(
                f"Zip Slip detectado: membro '{member.filename}' "
                f"aponta para fora do diretório de destino '{dest_resolved}'."
            )
        zf.extract(member, path=dest)


@contextmanager
def extract_zip_to_temp(zip_path: Path, tmp_root: Path) -> Iterator[Path]:
    """Extrai ZIP para um diretório temporário e garante cleanup ao final."""
    tmp_dir = tmp_root / f"_zip_{zip_path.stem}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            _safe_extract_zip(zf, tmp_dir)
        yield tmp_dir
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)