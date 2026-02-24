from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..domain.models import SourceMeta
from ..parse.nfe_parser import parse_nfe_xml
from ..write.parquet_writer import write_monthly_parquet


def run_mvp_from_xml_files(xml_files: Iterable[Path], source: str, source_root: Path, out_dir: Path, staging_dir: Path) -> None:
    """Runner mínimo para validar parser+writer antes de scan/zip/checkpoint."""
    ingested_at = datetime.utcnow()
    by_month: dict[str, list[dict]] = defaultdict(list)

    for xml_path in xml_files:
        meta = SourceMeta(
            source=source,
            source_root=source_root,
            source_file_path=xml_path,
            source_file_type="xml",
            source_entry_path=None,
            source_file_mtime=None,
        )
        xml_bytes = xml_path.read_bytes()
        result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

        # TODO: aqui vai filtro ano>=2026 + ref_aaaamm obrigatório
        month = result.record.get("ref_aaaamm")
        if month:
            by_month[str(month)].append(result.record)

    for month, records in by_month.items():
        write_monthly_parquet(records, out_dir=out_dir, month=month, staging_dir=staging_dir)