from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..config.models import AppConfig
from ..domain.models import SourceMeta
from ..io.scanner import scan_source
from ..io.zip_extract import extract_zip_to_temp
from ..observability.logger import get_logger
from ..parse.nfe_parser import parse_nfe_xml
from ..transform.filters import is_year_allowed
from ..write.parquet_writer import write_monthly_parquet


log = get_logger("nfe_parquet")


def run_once(cfg: AppConfig) -> None:
    ingested_at = datetime.utcnow()

    _run_source(
        cfg=cfg,
        source_name="importados",
        input_root=cfg.paths.input_importados,
        output_dir=cfg.paths.output_importados,
        ingested_at=ingested_at,
    )

    _run_source(
        cfg=cfg,
        source_name="processados",
        input_root=cfg.paths.input_processados,
        output_dir=cfg.paths.output_processados,
        ingested_at=ingested_at,
    )


def _run_source(
    cfg: AppConfig,
    source_name: str,
    input_root: Path,
    output_dir: Path,
    ingested_at: datetime,
) -> None:
    log.info(f"scan_start source={source_name} root={input_root}")
    items = list(scan_source(input_root, source_name))
    log.info(f"scan_done source={source_name} files={len(items)}")

    by_month: dict[str, list[dict]] = defaultdict(list)

    ok = 0
    filtered = 0
    errors = 0

    for it in items:
        try:
            if it.file_type == "xml":
                ok, filtered = _handle_xml(
                    cfg, it.source, it.source_root, it.file_path, ingested_at, by_month, ok, filtered
                )
            else:
                ok, filtered = _handle_zip(
                    cfg, it.source, it.source_root, it.file_path, ingested_at, by_month, ok, filtered
                )
        except Exception as e:
            errors += 1
            log.exception(f"file_error source={source_name} path={it.file_path} err={e}")

    written = 0
    for month, records in by_month.items():
        if not records:
            continue
        write_monthly_parquet(
            records=records,
            out_dir=output_dir,
            month=month,
            staging_dir=cfg.paths.staging_dir,
        )
        written += 1

    log.info(
        f"run_summary source={source_name} ok={ok} filtered={filtered} errors={errors} months_written={written}"
    )


def _handle_xml(
    cfg: AppConfig,
    source: str,
    source_root: Path,
    xml_path: Path,
    ingested_at: datetime,
    by_month: dict[str, list[dict]],
    ok: int,
    filtered: int,
) -> tuple[int, int]:
    meta = SourceMeta(
        source=source,
        source_root=source_root,
        source_file_path=xml_path,
        source_file_type="xml",
        source_entry_path=None,
        source_file_mtime=datetime.fromtimestamp(xml_path.stat().st_mtime),
    )
    xml_bytes = xml_path.read_bytes()
    result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

    dhEmi = result.record.get("dhEmi")
    if not is_year_allowed(dhEmi, cfg.rules.min_year):
        return ok, filtered + 1

    month = result.record.get("ref_aaaamm")
    if month:
        by_month[str(month)].append(result.record)
        return ok + 1, filtered
    return ok, filtered + 1


def _handle_zip(
    cfg: AppConfig,
    source: str,
    source_root: Path,
    zip_path: Path,
    ingested_at: datetime,
    by_month: dict[str, list[dict]],
    ok: int,
    filtered: int,
) -> tuple[int, int]:
    zip_mtime = datetime.fromtimestamp(zip_path.stat().st_mtime)

    with extract_zip_to_temp(zip_path, cfg.paths.tmp_extract_dir) as tmp_dir:
        for xml_path in tmp_dir.rglob("*.xml"):
            entry_path = str(xml_path.relative_to(tmp_dir))
            meta = SourceMeta(
                source=source,
                source_root=source_root,
                source_file_path=zip_path,
                source_file_type="zip",
                source_entry_path=entry_path,
                source_file_mtime=zip_mtime,
            )
            xml_bytes = xml_path.read_bytes()
            result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

            dhEmi = result.record.get("dhEmi")
            if not is_year_allowed(dhEmi, cfg.rules.min_year):
                filtered += 1
                continue

            month = result.record.get("ref_aaaamm")
            if month:
                by_month[str(month)].append(result.record)
                ok += 1
            else:
                filtered += 1

    return ok, filtered