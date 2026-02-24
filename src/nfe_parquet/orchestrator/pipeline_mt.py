from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from ..config.models import AppConfig
from ..domain.models import SourceMeta
from ..io.scanner import WorkItem, scan_source
from ..io.zip_extract import extract_zip_to_temp
from ..observability.logger import get_logger
from ..parse.nfe_parser import parse_nfe_xml
from ..transform.filters import is_year_allowed
from ..write.atomic_commit import atomic_replace
from ..schema.parquet_schema import get_arrow_schema

import pyarrow as pa
import pyarrow.parquet as pq

from .chunking import chunked

from ..checkpoint.store_sqlite import SQLiteCheckpointStore, CheckpointKey
from ..checkpoint.fingerprint import fingerprint_size_mtime
from ..transform.window import last_n_months_yyyymm

log = get_logger("nfe_parquet")


def run_once_mt(cfg: AppConfig) -> None:
    ingested_at = datetime.utcnow()

    _run_source_mt(
        ckpt = SQLiteCheckpointStore(cfg.checkpoint.sqlite_path),
        moving_months = last_n_months_yyyymm(ingested_at, cfg.rules.moving_window_months),
        cfg=cfg,
        source_name="importados",
        input_root=cfg.paths.input_importados,
        output_dir=cfg.paths.output_importados,
        ingested_at=ingested_at,
    )
    _run_source_mt(
        cfg=cfg,
        source_name="processados",
        input_root=cfg.paths.input_processados,
        output_dir=cfg.paths.output_processados,
        ingested_at=ingested_at,
    )


def _run_source_mt(
    cfg: AppConfig,
    source_name: str,
    input_root: Path,
    output_dir: Path,
    ingested_at: datetime,
) -> None:
    schema = get_arrow_schema()

    # staging: vamos separar "staging de parts" do "staging de commit tmp"
    parts_root = cfg.paths.staging_dir / "parts"
    commit_tmp_root = cfg.paths.staging_dir / "commit_tmp"

    # buffers por mês + contador de parts
    buffers: dict[str, list[dict]] = defaultdict(list)
    part_counter: dict[str, int] = defaultdict(int)

    # parâmetros
    chunk_size = cfg.performance.file_chunk_size
    max_workers = cfg.performance.max_workers
    max_buffer_records = cfg.performance.record_chunk_size

    log.info(f"scan_start source={source_name} root={input_root}")
    work_iter = scan_source(input_root, source_name)
    log.info(f"scan_stream_ready source={source_name}")

    ok = filtered = errors = 0
    months_seen: set[str] = set()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for batch in chunked(work_iter, chunk_size):
            futures = [pool.submit(_process_work_item, cfg, it, ingested_at) for it in batch]

            for fut in as_completed(futures):
                try:
                    records = fut.result()
                except Exception as e:
                    errors += 1
                    log.exception(f"work_item_error source={source_name} err={e}")
                    continue

                # records pode ser vazio (ex.: tudo filtrado)
                for rec in records:
                    dhEmi = rec.get("dhEmi")
                    if not is_year_allowed(dhEmi, cfg.rules.min_year):
                        filtered += 1
                        continue

                    month = rec.get("ref_aaaamm")
                    if not month:
                        filtered += 1
                        continue

                    months_seen.add(str(month))
                    buffers[str(month)].append(rec)
                    ok += 1

                    # flush por mês quando buffer atingir limite
                    if len(buffers[str(month)]) >= max_buffer_records:
                        _flush_month_buffer(
                            schema=schema,
                            source=source_name,
                            month=str(month),
                            records=buffers[str(month)],
                            parts_root=parts_root,
                            part_counter=part_counter,
                        )
                        buffers[str(month)].clear()

    # flush final de buffers
    for month, recs in list(buffers.items()):
        if recs:
            _flush_month_buffer(
                schema=schema,
                source=source_name,
                month=month,
                records=recs,
                parts_root=parts_root,
                part_counter=part_counter,
            )
            buffers[month].clear()

    # compactação final por mês
    months_written = 0
    for month in sorted(months_seen):
        try:
            _compact_month(
                schema=schema,
                parts_root=parts_root,
                commit_tmp_root=commit_tmp_root,
                source=source_name,
                month=month,
                output_dir=output_dir,
            )
            months_written += 1
        except Exception as e:
            errors += 1
            log.exception(f"compact_error source={source_name} month={month} err={e}")

    log.info(
        f"run_summary source={source_name} ok={ok} filtered={filtered} errors={errors} months_written={months_written}"
    )


def _process_work_item(cfg: AppConfig, it: WorkItem, ingested_at: datetime) -> list[dict]:
    """Processa um WorkItem e retorna 0..N registros (ZIP pode gerar vários)."""
    if it.file_type == "xml":
        return _process_xml(it, ingested_at)
    return _process_zip(cfg, it, ingested_at)


def _process_xml(it: WorkItem, ingested_at: datetime) -> list[dict]:
    meta = SourceMeta(
        source=it.source,
        source_root=it.source_root,
        source_file_path=it.file_path,
        source_file_type="xml",
        source_entry_path=None,
        source_file_mtime=datetime.fromtimestamp(it.file_path.stat().st_mtime),
    )
    xml_bytes = it.file_path.read_bytes()
    result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)
    return [result.record]


def _process_zip(cfg: AppConfig, it: WorkItem, ingested_at: datetime) -> list[dict]:
    zip_mtime = datetime.fromtimestamp(it.file_path.stat().st_mtime)
    out: list[dict] = []

    with extract_zip_to_temp(it.file_path, cfg.paths.tmp_extract_dir) as tmp_dir:
        for xml_path in tmp_dir.rglob("*.xml"):
            entry_path = str(xml_path.relative_to(tmp_dir))
            meta = SourceMeta(
                source=it.source,
                source_root=it.source_root,
                source_file_path=it.file_path,
                source_file_type="zip",
                source_entry_path=entry_path,
                source_file_mtime=zip_mtime,
            )
            xml_bytes = xml_path.read_bytes()
            result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)
            out.append(result.record)

    return out


def _flush_month_buffer(
    schema: pa.Schema,
    source: str,
    month: str,
    records: list[dict],
    parts_root: Path,
    part_counter: dict[str, int],
) -> None:
    """Escreve um fragmento part-XXXX para o mês."""
    if not records:
        return

    parts_dir = parts_root / source / month
    parts_dir.mkdir(parents=True, exist_ok=True)

    idx = part_counter[month]
    part_counter[month] += 1

    part_path = parts_dir / f"part-{idx:06d}.parquet"
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, part_path)

    log.info(f"flush_part source={source} month={month} part={part_path.name} rows={table.num_rows}")


def _compact_month(
    schema: pa.Schema,
    parts_root: Path,
    commit_tmp_root: Path,
    source: str,
    month: str,
    output_dir: Path,
) -> None:
    """Compacta partições staging em 1 arquivo final AAAAMM.parquet com commit atômico."""
    parts_dir = parts_root / source / month
    parts = sorted(parts_dir.glob("part-*.parquet"))
    if not parts:
        return

    # Streaming write (sem carregar tudo em RAM)
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / f"{month}.parquet"

    tmp_path = commit_tmp_root / source / f"{month}.parquet.tmp"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    writer = pq.ParquetWriter(tmp_path, schema=schema)

    total_rows = 0
    try:
        for p in parts:
            t = pq.read_table(p, schema=schema)
            writer.write_table(t)
            total_rows += t.num_rows
    finally:
        writer.close()

    atomic_replace(tmp_path, final_path)
    log.info(f"compact_done source={source} month={month} parts={len(parts)} rows={total_rows}")