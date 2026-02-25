"""
src/nfe_parquet/orchestrator/pipeline_cte_mt.py

Pipeline MT para CT-e — mesma lógica do pipeline_mt.py de NF-e, adaptado para:
  - Inspecionar tag raiz do XML (is_cte_xml) antes de parsear
  - Usar cte_parser e cte_schema
  - Gravar em output_cte separado
  - Compartilhar o mesmo SQLiteCheckpointStore (source = "processados_cte")
"""
from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from ..checkpoint.fingerprint import fingerprint_size_mtime
from ..checkpoint.store_sqlite import CheckpointKey, SQLiteCheckpointStore
from ..config.models import AppConfig
from ..domain.models import SourceMeta
from ..io.scanner import WorkItem, scan_source
from ..io.zip_extract import extract_zip_to_temp
from ..observability.setup import get_logger
from ..parse.cte_parser import is_cte_xml, parse_cte_xml
from ..schema.cte_schema import get_cte_arrow_schema
from ..transform.filters import is_year_allowed
from ..transform.window import last_n_months_yyyymm
from ..write.atomic_commit import atomic_replace
from .chunking import chunked

log = get_logger(__name__)

# Nome da origem no checkpoint — distinto de "processados" (NF-e)
_SOURCE_NAME = "processados_cte"


def run_cte_mt(cfg: AppConfig, ckpt: SQLiteCheckpointStore, moving_months: Set[str]) -> None:
    """
    Ponto de entrada do pipeline de CT-e.

    Recebe o ckpt já inicializado (cache carregada) e o conjunto de meses
    da janela móvel, ambos vindos do run_once_mt principal.
    """
    _run_source_cte(
        cfg=cfg,
        input_root=cfg.paths.input_processados,   # mesma pasta que NF-e processados
        output_dir=cfg.paths.output_cte,
        ingested_at=datetime.utcnow(),
        ckpt=ckpt,
        moving_months=moving_months,
    )


def _run_source_cte(
    cfg: AppConfig,
    input_root: Path,
    output_dir: Path,
    ingested_at: datetime,
    ckpt: SQLiteCheckpointStore,
    moving_months: Set[str],
) -> None:
    schema = get_cte_arrow_schema()
    parts_root = cfg.paths.staging_dir / "parts"
    commit_tmp_root = cfg.paths.staging_dir / "commit_tmp"

    buffers: dict[str, list[dict]] = defaultdict(list)
    part_counter: dict[str, int] = defaultdict(int)
    pending_checkpoints: List[CheckpointKey] = []

    chunk_size = cfg.performance.file_chunk_size
    max_workers = cfg.performance.max_workers
    max_buffer_records = cfg.performance.record_chunk_size

    ctx = {"source": _SOURCE_NAME}

    # ── Scan ─────────────────────────────────────────────────────────────────
    log.info("scan_start", extra={**ctx, "root": str(input_root)})
    t_scan = time.perf_counter()
    work_items = list(scan_source(input_root, _SOURCE_NAME))
    scan_elapsed = round((time.perf_counter() - t_scan) * 1000)

    n_xml = sum(1 for it in work_items if it.file_type == "xml")
    n_zip = sum(1 for it in work_items if it.file_type == "zip")
    log.info(
        "scan_done",
        extra={**ctx, "total": len(work_items), "xml": n_xml, "zip": n_zip, "elapsed_ms": scan_elapsed},
    )

    if not work_items:
        log.warning("scan_empty", extra=ctx)
        return

    # ── Processamento paralelo ────────────────────────────────────────────────
    ok = filtered = errors = skipped = not_cte = 0
    months_seen: set[str] = set()

    log.info("processing_start", extra={**ctx, "workers": max_workers, "chunk_size": chunk_size})
    t_proc = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for batch in chunked(iter(work_items), chunk_size):
            futures = {
                pool.submit(_process_work_item, cfg, it, ingested_at, ckpt): it
                for it in batch
            }

            for fut in as_completed(futures):
                it = futures[fut]
                try:
                    res = fut.result()
                except Exception as exc:
                    errors += 1
                    log.error(
                        "work_item_error",
                        extra={**ctx, "file": str(it.file_path), "file_type": it.file_type, "error": str(exc)},
                        exc_info=True,
                    )
                    continue

                if res is None:
                    skipped += 1
                    continue

                # Sinal explícito: XML não é CT-e (é NF-e ou outro documento)
                if res == "not_cte":
                    not_cte += 1
                    continue

                records, ckpt_key = res
                pending_checkpoints.append(ckpt_key)

                for rec in records:
                    dhEmi = rec.get("dhEmi")
                    if not is_year_allowed(dhEmi, cfg.rules.min_year):
                        filtered += 1
                        continue

                    month = rec.get("ref_aaaamm")
                    if not month or str(month) not in moving_months:
                        filtered += 1
                        continue

                    months_seen.add(str(month))
                    buffers[str(month)].append(rec)
                    ok += 1

                    if len(buffers[str(month)]) >= max_buffer_records:
                        try:
                            _flush_month_buffer(
                                schema=schema,
                                month=str(month),
                                records=buffers[str(month)],
                                parts_root=parts_root,
                                part_counter=part_counter,
                            )
                        except Exception as exc:
                            errors += 1
                            log.error(
                                "flush_error",
                                extra={**ctx, "month": month, "error": str(exc)},
                                exc_info=True,
                            )
                        finally:
                            buffers[str(month)].clear()

    proc_elapsed = round((time.perf_counter() - t_proc) * 1000)
    log.info(
        "processing_done",
        extra={
            **ctx,
            "ok": ok,
            "filtered": filtered,
            "skipped": skipped,
            "not_cte": not_cte,
            "errors": errors,
            "elapsed_ms": proc_elapsed,
        },
    )

    # ── Flush final ───────────────────────────────────────────────────────────
    for month, recs in list(buffers.items()):
        if not recs:
            continue
        try:
            _flush_month_buffer(
                schema=schema,
                month=month,
                records=recs,
                parts_root=parts_root,
                part_counter=part_counter,
            )
            buffers[month].clear()
        except Exception as exc:
            errors += 1
            log.error("final_flush_error", extra={**ctx, "month": month, "error": str(exc)}, exc_info=True)

    # ── Compactação final ─────────────────────────────────────────────────────
    months_written = 0
    for month in sorted(months_seen):
        try:
            _compact_month(
                schema=schema,
                parts_root=parts_root,
                commit_tmp_root=commit_tmp_root,
                month=month,
                output_dir=output_dir,
            )
            months_written += 1
        except Exception as exc:
            errors += 1
            log.error("compact_error", extra={**ctx, "month": month, "error": str(exc)}, exc_info=True)

    # ── Checkpoint commit ─────────────────────────────────────────────────────
    if errors == 0 and pending_checkpoints:
        log.info("checkpoint_commit_start", extra={**ctx, "count": len(pending_checkpoints)})
        t_ckpt = time.perf_counter()
        ckpt.mark_processed_batch(pending_checkpoints, ingested_at)
        log.info(
            "checkpoint_commit_done",
            extra={**ctx, "count": len(pending_checkpoints), "elapsed_ms": round((time.perf_counter() - t_ckpt) * 1000)},
        )
    elif errors > 0:
        log.warning(
            "checkpoint_commit_skipped",
            extra={**ctx, "errors": errors, "pending": len(pending_checkpoints), "reason": "pipeline_had_errors"},
        )

    log.info(
        "source_run_summary",
        extra={
            **ctx,
            "ok": ok,
            "filtered": filtered,
            "skipped": skipped,
            "not_cte": not_cte,
            "errors": errors,
            "months_written": months_written,
            "months": sorted(months_seen),
        },
    )


# ── Workers (executados em threads) ───────────────────────────────────────────

def _process_work_item(
    cfg: AppConfig,
    it: WorkItem,
    ingested_at: datetime,
    ckpt: SQLiteCheckpointStore,
) -> Tuple[list[dict], CheckpointKey] | None | str:
    if it.file_type == "xml":
        return _process_xml(it, ingested_at, ckpt)
    return _process_zip(cfg, it, ingested_at, ckpt)


def _process_xml(
    it: WorkItem,
    ingested_at: datetime,
    ckpt: SQLiteCheckpointStore,
) -> Tuple[list[dict], CheckpointKey] | None | str:
    fingerprint = fingerprint_size_mtime(it.file_path)
    key = CheckpointKey(
        source=_SOURCE_NAME,
        source_file_path=str(it.file_path),
        source_entry_path=None,
        fingerprint=fingerprint,
    )

    if ckpt.was_processed(key):
        log.debug("xml_skipped_checkpoint", extra={"source": _SOURCE_NAME, "file": str(it.file_path)})
        return None

    xml_bytes = it.file_path.read_bytes()

    # Arquivo vazio ou só whitespace: descarta sem quebrar o pipeline
    if not xml_bytes.strip():
        log.warning(
            "xml_empty_skipped",
            extra={"source": _SOURCE_NAME, "file": str(it.file_path)},
        )
        return None

    # Inspeciona tag raiz — descarta silenciosamente se não for CT-e
    if not is_cte_xml(xml_bytes):
        log.debug("xml_not_cte", extra={"source": _SOURCE_NAME, "file": str(it.file_path)})
        return "not_cte"

    meta = SourceMeta(
        source=_SOURCE_NAME,
        source_root=it.source_root,
        source_file_path=it.file_path,
        source_file_type="xml",
        source_entry_path=None,
        source_file_mtime=datetime.fromtimestamp(it.file_path.stat().st_mtime),
    )
    result = parse_cte_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

    if result.warnings:
        log.debug(
            "xml_parse_warnings",
            extra={"source": _SOURCE_NAME, "file": str(it.file_path), "warnings": result.warnings},
        )

    return [result.record], key


def _process_zip(
    cfg: AppConfig,
    it: WorkItem,
    ingested_at: datetime,
    ckpt: SQLiteCheckpointStore,
) -> Tuple[list[dict], CheckpointKey] | None | str:
    zip_fingerprint = fingerprint_size_mtime(it.file_path)
    zip_key = CheckpointKey(
        source=_SOURCE_NAME,
        source_file_path=str(it.file_path),
        source_entry_path=None,
        fingerprint=zip_fingerprint,
    )

    if ckpt.was_processed(zip_key):
        log.debug("zip_skipped_checkpoint", extra={"source": _SOURCE_NAME, "file": str(it.file_path)})
        return None

    zip_mtime = datetime.fromtimestamp(it.file_path.stat().st_mtime)
    out: list[dict] = []
    cte_count = 0
    skipped_not_cte = 0

    with extract_zip_to_temp(it.file_path, cfg.paths.tmp_extract_dir) as tmp_dir:
        for xml_path in tmp_dir.rglob("*.xml"):
            entry_path = str(xml_path.relative_to(tmp_dir))
            xml_bytes = xml_path.read_bytes()

            # Inspeciona tag raiz — pula XMLs que não são CT-e dentro do ZIP
            if not is_cte_xml(xml_bytes):
                skipped_not_cte += 1
                continue

            meta = SourceMeta(
                source=_SOURCE_NAME,
                source_root=it.source_root,
                source_file_path=it.file_path,
                source_file_type="zip",
                source_entry_path=entry_path,
                source_file_mtime=zip_mtime,
            )
            result = parse_cte_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

            if result.warnings:
                log.debug(
                    "zip_entry_parse_warnings",
                    extra={
                        "source": _SOURCE_NAME,
                        "zip": str(it.file_path),
                        "entry": entry_path,
                        "warnings": result.warnings,
                    },
                )

            out.append(result.record)
            cte_count += 1

    if cte_count == 0:
        # ZIP não continha nenhum CT-e — não marca checkpoint
        log.debug(
            "zip_no_cte_found",
            extra={"source": _SOURCE_NAME, "file": str(it.file_path), "skipped_not_cte": skipped_not_cte},
        )
        return "not_cte"

    log.debug(
        "zip_processed",
        extra={
            "source": _SOURCE_NAME,
            "file": str(it.file_path),
            "cte_count": cte_count,
            "skipped_not_cte": skipped_not_cte,
        },
    )
    return out, zip_key


# ── Escrita ───────────────────────────────────────────────────────────────────

def _flush_month_buffer(
    schema: pa.Schema,
    month: str,
    records: list[dict],
    parts_root: Path,
    part_counter: dict[str, int],
) -> None:
    if not records:
        return

    parts_dir = parts_root / _SOURCE_NAME / month
    parts_dir.mkdir(parents=True, exist_ok=True)

    idx = part_counter[month]
    part_counter[month] += 1

    part_path = parts_dir / f"part-{idx:06d}.parquet"
    t0 = time.perf_counter()
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, part_path)

    log.debug(
        "flush_part",
        extra={
            "source": _SOURCE_NAME,
            "month": month,
            "part": part_path.name,
            "rows": table.num_rows,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000),
        },
    )


def _compact_month(
    schema: pa.Schema,
    parts_root: Path,
    commit_tmp_root: Path,
    month: str,
    output_dir: Path,
) -> None:
    parts_dir = parts_root / _SOURCE_NAME / month
    parts = sorted(parts_dir.glob("part-*.parquet"))
    if not parts:
        log.warning("compact_no_parts", extra={"source": _SOURCE_NAME, "month": month})
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / f"{month}.parquet"
    tmp_path = commit_tmp_root / _SOURCE_NAME / f"{month}.parquet.tmp"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("compact_start", extra={"source": _SOURCE_NAME, "month": month, "parts": len(parts)})
    t0 = time.perf_counter()

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
    log.info(
        "compact_done",
        extra={
            "source": _SOURCE_NAME,
            "month": month,
            "parts": len(parts),
            "rows": total_rows,
            "output": str(final_path),
            "elapsed_ms": round((time.perf_counter() - t0) * 1000),
        },
    )