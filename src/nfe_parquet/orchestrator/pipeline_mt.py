from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Set, Tuple, List

from ..config.models import AppConfig
from ..domain.models import SourceMeta
from ..io.scanner import WorkItem, scan_source
from ..io.zip_extract import extract_zip_to_temp
from ..observability.setup import get_logger
from ..parse.nfe_parser import parse_nfe_xml
from ..transform.filters import is_year_allowed
from ..write.atomic_commit import atomic_replace
from ..schema.parquet_schema import get_arrow_schema

import pyarrow as pa
import pyarrow.parquet as pq

from .chunking import chunked
from .pipeline_cte_mt import run_cte_mt

from ..checkpoint.store_sqlite import SQLiteCheckpointStore, CheckpointKey
from ..checkpoint.fingerprint import fingerprint_size_mtime
from ..transform.window import last_n_months_yyyymm

log = get_logger(__name__)


def run_once_mt(cfg: AppConfig) -> bool:
    """
    Executa o pipeline completo.

    Returns:
        True  — se qualquer origem teve errors > 0 (agendador deve tratar como falha)
        False — tudo correu sem erros
    """
    ingested_at = datetime.now(timezone.utc)

    ckpt = SQLiteCheckpointStore(cfg.checkpoint.sqlite_path)

    log.info("checkpoint_cache_load_start")
    t0 = time.perf_counter()
    ckpt.load_cache()
    log.info("checkpoint_cache_load_done", extra={"elapsed_ms": round((time.perf_counter() - t0) * 1000)})

    moving_months = last_n_months_yyyymm(ingested_at, cfg.rules.moving_window_months)
    log.info("moving_window_set", extra={"months": sorted(moving_months)})

    had_errors = False

    # ── NF-e ──────────────────────────────────────────────────────────────────
    for source_name, input_root, output_dir in (
        ("importados", cfg.paths.input_importados, cfg.paths.output_importados),
        ("processados", cfg.paths.input_processados, cfg.paths.output_processados),
    ):
        source_had_errors = _run_source_mt(
            cfg=cfg,
            source_name=source_name,
            input_root=input_root,
            output_dir=output_dir,
            ingested_at=ingested_at,
            ckpt=ckpt,
            moving_months=moving_months,
        )
        if source_had_errors:
            had_errors = True

    # ── CT-e (mesma pasta de processados, mesmo ckpt, mesma janela) ───────────
    log.info("cte_pipeline_start")
    cte_had_errors = run_cte_mt(cfg=cfg, ckpt=ckpt, moving_months=moving_months)
    if cte_had_errors:
        had_errors = True
    log.info("cte_pipeline_finish")

    return had_errors


def _run_source_mt(
    cfg: AppConfig,
    source_name: str,
    input_root: Path,
    output_dir: Path,
    ingested_at: datetime,
    ckpt: SQLiteCheckpointStore,
    moving_months: Set[str],
) -> bool:
    ctx = {"source": source_name}

    if not input_root.exists():
        log.error(
            "input_dir_not_found",
            extra={**ctx, "root": str(input_root)},
        )
        return True  # propaga como error → exit code 1

    schema = get_arrow_schema()
    parts_root = cfg.paths.staging_dir / "parts"
    commit_tmp_root = cfg.paths.staging_dir / "commit_tmp"

    buffers: dict[str, list[dict]] = defaultdict(list)
    part_counter: dict[str, int] = defaultdict(int)
    pending_checkpoints: List[CheckpointKey] = []

    chunk_size = cfg.performance.file_chunk_size
    max_workers = cfg.performance.max_workers
    max_buffer_records = cfg.performance.record_chunk_size

    log.info("scan_start", extra={**ctx, "root": str(input_root)})
    t_scan = time.perf_counter()
    work_items = list(scan_source(input_root, source_name))
    scan_elapsed = round((time.perf_counter() - t_scan) * 1000)

    n_xml = sum(1 for it in work_items if it.file_type == "xml")
    n_zip = sum(1 for it in work_items if it.file_type == "zip")
    log.info(
        "scan_done",
        extra={**ctx, "total": len(work_items), "xml": n_xml, "zip": n_zip, "elapsed_ms": scan_elapsed},
    )

    if not work_items:
        log.warning("scan_empty", extra=ctx)
        return False

    ok = filtered = errors = skipped = 0
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
                                source=source_name,
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
        extra={**ctx, "ok": ok, "filtered": filtered, "skipped": skipped, "errors": errors, "elapsed_ms": proc_elapsed},
    )

    for month, recs in list(buffers.items()):
        if not recs:
            continue
        try:
            _flush_month_buffer(
                schema=schema,
                source=source_name,
                month=month,
                records=recs,
                parts_root=parts_root,
                part_counter=part_counter,
            )
            buffers[month].clear()
        except Exception as exc:
            errors += 1
            log.error("final_flush_error", extra={**ctx, "month": month, "error": str(exc)}, exc_info=True)

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
        except Exception as exc:
            errors += 1
            log.error("compact_error", extra={**ctx, "month": month, "error": str(exc)}, exc_info=True)

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
            "errors": errors,
            "months_written": months_written,
            "months": sorted(months_seen),
        },
    )

    return errors > 0


def _process_work_item(
    cfg: AppConfig, it: WorkItem, ingested_at: datetime, ckpt: SQLiteCheckpointStore
) -> Tuple[list[dict], CheckpointKey] | None:
    if it.file_type == "xml":
        return _process_xml(it, ingested_at, ckpt)
    return _process_zip(cfg, it, ingested_at, ckpt)


def _process_xml(
    it: WorkItem, ingested_at: datetime, ckpt: SQLiteCheckpointStore
) -> Tuple[list[dict], CheckpointKey] | None:
    fingerprint = fingerprint_size_mtime(it.file_path)
    key = CheckpointKey(
        source=it.source,
        source_file_path=str(it.file_path),
        source_entry_path=None,
        fingerprint=fingerprint,
    )

    if ckpt.was_processed(key):
        log.debug("xml_skipped_checkpoint", extra={"source": it.source, "file": str(it.file_path)})
        return None

    xml_bytes = it.file_path.read_bytes()

    if not xml_bytes.strip():
        log.warning("xml_empty_skipped", extra={"source": it.source, "file": str(it.file_path)})
        return None

    meta = SourceMeta(
        source=it.source,
        source_root=it.source_root,
        source_file_path=it.file_path,
        source_file_type="xml",
        source_entry_path=None,
        source_file_mtime=datetime.fromtimestamp(it.file_path.stat().st_mtime),
    )
    result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

    if result.warnings:
        log.debug(
            "xml_parse_warnings",
            extra={"source": it.source, "file": str(it.file_path), "warnings": result.warnings},
        )

    return [result.record], key


def _process_zip(
    cfg: AppConfig, it: WorkItem, ingested_at: datetime, ckpt: SQLiteCheckpointStore
) -> Tuple[list[dict], CheckpointKey] | None:
    zip_fingerprint = fingerprint_size_mtime(it.file_path)
    zip_key = CheckpointKey(
        source=it.source,
        source_file_path=str(it.file_path),
        source_entry_path=None,
        fingerprint=zip_fingerprint,
    )

    if ckpt.was_processed(zip_key):
        log.debug("zip_skipped_checkpoint", extra={"source": it.source, "file": str(it.file_path)})
        return None

    zip_mtime = datetime.fromtimestamp(it.file_path.stat().st_mtime)
    out: list[dict] = []
    xml_count = 0

    with extract_zip_to_temp(it.file_path, cfg.paths.tmp_extract_dir) as tmp_dir:
        for xml_path in tmp_dir.rglob("*.xml"):
            entry_path = str(xml_path.relative_to(tmp_dir))
            xml_bytes = xml_path.read_bytes()

            if not xml_bytes.strip():
                log.warning(
                    "zip_entry_empty_skipped",
                    extra={"source": it.source, "zip": str(it.file_path), "entry": entry_path},
                )
                continue

            meta = SourceMeta(
                source=it.source,
                source_root=it.source_root,
                source_file_path=it.file_path,
                source_file_type="zip",
                source_entry_path=entry_path,
                source_file_mtime=zip_mtime,
            )
            result = parse_nfe_xml(xml_bytes, meta=meta, ingested_at=ingested_at)

            if result.warnings:
                log.debug(
                    "zip_entry_parse_warnings",
                    extra={
                        "source": it.source,
                        "zip": str(it.file_path),
                        "entry": entry_path,
                        "warnings": result.warnings,
                    },
                )

            out.append(result.record)
            xml_count += 1

    log.debug("zip_processed", extra={"source": it.source, "file": str(it.file_path), "xml_count": xml_count})
    return out, zip_key


def _flush_month_buffer(
    schema: pa.Schema,
    source: str,
    month: str,
    records: list[dict],
    parts_root: Path,
    part_counter: dict[str, int],
) -> None:
    if not records:
        return

    parts_dir = parts_root / source / month
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
            "source": source,
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
    source: str,
    month: str,
    output_dir: Path,
) -> None:
    parts_dir = parts_root / source / month
    parts = sorted(parts_dir.glob("part-*.parquet"))
    if not parts:
        log.warning("compact_no_parts", extra={"source": source, "month": month})
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / f"{month}.parquet"
    tmp_path = commit_tmp_root / source / f"{month}.parquet.tmp"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("compact_start", extra={"source": source, "month": month, "parts": len(parts)})
    t0 = time.perf_counter()

    # ── Concatenar todas as parts ──────────────────────────────────────────────
    tables = []
    total_rows_raw = 0
    for p in parts:
        t = pq.read_table(p, schema=schema)
        tables.append(t)
        total_rows_raw += t.num_rows

    if not tables:
        log.warning("compact_no_data", extra={"source": source, "month": month})
        return

    combined = pa.compute.list_flatten(tables) if len(tables) > 1 else tables[0]

    # ── Dedupe por chNFe (NF-e) ────────────────────────────────────────────────
    # Critério: chave asc, source_file_mtime_ns desc, ingested_at desc, source_file_path desc, source_entry_path desc
    key_col = "chNFe"
    sort_keys = [
        (key_col, "ascending"),
        ("source_file_mtime_ns", "descending"),
        ("ingested_at", "descending"),
        ("source_file_path", "descending"),
        ("source_entry_path", "descending"),
    ]

    # Filtrar nulos de chave (logar e descartar)
    valid_mask = pc.is_null(combined[key_col], nan_is_null=True)
    null_keys_count = pc.count(valid_mask).as_py()
    combined = pc.filter(combined, pc.invert(valid_mask))

    # Ordenar
    sorted_indices = pc.sort_indices(combined, sort_keys=sort_keys)
    sorted_table = pc.take(combined, sorted_indices)

    # Manter primeira ocorrência por chave (dedupe)
    key_column = sorted_table[key_col]
    is_first = pc.unique(key_column, count_option="first").indices
    deduped = pc.take(sorted_table, is_first)

    total_rows_after = deduped.num_rows
    dupes_removed = total_rows_raw - total_rows_after

    log.info(
        "compact_dedupe",
        extra={
            "source": source,
            "month": month,
            "rows_before": total_rows_raw,
            "rows_after": total_rows_after,
            "dupes_removed": dupes_removed,
            "null_keys": null_keys_count,
        },
    )

    # ── Escrever final ─────────────────────────────────────────────────────────
    pq.write_table(deduped, tmp_path)
    atomic_replace(tmp_path, final_path)

    import shutil
    shutil.rmtree(parts_dir, ignore_errors=True)

    log.info(
        "compact_done",
        extra={
            "source": source,
            "month": month,
            "parts": len(parts),
            "rows": total_rows_after,
            "output": str(final_path),
            "elapsed_ms": round((time.perf_counter() - t0) * 1000),
        },
    )