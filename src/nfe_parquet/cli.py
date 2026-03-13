"""
src/nfe_parquet/cli.py

Ponto de entrada da aplicação.
setup_logging() é chamado AQUI, uma única vez, antes de qualquer outra coisa.

Subcomandos disponíveis:
  python -m nfe_parquet.cli run config.yaml         # executa o pipeline
  python -m nfe_parquet.cli dead-letter list config.yaml  # lista fila de dead-letter

Compatibilidade legada preservada:
  python -m nfe_parquet.cli config.yaml             # equivalente a "run config.yaml"
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

from .config.loader import load_config
from .observability.setup import get_logger, setup_logging
from .orchestrator.pipeline_mt import run_once_mt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL NF-e (XML/ZIP) -> Parquet mensal",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    # Subcomando: run
    run_parser = subparsers.add_parser("run", help="Executa o pipeline ETL")
    run_parser.add_argument("config", type=Path, help="Caminho para config.yaml")

    # Subcomando: dead-letter
    dl_parser = subparsers.add_parser("dead-letter", help="Gerencia a fila de dead-letter")
    dl_sub = dl_parser.add_subparsers(dest="dl_command")
    dl_list = dl_sub.add_parser("list", help="Lista os arquivos na fila de dead-letter")
    dl_list.add_argument("config", type=Path, help="Caminho para config.yaml")

    args = parser.parse_args()

    if args.command == "dead-letter":
        _cmd_dead_letter(args)
    elif args.command == "run":
        _cmd_run(args.config)
    else:
        # Compatibilidade legada: python -m nfe_parquet.cli config.yaml
        legacy = argparse.ArgumentParser()
        legacy.add_argument("config", type=Path)
        legacy_args = legacy.parse_args()
        _cmd_run(legacy_args.config)


def _cmd_run(config_path: Path) -> None:
    try:
        cfg = load_config(config_path)
    except Exception as exc:
        print(f"[FATAL] Falha ao carregar config: {exc}", file=sys.stderr)
        sys.exit(1)

    setup_logging(cfg.logging)
    log = get_logger(__name__)
    log.info("pipeline_start", extra={"config_path": str(config_path)})
    t0 = time.perf_counter()

    try:
        had_errors = run_once_mt(cfg)
    except KeyboardInterrupt:
        log.warning("pipeline_interrupted_by_user")
        sys.exit(130)
    except Exception:
        log.exception("pipeline_fatal_error")
        sys.exit(1)

    elapsed = time.perf_counter() - t0

    if had_errors:
        log.warning("pipeline_finish_with_errors", extra={"elapsed_s": round(elapsed, 3)})
        sys.exit(1)

    log.info("pipeline_finish", extra={"elapsed_s": round(elapsed, 3)})


def _cmd_dead_letter(args: argparse.Namespace) -> None:
    if args.dl_command != "list":
        print("Uso: python -m nfe_parquet.cli dead-letter list config.yaml")
        sys.exit(1)

    try:
        cfg = load_config(args.config)
    except Exception as exc:
        print(f"[FATAL] Falha ao carregar config: {exc}", file=sys.stderr)
        sys.exit(1)

    from .checkpoint.store_sqlite import SQLiteCheckpointStore
    store = SQLiteCheckpointStore(cfg.checkpoint.sqlite_path)

    with sqlite3.connect(store.db_path) as con:
        rows = con.execute(
            """
            SELECT fingerprint, source_path, error_type, error_message,
                   failed_at, retry_count
            FROM dead_letter
            ORDER BY failed_at DESC
            """
        ).fetchall()

    if not rows:
        print("Dead-letter queue vazia.")
        return

    print(f"{'FINGERPRINT':<30}  {'TIPO':<25}  {'TENTATIVAS':>10}  {'ÚLTIMA FALHA':<25}  ARQUIVO")
    print("-" * 130)
    for fp, path, etype, _emsg, failed_at, retries in rows:
        fp_short = fp[:28] + ".." if len(fp) > 30 else fp
        print(f"{fp_short:<30}  {etype:<25}  {retries:>10}  {failed_at:<25}  {path}")


if __name__ == "__main__":
    main()
