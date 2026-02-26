"""
src/nfe_parquet/cli.py

Ponto de entrada da aplicação.
setup_logging() é chamado AQUI, uma única vez, antes de qualquer outra coisa.
"""
from __future__ import annotations

import argparse
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
    parser.add_argument("config", type=Path, help="Caminho para config.yaml")
    args = parser.parse_args()

    # ── 1. Carrega configuração ──────────────────────────────────────────────
    try:
        cfg = load_config(args.config)
    except Exception as exc:
        # Ainda não há logger configurado — usa print para não perder o erro
        print(f"[FATAL] Falha ao carregar config: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── 2. Inicializa logging (UMA VEZ, aqui) ───────────────────────────────
    setup_logging(cfg.logging)
    log = get_logger(__name__)

    # ── 3. Executa pipeline ──────────────────────────────────────────────────
    log.info("pipeline_start", extra={"config_path": str(args.config)})
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
        log.warning(
            "pipeline_finish_with_errors",
            extra={"elapsed_s": round(elapsed, 3)},
        )
        sys.exit(1)

    log.info("pipeline_finish", extra={"elapsed_s": round(elapsed, 3)})


if __name__ == "__main__":
    main()