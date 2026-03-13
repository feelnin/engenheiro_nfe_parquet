"""
src/nfe_parquet/flatten/runner.py

Orquestrador do passo de flatten.

Responsabilidade:
-----------------
Após o pipeline principal escrever os Parquets normais (com listas), este runner
varre os diretórios de saída e gera as versões flat (_flat) correspondentes.

Integração com o pipeline principal:
-------------------------------------
Este runner é chamado ao final de run_once_mt(), após todos os pipelines NF-e e CT-e
terem concluído. Ele NÃO interfere no checkpoint, scanning, parsing ou escrita normal.

Comportamento por diretório:
-----------------------------
Para cada diretório de saída configurado (importados, processados, cte), o runner:
  1. Varre todos os arquivos *.parquet presentes
  2. Filtra apenas os que correspondem a meses na janela móvel (moving_months)
  3. Verifica se o flat já está atualizado (mtime flat >= mtime original) — se sim, pula
  4. Gera o flat via flatten_nfe_parquet ou flatten_cte_parquet

O diretório flat é sempre o diretório original com sufixo "_flat":
  C:\\saida\\processados      →  C:\\saida\\processados_flat
  C:\\saida\\importados       →  C:\\saida\\importados_flat
  C:\\saida\\cte              →  C:\\saida\\cte_flat

Tratamento de erros:
---------------------
Erros no flatten NÃO bloqueiam o checkpoint do pipeline principal (que já ocorreu antes).
O runner retorna True se houve qualquer erro, permitindo que o cli.py emita exit code 1.
"""
from __future__ import annotations

from pathlib import Path
from typing import Set

from ..config.models import AppConfig
from ..observability.setup import get_logger
from .flatten_nfe import flatten_nfe_parquet
from .flatten_cte import flatten_cte_parquet

log = get_logger(__name__)


def run_flatten(cfg: AppConfig, moving_months: Set[str]) -> bool:
    """
    Executa o flatten para todos os diretórios de saída configurados.

    Parâmetros
    ----------
    cfg:
        Configuração completa da aplicação.
    moving_months:
        Conjunto de meses AAAAMM da janela móvel (ex: {"202602", "202601"}).
        Apenas parquets destes meses serão processados.

    Retorna
    -------
    True  → se houve pelo menos 1 erro no flatten
    False → tudo concluído sem erros
    """
    had_errors = False

    # ── NF-e: importados ────────────────────────────────────────────────────
    errors = _run_flatten_dir(
        src_dir=cfg.paths.output_importados,
        flat_dir=_flat_dir(cfg.paths.output_importados),
        staging_dir=cfg.paths.staging_dir / "flatten",
        moving_months=moving_months,
        doc_type="nfe",
        source_label="importados",
    )
    if errors:
        had_errors = True

    # ── NF-e: processados ────────────────────────────────────────────────────
    errors = _run_flatten_dir(
        src_dir=cfg.paths.output_processados,
        flat_dir=_flat_dir(cfg.paths.output_processados),
        staging_dir=cfg.paths.staging_dir / "flatten",
        moving_months=moving_months,
        doc_type="nfe",
        source_label="processados",
    )
    if errors:
        had_errors = True

    # ── CT-e ────────────────────────────────────────────────────────────────
    errors = _run_flatten_dir(
        src_dir=cfg.paths.output_cte,
        flat_dir=_flat_dir(cfg.paths.output_cte),
        staging_dir=cfg.paths.staging_dir / "flatten",
        moving_months=moving_months,
        doc_type="cte",
        source_label="cte",
    )
    if errors:
        had_errors = True

    return had_errors


def _run_flatten_dir(
    src_dir: Path,
    flat_dir: Path,
    staging_dir: Path,
    moving_months: Set[str],
    doc_type: str,  # "nfe" | "cte"
    source_label: str,
) -> bool:
    """
    Processa um diretório de saída, gerando flats para todos os parquets do mês.

    Retorna True se houve erros.
    """
    ctx = {"source": source_label, "doc_type": doc_type}

    try:
        dir_exists = src_dir.exists()
    except OSError as exc:
        log.warning(
            "flatten_dir_inaccessible",
            extra={**ctx, "dir": str(src_dir), "error": str(exc),
                   "hint": "Verifique se a unidade de rede está mapeada e acessível"},
        )
        return True  # propaga como erro → exit code 1

    if not dir_exists:
        log.warning(
            "flatten_dir_not_found",
            extra={**ctx, "dir": str(src_dir),
                   "hint": "Diretório de saída não existe — pipeline ainda não gerou parquets ou unidade de rede desmapeada"},
        )
        return False

    try:
        parquet_files = sorted(src_dir.glob("*.parquet"))
    except OSError as exc:
        log.warning(
            "flatten_dir_inaccessible",
            extra={**ctx, "dir": str(src_dir), "error": str(exc),
                   "hint": "Verifique se a unidade de rede está mapeada e acessível"},
        )
        return True

    if not parquet_files:
        log.debug("flatten_dir_skip_empty", extra={**ctx, "dir": str(src_dir)})
        return False

    # Filtra apenas meses na janela móvel
    candidates = [
        p for p in parquet_files
        if p.stem in moving_months
    ]

    if not candidates:
        log.debug(
            "flatten_dir_no_candidates",
            extra={**ctx, "dir": str(src_dir), "window": sorted(moving_months)},
        )
        return False

    log.info(
        "flatten_dir_start",
        extra={**ctx, "dir": str(src_dir), "candidates": len(candidates)},
    )

    errors = 0
    skipped = 0
    processed = 0

    for src_path in candidates:
        flat_path = flat_dir / src_path.name

        # Verifica se o flat já está atualizado (evita regeneração desnecessária)
        if _flat_is_up_to_date(src_path, flat_path):
            log.debug(
                "flatten_skip_up_to_date",
                extra={**ctx, "month": src_path.stem, "flat": str(flat_path)},
            )
            skipped += 1
            continue

        try:
            if doc_type == "nfe":
                flatten_nfe_parquet(
                    src_path=src_path,
                    output_dir=flat_dir,
                    staging_dir=staging_dir,
                )
            else:
                flatten_cte_parquet(
                    src_path=src_path,
                    output_dir=flat_dir,
                    staging_dir=staging_dir,
                )
            processed += 1
        except Exception as exc:
            errors += 1
            log.error(
                "flatten_error",
                extra={
                    **ctx,
                    "month": src_path.stem,
                    "src": str(src_path),
                    "error": str(exc),
                },
                exc_info=True,
            )

    log.info(
        "flatten_dir_done",
        extra={
            **ctx,
            "dir": str(src_dir),
            "processed": processed,
            "skipped": skipped,
            "errors": errors,
        },
    )
    return errors > 0


def _flat_dir(src_dir: Path) -> Path:
    """
    Deriva o diretório flat a partir do diretório original.

    Exemplos:
      C:\\saida\\processados   →  C:\\saida\\processados_flat
      /dados/importados        →  /dados/importados_flat
    """
    return src_dir.parent / f"{src_dir.name}_flat"


def _flat_is_up_to_date(src_path: Path, flat_path: Path) -> bool:
    """
    Retorna True se o arquivo flat existe e é mais recente que o original.
    Isso evita regenerar flats que já estão atualizados.
    """
    if not flat_path.exists():
        return False
    try:
        return flat_path.stat().st_mtime >= src_path.stat().st_mtime
    except OSError:
        return False