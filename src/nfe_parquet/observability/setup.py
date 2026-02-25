"""
src/nfe_parquet/observability/setup.py

Ponto único de inicialização do sistema de logging.
Deve ser chamado UMA VEZ, no início do processo (cli.py → main()).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config.models import LoggingConfig

from .json_formatter import JsonFormatter

# Logger raiz do pacote – todos os sub-loggers herdam dele
_ROOT_LOGGER_NAME = "nfe_parquet"


def setup_logging(log_cfg: "LoggingConfig", log_file: Path | None = None) -> None:
    """
    Configura o logger raiz 'nfe_parquet' com:
      - Handler de console (stderr) sempre ativo
      - Handler de arquivo (opcional) via log_file ou log_cfg.file_path
      - Formato JSON (produção) ou texto legível (desenvolvimento)

    Args:
        log_cfg:  instância de LoggingConfig (level, json, file_path, etc.)
        log_file: caminho de arquivo de log; sobrescreve log_cfg.file_path se informado
    """
    root = logging.getLogger(_ROOT_LOGGER_NAME)

    # Evita registrar handlers duplicados se setup_logging for chamado mais de uma vez
    if root.handlers:
        root.handlers.clear()

    numeric_level = logging.getLevelName(log_cfg.level.upper())
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    root.setLevel(numeric_level)
    root.propagate = False  # não repassa para o root logger do Python

    # ── Formatter ──────────────────────────────────────────────────────────
    if log_cfg.json:
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    # ── Console handler (stderr) ────────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_level)
    root.addHandler(console_handler)

    # ── File handler (opcional) ─────────────────────────────────────────────
    effective_log_file = log_file or getattr(log_cfg, "file_path", None)
    if effective_log_file:
        effective_log_file = Path(effective_log_file)
        effective_log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(effective_log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(numeric_level)
        root.addHandler(file_handler)
        root.info(
            "logging_initialized",
            extra={"log_file": str(effective_log_file), "level": log_cfg.level, "json": log_cfg.json},
        )
    else:
        root.info(
            "logging_initialized",
            extra={"log_file": None, "level": log_cfg.level, "json": log_cfg.json},
        )


def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger filho de 'nfe_parquet'.

    Uso:
        log = get_logger(__name__)
        log.info("mensagem", extra={"source": "importados", "month": "202601"})
    """
    # Se name já começa com o prefixo do pacote, usa direto; caso contrário, prefixa
    if name.startswith(_ROOT_LOGGER_NAME):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")