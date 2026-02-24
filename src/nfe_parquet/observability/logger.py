from __future__ import annotations

import logging
from typing import Optional


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Cria logger básico (handler console)."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.propagate = False
    return logger