"""
src/nfe_parquet/observability/__init__.py

API pública do módulo de observabilidade.
Uso em qualquer módulo do pacote:

    from nfe_parquet.observability import get_logger
    log = get_logger(__name__)
"""
from .setup import get_logger, setup_logging

__all__ = ["get_logger", "setup_logging"]