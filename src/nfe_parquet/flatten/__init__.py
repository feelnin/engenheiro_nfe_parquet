"""
src/nfe_parquet/flatten/__init__.py

Módulo de achatamento (flatten) de Parquet para consumo direto pelo Qlik Sense SaaS.

Exporta as funções públicas de flatten de NF-e e CT-e.
"""
from .flatten_nfe import flatten_nfe_parquet
from .flatten_cte import flatten_cte_parquet

__all__ = ["flatten_nfe_parquet", "flatten_cte_parquet"]