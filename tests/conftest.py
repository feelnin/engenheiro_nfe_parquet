"""
tests/conftest.py

Fixtures compartilhados entre todos os módulos de teste.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

# ── Caminhos dos fixtures XML ─────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def nfe_nfeproc_bytes() -> bytes:
    """NF-e completa: nfeProc + protNFe, 2 itens, 2 duplicatas, 1 pagamento."""
    return (FIXTURES_DIR / "nfe_nfeproc.xml").read_bytes()


@pytest.fixture
def nfe_direta_demi_bytes() -> bytes:
    """NF-e sem nfeProc e com dEmi em vez de dhEmi (documento mais antigo)."""
    return (FIXTURES_DIR / "nfe_direta_demi.xml").read_bytes()


@pytest.fixture
def nfe_campos_opcionais_ausentes_bytes() -> bytes:
    """NF-e com campos opcionais ausentes (sem transportadora, sem cobr, sem infCpl)."""
    return (FIXTURES_DIR / "nfe_campos_opcionais_ausentes.xml").read_bytes()


@pytest.fixture
def cte_cteproc_bytes() -> bytes:
    """CT-e completo: cteProc + protCTe."""
    return (FIXTURES_DIR / "cte_cteproc.xml").read_bytes()


@pytest.fixture
def zip_com_xmls_path() -> Path:
    """ZIP contendo uma NF-e e um CT-e."""
    return FIXTURES_DIR / "zip_com_xmls.zip"


@pytest.fixture
def xml_vazio_bytes() -> bytes:
    """Bytes de um arquivo XML vazio."""
    return b""


@pytest.fixture
def xml_so_whitespace_bytes() -> bytes:
    """Bytes de um arquivo XML com apenas espaços/newlines."""
    return b"   \n\t  "


@pytest.fixture
def ingested_at() -> datetime:
    """Timestamp fixo para uso nos parsers."""
    return datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)