"""
tests/test_transform.py

Testes das funções puras de transformação e filtragem:
  - transform/filters.py  → is_year_allowed
  - transform/window.py   → last_n_months_yyyymm
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nfe_parquet.transform.filters import is_year_allowed
from nfe_parquet.transform.window import last_n_months_yyyymm


# ══════════════════════════════════════════════════════════════════════════════
# is_year_allowed
# ══════════════════════════════════════════════════════════════════════════════

class TestIsYearAllowed:
    def test_none_retorna_false(self):
        assert is_year_allowed(None, min_year=2026) is False

    def test_ano_igual_ao_minimo_retorna_true(self):
        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert is_year_allowed(dt, min_year=2026) is True

    def test_ano_acima_do_minimo_retorna_true(self):
        dt = datetime(2027, 6, 15, tzinfo=timezone.utc)
        assert is_year_allowed(dt, min_year=2026) is True

    def test_ano_abaixo_do_minimo_retorna_false(self):
        dt = datetime(2025, 12, 31, tzinfo=timezone.utc)
        assert is_year_allowed(dt, min_year=2026) is False

    def test_ano_muito_antigo_retorna_false(self):
        dt = datetime(2015, 1, 1, tzinfo=timezone.utc)
        assert is_year_allowed(dt, min_year=2026) is False

    def test_sem_timezone_funciona(self):
        """Datetime sem timezone também deve funcionar (vinda do fallback dEmi)."""
        dt = datetime(2026, 3, 10)
        assert is_year_allowed(dt, min_year=2026) is True

    def test_min_year_zero_aceita_tudo(self):
        dt = datetime(1900, 1, 1, tzinfo=timezone.utc)
        assert is_year_allowed(dt, min_year=0) is True


# ══════════════════════════════════════════════════════════════════════════════
# last_n_months_yyyymm
# ══════════════════════════════════════════════════════════════════════════════

class TestLastNMonthsYyyyMm:
    def test_n1_retorna_apenas_mes_atual(self):
        now = datetime(2026, 2, 25, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=1)
        assert result == {"202602"}

    def test_n2_retorna_dois_meses(self):
        now = datetime(2026, 2, 25, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=2)
        assert result == {"202602", "202601"}

    def test_n3_retorna_tres_meses(self):
        now = datetime(2026, 2, 25, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=3)
        assert result == {"202602", "202601", "202512"}

    def test_virada_de_ano(self):
        """Janeiro do ano atual deve incluir dezembro do ano anterior."""
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=2)
        assert result == {"202601", "202512"}

    def test_virada_de_ano_n12(self):
        """12 meses partindo de janeiro cobre o mês atual + 11 anteriores.
        Com n=12 e now=2026-01, o resultado é 202601..202502 (não inclui 202501,
        que seria o 13º mês).
        """
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=12)
        assert "202601" in result   # mês atual
        assert "202502" in result   # 11 meses atrás (último da janela)
        assert "202501" not in result  # fora da janela — seria o 13º mês
        assert len(result) == 12

    def test_resultado_e_set(self):
        now = datetime(2026, 2, 1, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=3)
        assert isinstance(result, set)

    def test_sem_duplicatas(self):
        """n=1 nunca deve ter duplicatas."""
        now = datetime(2026, 6, 30, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=1)
        assert len(result) == 1

    def test_n_grande_nao_repete_meses(self):
        now = datetime(2026, 2, 25, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=24)
        assert len(result) == 24  # set não tem duplicatas

    def test_formato_yyyymm(self):
        """Todos os elementos devem ser strings de 6 dígitos."""
        now = datetime(2026, 2, 25, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=6)
        for m in result:
            assert isinstance(m, str)
            assert len(m) == 6
            assert m.isdigit()

    def test_mes_de_dezembro_formatado_corretamente(self):
        now = datetime(2025, 12, 31, tzinfo=timezone.utc)
        result = last_n_months_yyyymm(now, n=1)
        assert "202512" in result