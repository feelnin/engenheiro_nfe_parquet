"""
tests/test_compact_month.py

Testes de integração para _compact_month (NF-e) e _compact_month (CT-e).

Contexto
--------
A função _compact_month é o coração da etapa final do pipeline: ela recebe
os arquivos parciais (parts) gerados durante o processamento paralelo,
os concatena em uma única tabela, remove duplicatas por chave de acesso
e grava o Parquet final de forma atômica.

Estes testes cobrem especificamente os bugs corrigidos:
  - Bug 1: pa.compute.list_flatten(tables) → pa.concat_tables(tables)
            (quebrava quando um mês tinha mais de record_chunk_size registros)
  - Bug 2: pc.unique(..., count_option="first").indices → API inexistente
            (dedupe nunca funcionava)
  - Bug 3: sort_key "source_file_mtime_ns" → coluna não existia no schema
            (pc.sort_indices falhava em toda execução com registros)

Cada cenário é documentado com o comportamento esperado e por que ele importa.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nfe_parquet.orchestrator.pipeline_mt import _compact_month
from nfe_parquet.orchestrator.pipeline_cte_mt import (
    _compact_month as _compact_month_cte,
)
from nfe_parquet.schema.parquet_schema import get_arrow_schema
from nfe_parquet.schema.cte_schema import get_cte_arrow_schema


# ── Constantes de teste ────────────────────────────────────────────────────────

_MONTH = "202602"
_SOURCE = "processados"

_TS_OLD = datetime(2026, 2, 1, 8, 0, tzinfo=timezone.utc)   # mtime antigo
_TS_NEW = datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc)  # mtime recente


# ── Helpers ────────────────────────────────────────────────────────────────────

def _nfe_record(
    chNFe: str,
    source_file_mtime: datetime | None = None,
    ingested_at: datetime | None = None,
    nNF: str = "1",
) -> dict:
    """Cria um registro NF-e mínimo e válido para testes de compactação."""
    ts = source_file_mtime or _TS_NEW
    return {
        "dhEmi": _TS_NEW,
        "nNF": nNF,
        "emit_CNPJ": "11222333000100",
        "emit_xNome": "EMITENTE TESTE LTDA",
        "dest_CNPJ": "44555666000100",
        "dest_xNome": "DESTINATARIO TESTE SA",
        "vBCST": None,
        "emit_UF": "SP",
        "dest_UF": "RJ",
        "emit_xMun": "SAO PAULO",
        "dest_xMun": "RIO DE JANEIRO",
        "chNFe": chNFe,
        "cMunFG": "3550308",
        "natOp": "VENDA DE MERCADORIA",
        "qBCMonoRet": None,
        "modFrete": "0",
        "infCpl": None,
        "transp_transporta_xNome": None,
        "transp_transporta_CNPJ": None,
        "transp_veic_placa": None,
        "itens_cProd": ["P001"],
        "itens_xProd": ["PRODUTO TESTE"],
        "itens_CFOP": ["5102"],
        "itens_qCom": [Decimal("1.000000000000")],
        "itens_uCom": ["UN"],
        "itens_vUnCom": [Decimal("100.000000000000")],
        "itens_vProd": [Decimal("100.000000000000")],
        "dup_nDup": ["001"],
        "dup_dVenc": [None],
        "pag_tPag": ["01"],
        "ref_aaaamm": _MONTH,
        "source": _SOURCE,
        "source_root": "C:/dados",
        "source_file_path": "C:/dados/nfe.xml",
        "source_file_type": "xml",
        "source_entry_path": None,
        "source_file_mtime": ts,
        "ingested_at": ingested_at or _TS_NEW,
        "parser_warnings": None,
    }


def _cte_record(
    chCTe: str,
    source_file_mtime: datetime | None = None,
    ingested_at: datetime | None = None,
) -> dict:
    """Cria um registro CT-e mínimo e válido para testes de compactação."""
    ts = source_file_mtime or _TS_NEW
    return {
        "serie": "005",
        "dhEmi": _TS_NEW,
        "nCT": "1001",
        "CFOP": "5353",
        "natOp": "PRESTACAO DE SERVICO DE TRANSPORTE",
        "mod": "57",
        "xMunEnv": "SAO PAULO",
        "toma": "3",
        "emit_CNPJ": "11222333000100",
        "emit_xNome": "TRANSPORTADORA TESTE LTDA",
        "emit_UF": "SP",
        "emit_xMun": "SAO PAULO",
        "dest_CNPJ": "44555666000100",
        "dest_xNome": "DESTINATARIO TESTE SA",
        "dest_UF": "RJ",
        "dest_xMun": "RIO DE JANEIRO",
        "rem_CNPJ": "77888999000100",
        "rem_xNome": "REMETENTE TESTE SA",
        "rem_xMun": "SAO PAULO",
        "exped_xNome": None,
        "receb_xNome": None,
        "transp_xNome": None,
        "transp_CNPJ": None,
        "vCarga": Decimal("10000.000000000000"),
        "proPred": "MERCADORIAS EM GERAL",
        "vTPrest": Decimal("500.000000000000"),
        "vICMS00": Decimal("60.000000000000"),
        "vICMS10": None,
        "xObs": None,
        "chCTe": chCTe,
        "infQ_qCarga": [Decimal("1000.000000000000")],
        "infNFe_chave": ["35260111222333000100550010000010011000010014"],
        "transp_qVol": [],
        "ref_aaaamm": _MONTH,
        "source": "processados_cte",
        "source_root": "C:/dados",
        "source_file_path": "C:/dados/cte.xml",
        "source_file_type": "xml",
        "source_entry_path": None,
        "source_file_mtime": ts,
        "ingested_at": ingested_at or _TS_NEW,
        "parser_warnings": None,
    }


def _write_part(records: list[dict], path: Path, schema: pa.Schema) -> None:
    """Grava uma lista de registros como arquivo part Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, path)


def _read_output(path: Path) -> list[dict]:
    """Lê o Parquet de saída e retorna como lista de dicts."""
    return pq.read_table(path).to_pylist()


def _setup_dirs(tmp_path: Path):
    """Retorna os diretórios padrão de staging para os testes."""
    parts_root = tmp_path / "staging" / "parts"
    commit_tmp_root = tmp_path / "staging" / "commit_tmp"
    output_dir = tmp_path / "output"
    return parts_root, commit_tmp_root, output_dir


# ══════════════════════════════════════════════════════════════════════════════
# NF-e — _compact_month
# ══════════════════════════════════════════════════════════════════════════════

class TestCompactMonthNfe:
    """
    Testa _compact_month do pipeline NF-e.

    Cada teste cria partes (part-*.parquet) no diretório de staging e chama
    _compact_month diretamente, verificando o resultado no diretório de saída.
    """

    def _call(self, tmp_path: Path, records_per_part: list[list[dict]]) -> Path:
        """
        Helper: grava N parts com os registros fornecidos e chama _compact_month.
        Retorna o caminho do Parquet final gerado.
        """
        schema = get_arrow_schema()
        parts_root, commit_tmp_root, output_dir = _setup_dirs(tmp_path)
        parts_dir = parts_root / _SOURCE / _MONTH

        for i, records in enumerate(records_per_part):
            _write_part(records, parts_dir / f"part-{i:06d}.parquet", schema)

        _compact_month(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            source=_SOURCE,
            month=_MONTH,
            output_dir=output_dir,
        )
        return output_dir / f"{_MONTH}.parquet"

    # ── Comportamento básico ───────────────────────────────────────────────────

    def test_single_part_creates_output_parquet(self, tmp_path):
        """Um único part deve gerar o Parquet de saída com os mesmos registros."""
        ch = "52260211222333000100550010000000011000000011"
        final = self._call(tmp_path, [[_nfe_record(ch)]])

        assert final.exists(), "Parquet final não foi criado"
        rows = _read_output(final)
        assert len(rows) == 1
        assert rows[0]["chNFe"] == ch

    def test_output_parquet_has_correct_month_name(self, tmp_path):
        """O arquivo de saída deve ser nomeado com o mês AAAAMM."""
        ch = "52260211222333000100550010000000021000000021"
        final = self._call(tmp_path, [[_nfe_record(ch)]])

        assert final.name == f"{_MONTH}.parquet"

    # ── Bug 1: pa.concat_tables ────────────────────────────────────────────────

    def test_multiple_parts_are_concatenated(self, tmp_path):
        """
        TESTA O BUG 1: pa.compute.list_flatten → pa.concat_tables

        Dois parts com chaves distintas devem resultar em 2 linhas no Parquet final.
        Antes da correção, isso causava TypeError ao tentar concatenar tabelas
        com a API errada.
        """
        ch_a = "52260211222333000100550010000000031000000031"
        ch_b = "52260211222333000100550010000000041000000041"

        final = self._call(
            tmp_path,
            [
                [_nfe_record(ch_a)],  # part-0: 1 registro
                [_nfe_record(ch_b)],  # part-1: 1 registro diferente
            ],
        )

        rows = _read_output(final)
        chaves = {r["chNFe"] for r in rows}
        assert len(rows) == 2, f"Esperado 2 linhas, obtido {len(rows)}"
        assert ch_a in chaves
        assert ch_b in chaves

    def test_three_parts_all_records_present(self, tmp_path):
        """Três parts com chaves distintas → 3 linhas no Parquet final."""
        chaves_input = [
            "52260211222333000100550010000000051000000051",
            "52260211222333000100550010000000061000000061",
            "52260211222333000100550010000000071000000071",
        ]

        final = self._call(
            tmp_path,
            [[_nfe_record(ch)] for ch in chaves_input],
        )

        rows = _read_output(final)
        assert len(rows) == 3
        assert {r["chNFe"] for r in rows} == set(chaves_input)

    # ── Bug 2: dedupe por chNFe ────────────────────────────────────────────────

    def test_duplicate_chNFe_across_parts_is_deduplicated(self, tmp_path):
        """
        TESTA O BUG 2: pc.unique(...).indices → lógica com set

        A mesma chNFe em dois parts diferentes deve resultar em apenas 1 linha
        no Parquet final. Antes da correção, a API pc.unique era chamada com
        parâmetros inexistentes e causaria erro em runtime.
        """
        ch = "52260211222333000100550010000000081000000081"

        final = self._call(
            tmp_path,
            [
                [_nfe_record(ch, nNF="100")],  # part-0: ocorrência 1
                [_nfe_record(ch, nNF="101")],  # part-1: ocorrência 2 (mesma chave)
            ],
        )

        rows = _read_output(final)
        assert len(rows) == 1, (
            f"Esperado 1 linha após dedupe, obtido {len(rows)}. "
            "Verifique se a lógica de dedupe por chNFe está funcionando."
        )
        assert rows[0]["chNFe"] == ch

    def test_mixed_unique_and_duplicate_keys(self, tmp_path):
        """
        Parts com 3 chaves, onde 2 são únicas e 1 é duplicada.
        Resultado esperado: 3 linhas (2 únicas + 1 deduplicada).
        """
        ch_unique_1 = "52260211222333000100550010000000091000000091"
        ch_unique_2 = "52260211222333000100550010000000101000000101"
        ch_dup = "52260211222333000100550010000000111000000111"

        final = self._call(
            tmp_path,
            [
                [_nfe_record(ch_unique_1), _nfe_record(ch_dup)],
                [_nfe_record(ch_unique_2), _nfe_record(ch_dup)],
            ],
        )

        rows = _read_output(final)
        assert len(rows) == 3
        chaves = {r["chNFe"] for r in rows}
        assert ch_unique_1 in chaves
        assert ch_unique_2 in chaves
        assert ch_dup in chaves

    # ── Bug 3: source_file_mtime_ns → source_file_mtime ───────────────────────

    def test_dedupe_keeps_record_with_newer_mtime(self, tmp_path):
        """
        TESTA O BUG 3: sort_key 'source_file_mtime_ns' → 'source_file_mtime'

        Quando a mesma chNFe aparece em dois parts com mtime diferentes,
        o registro com mtime mais recente deve ser mantido.
        Antes da correção, 'source_file_mtime_ns' não existia no schema e
        pc.sort_indices falharia com KeyError.
        """
        ch = "52260211222333000100550010000000121000000121"

        final = self._call(
            tmp_path,
            [
                [_nfe_record(ch, source_file_mtime=_TS_OLD, nNF="ANTIGO")],
                [_nfe_record(ch, source_file_mtime=_TS_NEW, nNF="RECENTE")],
            ],
        )

        rows = _read_output(final)
        assert len(rows) == 1
        assert rows[0]["nNF"] == "RECENTE", (
            "O registro com mtime mais recente deveria ter sido mantido, "
            f"mas foi mantido nNF={rows[0]['nNF']!r}"
        )

    # ── Registros com chave nula ───────────────────────────────────────────────

    def test_null_chNFe_is_discarded(self, tmp_path):
        """
        Registros com chNFe nula são descartados na compactação.
        O registro válido presente no mesmo part deve ser mantido.
        """
        ch_valida = "52260211222333000100550010000000131000000131"
        rec_valido = _nfe_record(ch_valida)
        rec_sem_chave = _nfe_record(ch_valida)
        rec_sem_chave = dict(rec_sem_chave)
        rec_sem_chave["chNFe"] = None  # simula NF-e sem chave

        final = self._call(tmp_path, [[rec_valido, rec_sem_chave]])

        rows = _read_output(final)
        chaves = [r["chNFe"] for r in rows]
        assert None not in chaves, "Registro com chNFe nula não deveria estar na saída"

    # ── Escrita atômica e limpeza ──────────────────────────────────────────────

    def test_no_tmp_file_remains_after_compact(self, tmp_path):
        """Nenhum arquivo .tmp deve existir após a compactação bem-sucedida."""
        ch = "52260211222333000100550010000000141000000141"
        _, commit_tmp_root, _ = _setup_dirs(tmp_path)
        self._call(tmp_path, [[_nfe_record(ch)]])

        tmp_files = list(commit_tmp_root.rglob("*.tmp"))
        assert tmp_files == [], f"Arquivos .tmp encontrados após compactação: {tmp_files}"

    def test_staging_parts_dir_is_cleaned_after_compact(self, tmp_path):
        """O diretório de parts do mês deve ser removido após a compactação."""
        ch = "52260211222333000100550010000000151000000151"
        parts_root, _, _ = _setup_dirs(tmp_path)
        self._call(tmp_path, [[_nfe_record(ch)]])

        parts_dir = parts_root / _SOURCE / _MONTH
        assert not parts_dir.exists(), (
            f"Diretório de parts não foi removido após compactação: {parts_dir}"
        )

    def test_compact_overwrites_existing_parquet(self, tmp_path):
        """
        Executar _compact_month duas vezes substitui o Parquet anterior.
        Isso garante que reprocessamentos parciais não acumulam dados.
        """
        schema = get_arrow_schema()
        parts_root, commit_tmp_root, output_dir = _setup_dirs(tmp_path)

        ch_a = "52260211222333000100550010000000161000000161"
        ch_b = "52260211222333000100550010000000171000000171"

        # Primeira execução: 1 registro
        parts_dir = parts_root / _SOURCE / _MONTH
        _write_part([_nfe_record(ch_a)], parts_dir / "part-000000.parquet", schema)
        _compact_month(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            source=_SOURCE,
            month=_MONTH,
            output_dir=output_dir,
        )

        rows_primeira = _read_output(output_dir / f"{_MONTH}.parquet")
        assert len(rows_primeira) == 1

        # Segunda execução: 1 registro diferente (simula reprocessamento)
        parts_dir.mkdir(parents=True, exist_ok=True)
        _write_part([_nfe_record(ch_b)], parts_dir / "part-000000.parquet", schema)
        _compact_month(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            source=_SOURCE,
            month=_MONTH,
            output_dir=output_dir,
        )

        rows_segunda = _read_output(output_dir / f"{_MONTH}.parquet")
        assert len(rows_segunda) == 1
        assert rows_segunda[0]["chNFe"] == ch_b, (
            "A segunda execução deveria ter substituído o Parquet anterior"
        )


# ══════════════════════════════════════════════════════════════════════════════
# CT-e — _compact_month (pipeline_cte_mt)
# ══════════════════════════════════════════════════════════════════════════════

class TestCompactMonthCte:
    """
    Testa _compact_month do pipeline CT-e.

    O CT-e usa a mesma lógica de compactação do NF-e, mas com schema e
    chave de dedupe diferentes (chCTe). A fonte é sempre 'processados_cte'.
    """

    _CTE_SOURCE = "processados_cte"

    def _call(self, tmp_path: Path, records_per_part: list[list[dict]]) -> Path:
        """Helper: grava N parts e chama _compact_month do pipeline CT-e."""
        schema = get_cte_arrow_schema()
        parts_root, commit_tmp_root, output_dir = _setup_dirs(tmp_path)
        parts_dir = parts_root / self._CTE_SOURCE / _MONTH

        for i, records in enumerate(records_per_part):
            _write_part(records, parts_dir / f"part-{i:06d}.parquet", schema)

        _compact_month_cte(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            month=_MONTH,
            output_dir=output_dir,
        )
        return output_dir / f"{_MONTH}.parquet"

    # ── Comportamento básico ───────────────────────────────────────────────────

    def test_single_part_creates_output_parquet(self, tmp_path):
        """Um único part CT-e deve gerar o Parquet de saída corretamente."""
        ch = "35260111222333000100570050000010011000010014"
        final = self._call(tmp_path, [[_cte_record(ch)]])

        assert final.exists()
        rows = _read_output(final)
        assert len(rows) == 1
        assert rows[0]["chCTe"] == ch

    # ── Bug 1: pa.concat_tables ────────────────────────────────────────────────

    def test_multiple_parts_are_concatenated(self, tmp_path):
        """
        TESTA O BUG 1 no pipeline CT-e: dois parts com chaves distintas
        devem resultar em 2 linhas no Parquet final.
        """
        ch_a = "35260111222333000100570050000010021000010024"
        ch_b = "35260111222333000100570050000010031000010034"

        final = self._call(
            tmp_path,
            [
                [_cte_record(ch_a)],
                [_cte_record(ch_b)],
            ],
        )

        rows = _read_output(final)
        assert len(rows) == 2, f"Esperado 2 linhas, obtido {len(rows)}"
        assert {r["chCTe"] for r in rows} == {ch_a, ch_b}

    # ── Bug 2: dedupe por chCTe ────────────────────────────────────────────────

    def test_duplicate_chCTe_across_parts_is_deduplicated(self, tmp_path):
        """
        TESTA O BUG 2 no pipeline CT-e: a mesma chCTe em dois parts distintos
        deve resultar em apenas 1 linha no Parquet final.
        """
        ch = "35260111222333000100570050000010041000010044"

        final = self._call(
            tmp_path,
            [
                [_cte_record(ch, source_file_mtime=_TS_OLD)],
                [_cte_record(ch, source_file_mtime=_TS_NEW)],
            ],
        )

        rows = _read_output(final)
        assert len(rows) == 1, (
            f"Esperado 1 linha após dedupe por chCTe, obtido {len(rows)}"
        )
        assert rows[0]["chCTe"] == ch

    # ── Bug 3: source_file_mtime ───────────────────────────────────────────────

    def test_dedupe_keeps_record_with_newer_mtime(self, tmp_path):
        """
        TESTA O BUG 3 no pipeline CT-e: quando a mesma chCTe aparece com
        mtime diferentes, o registro mais recente deve ser mantido.
        """
        ch = "35260111222333000100570050000010051000010054"

        final = self._call(
            tmp_path,
            [
                [_cte_record(ch, source_file_mtime=_TS_OLD)],
                [_cte_record(ch, source_file_mtime=_TS_NEW)],
            ],
        )

        rows = _read_output(final)
        assert len(rows) == 1
        mtime_result = rows[0]["source_file_mtime"]
        # Compara apenas o datetime sem timezone (PyArrow armazena como timestamp ms)
        expected = _TS_NEW.replace(tzinfo=None)
        assert mtime_result == expected, (
            f"Esperado mtime recente ({expected}), obtido {mtime_result}"
        )

    # ── Limpeza ───────────────────────────────────────────────────────────────

    def test_staging_parts_dir_is_cleaned_after_compact(self, tmp_path):
        """O diretório de parts CT-e deve ser removido após a compactação."""
        ch = "35260111222333000100570050000010061000010064"
        parts_root, _, _ = _setup_dirs(tmp_path)
        self._call(tmp_path, [[_cte_record(ch)]])

        parts_dir = parts_root / self._CTE_SOURCE / _MONTH
        assert not parts_dir.exists(), (
            f"Diretório de parts CT-e não foi removido: {parts_dir}"
        )

    def test_no_tmp_file_remains_after_compact(self, tmp_path):
        """Nenhum arquivo .tmp deve existir após compactação CT-e bem-sucedida."""
        ch = "35260111222333000100570050000010071000010074"
        _, commit_tmp_root, _ = _setup_dirs(tmp_path)
        self._call(tmp_path, [[_cte_record(ch)]])

        tmp_files = list(commit_tmp_root.rglob("*.tmp"))
        assert tmp_files == [], f"Arquivos .tmp encontrados: {tmp_files}"
