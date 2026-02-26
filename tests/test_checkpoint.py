"""
tests/test_checkpoint.py

Testes do SQLiteCheckpointStore:
  - Ciclo completo: load_cache → was_processed → mark_processed_batch
  - Comportamento sem cache carregada (fallback para disco)
  - Idempotência (INSERT OR REPLACE)
  - Isolamento entre sources distintas
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from nfe_parquet.checkpoint.fingerprint import fingerprint_size_mtime
from nfe_parquet.checkpoint.store_sqlite import CheckpointKey, SQLiteCheckpointStore


# ── Helpers ───────────────────────────────────────────────────────────────────

def _key(
    source: str = "importados",
    file_path: str = "/data/nfe_001.xml",
    entry_path: str | None = None,
    fingerprint: str = "1024|1708789200000000000",
) -> CheckpointKey:
    return CheckpointKey(
        source=source,
        source_file_path=file_path,
        source_entry_path=entry_path,
        fingerprint=fingerprint,
    )


def _store(tmp_path: Path) -> SQLiteCheckpointStore:
    return SQLiteCheckpointStore(tmp_path / "checkpoint.sqlite")


# ══════════════════════════════════════════════════════════════════════════════
# Criação e inicialização
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckpointStoreInit:
    def test_cria_arquivo_sqlite(self, tmp_path):
        store = _store(tmp_path)
        assert (tmp_path / "checkpoint.sqlite").exists()

    def test_cria_diretorios_pais_se_necessario(self, tmp_path):
        db_path = tmp_path / "a" / "b" / "c" / "checkpoint.sqlite"
        store = SQLiteCheckpointStore(db_path)
        assert db_path.exists()

    def test_cache_none_antes_de_load(self, tmp_path):
        store = _store(tmp_path)
        assert store._cache is None


# ══════════════════════════════════════════════════════════════════════════════
# was_processed — sem cache (fallback para disco)
# ══════════════════════════════════════════════════════════════════════════════

class TestWasProcessedSemCache:
    def test_arquivo_novo_retorna_false(self, tmp_path):
        store = _store(tmp_path)
        assert store.was_processed(_key()) is False

    def test_arquivo_processado_retorna_true(self, tmp_path):
        store = _store(tmp_path)
        key = _key()
        store.mark_processed(
            key,
            processed_at=datetime(2026, 2, 25, tzinfo=timezone.utc),
            ref_aaaamm="202602",
        )
        assert store.was_processed(key) is True

    def test_fingerprint_diferente_retorna_false(self, tmp_path):
        store = _store(tmp_path)
        key1 = _key(fingerprint="100|111")
        key2 = _key(fingerprint="200|222")
        store.mark_processed(
            key1,
            processed_at=datetime(2026, 2, 25, tzinfo=timezone.utc),
            ref_aaaamm="202602",
        )
        assert store.was_processed(key2) is False

    def test_source_diferente_retorna_false(self, tmp_path):
        store = _store(tmp_path)
        key_imp = _key(source="importados")
        key_proc = _key(source="processados")
        store.mark_processed(
            key_imp,
            processed_at=datetime(2026, 2, 25, tzinfo=timezone.utc),
            ref_aaaamm="202602",
        )
        assert store.was_processed(key_proc) is False


# ══════════════════════════════════════════════════════════════════════════════
# load_cache + was_processed — com cache em RAM
# ══════════════════════════════════════════════════════════════════════════════

class TestWasProcessedComCache:
    def test_load_cache_popula_set(self, tmp_path):
        store = _store(tmp_path)
        key = _key()
        store.mark_processed(key, datetime(2026, 2, 25, tzinfo=timezone.utc), "202602")
        store.load_cache()
        assert store._cache is not None
        assert len(store._cache) == 1

    def test_arquivo_processado_encontrado_em_cache(self, tmp_path):
        store = _store(tmp_path)
        key = _key()
        store.mark_processed(key, datetime(2026, 2, 25, tzinfo=timezone.utc), "202602")
        store.load_cache()
        assert store.was_processed(key) is True

    def test_arquivo_nao_processado_nao_encontrado_em_cache(self, tmp_path):
        store = _store(tmp_path)
        store.load_cache()
        assert store.was_processed(_key()) is False

    def test_cache_vazia_apos_load_sem_dados(self, tmp_path):
        store = _store(tmp_path)
        store.load_cache()
        assert store._cache == set()

    def test_cache_nao_e_afetada_por_inserts_posteriores(self, tmp_path):
        """Cache carregada antes do insert não deve "ver" registros novos."""
        store = _store(tmp_path)
        store.load_cache()  # cache vazia
        key = _key()
        store.mark_processed(key, datetime(2026, 2, 25, tzinfo=timezone.utc), "202602")
        # A cache ainda aponta para o estado anterior ao insert
        assert store.was_processed(key) is False


# ══════════════════════════════════════════════════════════════════════════════
# mark_processed_batch
# ══════════════════════════════════════════════════════════════════════════════

class TestMarkProcessedBatch:
    def test_batch_vazio_nao_lanca_excecao(self, tmp_path):
        store = _store(tmp_path)
        store.mark_processed_batch([], datetime(2026, 2, 25, tzinfo=timezone.utc))

    def test_batch_com_multiplas_chaves(self, tmp_path):
        store = _store(tmp_path)
        keys = [_key(file_path=f"/data/nfe_{i}.xml", fingerprint=f"{i}|{i}") for i in range(10)]
        store.mark_processed_batch(keys, datetime(2026, 2, 25, tzinfo=timezone.utc))

        store.load_cache()
        for key in keys:
            assert store.was_processed(key) is True

    def test_batch_idempotente(self, tmp_path):
        """INSERT OR REPLACE não deve falhar ao inserir a mesma chave duas vezes."""
        store = _store(tmp_path)
        key = _key()
        t = datetime(2026, 2, 25, tzinfo=timezone.utc)
        store.mark_processed_batch([key], t)
        store.mark_processed_batch([key], t)  # segunda vez — não deve lançar

        store.load_cache()
        assert store.was_processed(key) is True

    def test_batch_com_entry_path_none(self, tmp_path):
        """source_entry_path=None deve ser salvo como string vazia."""
        store = _store(tmp_path)
        key = _key(entry_path=None)
        store.mark_processed_batch([key], datetime(2026, 2, 25, tzinfo=timezone.utc))
        store.load_cache()
        assert store.was_processed(key) is True

    def test_batch_com_entry_path_preenchido(self, tmp_path):
        """ZIP com entry_path não-nulo."""
        store = _store(tmp_path)
        key = _key(entry_path="subdir/nfe_001.xml")
        store.mark_processed_batch([key], datetime(2026, 2, 25, tzinfo=timezone.utc))
        store.load_cache()
        assert store.was_processed(key) is True


# ══════════════════════════════════════════════════════════════════════════════
# fingerprint_size_mtime
# ══════════════════════════════════════════════════════════════════════════════

class TestFingerprintSizeMtime:
    def test_formato_tamanho_pipe_mtime(self, tmp_path):
        f = tmp_path / "arquivo.xml"
        f.write_bytes(b"<nfe/>")
        fp = fingerprint_size_mtime(f)
        parts = fp.split("|")
        assert len(parts) == 2
        assert parts[0].isdigit()
        assert parts[1].isdigit()

    def test_tamanho_correto(self, tmp_path):
        content = b"<nfe>12345</nfe>"
        f = tmp_path / "arquivo.xml"
        f.write_bytes(content)
        fp = fingerprint_size_mtime(f)
        tamanho = int(fp.split("|")[0])
        assert tamanho == len(content)

    def test_arquivos_iguais_mesmo_fingerprint(self, tmp_path):
        content = b"<nfe>igual</nfe>"
        f1 = tmp_path / "a.xml"
        f2 = tmp_path / "b.xml"
        f1.write_bytes(content)
        # copia mtime manualmente para garantir igualdade
        import shutil, os
        shutil.copy2(f1, f2)
        os.utime(f2, (f1.stat().st_atime, f1.stat().st_mtime))
        assert fingerprint_size_mtime(f1) == fingerprint_size_mtime(f2)

    def test_arquivo_modificado_muda_fingerprint(self, tmp_path):
        import time
        f = tmp_path / "arquivo.xml"
        f.write_bytes(b"<nfe>v1</nfe>")
        fp1 = fingerprint_size_mtime(f)
        time.sleep(0.01)
        f.write_bytes(b"<nfe>v2_maior</nfe>")
        fp2 = fingerprint_size_mtime(f)
        assert fp1 != fp2