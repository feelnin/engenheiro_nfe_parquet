from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from datetime import datetime, timezone


@dataclass(frozen=True)
class CheckpointKey:
    source: str
    source_file_path: str
    source_entry_path: str | None
    fingerprint: str


class SQLiteCheckpointStore:
    """Store SQLite para idempotência por ficheiro (ou entrada do ZIP)."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        # Inicializamos a cache como None para saber se foi carregada
        self._cache: set[tuple[str, str, str, str]] | None = None
        self._dead_letter_cache: set[str] | None = None

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as con:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoint (
                    source TEXT NOT NULL,
                    source_file_path TEXT NOT NULL,
                    source_entry_path TEXT NOT NULL DEFAULT '',
                    fingerprint TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    ref_aaaamm TEXT,
                    status TEXT NOT NULL,
                    notes TEXT,
                    PRIMARY KEY (source, source_file_path, source_entry_path, fingerprint)
                )
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_checkpoint_latest
                ON checkpoint (source, source_file_path, source_entry_path, processed_at)
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letter (
                    fingerprint   TEXT PRIMARY KEY,
                    source_path   TEXT NOT NULL,
                    error_type    TEXT NOT NULL,
                    error_message TEXT,
                    failed_at     TEXT NOT NULL,
                    retry_count   INTEGER NOT NULL DEFAULT 0
                )
                """
            )

    def load_cache(self) -> None:
        """Carrega todos os fingerprints já processados para a RAM (operações O(1))."""
        with sqlite3.connect(self.db_path) as con:
            # Selecionamos apenas as colunas que formam a nossa chave de idempotência
            rows = con.execute(
                """
                SELECT source, source_file_path, source_entry_path, fingerprint 
                FROM checkpoint
                """
            ).fetchall()
            
            # Armazenamos como um set de tuplos para pesquisas ultra-rápidas
            self._cache = set(rows)

            rows_dl = con.execute(
                "SELECT fingerprint FROM dead_letter"
            ).fetchall()
            self._dead_letter_cache = {row[0] for row in rows_dl}

    def was_processed(self, key: CheckpointKey) -> bool:
        """Verifica se já foi processado recorrendo à cache na RAM (se disponível)."""
        entry_path = key.source_entry_path or ""
        tuple_key = (key.source, key.source_file_path, entry_path, key.fingerprint)
        
        # Se a cache foi ativada, usamos a memória (MUITO mais rápido e thread-safe)
        if self._cache is not None:
            return tuple_key in self._cache

        # Fallback de segurança: vai ao disco se a cache não estiver carregada
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                """
                SELECT 1
                FROM checkpoint
                WHERE source = ? AND source_file_path = ? AND source_entry_path = ? AND fingerprint = ?
                LIMIT 1
                """,
                tuple_key,
            ).fetchone()
            return row is not None

    def mark_processed(
        self,
        key: CheckpointKey,
        processed_at: datetime,
        ref_aaaamm: str | None,
        status: str = "ok",
        notes: str | None = None,
    ) -> None:
        with sqlite3.connect(self.db_path) as con:
            entry_path = key.source_entry_path or ""
            con.execute(
                """
                INSERT OR REPLACE INTO checkpoint (
                    source, source_file_path, source_entry_path, fingerprint,
                    processed_at, ref_aaaamm, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key.source,
                    key.source_file_path,
                    entry_path,
                    key.fingerprint,
                    processed_at.isoformat(),
                    ref_aaaamm,
                    status,
                    notes,
                ),
            )

    def is_dead_letter(self, fingerprint: str) -> bool:
        """Retorna True se o fingerprint está na dead-letter queue."""
        if self._dead_letter_cache is not None:
            return fingerprint in self._dead_letter_cache
        # Fallback para disco quando a cache não foi carregada
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                "SELECT 1 FROM dead_letter WHERE fingerprint = ? LIMIT 1",
                (fingerprint,),
            ).fetchone()
            return row is not None

    def record_dead_letter(
        self,
        fingerprint: str,
        source_path: str,
        error_type: str,
        error_message: str | None,
    ) -> None:
        """
        Insere ou incrementa retry_count na dead-letter queue.

        Usa INSERT ... ON CONFLICT DO UPDATE para idempotência — failed_at é
        atualizado a cada tentativa.
        Não atualiza _dead_letter_cache intencionalmente: a cache reflete o estado
        no momento do load_cache(). Novas entradas ficam visíveis na próxima execução.
        """
        failed_at = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                """
                INSERT INTO dead_letter
                    (fingerprint, source_path, error_type, error_message, failed_at, retry_count)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(fingerprint) DO UPDATE SET
                    error_type    = excluded.error_type,
                    error_message = excluded.error_message,
                    failed_at     = excluded.failed_at,
                    retry_count   = retry_count + 1
                """,
                (fingerprint, source_path, error_type, (error_message or "")[:500], failed_at),
            )

    def mark_processed_batch(
        self,
        keys: list[CheckpointKey],
        processed_at: datetime,
        status: str = "ok",
        notes: str | None = None,
    ) -> None:
        """Realiza o commit de múltiplos checkpoints em uma única transação (Bulk Insert)."""
        if not keys:
            return

        processed_at_str = processed_at.isoformat()
        
        data_tuples = [
            (
                key.source,
                key.source_file_path,
                key.source_entry_path or "", 
                key.fingerprint,
                processed_at_str,
                None,
                status,
                notes,
            )
            for key in keys
        ]

        # isolation_level="IMMEDIATE" evita deadlocks em concorrência pesada
        with sqlite3.connect(self.db_path, isolation_level="IMMEDIATE") as con:
            # 1. Modo WAL (Write-Ahead Logging) ativo para esta conexão
            con.execute("PRAGMA journal_mode = WAL;")
            # 2. Relaxa o bloqueio de disco. Em vez de esperar o HD fisicamente gravar, 
            # ele confia no cache do Sistema Operacional. Isso deixa o INSERT centenas de vezes mais rápido.
            con.execute("PRAGMA synchronous = NORMAL;")
            # 3. Dá 64MB de RAM para o SQLite usar de cache durante a transação (o padrão é uns 2MB)
            con.execute("PRAGMA cache_size = -64000;")
            
            con.executemany(
                """
                INSERT OR REPLACE INTO checkpoint (
                    source, source_file_path, source_entry_path, fingerprint,
                    processed_at, ref_aaaamm, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                data_tuples,
            )