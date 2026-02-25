"""
src/nfe_parquet/observability/json_formatter.py

Formatter JSON estruturado para produção.
Substitui o arquivo json_looger.py (com typo no nome).
"""
from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """
    Formata cada LogRecord como uma linha JSON com campos padronizados.

    Campos fixos em toda linha:
        ts          – ISO-8601 UTC com timezone explícito
        level       – CRITICAL / ERROR / WARNING / INFO / DEBUG
        logger      – nome do logger (ex: nfe_parquet.orchestrator)
        message     – mensagem formatada (suporta % e f-string)

    Campos opcionais (presentes somente quando relevantes):
        exc_info    – traceback completo (somente em exceções)
        source      – ex: "importados" | "processados"  (se injetado via extra={})
        month       – "202601" etc.  (se injetado via extra={})
        [**extra]   – qualquer chave passada em extra={} no log call
    """

    # Campos internos do LogRecord que NÃO devem vazar para o JSON como "extra"
    _SKIP_ATTRS: frozenset[str] = frozenset(
        {
            "args", "created", "exc_info", "exc_text", "filename",
            "funcName", "levelname", "levelno", "lineno", "message",
            "module", "msecs", "msg", "name", "pathname", "process",
            "processName", "relativeCreated", "stack_info", "taskName",
            "thread", "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        # Garante que record.message existe antes de qualquer acesso
        record.message = record.getMessage()

        payload: dict[str, Any] = {
            "ts": datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        # Traceback estruturado – nunca perde stack em produção
        if record.exc_info and record.exc_info[0] is not None:
            payload["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        # Injeta campos extras passados via extra={} no call site
        for key, value in record.__dict__.items():
            if key not in self._SKIP_ATTRS:
                try:
                    # Garante que o valor seja serializável
                    json.dumps(value)
                    payload[key] = value
                except (TypeError, ValueError):
                    payload[key] = repr(value)

        return json.dumps(payload, ensure_ascii=False, default=str)