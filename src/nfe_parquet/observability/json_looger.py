from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formatter JSON simples para logs estruturados."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)