# backend/app/logging.py
import logging
import json
import sys
from datetime import datetime


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Attach structured extras if present
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            log_record.update(record.extra)

        return json.dumps(log_record)


def setup_logging(service_name: str):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)

    # Attach service name to all logs
    logging.LoggerAdapter(root, {"service": service_name})
