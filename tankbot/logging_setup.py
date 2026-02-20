import logging
import os
import datetime as dt
from collections import deque
from logging.handlers import RotatingFileHandler


def _int_env(key: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(key, str(default))
    try:
        value = int((raw or "").strip())
    except Exception:
        value = int(default)
    return max(int(minimum), value)


def _error_buffer_size() -> int:
    return _int_env("LOG_HEALTH_ERROR_BUFFER", 20, minimum=5)

_ERROR_EVENTS: deque[dict[str, str]] = deque(maxlen=_error_buffer_size())

class _HealthErrorBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.ERROR:
            return
        try:
            ts = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            msg = record.getMessage()
            _ERROR_EVENTS.append(
                {
                    "ts": ts,
                    "level": str(record.levelname),
                    "logger": str(record.name),
                    "message": str(msg),
                }
            )
        except Exception:
            return

def recent_failures(limit: int = 5) -> list[dict[str, str]]:
    lim = max(1, min(int(limit), 50))
    return list(_ERROR_EVENTS)[-lim:]

def setup_logging():
    # Basic structured-ish format
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(level)
    if getattr(logger, "_tankbot_logging_initialized", False):
        return

    fmt = logging.Formatter(
        fmt="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file handler
    log_path = os.getenv("LOG_PATH", "tankbot.log")
    max_bytes = _int_env("LOG_MAX_BYTES", 1_000_000, minimum=1)
    backup_count = _int_env("LOG_BACKUP_COUNT", 5, minimum=1)
    try:
        fh = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        # If file logging fails (permissions), keep console logging only.
        pass

    eh = _HealthErrorBufferHandler()
    eh.setLevel(logging.ERROR)
    logger.addHandler(eh)

    logging.getLogger("discord").setLevel(logging.WARNING)
    logger._tankbot_logging_initialized = True  # type: ignore[attr-defined]
