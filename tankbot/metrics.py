from __future__ import annotations

import math
import threading
from collections import deque

_MAX_LATENCY_SAMPLES = 2048

_db_latencies_ms: deque[float] = deque(maxlen=_MAX_LATENCY_SAMPLES)
_command_latencies_ms: deque[float] = deque(maxlen=_MAX_LATENCY_SAMPLES)
_lock = threading.Lock()


def _sanitize_latency_ms(value: float) -> float | None:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v) or v < 0:
        return None
    return v


def _percentile(sorted_values: list[float], p: float) -> float | None:
    if not sorted_values:
        return None
    if p <= 0:
        return float(sorted_values[0])
    if p >= 1:
        return float(sorted_values[-1])
    pos = (len(sorted_values) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_values[lo])
    frac = pos - lo
    return float(sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac)


def _summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "p90_ms": None, "p95_ms": None}
    sorted_values = sorted(values)
    return {
        "count": len(sorted_values),
        "p90_ms": _percentile(sorted_values, 0.90),
        "p95_ms": _percentile(sorted_values, 0.95),
    }


def record_db_latency_ms(value: float) -> None:
    sanitized = _sanitize_latency_ms(value)
    if sanitized is None:
        return
    with _lock:
        _db_latencies_ms.append(sanitized)


def record_command_latency_ms(value: float) -> None:
    sanitized = _sanitize_latency_ms(value)
    if sanitized is None:
        return
    with _lock:
        _command_latencies_ms.append(sanitized)


def db_latency_summary() -> dict[str, float | int | None]:
    with _lock:
        snapshot = list(_db_latencies_ms)
    return _summary(snapshot)


def command_latency_summary() -> dict[str, float | int | None]:
    with _lock:
        snapshot = list(_command_latencies_ms)
    return _summary(snapshot)
