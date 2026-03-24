"""Structured JSON events alongside human-readable logs."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

EVENT_SIGNAL_GENERATED = "signal_generated"
EVENT_TRADE_BLOCKED = "trade_blocked"
EVENT_ORDER_SUBMITTED = "order_submitted"
EVENT_ORDER_FILLED = "order_filled"
EVENT_POSITION_OPENED = "position_opened"
EVENT_POSITION_REDUCED = "position_reduced"
EVENT_POSITION_CLOSED = "position_closed"
EVENT_KILL_SWITCH_TRIPPED = "kill_switch_tripped"
EVENT_STARTUP_RECONCILIATION_DELTA = "startup_reconciliation_delta"

STRUCTURED_LOGGER_NAME = "crypto_bot_structured"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def emit_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    structured_logger: logging.Logger | None = None,
) -> None:
    """Emit one JSON object per line to optional JSONL logger and STRUCT_JSON on crypto_bot."""
    record = {"event": event_type, "ts": _now_iso(), **payload}
    line = json.dumps(record, default=str, separators=(",", ":"))
    if structured_logger:
        structured_logger.info("%s", line)
    logging.getLogger("crypto_bot").info("STRUCT_JSON %s", line)


def attach_structured_jsonl_handler(log_file_path: str | None) -> logging.Logger | None:
    """Append JSON lines to path; idempotent per path string."""
    if not log_file_path:
        return None
    slog = logging.getLogger(STRUCTURED_LOGGER_NAME)
    slog.setLevel(logging.INFO)
    slog.propagate = False
    for h in slog.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None):
            try:
                if h.baseFilename and log_file_path in h.baseFilename:
                    return slog
            except Exception:
                pass
    fh = logging.FileHandler(log_file_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(message)s"))
    slog.addHandler(fh)
    return slog


def get_structured_logger(config: dict) -> logging.Logger | None:
    log_cfg = config.get("logging") or {}
    path = log_cfg.get("structured_events_file")
    if not path:
        return None
    return attach_structured_jsonl_handler(str(path))
