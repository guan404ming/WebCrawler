from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from libs.ipc.bus import MessageProducer


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class StatsDeltaWriter:
    def __init__(self, producer: MessageProducer):
        self.producer = producer

    def write(self, source: str, **kwargs) -> None:
        payload: dict[str, Any] = {
            "generated_at": now_iso(),
            "source": source,
            **kwargs
        }
        self.producer.send("stats_delta", 0, payload)
