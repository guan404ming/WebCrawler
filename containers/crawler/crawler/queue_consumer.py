from __future__ import annotations

from libs.ipc.bus import MessageConsumer


class QueueConsumer:
    """
    Consumes URL batches from the IPC bus.
    Backend (filesystem or redis) is determined by the injected consumer.
    """

    def __init__(self, consumer: MessageConsumer, crawler_id: int):
        self.consumer = consumer
        self.crawler_id = crawler_id

    def pop_batch(self) -> list[str]:
        messages = self.consumer.poll("url_queue", self.crawler_id, max_messages=1)
        if not messages:
            return []

        data = messages[0]
        urls = data.get("urls")
        if not isinstance(urls, list):
            return []
        return [str(u) for u in urls]
