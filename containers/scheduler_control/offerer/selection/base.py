from __future__ import annotations

from abc import ABC, abstractmethod
from typing import NamedTuple


class CandidateDomain(NamedTuple):
    domain_id: int
    shard_id: int
    domain_score: float


class SelectionStrategy(ABC):
    """
    Strategy contract for per-domain URL selection:
      - pick up to `per_domain_cap` eligible URLs for up to `max_domains` distinct domains
      - skip domains in `exclude_domain_ids`
      - perform all necessary updates atomically (e.g. should_crawl=false, last_scheduled)
      - return {domain_id: [url, ...]}
    """

    @abstractmethod
    def select_by_domain(
        self,
        shard_id: int,
        exclude_domain_ids: set[int],
        per_domain_cap: int,
        max_domains: int,
    ) -> dict[int, list[str]]:
        raise NotImplementedError
