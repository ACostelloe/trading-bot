"""Consecutive failure counting for live kill switch (testable in isolation)."""


from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ConsecutiveFailureTracker:
    max_consecutive: int
    count: int = field(default=0)

    def reset(self) -> None:
        self.count = 0

    def record_success(self) -> None:
        self.count = 0

    def record_failure(self) -> bool:
        """Increment failures; return True if kill threshold reached."""
        self.count += 1
        return self.count >= self.max_consecutive
