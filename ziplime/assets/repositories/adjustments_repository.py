from typing import Any, Self


class AdjustmentRepository:
    def to_json(self): ...

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self: ...
