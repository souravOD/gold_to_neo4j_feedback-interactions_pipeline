from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class OutboxEvent:
    id: str
    aggregate_type: str
    table_name: str
    op: str
    aggregate_id: str
    payload: Optional[Any]
    created_at: datetime
    attempts: int = 0
