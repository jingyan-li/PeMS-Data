from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class LedgerRecord:
    chunk_id: str
    status: str
    target_path: str
    updated_at: str
    details: dict[str, Any]


class DownloadLedger:
    def __init__(self, ledger_path: Path) -> None:
        self.ledger_path = ledger_path
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self._records = self._load()

    def _load(self) -> dict[str, LedgerRecord]:
        if not self.ledger_path.exists():
            return {}
        raw = json.loads(self.ledger_path.read_text())
        return {key: LedgerRecord(**value) for key, value in raw.items()}

    def save(self) -> None:
        payload = {key: asdict(value) for key, value in self._records.items()}
        self.ledger_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def get(self, chunk_id: str) -> LedgerRecord | None:
        return self._records.get(chunk_id)

    def mark(self, chunk_id: str, status: str, target_path: Path, **details: Any) -> None:
        self._records[chunk_id] = LedgerRecord(
            chunk_id=chunk_id,
            status=status,
            target_path=str(target_path),
            updated_at=utc_now(),
            details=details,
        )
        self.save()
