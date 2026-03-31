from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from .config import LoadRequest


def _sanitize(value: str) -> str:
    keep = []
    for char in value:
        keep.append(char if char.isalnum() or char in {"-", "_", "."} else "_")
    return "".join(keep).strip("_") or "download"


@dataclass
class DownloadChunk:
    request_name: str
    district: int
    data_type: str
    start_date: date
    end_date: date
    download_strategy: str
    form_values: dict[str, object]
    target_path: Path
    chunk_id: str


def expand_request(request: LoadRequest, output_root: Path) -> list[DownloadChunk]:
    chunks: list[DownloadChunk] = []
    current = request.start_date
    step = timedelta(days=max(request.chunk_days, 1))
    output_dir = output_root / (request.output_subdir or _sanitize(request.name))
    while current <= request.end_date:
        chunk_end = min(current + step - timedelta(days=1), request.end_date)
        filename = request.filename_template.format(
            name=_sanitize(request.name),
            district=request.district,
            data_type=_sanitize(request.data_type),
            start=current.isoformat(),
            end=chunk_end.isoformat(),
            start_ymd=current.strftime("%Y_%m_%d"),
            end_ymd=chunk_end.strftime("%Y_%m_%d"),
        )
        chunk_id = f"{_sanitize(request.name)}__{current.isoformat()}__{chunk_end.isoformat()}"
        chunks.append(
            DownloadChunk(
                request_name=request.name,
                district=request.district,
                data_type=request.data_type,
                start_date=current,
                end_date=chunk_end,
                download_strategy=request.download_strategy,
                form_values=dict(request.form_values),
                target_path=output_dir / filename,
                chunk_id=chunk_id,
            )
        )
        current = chunk_end + timedelta(days=1)
    return chunks
