from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


@dataclass
class LoginSelectors:
    username: str | None = None
    password: str | None = None
    submit: str | None = None
    success_indicator: str | None = None


@dataclass
class DownloadSelectors:
    submit: str
    post_submit_download: str | None = None
    completion_indicator: str | None = None


@dataclass
class SiteProfile:
    name: str
    login_url: str
    clearinghouse_url: str
    login: LoginSelectors
    field_selectors: dict[str, str]
    download: DownloadSelectors
    static_form_values: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> "SiteProfile":
        data = json.loads(Path(path).read_text())
        return cls(
            name=data["name"],
            login_url=data["login_url"],
            clearinghouse_url=data["clearinghouse_url"],
            login=LoginSelectors(**data.get("login", {})),
            field_selectors=data.get("field_selectors", {}),
            download=DownloadSelectors(**data["download"]),
            static_form_values=data.get("static_form_values", {}),
        )


@dataclass
class LoadRequest:
    name: str
    district: int
    data_type: str
    start_date: date
    end_date: date
    chunk_days: int = 1
    output_subdir: str | None = None
    filename_template: str = "{name}_{district}_{data_type}_{start}_{end}.zip"
    download_strategy: str = "direct"
    form_values: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LoadRequest":
        return cls(
            name=data["name"],
            district=int(data["district"]),
            data_type=data["data_type"],
            start_date=_parse_date(data["start_date"]),
            end_date=_parse_date(data["end_date"]),
            chunk_days=int(data.get("chunk_days", 1)),
            output_subdir=data.get("output_subdir"),
            filename_template=data.get(
                "filename_template",
                "{name}_{district}_{data_type}_{start}_{end}.zip",
            ),
            download_strategy=data.get("download_strategy", "direct"),
            form_values=data.get("form_values", {}),
        )


@dataclass
class JobConfig:
    job_name: str
    site_profile_path: Path
    storage_state_path: Path
    output_root: Path
    requests: list[LoadRequest]
    timeout_ms: int = 120_000
    headed_auth: bool = True
    headed_downloads: bool = False
    slow_mo_ms: int = 0
    overwrite_existing: bool = False

    @classmethod
    def load(cls, path: str | Path) -> "JobConfig":
        job_path = Path(path)
        data = json.loads(job_path.read_text())
        base_dir = job_path.parent

        def _resolve(value: str) -> Path:
            candidate = Path(value)
            if candidate.is_absolute():
                return candidate
            return (base_dir / candidate).resolve()

        return cls(
            job_name=data["job_name"],
            site_profile_path=_resolve(data["site_profile"]),
            storage_state_path=_resolve(data["storage_state_path"]),
            output_root=_resolve(data["output_root"]),
            requests=[LoadRequest.from_dict(item) for item in data["requests"]],
            timeout_ms=int(data.get("timeout_ms", 120_000)),
            headed_auth=bool(data.get("headed_auth", True)),
            headed_downloads=bool(data.get("headed_downloads", False)),
            slow_mo_ms=int(data.get("slow_mo_ms", 0)),
            overwrite_existing=bool(data.get("overwrite_existing", False)),
        )
