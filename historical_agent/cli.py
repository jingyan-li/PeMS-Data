from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .browser import PeMSBrowserAgent
from .config import JobConfig, SiteProfile
from .planner import DownloadChunk, expand_request
from .storage import DownloadLedger


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Historical PeMS data downloader.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth_parser = subparsers.add_parser("auth", help="Bootstrap a browser session and save login state.")
    auth_parser.add_argument("--job", required=True, help="Path to the job JSON file.")

    plan_parser = subparsers.add_parser("plan", help="Expand requests into date chunks.")
    plan_parser.add_argument("--job", required=True, help="Path to the job JSON file.")

    run_parser = subparsers.add_parser("run", help="Execute historical downloads.")
    run_parser.add_argument("--job", required=True, help="Path to the job JSON file.")
    run_parser.add_argument("--limit", type=int, default=None, help="Maximum number of chunks to process.")

    init_parser = subparsers.add_parser("init", help="Write example config files into a target directory.")
    init_parser.add_argument("--target-dir", required=True, help="Directory that will receive example configs.")
    return parser


def _load_runtime(job_path: str) -> tuple[JobConfig, SiteProfile]:
    job = JobConfig.load(job_path)
    site = SiteProfile.load(job.site_profile_path)
    return job, site


def _collect_chunks(job: JobConfig) -> list[DownloadChunk]:
    chunks: list[DownloadChunk] = []
    for request in job.requests:
        chunks.extend(expand_request(request, job.output_root))
    return chunks


def _print_plan(job: JobConfig) -> None:
    chunks = _collect_chunks(job)
    payload = [
        {
            "chunk_id": chunk.chunk_id,
            "request_name": chunk.request_name,
            "district": chunk.district,
            "data_type": chunk.data_type,
            "start_date": chunk.start_date.isoformat(),
            "end_date": chunk.end_date.isoformat(),
            "target_path": str(chunk.target_path),
        }
        for chunk in chunks
    ]
    print(json.dumps({"job_name": job.job_name, "chunks": payload}, indent=2))


async def _run_downloads(job: JobConfig, site: SiteProfile, limit: int | None) -> None:
    ledger = DownloadLedger(job.output_root / "_state" / "download_ledger.json")
    agent = PeMSBrowserAgent(job, site)
    processed = 0
    for chunk in _collect_chunks(job):
        if limit is not None and processed >= limit:
            break
        existing = ledger.get(chunk.chunk_id)
        if (
            existing
            and existing.status == "downloaded"
            and Path(existing.target_path).exists()
            and not job.overwrite_existing
        ):
            print(f"Skipping completed chunk {chunk.chunk_id}")
            continue

        ledger.mark(chunk.chunk_id, "running", chunk.target_path)
        try:
            path = await agent.download_chunk(chunk)
        except Exception as exc:  # noqa: BLE001
            debug_dir = job.output_root / "_state" / "failures"
            debug_dir.mkdir(parents=True, exist_ok=True)
            ledger.mark(
                chunk.chunk_id,
                "failed",
                chunk.target_path,
                error=str(exc),
                suggestion="Inspect selectors or rerun with --limit 1 in headed mode.",
            )
            print(f"Failed {chunk.chunk_id}: {exc}")
        else:
            ledger.mark(chunk.chunk_id, "downloaded", path)
            print(f"Downloaded {chunk.chunk_id} -> {path}")
        processed += 1


def _write_examples(target_dir: str) -> None:
    base = Path(target_dir)
    config_dir = base / "config"
    output_dir = base / "downloads"
    state_dir = base / "state"
    config_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    site_profile = {
        "name": "pems-default",
        "login_url": "https://pems.dot.ca.gov/",
        "clearinghouse_url": "https://pems.dot.ca.gov/?dnode=Clearinghouse",
        "login": {
            "username": "input[name='username']",
            "password": "input[name='password']",
            "submit": "input[type='submit'], button[type='submit']",
            "success_indicator": "text=Welcome to PeMS",
        },
        "field_selectors": {
            "district": "select[name='district_id']",
            "data_type": "select[name='type']",
            "start_date": "input[name='s_time_id']",
            "end_date": "input[name='e_time_id']",
        },
        "download": {
            "submit": "input[type='submit'][value='Submit'], button:has-text('Submit')",
            "post_submit_download": None,
            "completion_indicator": None,
        },
        "static_form_values": {},
    }
    example_job = {
        "job_name": "district_7_historical",
        "site_profile": "./config/site_profile.json",
        "storage_state_path": "./state/pems_storage_state.json",
        "output_root": "./downloads",
        "timeout_ms": 120000,
        "headed_auth": True,
        "headed_downloads": False,
        "slow_mo_ms": 0,
        "overwrite_existing": False,
        "requests": [
            {
                "name": "district7_meta",
                "district": 7,
                "data_type": "meta",
                "start_date": "2023-12-01",
                "end_date": "2023-12-31",
                "chunk_days": 31,
                "output_subdir": "d07_meta",
                "filename_template": "{name}_{start}_{end}.csv",
                "form_values": {},
            }
        ],
    }
    (config_dir / "site_profile.json").write_text(json.dumps(site_profile, indent=2))
    (base / "job.example.json").write_text(json.dumps(example_job, indent=2))
    print(f"Wrote example files to {base}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "init":
        _write_examples(args.target_dir)
        return

    job, site = _load_runtime(args.job)

    if args.command == "plan":
        _print_plan(job)
        return

    agent = PeMSBrowserAgent(job, site)
    if args.command == "auth":
        asyncio.run(agent.bootstrap_auth())
        print(f"Saved auth state to {job.storage_state_path}")
        return

    if args.command == "run":
        asyncio.run(_run_downloads(job, site, args.limit))
        return


if __name__ == "__main__":
    main()
