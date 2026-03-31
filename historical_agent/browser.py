from __future__ import annotations

import asyncio
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import JobConfig, SiteProfile
from .planner import DownloadChunk


class BrowserDependencyError(RuntimeError):
    pass


def _coerce_form_value(chunk: DownloadChunk, value: Any) -> str | bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        replacements = {
            "{district}": str(chunk.district),
            "{data_type}": chunk.data_type,
            "{start_date}": chunk.start_date.strftime("%Y-%m-%d"),
            "{end_date}": chunk.end_date.strftime("%Y-%m-%d"),
            "{start_mmddyyyy}": chunk.start_date.strftime("%m/%d/%Y"),
            "{end_mmddyyyy}": chunk.end_date.strftime("%m/%d/%Y"),
        }
        for old, new in replacements.items():
            value = value.replace(old, new)
    return str(value)


class PeMSBrowserAgent:
    def __init__(self, job: JobConfig, site: SiteProfile) -> None:
        self.job = job
        self.site = site

    async def bootstrap_auth(self) -> None:
        playwright = self._load_playwright()
        self.job.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        async with playwright() as p:
            browser = await p.chromium.launch(
                headless=not self.job.headed_auth,
                slow_mo=self.job.slow_mo_ms,
            )
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()
            await page.goto(self.site.login_url, wait_until="domcontentloaded")

            username = os.getenv("PEMS_USERNAME")
            password = os.getenv("PEMS_PASSWORD")
            can_auto_login = all(
                [
                    username,
                    password,
                    self.site.login.username,
                    self.site.login.password,
                    self.site.login.submit,
                ]
            )

            if can_auto_login:
                await page.fill(self.site.login.username, username or "")
                await page.fill(self.site.login.password, password or "")
                await page.click(self.site.login.submit)
            else:
                print("Finish the PeMS login in the browser, then press Enter here to save the session.")
                await asyncio.to_thread(input)

            if self.site.login.success_indicator:
                await page.wait_for_selector(self.site.login.success_indicator, timeout=self.job.timeout_ms)

            await context.storage_state(path=str(self.job.storage_state_path))
            await browser.close()

    async def download_chunk(self, chunk: DownloadChunk) -> Path:
        if not self.job.storage_state_path.exists():
            raise FileNotFoundError(
                f"Missing storage state: {self.job.storage_state_path}. Run auth bootstrap first."
            )

        playwright = self._load_playwright()
        chunk.target_path.parent.mkdir(parents=True, exist_ok=True)
        async with playwright() as p:
            browser = await p.chromium.launch(
                headless=not self.job.headed_downloads,
                slow_mo=self.job.slow_mo_ms,
            )
            context = await browser.new_context(
                accept_downloads=True,
                storage_state=str(self.job.storage_state_path),
            )
            page = await context.new_page()
            page.set_default_timeout(self.job.timeout_ms)
            await page.goto(self.site.clearinghouse_url, wait_until="domcontentloaded")
            await self._apply_form(page, chunk)
            if chunk.download_strategy == "listing" or self._is_station_metadata(chunk):
                saved_path = await self._download_from_listing(page, chunk)
            else:
                saved_path = await self._submit_and_save(page, chunk.target_path)
            await browser.close()
            return saved_path

    async def _apply_form(self, page: Any, chunk: DownloadChunk) -> None:
        merged = dict(self.site.static_form_values)
        merged.update(
            {
                "data_type": "{data_type}",
                "district": "{district}",
                "start_date": "{start_mmddyyyy}",
                "end_date": "{end_mmddyyyy}",
            }
        )
        merged.update(chunk.form_values)
        ordered_names = [name for name in ("data_type", "district") if name in merged]
        ordered_names.extend(name for name in merged if name not in ordered_names)

        for logical_name in ordered_names:
            selector = self.site.field_selectors.get(logical_name)
            if not selector:
                continue
            if await page.locator(selector).count() == 0:
                continue

            raw_value = merged[logical_name]
            value = _coerce_form_value(chunk, raw_value)
            if isinstance(value, bool):
                if value:
                    await page.check(selector)
                else:
                    await page.uncheck(selector)
                continue

            tag_name = await page.locator(selector).evaluate("(el) => el.tagName.toLowerCase()")
            if tag_name == "select":
                await self._select_option_flex(page, selector, str(value), logical_name)
                if logical_name == "data_type" and self.site.field_selectors.get("district"):
                    await page.wait_for_function(
                        """(selector) => {
                            const select = document.querySelector(selector);
                            return !!select && select.options.length > 1;
                        }""",
                        arg=self.site.field_selectors["district"],
                        timeout=self.job.timeout_ms,
                    )
            else:
                await page.fill(selector, str(value))

    async def _submit_and_save(self, page: Any, target_path: Path) -> Path:
        if target_path.exists() and not self.job.overwrite_existing:
            print(f"File already exists, skipping download: {target_path}")
            return target_path

        submit = self.site.download.submit
        if self.site.download.post_submit_download:
            await page.click(submit)
            if self.site.download.completion_indicator:
                await page.wait_for_selector(
                    self.site.download.completion_indicator,
                    timeout=self.job.timeout_ms,
                )
            async with page.expect_download(timeout=self.job.timeout_ms) as download_info:
                await page.click(self.site.download.post_submit_download)
            download = await download_info.value
        else:
            async with page.expect_download(timeout=self.job.timeout_ms) as download_info:
                await page.click(submit)
            download = await download_info.value

        await download.save_as(str(target_path))
        return target_path

    async def _download_from_listing(self, page: Any, chunk: DownloadChunk) -> Path:
        await page.click(self.site.download.submit)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(3000)

        if self._is_station_metadata(chunk):
            return await self._download_latest_metadata(page, chunk)

        type_selector = self.site.field_selectors.get("data_type")
        if not type_selector:
            raise ValueError("Listing download strategy requires a data_type selector.")
        type_value = await page.locator(type_selector).input_value()

        expected_by_month: dict[tuple[int, int], list[str]] = defaultdict(list)
        cursor = chunk.start_date
        while cursor <= chunk.end_date:
            expected_by_month[(cursor.year, cursor.month)].append(
                self._build_station_filename(chunk, cursor)
            )
            cursor += timedelta(days=1)

        saved_paths: list[Path] = []
        for (year, month), expected_names in expected_by_month.items():
            await page.evaluate(
                """([district, year, typeValue, monthIndex]) => {
                    processFiles(String(district), '', year, typeValue, 'text', monthIndex);
                }""",
                [chunk.district, year, type_value, month - 1],
            )
            await page.wait_for_timeout(5000)

            available_links = await page.locator("a").evaluate_all(
                """els => els.map(el => ({
                    text: (el.textContent || '').trim(),
                    href: el.href || ''
                }))"""
            )
            matches = [
                link for link in available_links
                if link["text"] in expected_names and "download=" in link["href"]
            ]
            if not matches:
                raise ValueError(
                    f"No downloadable files matched {expected_names!r} after loading {year}-{month:02d}. "
                    "The listing page loaded, but the expected filenames were not present."
                )

            for link in matches:
                save_path = (
                    chunk.target_path if len(expected_names) == 1 else chunk.target_path.parent / link["text"]
                )
                if save_path.exists() and not self.job.overwrite_existing:
                    print(f"File already exists, skipping download: {save_path}")
                    saved_paths.append(save_path)
                    continue

                async with page.expect_download(timeout=self.job.timeout_ms) as download_info:
                    await page.locator("a", has_text=link["text"]).first.click()
                download = await download_info.value
                save_path.parent.mkdir(parents=True, exist_ok=True)
                await download.save_as(str(save_path))
                saved_paths.append(save_path)

        return saved_paths[0]

    @staticmethod
    def _build_station_filename(chunk: DownloadChunk, day: Any) -> str:
        prefix = f"d{int(chunk.district):02d}_text_"
        data_type = str(chunk.data_type).strip().lower()
        if data_type in {"station 5-minute", "station_5min", "station-5min", "station_5-minute"}:
            return f"{prefix}station_5min_{day.strftime('%Y_%m_%d')}.txt.gz"
        raise ValueError(
            "Listing download strategy currently needs a known filename pattern for the dataset. "
            f"Unsupported data_type: {chunk.data_type!r}"
        )

    async def _download_latest_metadata(self, page: Any, chunk: DownloadChunk) -> Path:
        rows = await page.locator("a[href*='download=']").evaluate_all(
            """els => els.map(el => {
                const row = el.closest('tr');
                const cells = row ? Array.from(row.querySelectorAll('td')).map(td => (td.textContent || '').trim()) : [];
                return {
                    text: (el.textContent || '').trim(),
                    href: el.href || '',
                    size_text: cells.length ? cells[cells.length - 1] : ''
                };
            })"""
        )
        candidates = []
        for row in rows:
            parsed = self._parse_metadata_link(chunk, row)
            if parsed:
                candidates.append(parsed)

        if not candidates:
            raise ValueError(
                f"No metadata downloads were found for district {chunk.district} on the listing page."
            )

        chosen = max(candidates, key=lambda item: (item["size_bytes"], item["file_date"], item["text"]))
        target_path = chunk.target_path.parent / chosen["text"]
        if target_path.exists() and not self.job.overwrite_existing:
            print(f"File already exists, skipping download: {target_path}")
            return target_path

        async with page.expect_download(timeout=self.job.timeout_ms) as download_info:
            await page.locator("a", has_text=chosen["text"]).first.click()
        download = await download_info.value
        target_path.parent.mkdir(parents=True, exist_ok=True)
        await download.save_as(str(target_path))
        return target_path

    @staticmethod
    def _is_station_metadata(chunk: DownloadChunk) -> bool:
        data_type = str(chunk.data_type).strip().lower()
        return data_type in {"station metadata", "meta", "metadata"}

    @staticmethod
    def _parse_metadata_link(chunk: DownloadChunk, link: dict[str, str]) -> dict[str, Any] | None:
        text = link.get("text", "").strip()
        href = link.get("href", "")
        size_text = link.get("size_text", "").strip()
        district = int(chunk.district)
        match = re.fullmatch(
            rf"d{district:02d}_text_meta_(\d{{4}}_\d{{2}}_\d{{2}})\.(txt|csv)",
            text,
        )
        if not match or "download=" not in href:
            return None
        file_date = datetime.strptime(match.group(1), "%Y_%m_%d").date()
        size_bytes = PeMSBrowserAgent._parse_size_bytes(size_text)
        return {
            "text": text,
            "href": href,
            "file_date": file_date,
            "size_bytes": size_bytes,
        }

    @staticmethod
    def _parse_size_bytes(size_text: str) -> int:
        digits = re.sub(r"[^0-9]", "", size_text)
        return int(digits) if digits else 0

    @staticmethod
    def _load_playwright() -> Any:
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise BrowserDependencyError(
                "Playwright is not installed. Install it with `pip install playwright` "
                "and then run `playwright install chromium`."
            ) from exc
        return async_playwright

    async def _select_option_flex(
        self,
        page: Any,
        selector: str,
        requested_value: str,
        logical_name: str,
    ) -> None:
        options = await page.locator(f"{selector} option").evaluate_all(
            """els => els.map(el => ({
                value: el.value,
                label: (el.textContent || '').trim()
            }))"""
        )
        match = self._match_option(options, requested_value, logical_name)
        if not match:
            available = ", ".join(
                f"{item['label']} [{item['value']}]" for item in options
            )
            raise ValueError(
                f"Could not match {logical_name!r} value {requested_value!r} for selector {selector!r}. "
                f"Available options: {available}"
            )
        await page.select_option(selector, value=match["value"])

    @staticmethod
    def _match_option(
        options: list[dict[str, str]],
        requested_value: str,
        logical_name: str,
    ) -> dict[str, str] | None:
        requested = requested_value.strip()
        lower_requested = requested.lower()

        for key in ("value", "label"):
            for option in options:
                candidate = option.get(key, "").strip()
                if candidate == requested:
                    return option

        for key in ("value", "label"):
            for option in options:
                candidate = option.get(key, "").strip().lower()
                if candidate == lower_requested:
                    return option

        if logical_name == "district" and requested.isdigit():
            district_label = f"district {requested}"
            for option in options:
                if option.get("label", "").strip().lower() == district_label:
                    return option

        return None
