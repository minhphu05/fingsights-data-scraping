from __future__ import annotations

import hashlib
import time
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any


class HeadlessOfficialBrowser(AbstractContextManager):
    """Bounded headless Chromium session for official sites that render via ADF/AJAX."""

    def __init__(self, *, timeout_ms: int = 30000, user_agent: str | None = None):
        self.timeout_ms = timeout_ms
        self.user_agent = user_agent or "FinsightsDataCollector/1.0 (+https://finsights.vn)"
        self._playwright = None
        self.browser = None
        self.page = None

    def __enter__(self):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError("Playwright is required for this source; install project dependencies and Chromium") from exc
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = self.browser.new_context(
            user_agent=self.user_agent,
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
        )
        self.page = context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.browser:
                self.browser.close()
        finally:
            if self._playwright:
                self._playwright.stop()
        return False

    def goto(self, url: str) -> tuple[str, str, int | None, float]:
        started = time.monotonic()
        response = self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        try:
            self.page.wait_for_load_state("networkidle", timeout=min(self.timeout_ms, 12000))
        except Exception:
            # Long polling and ad networks should not block extraction from the official document body.
            pass
        return self.page.content(), self.page.url, response.status if response else None, (time.monotonic() - started) * 1000

    def change_page(self, page_number: int) -> tuple[str, str, float]:
        started = time.monotonic()
        self.page.evaluate("(pageNo) => { if (typeof changePage !== 'function') throw new Error('changePage() is unavailable'); changePage(pageNo); }", page_number)
        self.page.wait_for_function(
            "(pageNo) => { const active = document.querySelector('.paging .a-active'); return active && Number(active.id) === Number(pageNo); }",
            arg=page_number,
            timeout=self.timeout_ms,
        )
        return self.page.content(), self.page.url, (time.monotonic() - started) * 1000

    @staticmethod
    def attempt(url: str, html: str, *, status: int | None, duration_ms: float) -> dict[str, Any]:
        now = datetime.now().astimezone()
        return {
            "resource_kind": "page",
            "url": url,
            "requested_at": now,
            "completed_at": now,
            "http_status": status,
            "content_type": "text/html; browser-rendered",
            "size_bytes": len(html.encode("utf-8")),
            "response_sha256": hashlib.sha256(html.encode("utf-8")).hexdigest(),
            "duration_ms": round(duration_ms),
            "outcome": "SUCCESS" if status is None or 200 <= status < 400 else "HTTP_ERROR",
        }
