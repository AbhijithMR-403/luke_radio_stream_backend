"""
Async multi-page dashboard PDF generation using PyPPeteer.
Uses FRONTEND_URL from settings; accepts channelId and accessToken.
Runs in a subprocess so signal handlers (used by pyppeteer/Chromium) work.
"""
import asyncio
from datetime import datetime, time
import multiprocessing
import os
import tempfile
import urllib.parse
import uuid

# Set before importing pyppeteer (for consistent Chromium revision)
PYPPETEER_CHROMIUM_REVISION = "1263111"
os.environ.setdefault("PYPPETEER_CHROMIUM_REVISION", PYPPETEER_CHROMIUM_REVISION)

from PyPDF2 import PdfMerger
from pyppeteer import launch


def _build_dashboard_url(
    base_url: str,
    start_time: str | None = None,
    end_time: str | None = None,
    shift_id: str | int | None = None,
) -> str:
    """Build dashboard-v2 URL with hideUI=true and optional start_time, end_time, shift_id."""
    params: dict[str, str] = {"hideUI": "true"}
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time
    if shift_id is not None:
        params["shift_id"] = str(shift_id)
    query = urllib.parse.urlencode(params)
    return f"{base_url.rstrip('/')}/dashboard-v2?{query}"


async def _generate_multi_page_pdf_async(
    base_url: str,
    pdf_path: str,
    access_token: str,
    channel_id: str,
    channel_name: str = "",
    slides: list[int] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    shift_id: str | int | None = None,
) -> None:
    if slides is None:
        slides = [0, 1]

    dashboard_url = _build_dashboard_url(base_url, start_time=start_time, end_time=end_time, shift_id=shift_id)

    # Optimized Browser Launch
    browser = await launch(
        # headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",  # Vital for Docker/Linux to prevent crashes
            "--font-render-hinting=none",
        ],
    )
    temp_pdfs = []

    try:
        for slide_num in slides:
            start_time = datetime.now()
            print(f"Starting slide {slide_num} at {start_time}")
            page = await browser.newPage()
            end_time = datetime.now()
            print(f"Time taken to create page: {end_time - start_time}")
            page.on("console", lambda msg: None)  # quiet by default
            page.on("pageerror", lambda err: None)
            page.on("requestfailed", lambda req: None)
            page.on("response", lambda res: None)

            temp_pdf = os.path.join(tempfile.gettempdir(), f"temp_slide_{uuid.uuid4().hex}.pdf")
            temp_pdfs.append(temp_pdf)

            await page.evaluateOnNewDocument(
                """
                (token, channelId, channelName, slideNum) => {
                    localStorage.setItem("accessToken", token);
                    localStorage.setItem("refreshToken", token);
                    localStorage.setItem("channelId", channelId.toString());
                    localStorage.setItem("channelName", channelName || "Dashboard");
                    localStorage.setItem("dashboardV2CurrentSlide", slideNum.toString());
                    localStorage.setItem("channelTimezone", "Australia/Melbourne");
                }
                """,
                access_token,
                str(channel_id),
                channel_name or "Dashboard",
                slide_num,
            )
            print("Waiting for API response")
            print(page.url)
            wait_api = asyncio.create_task(
                page.waitForResponse(
                    lambda r: "/api/v2/dashboard" in r.url and r.status == 200
                )
            )
            end_time = datetime.now()
            print(f"Time taken to wait for API response: {end_time - start_time}")

            await page.goto(
                dashboard_url,
                {"waitUntil": "networkidle2"},
            )
            end_time = datetime.now()
            print(f"Time taken to goto dashboard: {end_time - start_time}")
            await wait_api
            await asyncio.sleep(1)


            await page.pdf(
                path=temp_pdf,
                format="A4",
                printBackground=True,
                landscape=True,
            )
            await page.close()
            end_time = datetime.now()
            print(f"Time taken to close page: {end_time - start_time}")

        await browser.close()
        start_time = datetime.now()
        merger = PdfMerger()
        for tp in temp_pdfs:
            merger.append(tp)
        merger.write(pdf_path)
        merger.close()
        end_time = datetime.now()
        print(f"Time taken to merge PDFs: {end_time - start_time}")
    finally:
        for tp in temp_pdfs:
            try:
                if os.path.exists(tp):
                    os.remove(tp)
            except OSError:
                pass


def _run_in_subprocess(
    base_url: str,
    pdf_path: str,
    access_token: str,
    channel_id: str,
    channel_name: str,
    slides: list[int],
    start_time: str | None,
    end_time: str | None,
    shift_id: str | None,
) -> None:
    """Entry point for subprocess: runs async PDF generation in main thread (signals OK)."""
    asyncio.run(
        _generate_multi_page_pdf_async(
            base_url=base_url,
            pdf_path=pdf_path,
            access_token=access_token,
            channel_id=channel_id,
            channel_name=channel_name,
            slides=slides,
            start_time=start_time,
            end_time=end_time,
            shift_id=shift_id,
        )
    )


def generate_multi_page_pdf(
    base_url: str,
    pdf_path: str,
    access_token: str,
    channel_id: str,
    channel_name: str = "",
    slides: list[int] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    shift_id: str | int | None = None,
) -> None:
    """Generate a multi-page PDF from the dashboard.
    Runs in a subprocess so pyppeteer/Chromium signal handlers work (main thread only).
    """
    if slides is None:
        slides = [0, 1, 2, 3, 4, 5, 6, 7]
    proc = multiprocessing.Process(
        target=_run_in_subprocess,
        args=(
            base_url,
            pdf_path,
            access_token,
            str(channel_id),
            channel_name or "Dashboard",
            slides,
            start_time,
            end_time,
            str(shift_id) if shift_id is not None else None,
        ),
    )
    proc.start()
    proc.join()
    if proc.exitcode != 0:
        raise RuntimeError(f"PDF generation subprocess exited with code {proc.exitcode}")
