"""
Async multi-page dashboard PDF generation using Playwright.
Uses FRONTEND_URL from settings; accepts channelId and accessToken.
Renders up to 2 slides concurrently to avoid overloading the server.
"""
import asyncio
import json
import os
import tempfile
import urllib.parse
import uuid

from PyPDF2 import PdfMerger
from playwright.async_api import async_playwright

BROWSER_ARGS = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-accelerated-2d-canvas',
    '--disable-gpu',
    '--no-zygote',
    # '--single-process', # Warning: Use with caution, but saves massive RAM on tiny servers
]


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
    return f"{base_url.rstrip('/')}/dashboard?{query}"


async def _render_slide(
    browser,
    slide_num: int,
    dashboard_url: str,
    access_token: str,
    channel_id: str,
    channel_name: str,
    channel_timezone: str,
) -> str | None:
    """Render a single slide to a temp PDF; returns path or None on error."""
    context = await browser.new_context()
    page = await context.new_page()

    temp_path = os.path.join(tempfile.gettempdir(), f"slide_{slide_num}_{uuid.uuid4().hex}.pdf")

    init_script = f"""
        localStorage.setItem("accessToken", {json.dumps(access_token)});
        localStorage.setItem("refreshToken", {json.dumps(access_token)});
        localStorage.setItem("channelId", {json.dumps(str(channel_id))});
        localStorage.setItem("channelName", {json.dumps(channel_name)});
        localStorage.setItem("dashboardV2CurrentSlide", {json.dumps(str(slide_num))});
        localStorage.setItem("channelTimezone", {json.dumps(channel_timezone)});
    """
    await page.add_init_script(init_script)

    try:
        # Use "load" instead of "networkidle" — dashboards often have ongoing requests
        await page.goto(dashboard_url, wait_until="load", timeout=60000)
        
        await page.wait_for_selector(".dashboard-slide-ready", state="visible", timeout=45000)
        pdf_options = dict(path=temp_path, print_background=True, landscape=True, format="A4")
        if slide_num == 6:
            pdf_options["scale"] = 0.8
        await page.pdf(**pdf_options)
        return temp_path
    except Exception as e:
        print(f"Error on slide {slide_num}: {e}")
        return None
    finally:
        await context.close()


async def _generate_multi_page_pdf_async(
    base_url: str,
    pdf_path: str,
    access_token: str,
    channel_id: str,
    channel_name: str = "",
    channel_timezone: str = "UTC",
    slides: list[int] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    shift_id: str | int | None = None,
) -> None:
    if slides is None:
        slides = [0, 1, 2, 3, 4, 5, 6, 7]

    dashboard_url = _build_dashboard_url(
        base_url, start_time=start_time, end_time=end_time, shift_id=shift_id
    )
    channel_name = channel_name or "Dashboard"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)

        tasks = [
            _render_slide(
                browser, s, dashboard_url, access_token, str(channel_id), channel_name, channel_timezone
            )
            for s in slides
        ]
        pdf_paths = await asyncio.gather(*tasks)

        await browser.close()

    # Merge PDFs (only existing paths, in slide order)
    valid_paths = [p for p in pdf_paths if p and os.path.exists(p)]
    if not valid_paths:
        raise RuntimeError("PDF generation failed: no slides could be rendered")
    merger = PdfMerger()
    for path in valid_paths:
        merger.append(path)
    merger.write(pdf_path)
    merger.close()

    # Cleanup temp files
    for path in pdf_paths:
        if path:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass


def generate_multi_page_pdf(
    base_url: str,
    pdf_path: str,
    access_token: str,
    channel_id: str,
    channel_name: str = "",
    channel_timezone: str = "UTC",
    slides: list[int] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    shift_id: str | int | None = None,
) -> None:
    """Generate a multi-page PDF from the dashboard using Playwright."""
    if slides is None:
        slides = [0, 1, 2, 3, 4, 5, 6, 7]
    asyncio.run(
        _generate_multi_page_pdf_async(
            base_url=base_url,
            pdf_path=pdf_path,
            access_token=access_token,
            channel_id=channel_id,
            channel_name=channel_name,
            channel_timezone=channel_timezone,
            slides=slides,
            start_time=start_time,
            end_time=end_time,
            shift_id=shift_id,
        )
    )
