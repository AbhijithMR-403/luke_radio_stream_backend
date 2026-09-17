"""
ACRCloud Identification API - recognise what is playing in an audio file by
sending it in short chunks and reading back the music / custom-file matches.

Not wired into the ingest flow yet - these are just the building blocks.

Credentials come from the ACRCloud project console (access_key / access_secret).
Set REQ_URL to the identify host for the project's region.
`identify_audio_file` shells out to ffmpeg/ffprobe; the rest is stdlib + requests.
"""

import base64
import hashlib
import hmac
import io
import subprocess
import tempfile
import time
from pathlib import Path

import requests

REQ_URL = "http://identify-ap-southeast-1.acrcloud.com/v1/identify"  # match your region host

CHUNK_SECONDS = 15        # ACRCloud recommends samples around 15s
REQUEST_GAP_SECONDS = 1   # stay under ACRCloud's QPS limit


def build_signature(access_key: str, access_secret: str, timestamp: int,
                    data_type: str = "audio", signature_version: str = "1") -> str:
    """HMAC-SHA1 signature for an identify request, base64 encoded."""
    string_to_sign = "\n".join([
        "POST",
        "/v1/identify",
        access_key,
        data_type,
        signature_version,
        str(timestamp),
    ])
    digest = hmac.new(
        access_secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def identify_chunk(audio_bytes: bytes, access_key: str, access_secret: str) -> dict:
    """Send one audio sample to ACRCloud and return the parsed JSON response."""
    timestamp = int(time.time())
    data = {
        "access_key": access_key,
        "sample_bytes": len(audio_bytes),
        "timestamp": str(timestamp),
        "signature": build_signature(access_key, access_secret, timestamp),
        "data_type": "audio",
        "signature_version": "1",
    }
    files = {"sample": ("sample.mp3", io.BytesIO(audio_bytes), "audio/mpeg")}

    response = requests.post(REQ_URL, data=data, files=files, timeout=30)
    response.raise_for_status()
    return response.json()


def audio_duration_seconds(path) -> int:
    """Whole seconds of audio in a file, via ffprobe."""
    out = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=nk=1:nw=1", str(path),
        ],
        capture_output=True, text=True, check=True,
    )
    return int(float(out.stdout.strip()))


def identify_audio_file(path, access_key: str, access_secret: str,
                        chunk_seconds: int = CHUNK_SECONDS) -> list[dict]:
    """Split an audio file into fixed-length chunks and identify each one.

    ffmpeg slices the file in one pass with no re-encode; each chunk is then sent
    to ACRCloud. Returns a list of {"start_sec", "end_sec", "response"}, in order.
    """
    total = audio_duration_seconds(path)

    with tempfile.TemporaryDirectory(prefix="acr_chunks_") as tmp_dir:
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", str(path),
                "-f", "segment", "-segment_time", str(chunk_seconds),
                "-reset_timestamps", "1", "-c", "copy",
                str(Path(tmp_dir) / "chunk_%05d.mp3"),
            ],
            check=True,
        )

        results = []
        for index, chunk_path in enumerate(sorted(Path(tmp_dir).glob("chunk_*.mp3"))):
            results.append({
                "start_sec": index * chunk_seconds,
                "end_sec": min((index + 1) * chunk_seconds, total),
                "response": identify_chunk(
                    chunk_path.read_bytes(), access_key, access_secret,
                ),
            })
            time.sleep(REQUEST_GAP_SECONDS)

    return results


def first_match(response: dict):
    """Pick the recognised track out of one chunk response.

    Returns ``(kind, entry)`` where kind is "music" or "custom_files" and entry
    is the raw ACRCloud match dict, or ``(None, None)`` when the chunk matched
    nothing (status code != 0, e.g. 1001 "No result").
    """
    if (response.get("status") or {}).get("code") != 0:
        return None, None
    metadata = response.get("metadata") or {}
    for kind in ("music", "custom_files"):
        entries = metadata.get(kind)
        if entries:
            return kind, entries[0]
    return None, None


def collapse_matches(chunk_results: list[dict]) -> list[dict]:
    """Merge consecutive chunks that recognised the same track into spans.

    ``chunk_results`` is the list produced by :func:`identify_audio_file`
    (``[{"start_sec", "end_sec", "response"}, ...]``). Chunks that matched
    nothing are dropped - they are the gaps between spans.

    Each span::

        {
            "start_sec": int,          # offset from the start of the scanned file
            "end_sec": int,
            "kind": "music" | "custom_files",
            "title": str,
            "acrid": str,
            "score": int,
            "match": {...},            # the raw ACRCloud match entry
        }
    """
    spans: list[dict] = []
    for chunk in chunk_results:
        kind, entry = first_match(chunk.get("response") or {})
        if entry is None:
            continue

        acrid = entry.get("acrid")
        previous = spans[-1] if spans else None
        if previous and previous["acrid"] == acrid and chunk["start_sec"] <= previous["end_sec"]:
            previous["end_sec"] = chunk["end_sec"]
            continue

        spans.append({
            "start_sec": chunk["start_sec"],
            "end_sec": chunk["end_sec"],
            "kind": kind,
            "title": entry.get("title"),
            "acrid": acrid,
            "score": entry.get("score"),
            "match": entry,
        })
    return spans


def join_no_result_chunks(chunk_results: list[dict]) -> list[dict]:
    """Merge consecutive "No result" (status code 1001) chunks into one.

    Every other chunk (a match, or anything else) is kept as-is. Only adjacent
    1001 chunks collapse into a single ``{"start_sec", "end_sec", "response"}``
    spanning the first one's start to the last one's end.

    Example::

        [{"start_sec": 1050, "end_sec": 1060, "response": {"status": {"code": 1001, ...}}},
         {"start_sec": 1060, "end_sec": 1070, "response": {"status": {"code": 1001, ...}}}]
        ->
        [{"start_sec": 1050, "end_sec": 1070, "response": {"status": {"code": 1001, ...}}}]
    """
    joined: list[dict] = []
    for chunk in chunk_results:
        code = ((chunk.get("response") or {}).get("status") or {}).get("code")
        previous = joined[-1] if joined else None
        previous_code = (
            ((previous.get("response") or {}).get("status") or {}).get("code")
            if previous else None
        )

        if code == 1001 and previous_code == 1001 and chunk["start_sec"] <= previous["end_sec"]:
            previous["end_sec"] = chunk["end_sec"]
            continue

        joined.append(dict(chunk))
    return joined
