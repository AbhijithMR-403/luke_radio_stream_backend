"""
audio_recovery services.

- `save_json`                    - dump data to a .json file, return its path
- `create_pending_recovery`      - create the RecoveredAudioFile row up front,
                                   before the async work starts
- `recover_audio_from_url`       - download a recording and run ACRCloud
                                   identification on it; run inside
                                   audio_recovery.tasks.recover_audio_task
- `trim_audio_from_url`          - save a slice of a remote audio file into media/
- `create_segments_from_chunks`  - turn joined ACRCloud chunks into AudioSegments rows
"""

import json
import subprocess
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from core_admin.models import Channel
from data_analysis.models import AudioSegments
from audio_recovery.acrcloud import audio_duration_seconds, first_match, identify_audio_file, join_no_result_chunks
from audio_recovery.models import RecoveredAudioFile, RecoveredAudioSegment

MEDIA_SUBDIR = "audio_recovery"


def save_json(data) -> str:
    """Write `data` to a timestamped .json file under media/audio_recovery/ and
    return its path. Handy for keeping a scan's results around to inspect."""
    path = Path(settings.MEDIA_ROOT) / "json_files" / f"result_{int(time.time())}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    return str(path)


def create_pending_recovery(
    channel,
    recorded_at,
    created_by=None,
) -> RecoveredAudioFile:
    """Create the RecoveredAudioFile row (status=PENDING) before the async job
    starts, so the caller has an id to return / poll right away.

    Nothing here is user-supplied except channel/recorded_at - the source URL
    is what's kept for reference, on the row itself.
    """
    if not isinstance(channel, Channel):
        channel = Channel.objects.get(pk=channel)

    return RecoveredAudioFile.objects.create(
        channel=channel,
        recorded_at=recorded_at,
        created_by=created_by,
        status=RecoveredAudioFile.Status.PENDING,
    )


def recover_audio_from_url(
    recovered_audio_file_id: int,
    url: str,
    access_key: str,
    access_secret: str,
    folder: str,
) -> list[AudioSegments]:
    """Recover one recording end to end, for the RecoveredAudioFile row created by
    `create_pending_recovery`. Meant to run inside a Celery task, not the request
    thread - it downloads the file (ffmpeg), runs ACRCloud identification chunk
    by chunk, then creates an AudioSegments row per chunk via
    `create_segments_from_chunks`, each linked back to the RecoveredAudioFile
    through a RecoveredAudioSegment.

    Updates the row's status as it goes (RUNNING -> SUCCESS/FAILED).

    Returns the created AudioSegments instances.
    """
    recovered_audio_file = RecoveredAudioFile.objects.get(pk=recovered_audio_file_id)
    recovered_audio_file.status = RecoveredAudioFile.Status.RUNNING
    recovered_audio_file.started_at = timezone.now()
    recovered_audio_file.save(update_fields=["status", "started_at"])

    try:
        # The full download is only a working copy used to run ACRCloud
        # identification - segments are trimmed straight from `url` afterwards,
        # so it doesn't need to be kept around; a tmp dir cleans it up for us.
        with tempfile.TemporaryDirectory(prefix="audio_recovery_") as tmp_dir:
            full_path = Path(tmp_dir) / "full.mp3"

            subprocess.run(
                ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", url, "-c", "copy", str(full_path)],
                check=True,
            )

            recovered_audio_file.source_url = url
            recovered_audio_file.duration_seconds = audio_duration_seconds(full_path)
            recovered_audio_file.save(update_fields=["source_url", "duration_seconds"])

            results = identify_audio_file(str(full_path), access_key, access_secret)
            save_json(results)
            results = join_no_result_chunks(results)
            save_json(results)

        segments = create_segments_from_chunks(
            recovered_audio_file, results, folder, created_by=recovered_audio_file.created_by
        )

        recovered_audio_file.status = RecoveredAudioFile.Status.SUCCESS
        recovered_audio_file.finished_at = timezone.now()
        recovered_audio_file.save(update_fields=["status", "finished_at"])
        return segments

    except Exception as exc:
        recovered_audio_file.status = RecoveredAudioFile.Status.FAILED
        recovered_audio_file.error = str(exc)
        recovered_audio_file.finished_at = timezone.now()
        recovered_audio_file.save(update_fields=["status", "error", "finished_at"])
        raise


def trim_audio_from_url(
    url: str,
    start_seconds: int,
    folder: str,
    duration_seconds: int | None = None,
    end_seconds: int | None = None,
    name: str | None = None,
) -> dict:
    """Save a slice of a remote audio file into MEDIA_ROOT/audio_recovery/<folder>/.

    ffmpeg reads straight from `url` (use a direct-download link, e.g. a Dropbox
    link ending in ?dl=1) and copies the slice without re-encoding.

    Give the end of the slice as **either** `duration_seconds` **or** `end_seconds`
    (end wins if both are passed). `folder` is required and groups trims into a
    subfolder under audio_recovery, chosen by the caller (e.g. a date, an incident
    name, a channel) - so the top-level folder doesn't become one giant flat directory.

    Returns::

        {
            "file_path": "media/audio_recovery/<folder>/<name>.mp3",       # store this
            "media_url": "/api/media/audio_recovery/<folder>/<name>.mp3",  # servable URL
            "full_path": "<absolute path on disk>",
        }
    """
    start_seconds = int(start_seconds)
    if end_seconds is not None:
        duration_seconds = int(end_seconds) - start_seconds
    elif duration_seconds is not None:
        duration_seconds = int(duration_seconds)
    else:
        raise ValueError("pass either duration_seconds or end_seconds")
    if duration_seconds <= 0:
        raise ValueError("the slice is empty - check start_seconds / end_seconds")

    if not name:
        name = f"trim_{start_seconds}s_{duration_seconds}s_{int(time.time())}"
    if not name.endswith(".mp3"):
        name += ".mp3"

    relative_path = f"media/{MEDIA_SUBDIR}/{folder}/{name}"
    full_path = Path(settings.MEDIA_ROOT) / MEDIA_SUBDIR / folder / name
    full_path.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-ss", str(start_seconds),
            "-i", url,
            "-t", str(duration_seconds),
            "-c", "copy",
            str(full_path),
        ],
        check=True,
    )

    return {
        "file_path": relative_path,
        "media_url": f"/api/{relative_path}",
        "full_path": str(full_path),
    }


def create_segments_from_chunks(
    recovered_audio_file: RecoveredAudioFile,
    chunks: list[dict],
    folder: str,
    created_by=None,
) -> list[AudioSegments]:
    """Turn joined ACRCloud chunks (from `join_no_result_chunks`) into AudioSegments rows.

    For each chunk: trims that second-range out of `recovered_audio_file.source_url`
    into media/ (via `trim_audio_from_url`), creates an AudioSegments row for it
    with `is_recovered_audio=True`, then links the two with a RecoveredAudioSegment.

    Chunk `start_sec`/`end_sec` are offsets from `recovered_audio_file.recorded_at`,
    used to get each segment's actual start_time/end_time.

    Returns the created AudioSegments instances.
    """
    url = recovered_audio_file.source_url
    channel = recovered_audio_file.channel
    recorded_at = recovered_audio_file.recorded_at

    segments = []
    for index, chunk in enumerate(chunks):
        start_sec = chunk["start_sec"]
        end_sec = chunk["end_sec"]
        duration = end_sec - start_sec
        if duration <= 0:
            continue

        seg_start = recorded_at + timedelta(seconds=start_sec)
        seg_end = recorded_at + timedelta(seconds=end_sec)

        name = f"audio_{channel.project_id}_{channel.channel_id}_{seg_start:%Y%m%d%H%M%S}_{duration}"
        trimmed = trim_audio_from_url(url, start_sec, folder, duration_seconds=duration, name=name)

        kind, entry = first_match(chunk.get("response") or {})

        title_before = title_after = None
        if entry is None:
            # Unrecognized gap - title_before/title_after are the tracks that
            # were recognised immediately before/after it, same as the main
            # ingest pipeline (data_analysis/services/audio_segments.py).
            prev_chunk = chunks[index - 1] if index > 0 else None
            next_chunk = chunks[index + 1] if index + 1 < len(chunks) else None
            _, prev_entry = first_match((prev_chunk or {}).get("response") or {})
            _, next_entry = first_match((next_chunk or {}).get("response") or {})
            title_before = prev_entry.get("title") if prev_entry else "UNKNOWN"
            title_after = next_entry.get("title") if next_entry else "UNKNOWN"

        segment = AudioSegments.objects.create(
            segment_type="broadcast",
            start_time=seg_start,
            end_time=seg_end,
            duration_seconds=duration,
            file_name=Path(trimmed["file_path"]).name,
            file_path=trimmed["file_path"],
            audio_location_type="file_path",
            is_recognized=entry is not None,
            title=entry.get("title") if entry else None,
            title_before=title_before,
            title_after=title_after,
            metadata_json={"source": kind, **entry} if entry else None,
            channel=channel,
            source="user",
            is_recovered_audio=True,
            is_audio_downloaded=True,
            created_by=created_by,
        )
        RecoveredAudioSegment.objects.create(
            recovered_audio_file=recovered_audio_file,
            audio_segment=segment,
            start_sec=start_sec,
            end_sec=end_sec,
        )
        segments.append(segment)

    return segments
