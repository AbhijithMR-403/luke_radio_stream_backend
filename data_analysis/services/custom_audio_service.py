from pathlib import Path
from datetime import datetime, timedelta

from django.core.exceptions import ValidationError
from django.core.files.storage import default_storage
from django.utils import timezone
from django.utils.text import slugify

from core_admin.models import Channel
from data_analysis.models import AudioSegments
from data_analysis.services.transcription_service import RevAISpeechToText
from mutagen import File as MutagenFile

class CustomAudioService:
    """
    Helpers for saving uploaded custom audio files and inserting
    corresponding AudioSegments records.
    
    Files are stored under: custom_audio/{date}/{filename}
    """

    ALLOWED_EXTENSIONS = {".mp3", ".wav", ".aac"}
    MAX_SIZE_BYTES = 100 * 1024 * 1024  # 100 MB

    @staticmethod
    def _get_filename_from_file(file) -> str:
        """Extract the original filename from the uploaded file object."""
        if hasattr(file, "name"):
            filename = file.name
        elif hasattr(file, "filename"):
            filename = file.filename
        else:
            raise ValidationError("file does not have a name attribute")

        if not filename or not filename.strip():
            raise ValidationError("filename is invalid (empty)")

        return filename.strip()

    @staticmethod
    def download_audio(file):
        """
        Save an uploaded audio file to custom_audio/{date}/{filename} using
        Django's default storage. Uses the filename from the uploaded file.

        Args:
            file: Django UploadedFile object or file-like object.

        Returns:
            dict: {
                "path": "<relative storage path>",
                "file_name": "<sanitized file name>",
            }
        """
        if not file:
            raise ValidationError("file is required")

        # Get and sanitize filename
        filename = CustomAudioService._get_filename_from_file(file)

        # Prevent path traversal: use only the final path component (no .. or slashes)
        filename_base = Path(filename).name
        if not filename_base:
            raise ValidationError("filename is invalid (empty after sanitization)")

        # Extract extension and slugify the base name
        path_obj = Path(filename_base)
        extension = path_obj.suffix  # e.g., ".mp3"
        base_name = path_obj.stem    # e.g., "My Interview @ Cafe!!"

        # Slugify the base name to make it filesystem-safe
        slugified_base = slugify(base_name)
        if not slugified_base:
            # If slugify results in empty string, use a default name
            slugified_base = "audio_file"

        # Reconstruct filename with slugified base and original extension
        safe_filename = f"{slugified_base}{extension}" if extension else slugified_base

        date_str = datetime.now().strftime("%Y%m%d")
        # Build storage path: custom_audio/{date}/{filename}
        storage_name = f"custom_audio/{date_str}/{safe_filename}"

        # Additional validation: ensure no path traversal in storage_name
        if ".." in storage_name or storage_name.startswith("/"):
            raise ValidationError("filename is invalid (path traversal not allowed)")

        # Check file size if available
        if hasattr(file, "size") and file.size is not None:
            if file.size > CustomAudioService.MAX_SIZE_BYTES:
                raise ValidationError(
                    f"Audio file size ({file.size / (1024 * 1024):.1f} MB) exceeds "
                    f"maximum allowed (100 MB). Upload skipped."
                )

        # Save the uploaded file using Django's default storage
        saved_name = default_storage.save(storage_name, file)
        actual_filename = Path(saved_name).name
        return {
            "path": saved_name,
            "file_name": actual_filename,
        }

    @staticmethod
    def _get_audio_duration_seconds_from_storage_path(storage_path: str) -> int:
        """
        Read the audio duration (in whole seconds) from a file stored in
        Django's default storage. Uses mutagen when available.

        Returns 0 if duration cannot be determined.
        """
        if not storage_path:
            return 0

        try:
            with default_storage.open(storage_path, "rb") as fh:
                audio = MutagenFile(fh)
            if not audio or not getattr(audio, "info", None):
                return 0
            length = getattr(audio.info, "length", None)
            if length is None:
                return 0
            return max(0, int(length))
        except Exception:
            return 0

    @staticmethod
    def insert_custom_audio_segment(
        file,
        channel_id: int,
        title: str,
        notes: str | None = None,
        recorded_at=None,
    ) -> dict:
        """
        Save an uploaded custom audio file and insert a corresponding
        AudioSegments row.

        Args:
            file: Uploaded audio file (MP3 / WAV / AAC).
            channel_id: ID of the Channel to associate.
            title: Title for the custom audio segment.
            notes: Optional notes for the segment.
            recorded_at: Optional datetime; stored in pub_date. When provided,
                         start_time is set to this value and end_time is
                         start_time + duration (currently 0 seconds).

        Returns:
            dict with basic info about the created segment and stored file.
        """
        if not file:
            raise ValidationError("file is required")

        # Validate extension (MP3 / WAV / AAC)
        original_name = CustomAudioService._get_filename_from_file(file)
        ext = Path(original_name).suffix.lower()
        if ext not in CustomAudioService.ALLOWED_EXTENSIONS:
            allowed = ", ".join(sorted(CustomAudioService.ALLOWED_EXTENSIONS))
            raise ValidationError(
                f"Unsupported audio format '{ext or 'unknown'}'. "
                f"Allowed extensions are: {allowed}."
            )

        # Validate channel
        try:
            channel = Channel.objects.get(pk=channel_id)
        except Channel.DoesNotExist:
            raise ValidationError(f"Channel with id {channel_id} does not exist")

        if not title or not str(title).strip():
            raise ValidationError("title is required for custom audio segments")

        # First, persist the file
        audio_info = CustomAudioService.download_audio(file)
        storage_path = audio_info["path"]
        file_name = audio_info["file_name"]

        # Compute duration from the stored file (in seconds)
        duration_seconds = CustomAudioService._get_audio_duration_seconds_from_storage_path(
            storage_path
        )

        print(f"Duration seconds: {duration_seconds}")
        # Handle recorded_at / pub_date and timestamps
        if recorded_at is not None:
            if timezone.is_naive(recorded_at):
                start_time = timezone.make_aware(recorded_at)
            else:
                start_time = recorded_at
            pub_date = start_time
        else:
            start_time = timezone.now()
            pub_date = None

        # Derive end_time from duration
        end_time = start_time + timedelta(seconds=duration_seconds)

        segment = AudioSegments.objects.create(
            segment_type="custom",
            channel=channel,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            file_name=file_name,
            file_path="media/" + storage_path,
            audio_url=None,
            rss_guid=None,
            pub_date=pub_date,
            is_recognized=True,
            is_active=True,
            is_analysis_completed=False,
            is_audio_downloaded=True,
            is_manually_processed=False,
            is_delete=False,
            title=title,
            notes=notes or "",
            source="user",
            audio_location_type="file_path",
        )

        RevAISpeechToText.trigger_transcription_for_single_segment(segment)

        return {
            "segment_id": segment.id,
            "file_path": segment.file_path,
            "file_name": segment.file_name,
            "segment_type": segment.segment_type,
            "pub_date": segment.pub_date,
            "start_time": segment.start_time,
            "end_time": segment.end_time,
            "duration_seconds": segment.duration_seconds,
            "title": segment.title,
            "notes": segment.notes,
        }

