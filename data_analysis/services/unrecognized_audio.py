from datetime import datetime, timedelta
import requests
from typing import Optional
import os
from django.utils import timezone
from decouple import config
from acr_admin.models import GeneralSetting, Channel
from openai import OpenAI
from django.core.exceptions import ValidationError
from config.validation import ValidationUtils

from data_analysis.models import RevTranscriptionJob, TranscriptionAnalysis, TranscriptionDetail, UnrecognizedAudio
from data_analysis.services.transcription_service import RevAISpeechToText

class UnrecognizedAudioTimestamps:
    BASE_URL = "https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels/{channel_id}/results"

    @staticmethod
    def _get_default_date():
        return datetime.utcnow().strftime("%Y%m%d")

    @staticmethod
    def _construct_url(project_id: int, channel_id: int, date: Optional[str] = None):
        # Validate parameters
        ValidationUtils.validate_positive_integer(project_id, "project_id")
        ValidationUtils.validate_positive_integer(channel_id, "channel_id")
        
        query_date = date or UnrecognizedAudioTimestamps._get_default_date()
        return f"{UnrecognizedAudioTimestamps.BASE_URL}?type=day&date={query_date}".format(
            pid=project_id,
            channel_id=channel_id
        )

    @staticmethod
    def fetch_data(project_id: int, channel_id: int, date: Optional[str] = None):
        # Validate parameters
        ValidationUtils.validate_positive_integer(project_id, "project_id")
        ValidationUtils.validate_positive_integer(channel_id, "channel_id")
        
        token = ValidationUtils.validate_acr_cloud_api_key()
        url = UnrecognizedAudioTimestamps._construct_url(project_id, channel_id, date)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def find_unrecognized_segments(data, hour_offset=0, date=None):
        results = []

        for item in data.get("data", []):
            metadata = item.get("metadata", {})
            timestamp_str = metadata.get("timestamp_utc")
            played_duration = metadata.get("played_duration", 0)
            music_list = metadata.get("music", [])
            title = music_list[0].get("title", "Unknown Title") if music_list else "Unknown Title"

            if timestamp_str and played_duration:
                start_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                end_time = start_time + timedelta(seconds=played_duration)
                results.append((start_time, end_time, title))

        results.sort()

        # Filter for only the desired 1-hour window
        if results:
            # Determine if we're looking at today or a past date
            is_today = date is None or date == datetime.utcnow().strftime("%Y%m%d")
            
            if is_today:
                # For today: extract 1 hour from right now
                current_time = datetime.utcnow()
                current_hour = current_time.replace(minute=0, second=0, microsecond=0)
                window_end = current_hour
                window_start = window_end - timedelta(hours=hour_offset + 1)
                window_end = window_start + timedelta(hours=1)
            else:
                # For past days: show the last hour of that day (11pm-12am)
                # hour_offset=0 means 11pm-12am, hour_offset=1 means 10pm-11pm, etc.
                day_start = datetime.strptime(date, "%Y%m%d")
                day_end = day_start + timedelta(days=1) - timedelta(seconds=1)  # 11:59:59
                
                # Calculate window by going backward from the end of the day
                window_end = day_end.replace(minute=59, second=59, microsecond=999999)
                window_start = window_end - timedelta(hours=hour_offset + 1)
                window_end = window_start + timedelta(hours=1)
                
                # Ensure we don't go before 12am of the target day
                day_start_12am = day_start
                if window_start < day_start_12am:
                    window_start = day_start_12am
                    window_end = window_start + timedelta(hours=1)

            results = [r for r in results if window_start <= r[0] < window_end]

        unrecognized = []
        for i in range(len(results) - 1):
            _, end_time_current, title_current = results[i]
            start_time_next, _, title_next = results[i + 1]

            if start_time_next > end_time_current:
                gap_duration = (start_time_next - end_time_current).total_seconds()
                unrecognized.append({
                    "start_time": end_time_current,
                    "end_time": start_time_next,
                    "duration_seconds": int(gap_duration),
                    "before_title": title_current,
                    "after_title": title_next
                })

        return unrecognized


class AudioDownloader:
    BASE_URL = "https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels/{channel_id}/recordings"

    @staticmethod
    def validate_download_parameters(project_id: int, channel_id: int, start_time, duration_seconds: int):
        """Validate parameters before downloading audio"""
        # Validate project_id
        if not isinstance(project_id, int) or project_id <= 0:
            raise ValidationError(f"Invalid project_id: {project_id}. Must be a positive integer.")
        
        # Validate channel_id
        if not isinstance(channel_id, int) or channel_id <= 0:
            raise ValidationError(f"Invalid channel_id: {channel_id}. Must be a positive integer.")
        
        # Validate start_time
        if not start_time:
            raise ValidationError("start_time is required")
        
        # Validate duration_seconds
        if not isinstance(duration_seconds, (int, float)) or duration_seconds <= 0:
            raise ValidationError(f"Invalid duration_seconds: {duration_seconds}. Must be a positive number.")
        
        # Convert start_time to string format if it's a datetime
        if isinstance(start_time, datetime):
            start_time_str = start_time.strftime("%Y%m%d%H%M%S")
        else:
            start_time_str = str(start_time)
        
        return start_time_str, int(duration_seconds)

    @staticmethod
    def download_audio(project_id: int, channel_id: int, start_time, duration_seconds: int):
        """
        Downloads audio from the ACRCloud API for the given parameters and saves it as an mp3 file.
        - start_time: timestamp_utc (format: YYYYMMDDHHMMSS) or datetime object
        - duration_seconds: played_duration (int)
        - If duration_seconds > 600, sets record_after=duration_seconds-600
        Returns the file path of the downloaded mp3.
        """
        # Validate parameters before proceeding
        start_time_str, duration_seconds = AudioDownloader.validate_download_parameters(
            project_id, channel_id, start_time, duration_seconds
        )
        
        settings = GeneralSetting.objects.first()
        if not settings or not settings.acr_cloud_api_key:
            raise ValueError("ACRCloud API key not configured")
        token = settings.acr_cloud_api_key
        params = {
            "timestamp_utc": start_time_str,
            "played_duration": min(duration_seconds, 600)
        }
        if duration_seconds > 600:
            params["record_after"] = duration_seconds - 600

        url = AudioDownloader.BASE_URL.format(pid=project_id, channel_id=channel_id)
        headers = {
            "Authorization": f"Bearer {token}",
        }
        # response = requests.get(url, headers=headers, params=params, stream=True)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        # Ensure media directory exists
        media_dir = os.path.join(os.getcwd(), "media")
        os.makedirs(media_dir, exist_ok=True)
        filename = f"audio_{project_id}_{channel_id}_{start_time_str}_{duration_seconds}.mp3"
        file_path = os.path.join(media_dir, filename)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        media_url = f"/api/media/{filename}"
        return media_url

    @staticmethod
    def validate_bulk_download_parameters(project_id: int, channel_id: int, segments: list):
        """Validate all parameters before bulk download"""
        # Validate project_id and channel_id
        if not isinstance(project_id, int) or project_id <= 0:
            raise ValidationError(f"Invalid project_id: {project_id}. Must be a positive integer.")
        
        if not isinstance(channel_id, int) or channel_id <= 0:
            raise ValidationError(f"Invalid channel_id: {channel_id}. Must be a positive integer.")
        
        # Validate segments is a list
        if not isinstance(segments, list):
            raise ValidationError(f"segments must be a list, got: {type(segments)}")
        
        if not segments:
            raise ValidationError("segments list cannot be empty")
        
        # Validate each segment
        validated_segments = []
        for i, segment in enumerate(segments):
            try:
                validated_segment = UnrecognizedAudio.validate_segment_data(segment)
                validated_segments.append(validated_segment)
            except ValidationError as e:
                raise ValidationError(f"Segment {i}: {str(e)}")
        
        return validated_segments

    @staticmethod
    def bulk_download_audio(project_id: int, channel_id: int, segments: list):
        """
        Downloads multiple audio segments in bulk.
        segments: list of dicts with keys 'start_time' and 'duration_seconds'.
        Returns a list of file paths for the downloaded mp3 files.
        Also inserts into UnrecognizedAudio and TranscriptionDetail for each segment.
        """
        
        # Validate all parameters before any processing
        validated_segments = AudioDownloader.validate_bulk_download_parameters(project_id, channel_id, segments)
        
        # Get the channel object
        try:
            channel = Channel.objects.get(project_id=project_id, channel_id=channel_id, is_deleted=False)
        except Channel.DoesNotExist:
            raise ValueError(f"Channel with project_id {project_id} and channel_id {channel_id} not found")
        
        file_paths = []
        for segment in validated_segments:
            start_time = segment["start_time"]
            duration_seconds = segment["duration_seconds"]
            end_time = segment["end_time"]
            
            # Convert datetime to string format for download
            start_time_str = start_time.strftime("%Y%m%d%H%M%S")
            
            # Download audio
            file_path = AudioDownloader.download_audio(
                project_id,
                channel_id,
                start_time_str,
                duration_seconds,
            )
            file_paths.append(file_path)

            try:
                # Insert into UnrecognizedAudio, avoid duplicates by media_path
                ua, created = UnrecognizedAudio.objects.get_or_create(
                    media_path=file_path,
                    defaults={
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration": duration_seconds,
                        "channel": channel,
                    }
                )
                
                # Validate the created object
                ua.full_clean()
                
            except Exception as e:
                print(f"Error creating UnrecognizedAudio record: {e}")
                # Continue with the next segment even if this one fails
                continue

            # Call Rev.ai transcription
            try:
                transcription_response = RevAISpeechToText.create_transcription_job(file_path)
            except Exception as e:
                print(f"Error creating transcription job: {e}")
                # Continue with the next segment even if transcription fails
                continue
                
        return file_paths

