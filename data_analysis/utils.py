from datetime import datetime, timedelta
import requests
from typing import Optional
import os
from django.utils import timezone
from decouple import config
from acr_admin.models import GeneralSetting, Channel
from openai import OpenAI
from django.core.exceptions import ValidationError

from data_analysis.models import RevTranscriptionJob, TranscriptionAnalysis, TranscriptionDetail, UnrecognizedAudio

class ValidationUtils:
    """Utility class for validating function calls and parameters"""
    
    @staticmethod
    def validate_channel_exists(project_id: int, channel_id: int):
        """Validate that the channel exists and is not deleted"""
        try:
            channel = Channel.objects.get(project_id=project_id, channel_id=channel_id, is_deleted=False)
            return channel
        except Channel.DoesNotExist:
            raise ValidationError(f"Channel with project_id {project_id} and channel_id {channel_id} not found or is deleted")
    
    @staticmethod
    def validate_settings_exist():
        """Validate that GeneralSetting exists"""
        settings = GeneralSetting.objects.first()
        if not settings:
            raise ValidationError("GeneralSetting not found. Please configure the application settings.")
        return settings
    
    @staticmethod
    def validate_acr_cloud_api_key():
        """Validate that ACRCloud API key is configured"""
        settings = ValidationUtils.validate_settings_exist()
        if not settings.acr_cloud_api_key:
            raise ValidationError("ACRCloud API key not configured in GeneralSetting")
        return settings.acr_cloud_api_key
    
    @staticmethod
    def validate_revai_api_key():
        """Validate that Rev.ai API key is configured"""
        settings = ValidationUtils.validate_settings_exist()
        if not settings.revai_access_token:
            raise ValidationError("Rev.ai API key not configured in GeneralSetting")
        return settings.revai_access_token
    
    @staticmethod
    def validate_openai_api_key():
        """Validate that OpenAI API key is configured"""
        settings = ValidationUtils.validate_settings_exist()
        if not settings.openai_api_key:
            raise ValidationError("OpenAI API key not configured in GeneralSetting")
        return settings.openai_api_key
    
    @staticmethod
    def validate_positive_integer(value, field_name: str):
        """Validate that a value is a positive integer"""
        if not isinstance(value, int) or value <= 0:
            raise ValidationError(f"{field_name} must be a positive integer, got: {value}")
        return value
    
    @staticmethod
    def validate_positive_number(value, field_name: str):
        """Validate that a value is a positive number"""
        if not isinstance(value, (int, float)) or value <= 0:
            raise ValidationError(f"{field_name} must be a positive number, got: {value}")
        return value
    
    @staticmethod
    def validate_required_field(value, field_name: str):
        """Validate that a required field is not None or empty"""
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValidationError(f"{field_name} is required and cannot be empty")
        return value
    
    @staticmethod
    def validate_list_not_empty(value, field_name: str):
        """Validate that a list is not empty"""
        if not isinstance(value, list):
            raise ValidationError(f"{field_name} must be a list, got: {type(value)}")
        if not value:
            raise ValidationError(f"{field_name} cannot be empty")
        return value
    
    @staticmethod
    def validate_file_path(file_path: str):
        """Validate that a file path is valid"""
        if not file_path or not isinstance(file_path, str):
            raise ValidationError("File path must be a non-empty string")
        if not file_path.startswith('/'):
            raise ValidationError("File path must start with '/'")
        return file_path
    
    @staticmethod
    def validate_url(url: str):
        """Validate that a URL is valid"""
        if not url or not isinstance(url, str):
            raise ValidationError("URL must be a non-empty string")
        if not url.startswith(('http://', 'https://')):
            raise ValidationError("URL must start with http:// or https://")
        return url


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


class RevAISpeechToText:
    @staticmethod
    def create_transcription_job(media_path: str):
        """
        Creates a transcription job on Rev.ai using the provided media URL and notification URL.
        Uses revai_access_token from GeneralSetting if api_key is not provided.
        Returns the API response as JSON.
        """
        # Validate parameters
        ValidationUtils.validate_file_path(media_path)
        
        base_url = config('PUBLIC_BASE_URL')
        notification_url = f"{base_url}/api/rev-callback"
        media_url = f"{base_url}{media_path}"
        
        api_key = ValidationUtils.validate_revai_api_key()
        url = "https://api.rev.ai/speechtotext/v1/jobs"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        data = {
            "media_url": media_url,
            "notification_config": {
                "url": notification_url
            },
            "options": {
                "timestamps": False
            }
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def get_transcript_by_job_id(revid: RevTranscriptionJob, media_path: str):
        """
        Fetches the transcript for a given Rev.ai job ID using the access token from GeneralSetting.
        Inserts a TranscriptionDetail linked to the UnrecognizedAudio (by media_path) and the RevTranscriptionJob.
        Returns the transcript as plain text.
        """
        # Validate parameters
        if not isinstance(revid, RevTranscriptionJob):
            raise ValidationError("revid must be a RevTranscriptionJob instance")
        ValidationUtils.validate_file_path(media_path)
        
        api_key = ValidationUtils.validate_revai_api_key()
        url = f"https://api.rev.ai/speechtotext/v1/jobs/{revid.job_id}/transcript"
        headers = {
            "Authorization": f"Bearer {api_key}",
            # "Accept": "application/json",
            "Accept":"text/plain"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        transcript = response.text

        # Find the UnrecognizedAudio object by media_path
        try:
            unrec_audio = UnrecognizedAudio.objects.get(media_path=media_path)
        except UnrecognizedAudio.DoesNotExist:
            raise ValueError(f"UnrecognizedAudio with media_path {media_path} not found")

        # Check for existing TranscriptionDetail by unrecognized_audio and rev_job
        from django.core.exceptions import ObjectDoesNotExist
        existing_by_audio = None
        existing_by_job = None
        try:
            existing_by_audio = TranscriptionDetail.objects.get(unrecognized_audio=unrec_audio)
        except ObjectDoesNotExist:
            pass
        try:
            existing_by_job = TranscriptionDetail.objects.get(rev_job=revid)
        except ObjectDoesNotExist:
            pass

        if existing_by_audio and existing_by_job:
            if existing_by_audio == existing_by_job:
                # Both point to the same TranscriptionDetail (already present)
                print("\033[93mAlready present\033[0m")  # Yellow
                return existing_by_audio
            else:
                # Conflict: both exist but are different objects
                print("\033[91mConflict: UnrecognizedAudio and RevTranscriptionJob do not match for existing TranscriptionDetail\033[0m")  # Red
                return existing_by_audio or existing_by_job
        elif existing_by_audio or existing_by_job:
            # Conflict: one exists but not both together
            print("\033[91mConflict: UnrecognizedAudio or RevTranscriptionJob already linked to a different TranscriptionDetail\033[0m")  # Red
            return existing_by_audio or existing_by_job

        # Create TranscriptionDetail if neither exists
        transcription_detail = TranscriptionDetail.objects.create(
            unrecognized_audio=unrec_audio,
            rev_job=revid,
            transcript=transcript
        )
        return transcription_detail


class TranscriptionAnalyzer:
    @staticmethod
    def analyze_transcription(transcription_detail):
        if not isinstance(transcription_detail, TranscriptionDetail):
            raise ValidationError("transcription_detail must be a TranscriptionDetail instance")
        
        # Validate that transcription_detail has a transcript
        if not transcription_detail.transcript or not transcription_detail.transcript.strip():
            raise ValidationError("transcription_detail must have a non-empty transcript")
        
        # Validate OpenAI API key
        api_key = ValidationUtils.validate_openai_api_key()
        settings = ValidationUtils.validate_settings_exist()
        client = OpenAI(api_key=api_key)
        transcript = transcription_detail.transcript

        def chat_params(prompt, transcript, max_tokens):
            return {
                "model": settings.chatgpt_model or "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": transcript}
                ],
                "max_tokens": max_tokens if max_tokens > 0 else None,
                "temperature": settings.chatgpt_temperature,
                "top_p": settings.chatgpt_top_p,
                "frequency_penalty": settings.chatgpt_frequency_penalty,
                "presence_penalty": settings.chatgpt_presence_penalty,
            }

        # Summary
        summary_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.summarize_transcript_prompt, transcript, 150).items() if v is not None}
        )
        summary = summary_resp.choices[0].message.content.strip()
        # Sentiment
        sentiment_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.sentiment_analysis_prompt, transcript, 10).items() if v is not None}
        )
        sentiment = sentiment_resp.choices[0].message.content.strip()

        # General topics
        general_topics_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.general_topics_prompt, transcript, 100).items() if v is not None}
        )
        general_topics = general_topics_resp.choices[0].message.content.strip()

        # IAB topics
        iab_topics_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.iab_topics_prompt, transcript, 100).items() if v is not None}
        )
        iab_topics = iab_topics_resp.choices[0].message.content.strip()

        # Store in TranscriptionAnalysis
        TranscriptionAnalysis.objects.create(
            transcription_detail=transcription_detail,
            summary=summary,
            sentiment=sentiment,
            general_topics=general_topics,
            iab_topics=iab_topics
        )

