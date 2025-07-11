from datetime import datetime, timedelta
import requests
from acr_admin.models import GeneralSetting, RevTranscriptionJob
from typing import Optional
import os
from django.utils import timezone
from decouple import config

class ACRCloudUtils:
    @staticmethod
    def get_channel_name_by_id(pid: int, channel_id, access_token: str = None):
        """
        Fetches the channel list for the given project id (pid) from ACRCloud API,
        finds the channel with the given channel_id, and returns its name.
        If pid is invalid, returns error dict and 403 status code with a project permission message.
        If channel_id is not found, returns error dict and 403 status code with a channel not found message.
        On success, returns (channel_name, None).
        """
        # Ensure channel_id is an integer, or return error if not valid
        try:
            channel_id_int = int(channel_id)
        except (ValueError, TypeError):
            return {"error": "Invalid channel ID. Must be an integer or string of digits."}, 400
        url = f"https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels"
        if not access_token:
            settings = GeneralSetting.objects.first()
            if not settings or not settings.arc_cloud_api_key:
                return {"error": "ACRCloud API key not configured"}, 403
            access_token = settings.arc_cloud_api_key
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 403:
                return {"error": "You don't have permission to access this project (invalid project id)"}, 403
            response.raise_for_status()
            data = response.json().get("data", [])
            for channel in data:
                if channel.get("id") == channel_id_int:
                    return channel.get("name"), None
            # If channel_id not found
            return {"error": "Channel ID not found in this project"}, 403
        except Exception as e:
            # Optionally log the error
            return {"error": "You don't have permission to access this project"}, 403



class UnrecognizedAudioTimestamps:
    BASE_URL = "https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels/{channel_id}/results"

    @staticmethod
    def _get_default_date():
        return datetime.utcnow().strftime("%Y%m%d")

    @staticmethod
    def _construct_url(project_id: int, channel_id: int, date: Optional[str] = None):
        query_date = date or UnrecognizedAudioTimestamps._get_default_date()
        return f"{UnrecognizedAudioTimestamps.BASE_URL}?type=day&date={query_date}".format(
            pid=project_id,
            channel_id=channel_id
        )

    @staticmethod
    def fetch_data(project_id: int, channel_id: int, date: Optional[str] = None):
        settings = GeneralSetting.objects.first()
        if not settings or not settings.arc_cloud_api_key:
            raise ValueError("ACRCloud API key not configured")
        token = settings.arc_cloud_api_key
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
                    "start_time": end_time_current.strftime("%Y%m%d%H%M%S"),
                    "end_time": start_time_next.strftime("%Y%m%d%H%M%S"),
                    "duration_seconds": int(gap_duration),
                    "before_title": title_current,
                    "after_title": title_next
                })

        return unrecognized


class AudioDownloader:
    BASE_URL = "https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels/{channel_id}/recordings"

    @staticmethod
    def download_audio(project_id: int, channel_id: int, start_time: str, duration_seconds: int):
        """
        Downloads audio from the ACRCloud API for the given parameters and saves it as an mp3 file.
        - start_time: timestamp_utc (format: YYYYMMDDHHMMSS)
        - duration_seconds: played_duration (int)
        - If duration_seconds > 600, sets record_after=duration_seconds-600
        Returns the file path of the downloaded mp3.
        """
        settings = GeneralSetting.objects.first()
        if not settings or not settings.arc_cloud_api_key:
            raise ValueError("ACRCloud API key not configured")
        token = settings.arc_cloud_api_key
        params = {
            "timestamp_utc": start_time,
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
        filename = f"audio_{project_id}_{channel_id}_{start_time}_{duration_seconds}.mp3"
        file_path = os.path.join(media_dir, filename)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        media_url = f"/media/{filename}"
        return media_url

    @staticmethod
    def bulk_download_audio(project_id: int, channel_id: int, segments: list):
        """
        Downloads multiple audio segments in bulk.
        segments: list of dicts with keys 'start_time' and 'duration_seconds'.
        Returns a list of file paths for the downloaded mp3 files.
        Also inserts into UnrecognizedAudio and TranscriptionDetail for each segment.
        """
        from acr_admin.models import UnrecognizedAudio, TranscriptionDetail
        from datetime import datetime, timedelta
        file_paths = []
        for segment in segments:
            start_time = segment.get("start_time")
            duration_seconds = segment.get("duration_seconds")
            if start_time and duration_seconds:
                # Download audio
                file_path = AudioDownloader.download_audio(
                    project_id,
                    channel_id,
                    start_time,
                    duration_seconds,
                )
                file_paths.append(file_path)

                try:

                    # Insert into UnrecognizedAudio, avoid duplicates by media_path
                    ua, created = UnrecognizedAudio.objects.get_or_create(
                        media_path=file_path,
                        defaults={
                            "start_time": start_time,
                            "end_time": segment.get("end_time"),
                            "duration": duration_seconds,
                        }
                    )
                except Exception as e:
                    print(f"Error creating UnrecognizedAudio record: {e}")
                    # Continue with the next segment even if this one fails
                    continue

                # Call Rev.ai transcription
                transcription_response = RevAISpeechToText.create_transcription_job(file_path)
        return file_paths


class RevAISpeechToText:
    @staticmethod
    def create_transcription_job(media_path: str):
        """
        Creates a transcription job on Rev.ai using the provided media URL and notification URL.
        Uses revai_access_token from GeneralSetting if api_key is not provided.
        Returns the API response as JSON.
        """
        base_url = config('PUBLIC_BASE_URL', default='http://localhost:8000')
        notification_url = f"{base_url}/api/rev-callback"
        media_url = f"{base_url}{media_path}"
        settings = GeneralSetting.objects.first()
        if not settings or not settings.revai_access_token:
            raise ValueError("Rev.ai API key not configured")
        api_key = settings.revai_access_token
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
        from acr_admin.models import UnrecognizedAudio, TranscriptionDetail
        settings = GeneralSetting.objects.first()
        if not settings or not settings.revai_access_token:
            raise ValueError("Rev.ai API key not configured")
        api_key = settings.revai_access_token
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
                return transcript
            else:
                # Conflict: both exist but are different objects
                print("\033[91mConflict: UnrecognizedAudio and RevTranscriptionJob do not match for existing TranscriptionDetail\033[0m")  # Red
                return transcript
        elif existing_by_audio or existing_by_job:
            # Conflict: one exists but not both together
            print("\033[91mConflict: UnrecognizedAudio or RevTranscriptionJob already linked to a different TranscriptionDetail\033[0m")  # Red
            return transcript

        # Create TranscriptionDetail if neither exists
        TranscriptionDetail.objects.create(
            unrecognized_audio=unrec_audio,
            rev_job=revid,
            transcript=transcript
        )
        return transcript

