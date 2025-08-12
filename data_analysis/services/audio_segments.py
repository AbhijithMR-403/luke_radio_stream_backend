from datetime import datetime, timedelta, timezone as dt_timezone
import requests
from typing import Optional
import os
from django.utils import timezone
from decouple import config
from acr_admin.models import GeneralSetting, Channel
from openai import OpenAI
from django.core.exceptions import ValidationError
from config.validation import ValidationUtils

from data_analysis.models import UnrecognizedAudio, AudioSegments as AudioSegmentsModel
from data_analysis.services.transcription_service import RevAISpeechToText

class AudioSegments:
    BASE_URL = "https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels/{channel_id}/results"

    @staticmethod
    def _get_default_date():
        return timezone.now().strftime("%Y%m%d")

    @staticmethod
    def _construct_url(project_id: int, channel_id: int, date: Optional[str] = None):
        # Validate parameters
        ValidationUtils.validate_positive_integer(project_id, "project_id")
        ValidationUtils.validate_positive_integer(channel_id, "channel_id")
        
        query_date = date or AudioSegments._get_default_date()
        return f"{AudioSegments.BASE_URL}?type=day&date={query_date}".format(
            pid=project_id,
            channel_id=channel_id
        )

    @staticmethod
    def fetch_data(project_id: int, channel_id: int, date: Optional[str] = None, hours: Optional[list] = None):
        # Validate parameters
        ValidationUtils.validate_positive_integer(project_id, "project_id")
        ValidationUtils.validate_positive_integer(channel_id, "channel_id")
        
        token = ValidationUtils.validate_acr_cloud_api_key()
        url = AudioSegments._construct_url(project_id, channel_id, date)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Filter data by hours if specified
        if hours is not None and 'data' in data:
            filtered_data = {'data': []}
            for item in data['data']:
                if 'metadata' in item and 'record_timestamp' in item['metadata']:
                    # Extract hour from record_timestamp (format: "20250802000011")
                    timestamp = item['metadata']['record_timestamp']
                    if len(timestamp) >= 10:  # Ensure we have enough characters
                        hour = int(timestamp[8:10])  # Extract hour from position 8-9
                        if hour in hours:
                            filtered_data['data'].append(item)
            return filtered_data
        
        return data

    @staticmethod
    def get_today_data_excluding_last_hour(project_id: int, channel_id: int):
        """
        Fetches all data from today excluding the last 1 hour from current time.
        For example, if current time is 11:25, it excludes everything after 10:25.
        
        Args:
            project_id: ACRCloud project ID
            channel_id: ACRCloud channel ID
            
        Returns:
            dict: All today's data except the last hour from current time
        """
        # Validate parameters
        ValidationUtils.validate_positive_integer(project_id, "project_id")
        ValidationUtils.validate_positive_integer(channel_id, "channel_id")
        
        # Get current time and calculate the cutoff time (1 hour ago)
        current_time = timezone.now()
        cutoff_time = current_time - timedelta(hours=1)
        
        # Get the hour of the cutoff time
        cutoff_hour = cutoff_time.hour
        print(f"Cutoff hour: {cutoff_hour}")
        # If cutoff is in a previous day, return empty data
        if cutoff_time.date() < current_time.date():
            return {'data': []}
        
        # Create list of all hours from 0 to cutoff_hour-1
        # This excludes the cutoff hour and current hour
        if cutoff_hour == 0:
            # If cutoff is at midnight (hour 0), return empty data
            return {'data': []}
        else:
            # Get all hours from 0 to cutoff_hour-1
            past_hours = list(range(cutoff_hour))
            return AudioSegments.fetch_data(project_id, channel_id, hours=past_hours)

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
                # Parse UTC timestamp and make it timezone-aware
                naive_start_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                start_time = timezone.make_aware(naive_start_time, timezone=dt_timezone.utc)
                end_time = start_time + timedelta(seconds=played_duration)
                results.append((start_time, end_time, title))

        results.sort()

        # Filter for only the desired 1-hour window
        if results:
            # Determine if we're looking at today or a past date
            is_today = date is None or date == timezone.now().strftime("%Y%m%d")
            
            if is_today:
                # For today: extract 1 hour from right now
                current_time = timezone.now()
                current_hour = current_time.replace(minute=0, second=0, microsecond=0)
                window_end = current_hour
                window_start = window_end - timedelta(hours=hour_offset + 1)
                window_end = window_start + timedelta(hours=1)
            else:
                # For past days: show the last hour of that day (11pm-12am)
                # hour_offset=0 means 11pm-12am, hour_offset=1 means 10pm-11pm, etc.
                naive_day_start = datetime.strptime(date, "%Y%m%d")
                day_start = timezone.make_aware(naive_day_start, timezone=dt_timezone.utc)
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

    @staticmethod
    def get_audio_segments_with_recognition_status(data, channel=None, date=None):
        """
        Fetches both recognized and unrecognized audio segments with recognition status.
        
        Args:
            data: API response data from ACRCloud
            date: Date string in YYYYMMDD format (default: None for today)
            channel: Channel object for database insertion
            
        Returns:
            list: List of dictionaries containing audio segments with recognition status
                  Each dict has keys: start_time, end_time, duration_seconds, title, is_recognized
        """
        results = []

        # Extract recognized segments from the data
        for item in data.get("data", []):
            metadata = item.get("metadata", {})
            timestamp_str = metadata.get("timestamp_utc")
            played_duration = metadata.get("played_duration", 0)
            music_list = metadata.get("music", [])
            title = music_list[0].get("title", "Unknown Title") if music_list else "Unknown Title"

            if timestamp_str and played_duration:
                # Parse UTC timestamp and make it timezone-aware
                naive_start_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                start_time = timezone.make_aware(naive_start_time, timezone=dt_timezone.utc)
                end_time = start_time + timedelta(seconds=played_duration)
                
                # Skip segments with zero duration (same start and end time)
                if start_time == end_time:
                    print(f"Warning: Skipping zero-duration segment at {start_time}")
                    continue
                
                results.append({
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": int(played_duration),
                    "title": title,
                    "is_recognized": True,
                    "is_active": True
                })

        # Sort by start time
        results.sort(key=lambda x: x["start_time"])

        # Filter for the specified date
        if results and date:
            # Parse the target date and make it timezone-aware
            naive_target_date = datetime.strptime(date, "%Y%m%d")
            target_date = timezone.make_aware(naive_target_date, timezone=dt_timezone.utc)
            next_date = target_date + timedelta(days=1)
            
            # Filter results to only include segments within the target date
            filtered_results = [r for r in results if target_date <= r["start_time"] < next_date]
        else:
            filtered_results = results

        # Find unrecognized segments (gaps between recognized segments)
        all_segments = []
        
        # Add recognized segments
        all_segments.extend(filtered_results)
        
        # Find gaps between recognized segments
        for i in range(len(filtered_results) - 1):
            current_segment = filtered_results[i]
            next_segment = filtered_results[i + 1]
            
            # Only create gap if there's actually a gap (not touching segments)
            if next_segment["start_time"] > current_segment["end_time"]:
                gap_duration = (next_segment["start_time"] - current_segment["end_time"]).total_seconds()
                unrecognized_segment = {
                    "start_time": current_segment["end_time"],
                    "end_time": next_segment["start_time"],
                    "duration_seconds": int(gap_duration),
                    "title_before": current_segment["title"],
                    "title_after": next_segment["title"],
                    "is_recognized": False,
                    "is_active": True
                }
                all_segments.append(unrecognized_segment)
            elif next_segment["start_time"] == current_segment["end_time"]:
                # Segments are touching (no gap), skip creating unrecognized segment
                print(f"Segments touching at {current_segment['end_time']}, skipping gap creation")
        
        # Sort all segments by start time
        all_segments.sort(key=lambda x: x["start_time"])
        
        # Insert segments into database if channel is provided
        if channel:
            AudioSegments._insert_segments_to_database(all_segments, channel)
        
        return all_segments

    @staticmethod
    def process_audio_data(data_list, channel=None):
        """
        Process data from custom_files or music format and create unrecognized audio segments.
        
        Args:
            data_list: List of dictionaries with metadata containing custom_files or music
            channel: Channel object for generating file names and paths
            
        Returns:
            list: List of dictionaries containing all segments (recognized and unrecognized)
        """
        results = []
        
        # Extract recognized segments from the data
        for item in data_list:
            metadata = item.get("metadata", {})
            timestamp_str = metadata.get("timestamp_utc")
            played_duration = metadata.get("played_duration", 0)
            custom_files = metadata.get("custom_files", [])
            music = metadata.get("music", [])
            
            # Check if we have either custom_files or music data
            if timestamp_str and played_duration and (custom_files or music):
                # Parse UTC timestamp and make it timezone-aware
                naive_start_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                start_time = timezone.make_aware(naive_start_time, timezone=dt_timezone.utc)
                end_time = start_time + timedelta(seconds=played_duration)
                
                # Get title from either custom_files or music
                title = "Unknown Title"
                if custom_files:
                    title = custom_files[0].get("title", "Unknown Title")
                elif music:
                    title = music[0].get("title", "Unknown Title")
                
                # Skip segments with zero duration (same start and end time)
                if start_time == end_time:
                    print(f"Warning: Skipping zero-duration segment at {start_time}")
                    continue
                
                # Set is_active to False if duration is less than 10 seconds
                is_active = played_duration >= 10
                
                # Generate file name and path if channel is provided
                file_name = None
                file_path = None
                if channel:
                    start_time_str = start_time.strftime("%Y%m%d%H%M%S")
                    file_name = f"audio_{channel.project_id}_{channel.channel_id}_{start_time_str}_{int(played_duration)}.mp3"
                    # Create folder structure: start_date/file_name
                    start_date = start_time.strftime("%Y%m%d")
                    file_path = f"media/{start_date}/{file_name}"
                
                results.append({
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": int(played_duration),
                    "title": title,
                    "is_recognized": True,
                    "is_active": is_active,
                    "file_name": file_name,
                    "file_path": file_path
                })

        # Sort by start time
        results.sort(key=lambda x: x["start_time"])

        # Find unrecognized segments (gaps between recognized segments)
        all_segments = []
        
        # Add recognized segments
        all_segments.extend(results)
        
        # Find gaps between recognized segments
        for i in range(len(results) - 1):
            current_segment = results[i]
            next_segment = results[i + 1]
            
            # Only create gap if there's actually a gap (not touching segments)
            if next_segment["start_time"] > current_segment["end_time"]:
                gap_duration = (next_segment["start_time"] - current_segment["end_time"]).total_seconds()
                # Set is_active to False if duration is less than 10 seconds
                is_active = gap_duration >= 10
                
                # Generate file name and path if channel is provided
                file_name = None
                file_path = None
                if channel:
                    start_time_str = current_segment["end_time"].strftime("%Y%m%d%H%M%S")
                    file_name = f"audio_{channel.project_id}_{channel.channel_id}_{start_time_str}_{int(gap_duration)}.mp3"
                    # Create folder structure: start_date/file_name
                    start_date = current_segment["end_time"].strftime("%Y%m%d")
                    file_path = f"media/{start_date}/{file_name}"
                
                unrecognized_segment = {
                    "start_time": current_segment["end_time"],
                    "end_time": next_segment["start_time"],
                    "duration_seconds": int(gap_duration),
                    "title_before": current_segment["title"],
                    "title_after": next_segment["title"],
                    "is_recognized": False,
                    "is_active": is_active,
                    "file_name": file_name,
                    "file_path": file_path
                }
                all_segments.append(unrecognized_segment)
            elif next_segment["start_time"] == current_segment["end_time"]:
                # Segments are touching (no gap), skip creating unrecognized segment
                print(f"Segments touching at {current_segment['end_time']}, skipping gap creation")
        
        # Sort all segments by start time
        all_segments.sort(key=lambda x: x["start_time"])
        
        return all_segments

    @staticmethod
    def _deactivate_overlapping_segments(start_time, end_time, duration_seconds, channel, tolerance_seconds=1):
        """
        Deactivate existing segments that overlap with the given time range.
        Only deactivate segments that were already in the database (not newly added).
        
        Args:
            start_time: Start time of the new segment
            end_time: End time of the new segment
            channel: Channel object
            tolerance_seconds: Tolerance in seconds for overlap detection (default: 1)
            
        Returns:
            int: Number of segments deactivated
        """
        from datetime import timedelta
        
        # First check if exact same start/end times already exist
        exact_matches = AudioSegmentsModel.objects.filter(
            channel=channel,
            start_time=start_time,
            end_time=end_time
        )
        
        if exact_matches.exists():
            print(f"Found {exact_matches.count()} exact match(es) for {start_time} - {end_time}, skipping deactivation")
            return 0
        
        # Calculate tolerance window
        start_tolerance = start_time - timedelta(seconds=tolerance_seconds)
        end_tolerance = end_time + timedelta(seconds=tolerance_seconds)
        
        # Find and deactivate overlapping segments that were already in the database
        # We exclude segments created in the current session by checking created_at
        current_time = timezone.now()
        session_start = current_time - timedelta(minutes=5)  # Consider segments from last 5 minutes as "new"
        
        overlapping_segments = AudioSegmentsModel.objects.filter(
            channel=channel,
            is_active=True,  # Only deactivate active segments
            start_time__lte=end_tolerance,
            end_time__gte=start_tolerance,
            created_at__lt=session_start  # Only deactivate segments created before current session
        ).exclude(
            start_time=start_time,
            end_time=end_time
        )
        
        deactivated_count = overlapping_segments.count()
        
        if deactivated_count > 0:
            # Get the new segment ID that will be created (we'll use a placeholder for now)
            new_segment_identifier = f"{start_time.strftime('%Y%m%d_%H%M%S')}_{duration_seconds}s"
            
            # Update overlapping segments to inactive with detailed notes
            for seg in overlapping_segments:
                seg.is_active = False
                seg.notes = f"🔴 Deactivated due to overlap with newer segment: {new_segment_identifier} (time: {start_time} - {end_time})"
                seg.save()
            
            # Log the deactivation
            segment_details = []
            for seg in overlapping_segments[:3]:  # Limit to first 3 for logging
                segment_details.append(f"ID:{seg.id} {seg.start_time}-{seg.end_time} (created: {seg.created_at})")
            
            print(f"🔴 Deactivated {deactivated_count} existing overlapping segments: {', '.join(segment_details)}")
            print(f"   Due to overlap with newer segment: {new_segment_identifier}")
        
        return deactivated_count

    @staticmethod
    def _insert_segments_to_database(segments, channel):
        """
        Insert audio segments into the AudioSegments database table.
        
        Args:
            segments: List of segment dictionaries
            channel: Channel object
        """
        from data_analysis.tasks import download_audio_task
        
        inserted_count = 0
        deactivated_count = 0
        
        for segment in segments:
            # Deactivate any overlapping segments first
            deactivated = AudioSegments._deactivate_overlapping_segments(
                segment["start_time"], 
                segment["end_time"], 
                segment["duration_seconds"],
                channel
            )
            deactivated_count += deactivated
            
            # Generate filename for the audio file
            start_time_str = segment["start_time"].strftime("%Y%m%d%H%M%S")
            filename = f"audio_{channel.project_id}_{channel.channel_id}_{start_time_str}_{segment['duration_seconds']}.mp3"
            
            # Prepare data for database insertion
            segment_data = {
                'start_time': segment["start_time"],
                'end_time': segment["end_time"],
                'duration_seconds': segment["duration_seconds"],
                'is_recognized': segment["is_recognized"],
                'is_active': True,  # New segment is always active
                'channel': channel,
                'file_name': filename,
                'file_path': f"/api/media/{filename}",
            }
            
            # Add title fields based on recognition status
            if segment["is_recognized"]:
                segment_data['title'] = segment.get("title", "Unknown Title")
            else:
                segment_data['title_before'] = segment.get("title_before", "")
                segment_data['title_after'] = segment.get("title_after", "")
            
            # Create or update the AudioSegments record
            try:
                # Check if this exact segment already exists
                existing_segments = AudioSegmentsModel.objects.filter(
                    start_time=segment["start_time"],
                    end_time=segment["end_time"],
                    channel=channel
                )
                
                if existing_segments.exists():
                    # Update the first existing segment (or create if none exist)
                    obj, created = AudioSegmentsModel.objects.update_or_create(
                        start_time=segment["start_time"],
                        end_time=segment["end_time"],
                        channel=channel,
                        defaults=segment_data
                    )
                    action = "updated" if not created else "created"
                    print(f"Segment {action}: {segment['start_time']} - {segment['end_time']} (ID: {obj.id})")
                else:
                    # Create new segment
                    obj = AudioSegmentsModel.objects.create(**segment_data)
                    print(f"Segment created: {segment['start_time']} - {segment['end_time']} (ID: {obj.id})")
                    created = True
                
                # Update the notes of deactivated segments with the actual new segment ID
                if deactivated > 0:
                    deactivated_segments = AudioSegmentsModel.objects.filter(
                        channel=channel,
                        is_active=False,
                        notes__contains="🔴 Deactivated due to overlap with newer segment"
                    ).exclude(
                        start_time=segment["start_time"],
                        end_time=segment["end_time"]
                    )
                    
                    for seg in deactivated_segments:
                        if f"{segment['start_time'].strftime('%Y%m%d_%H%M%S')}_{segment['duration_seconds']}s" in seg.notes:
                            seg.notes = f"🔴 Deactivated due to overlap with segment ID:{obj.id} (time: {segment['start_time']} - {segment['end_time']})"
                            seg.save()



                
                # Call Celery task to download audio
                download_audio_task.delay(
                    project_id=channel.project_id,
                    channel_id=channel.channel_id,
                    start_time=segment["start_time"],
                    duration_seconds=segment["duration_seconds"],
                    filename=filename
                )
                print(f"Triggered audio download task for audio segment: {start_time_str}")
                inserted_count += 1
                    
            except Exception as e:
                # Log error but continue processing other segments
                print(f"Error inserting segment: {str(e)}")
        
        print(f"Database insertion complete: {inserted_count} inserted, {deactivated_count} deactivated")

    @staticmethod
    def get_separated_audio_segments(project_id: int, channel_id: int, date: Optional[str] = None, channel=None):
        """
        Fetches audio data from ACRCloud API and separates recognized and unrecognized segments.
        
        Args:
            project_id: ACRCloud project ID
            channel_id: ACRCloud channel ID  
            date: Date string in YYYYMMDD format (default: None for today)
            channel: Channel object for database insertion
            
        Returns:
            dict: Dictionary containing 'recognized' and 'unrecognized' lists of segments
        """
        try:
            # Fetch data from ACRCloud API
            data = AudioSegments.fetch_data(project_id, channel_id, date)
            
            # Get all segments with recognition status
            all_segments = AudioSegments.get_audio_segments_with_recognition_status(data, date, channel)
            
            # Separate recognized and unrecognized segments
            recognized_segments = [segment for segment in all_segments if segment["is_recognized"]]
            unrecognized_segments = [segment for segment in all_segments if not segment["is_recognized"]]
            
            return {
                "recognized": recognized_segments,
                "unrecognized": unrecognized_segments,
                "total_recognized": len(recognized_segments),
                "total_unrecognized": len(unrecognized_segments),
                "total_segments": len(all_segments)
            }
            
        except Exception as e:
            raise Exception(f"Error fetching audio segments: {str(e)}")
    

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

