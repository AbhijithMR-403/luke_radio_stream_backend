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

from data_analysis.models import AudioSegments as AudioSegmentsModel
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
                
                # Skip segments with zero duration (same start and end time)
                if start_time == end_time:
                    print(f"Warning: Skipping zero-duration segment at {start_time}")
                    continue
                
                # Create segment dictionary for overlap checking
                new_segment = {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": int(played_duration)
                }
                
                # Check for overlaps with existing segments
                if not AudioSegments._check_segment_overlap(new_segment, results):
                    print(f"Skipping overlapping segment: {start_time} - {end_time}")
                    continue
                
                # Get title and metadata from either custom_files or music
                title = "Unknown Title"
                metadata_json = None
                
                if music:
                    music_data = music[0] if music else {}
                    title = music_data.get("title", "")
                    
                    # Extract comprehensive metadata from music data
                    metadata_json = {
                        "source": "music",
                        "artists": music_data.get("artists", []),
                        "external_metadata": music_data.get("external_metadata", {}),
                        "external_ids": music_data.get("external_ids", {}),
                        "sample_begin_time_offset_ms": music_data.get("sample_begin_time_offset_ms"),
                        "sample_end_time_offset_ms": music_data.get("sample_end_time_offset_ms"),
                        "play_offset_ms": music_data.get("play_offset_ms"),
                        "result_from": music_data.get("result_from"),
                        "created_at": music_data.get("created_at"),
                    }
                elif custom_files:
                    # Use first custom file's title as main title
                    custom_file_data = custom_files[0] if custom_files else {}
                    title = custom_file_data.get("title", "")
                    
                    # Extract all titles from custom files for metadata
                    all_titles = [cf.get("title", "") for cf in custom_files if cf.get("title")]
                    
                    # Extract metadata from custom file data
                    metadata_json = {
                        "source": "custom_file",
                        "titles": all_titles,  # List of all titles
                    }
                
                # Keep all segments active regardless of duration
                is_active = True
                
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
                    "file_path": file_path,
                    "metadata_json": metadata_json
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
                # Keep all segments active regardless of duration
                is_active = True
                
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
    def _check_segment_overlap(new_segment, existing_segments, gap_threshold_seconds=2):
        """
        Check if a new segment should be included based on overlap rules.
        
        Args:
            new_segment: Dictionary with start_time and end_time
            existing_segments: List of existing segment dictionaries
            gap_threshold_seconds: Minimum gap required after main segment end (default: 2)
            
        Returns:
            bool: True if segment should be included, False if it should be ignored
        """
        new_start = new_segment["start_time"]
        new_end = new_segment["end_time"]
        
        for existing in existing_segments:
            existing_start = existing["start_time"]
            existing_end = existing["end_time"]
            
            # Check for complete overlap (new segment is completely within existing)
            if (new_start >= existing_start and new_end <= existing_end):
                print(f"Complete overlap detected: New segment {new_start}-{new_end} is within existing {existing_start}-{existing_end}")
                return False
            
            # Check for partial overlap with extension
            if (new_start < existing_end and new_end > existing_start):
                # There's an overlap, check if new segment extends beyond existing with gap
                if new_end > existing_end:
                    gap_after_existing = (new_end - existing_end).total_seconds()
                    if gap_after_existing >= gap_threshold_seconds:
                        print(f"Partial overlap with sufficient gap: New segment extends {gap_after_existing}s beyond existing segment")
                        return True
                    else:
                        print(f"Partial overlap with insufficient gap: Only {gap_after_existing}s gap (need {gap_threshold_seconds}s)")
                        return False
                else:
                    print(f"Partial overlap without extension: New segment ends before existing segment")
                    return False
        
        # No overlap detected
        return True

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

