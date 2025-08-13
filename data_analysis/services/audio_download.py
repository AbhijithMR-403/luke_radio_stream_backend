import os
import requests
from datetime import datetime
from django.core.exceptions import ValidationError
from acr_admin.models import GeneralSetting
from data_analysis.models import AudioSegments


class ACRCloudAudioDownloader:
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
    def download_audio(project_id: int, channel_id: int, start_time, duration_seconds: int, filename: str = None, filepath: str = None):
        """
        Downloads audio from the ACRCloud API for the given parameters and saves it as an mp3 file.
        - start_time: timestamp_utc (format: YYYYMMDDHHMMSS) or datetime object
        - duration_seconds: played_duration (int)
        - filename: optional custom filename for the downloaded file
        - filepath: optional custom filepath for the downloaded file
        - If duration_seconds > 600, sets record_after=duration_seconds-600
        Returns the file path of the downloaded mp3.
        """
        # Validate parameters before proceeding
        start_time_str, duration_seconds = ACRCloudAudioDownloader.validate_download_parameters(
            project_id, channel_id, start_time, duration_seconds
        )
        
        # Handle filepath and filename logic
        if filepath:
            # If custom filepath is provided, use it directly
            # Ensure the directory exists
            filepath_dir = os.path.dirname(filepath)
            if filepath_dir:
                os.makedirs(filepath_dir, exist_ok=True)
            file_path = filepath
            # Extract filename from filepath for media_url
            filename = os.path.basename(filepath)
        else:
            # Use default media directory
            media_dir = os.path.join(os.getcwd(), "media")
            os.makedirs(media_dir, exist_ok=True)
            
            # Use custom filename if provided, otherwise generate default filename
            if filename:
                # Ensure filename has .mp3 extension
                if not filename.endswith('.mp3'):
                    filename += '.mp3'
            else:
                filename = f"audio_{project_id}_{channel_id}_{start_time_str}_{duration_seconds}.mp3"
            
            file_path = os.path.join(media_dir, filename)
        
        # Check if file already exists
        if os.path.exists(file_path):
            print(f"Found existing exact audio file: {filename}")
            media_url = f"/api/media/{filename}"
            return media_url
        
        # No existing file found, proceed with download
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

        url = ACRCloudAudioDownloader.BASE_URL.format(pid=project_id, channel_id=channel_id)
        headers = {
            "Authorization": f"Bearer {token}",
        }
        # response = requests.get(url, headers=headers, params=params, stream=True)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        media_url = f"/api/media/{filename}"
        return media_url

    @staticmethod
    def download_audio_segments_batch(audio_segments: list[AudioSegments]):
        """
        Downloads audio for a list of AudioSegments and updates their is_audio_downloaded field.
        
        Args:
            audio_segments (list[AudioSegments]): List of AudioSegments objects
        
        Returns:
            dict: Dictionary with results for each segment
                 {
                     'success': [list of successfully downloaded segments],
                     'failed': [list of failed segments with error messages],
                     'skipped': [list of already downloaded segments]
                 }
        """
        results = {
            'success': [],
            'failed': [],
            'skipped': []
        }
        
        # Process each segment
        for segment in audio_segments:
            print(segment)
            try:
                # Skip if already downloaded
                if segment.is_audio_downloaded:
                    results['skipped'].append({
                        'segment_id': segment.id,
                        'file_name': segment.file_name,
                        'reason': 'Already downloaded'
                    })
                    continue
                
                # Get project_id from the first segment's channel
                project_id = segment.channel.project_id
                channel_id = segment.channel.channel_id
                
                # Use default media directory
                media_dir = os.path.join(os.getcwd(), "media")
                os.makedirs(media_dir, exist_ok=True)
                
                # Check if file_name and file_path are present
                if not segment.file_name or not segment.file_path:
                    print(f"Skipping segment {segment.id}: Missing file_name or file_path")
                    results['skipped'].append({
                        'segment_id': segment.id,
                        'file_name': segment.file_name,
                        'reason': 'Missing file_name or file_path'
                    })
                    continue
                
                # Use segment's file_name and file_path
                filename = segment.file_name
                file_path = segment.file_path
                
                # Check if file already exists
                if os.path.exists(file_path):
                    # File exists, just update the database
                    segment.is_audio_downloaded = True
                    segment.save()
                    results['success'].append({
                        'segment_id': segment.id,
                        'file_name': filename,
                        'file_path': file_path,
                        'status': 'File already existed, marked as downloaded'
                    })
                    continue
                print("-----------------")
                # Download the audio
                media_url = ACRCloudAudioDownloader.download_audio(
                    project_id=project_id,
                    channel_id=channel_id,
                    start_time=segment.start_time,
                    duration_seconds=segment.duration_seconds,
                    filepath=file_path
                )
                print(media_url)
                # Update the segment
                segment.is_audio_downloaded = True
                segment.save()
                
                results['success'].append({
                    'segment_id': segment.id,
                    'file_name': filename,
                    'file_path': file_path,
                    'media_url': media_url,
                    'status': 'Successfully downloaded'
                })
                
            except Exception as e:
                results['failed'].append({
                    'segment_id': segment.id if hasattr(segment, 'id') else 'Unknown',
                    'file_name': getattr(segment, 'file_name', 'Unknown'),
                    'error': str(e)
                })
        
        return results


# Usage Example for Celery:
#
# from data_analysis.services.audio_download import ACRCloudAudioDownloader
# from data_analysis.models import AudioSegments
#
# # Get AudioSegments objects
# segments = AudioSegments.objects.filter(channel_id=5, is_audio_downloaded=False)
#
# # Download audio for all segments
# results = ACRCloudAudioDownloader.download_audio_segments_batch(
#     audio_segments=segments
# )
#
# # Results will contain:
# # - success: list of successfully downloaded segments
# # - failed: list of failed segments with error messages  
# # - skipped: list of already downloaded segments
