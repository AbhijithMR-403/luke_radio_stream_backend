from datetime import datetime, timedelta
from django.db import models
from django.core.exceptions import ValidationError
from django.utils import timezone
import json

from acr_admin.models import Channel

# Create your models here.

class UnrecognizedAudio(models.Model):
    start_time = models.DateTimeField(help_text="Start time as datetime")
    end_time = models.DateTimeField(help_text="End time as datetime")
    duration = models.PositiveIntegerField(help_text="Duration in seconds")
    media_path = models.CharField(max_length=512, unique=True)
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name="unrecognized_audios")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"UnrecognizedAudio {self.start_time} - {self.end_time} ({self.duration}s)"

    def clean(self):
        """Validate the model data"""
        super().clean()
        
        # Validate that end_time is after start_time
        if self.start_time and self.end_time and self.end_time <= self.start_time:
            raise ValidationError("End time must be after start time")
        
        # Validate duration matches the time difference
        if self.start_time and self.end_time and self.duration:
            expected_duration = int((self.end_time - self.start_time).total_seconds())
            if self.duration != expected_duration:
                raise ValidationError(f"Duration ({self.duration}s) doesn't match time difference ({expected_duration}s)")

    @staticmethod
    def validate_duration(duration):
        """Validate duration is a positive integer"""
        if not isinstance(duration, (int, float)) or duration <= 0:
            raise ValidationError(f"Duration must be a positive number, got: {duration}")
        return int(duration)

    @staticmethod
    def validate_segment_data(segment):
        """Validate segment data before processing"""
        if not isinstance(segment, dict):
            raise ValidationError("Segment must be a dictionary")
        
        required_fields = ['start_time', 'duration_seconds']
        for field in required_fields:
            if field not in segment:
                raise ValidationError(f"Segment missing required field: {field}")
        
        # Validate start_time - should be a datetime object
        start_time = segment['start_time']
        if not isinstance(start_time, datetime):
            raise ValidationError(f"start_time must be a datetime object, got: {type(start_time)}")
        
        # Validate duration
        duration = UnrecognizedAudio.validate_duration(segment['duration_seconds'])
        
        # Calculate end_time if not provided
        end_time = segment.get('end_time')
        if end_time:
            if not isinstance(end_time, datetime):
                raise ValidationError(f"end_time must be a datetime object, got: {type(end_time)}")
        else:
            end_time = start_time + timedelta(seconds=duration)
        
        return {
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration
        }

class TranscriptionDetail(models.Model):
    unrecognized_audio = models.OneToOneField(UnrecognizedAudio, on_delete=models.CASCADE, related_name="transcription_detail", null=True, blank=True)
    audio_segment = models.OneToOneField('AudioSegments', on_delete=models.CASCADE, related_name="transcription_detail", null=True, blank=True)
    rev_job = models.OneToOneField('RevTranscriptionJob', on_delete=models.CASCADE, related_name="transcription_detail")
    transcript = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        if self.unrecognized_audio:
            return f"Transcription for {self.unrecognized_audio} at {self.created_at}"
        elif self.audio_segment:
            return f"Transcription for {self.audio_segment} at {self.created_at}"
        else:
            return f"Transcription at {self.created_at}"

    def clean(self):
        """Validate that either unrecognized_audio or audio_segment is set, but not both"""
        super().clean()
        if not self.unrecognized_audio and not self.audio_segment:
            raise ValidationError("Either unrecognized_audio or audio_segment must be set")
        if self.unrecognized_audio and self.audio_segment:
            raise ValidationError("Cannot set both unrecognized_audio and audio_segment")

class RevTranscriptionJob(models.Model):
    """Model to store Rev API callback data for transcription jobs"""
    
    # Job identification
    job_id = models.CharField(max_length=255, unique=True)
    job_name = models.CharField(max_length=255)
    media_url = models.URLField(max_length=512)
    
    # Status and timing
    status = models.CharField(max_length=50)  # 'transcribed', 'failed', etc.
    created_on = models.DateTimeField()
    completed_on = models.DateTimeField(null=True, blank=True)
    
    # Job configuration
    job_type = models.CharField(max_length=50, default='async')  # 'async', 'sync'
    language = models.CharField(max_length=10, default='en')
    strict_custom_vocabulary = models.BooleanField(default=False)
    
    # Duration (for successful transcriptions)
    duration_seconds = models.FloatField(null=True, blank=True)
    
    # Failure details (for failed jobs)
    failure = models.CharField(max_length=100, null=True, blank=True)
    failure_detail = models.TextField(null=True, blank=True)
    
    # Related audio segment
    audio_segment = models.ForeignKey('AudioSegments', on_delete=models.CASCADE, related_name='rev_transcription_jobs', null=True, blank=True)
    
    # Retry tracking
    retry_count = models.PositiveIntegerField(default=0)
    last_retry_at = models.DateTimeField(null=True, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.job_id} - {self.job_name} ({self.status})"
    
    def clean(self):
        """Validate the model data"""
        super().clean()
        
        # Validate retry count
        if self.retry_count < 0:
            raise ValidationError("Retry count cannot be negative")
    
    @property
    def is_completed(self):
        """Check if the job is completed (successfully or failed)"""
        return self.status in ['transcribed', 'failed', 'cancelled']
    
    @property
    def is_successful(self):
        """Check if the job completed successfully"""
        return self.status == 'transcribed'
    
    @property
    def is_failed(self):
        """Check if the job failed"""
        return self.status == 'failed'
    
    def increment_retry_count(self):
        """Increment retry count and update last retry timestamp"""
        self.retry_count += 1
        self.last_retry_at = timezone.now()
        self.save(update_fields=['retry_count', 'last_retry_at'])


class TranscriptionAnalysis(models.Model):
    transcription_detail = models.OneToOneField('TranscriptionDetail', on_delete=models.CASCADE, related_name='analysis')
    summary = models.TextField()
    sentiment = models.CharField(max_length=50)
    general_topics = models.TextField(help_text="General topics identified in the transcript")
    iab_topics = models.TextField(help_text="IAB topics identified in the transcript")
    bucket_prompt = models.TextField(help_text="Bucket prompt for categorization")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Analysis for {self.transcription_detail}"  
    
class AudioSegments(models.Model):
    """Model to store audio segments with recognition status and title relationships"""
    start_time = models.DateTimeField(help_text="Start time of the audio segment")
    end_time = models.DateTimeField(help_text="End time of the audio segment")
    duration_seconds = models.PositiveIntegerField(help_text="Duration in seconds")
    is_recognized = models.BooleanField(default=False, help_text="Whether the segment was recognized")
    is_active = models.BooleanField(default=True, help_text="Whether the segment is active (not superseded by newer data)")
    is_analysis_completed = models.BooleanField(default=False, help_text="Whether data analysis has been completed for this audio segment")
    is_audio_downloaded = models.BooleanField(default=False, help_text="Whether the audio file has been downloaded")
    file_name = models.CharField(max_length=255)  # e.g., "def_channel_20250804_101500"
    file_path = models.CharField(max_length=512)  # e.g., "/mnt/audio_storage/def_channel_20250804_101500.wav"
    title = models.CharField(
        max_length=500,
        null=True, 
        blank=True, 
        help_text="To store Title of recognized segments"
    )
    title_before = models.CharField(
        max_length=500,
        null=True, 
        blank=True, 
        help_text="To store the before Title of recognized segments"
    )
    title_after = models.CharField(
        max_length=500,
        null=True, 
        blank=True, 
        help_text="To store the after Title of recognized segments"
    )
    metadata_json = models.JSONField(
        null=True, 
        blank=True, 
        help_text="JSON field to store metadata like artists, albums, external IDs, etc. from music recognition data"
    )
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name="audios_segments")
    notes = models.TextField(null=True, blank=True, help_text="Optional reason/log/debug info")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        status = "ACTIVE" if self.is_active else "INACTIVE"
        if self.is_recognized and self.title:
            return f"Recognized: {self.title} ({self.start_time} - {self.end_time}) [{status}]"
        else:
            return f"Unrecognized: {self.start_time} - {self.end_time} ({self.duration_seconds}s) [{status}]"

    def clean(self):
        """Validate the model data"""
        super().clean()
        
        # Validate that end_time is after start_time
        if self.start_time and self.end_time and self.end_time <= self.start_time:
            raise ValidationError("End time must be after start time")
        
        # Validate duration matches the time difference
        if self.start_time and self.end_time and self.duration_seconds:
            expected_duration = int((self.end_time - self.start_time).total_seconds())
            if self.duration_seconds != expected_duration:
                raise ValidationError(f"Duration ({self.duration_seconds}s) doesn't match time difference ({expected_duration}s)")
        
        # Validate business rules for recognized vs unrecognized segments
        if self.is_recognized:
            # For recognized segments: title is mandatory
            if not self.title:
                raise ValidationError("Recognized segments must have a title")
        else:
            # For unrecognized segments: title_before and title_after are mandatory
            if not self.title_before:
                raise ValidationError("Unrecognized segments must have a title_before")
            if not self.title_after:
                raise ValidationError("Unrecognized segments must have a title_after")
        
        # Validate metadata_json if provided
        if self.metadata_json is not None:
            if not isinstance(self.metadata_json, dict):
                raise ValidationError("metadata_json must be a dictionary")

    def set_metadata(self, metadata_dict):
        """
        Set metadata from music recognition data
        
        Args:
            metadata_dict (dict): Dictionary containing metadata like artists, albums, etc.
        """
        if not isinstance(metadata_dict, dict):
            raise ValidationError("metadata_dict must be a dictionary")
        
        self.metadata_json = metadata_dict
        self.save(update_fields=['metadata_json'])

    def get_artists(self):
        """
        Get list of artists from metadata
        
        Returns:
            list: List of artist names, empty list if no artists found
        """
        if not self.metadata_json:
            return []
        
        artists = self.metadata_json.get('artists', [])
        if isinstance(artists, list):
            return [artist.get('name', '') for artist in artists if isinstance(artist, dict) and artist.get('name')]
        return []

    def get_external_metadata(self):
        """
        Get external metadata (Spotify, Deezer, etc.) from metadata
        
        Returns:
            dict: External metadata dictionary, empty dict if not found
        """
        if not self.metadata_json:
            return {}
        
        return self.metadata_json.get('external_metadata', {})

    def get_external_ids(self):
        """
        Get external IDs (UPC, ISRC, etc.) from metadata
        
        Returns:
            dict: External IDs dictionary, empty dict if not found
        """
        if not self.metadata_json:
            return {}
        
        return self.metadata_json.get('external_ids', {})

    class Meta:
        ordering = ['start_time'] 
    
    @staticmethod
    def insert_audio_segments(segments_data, channel_id=None):
        """
        Insert multiple audio segments into the database.
        
        Args:
            segments_data (list): List of dictionaries containing segment data
            channel_id (int, optional): Channel ID for the segments. If not provided, 
                                      must be included in each segment data.
        
        Returns:
            list: List of created AudioSegments instances
        
        Example:
            segments_data = [
                {
                    'start_time': datetime.datetime(2025, 8, 12, 9, 59, 29, tzinfo=datetime.timezone.utc),
                    'end_time': datetime.datetime(2025, 8, 12, 9, 59, 34, tzinfo=datetime.timezone.utc),
                    'duration_seconds': 5,
                    'title_before': '024010108',
                    'title_after': 'TRUSTFALL',
                    'is_recognized': False,
                    'is_active': False,
                    'file_name': 'def_channel_20250812_095929',
                    'file_path': '/mnt/audio_storage/def_channel_20250812_095929.wav',
                    'channel': channel_instance  # or channel_id if channel_id not provided globally
                },
                {
                    'start_time': datetime.datetime(2025, 8, 12, 9, 59, 34, tzinfo=datetime.timezone.utc),
                    'end_time': datetime.datetime(2025, 8, 12, 10, 3, 13, tzinfo=datetime.timezone.utc),
                    'duration_seconds': 219,
                    'title': 'TRUSTFALL',
                    'is_recognized': True,
                    'is_active': True,
                    'file_name': 'def_channel_20250812_095934',
                    'file_path': '/mnt/audio_storage/def_channel_20250812_095934.wav',
                    'metadata_json': {
                        'source': 'music',
                        'artists': [{'name': 'Lewis Capaldi'}],
                        'external_metadata': {
                            'spotify': [{'track': {'id': '7ce20yLkzuXXLUhzIDoZih', 'name': 'Before You Go'}}]
                        },
                        'external_ids': {'isrc': ['DEUM72000017']}
                    },
                    'channel': channel_instance  # or channel_id if channel_id not provided globally
                }
            ]
        """
        if not isinstance(segments_data, list):
            raise ValidationError("segments_data must be a list")
        
        created_segments = []
        
        for i, segment_data in enumerate(segments_data):
            if not isinstance(segment_data, dict):
                raise ValidationError(f"Segment data at index {i} must be a dictionary")
            
            # Create a copy to avoid modifying the original data
            segment_dict = segment_data.copy()
            
            # Handle channel assignment
            if channel_id and 'channel' not in segment_dict:
                try:
                    channel = Channel.objects.get(id=channel_id)
                    segment_dict['channel'] = channel
                except Channel.DoesNotExist:
                    raise ValidationError(f"Channel with id {channel_id} does not exist")
            elif 'channel' not in segment_dict:
                raise ValidationError(f"Channel must be provided either globally or in segment data at index {i}")
            
            # Validate required fields
            required_fields = ['start_time', 'end_time', 'duration_seconds', 'file_name', 'file_path']
            for field in required_fields:
                if field not in segment_dict:
                    raise ValidationError(f"Required field '{field}' missing in segment data at index {i}")
            
            # Validate datetime fields
            if not isinstance(segment_dict['start_time'], datetime):
                raise ValidationError(f"start_time must be a datetime object in segment data at index {i}")
            if not isinstance(segment_dict['end_time'], datetime):
                raise ValidationError(f"end_time must be a datetime object in segment data at index {i}")
            
            # Validate duration
            if not isinstance(segment_dict['duration_seconds'], (int, float)) or segment_dict['duration_seconds'] <= 0:
                raise ValidationError(f"duration_seconds must be a positive number in segment data at index {i}")
            
            # Validate business rules for recognized vs unrecognized segments
            is_recognized = segment_dict.get('is_recognized', False)
            if is_recognized:
                if 'title' not in segment_dict or not segment_dict['title']:
                    raise ValidationError(f"Recognized segments must have a title in segment data at index {i}")
            else:
                if 'title_before' not in segment_dict or not segment_dict['title_before']:
                    raise ValidationError(f"Unrecognized segments must have a title_before in segment data at index {i}")
                if 'title_after' not in segment_dict or not segment_dict['title_after']:
                    raise ValidationError(f"Unrecognized segments must have a title_after in segment data at index {i}")
            
            # Check if file_path already exists
            existing_segment = AudioSegments.objects.filter(file_path=segment_dict['file_path']).first()
            if existing_segment:
                # If segment with same file_path exists, add it to the return list and continue
                created_segments.append(existing_segment)
                continue
            
            # Create the AudioSegments instance
            try:
                audio_segment = AudioSegments(**segment_dict)
                audio_segment.full_clean()  # Validate the model
                audio_segment.save()
                created_segments.append(audio_segment)
            except Exception as e:
                raise ValidationError(f"Error creating audio segment at index {i}: {str(e)}")
        
        return created_segments
    
    @staticmethod
    def insert_single_audio_segment(segment_data, channel_id=None):
        """
        Insert a single audio segment into the database.
        
        Args:
            segment_data (dict): Dictionary containing segment data
            channel_id (int, optional): Channel ID for the segment. If not provided, 
                                      must be included in segment data.
        
        Returns:
            AudioSegments: Created AudioSegments instance
        """
        return AudioSegments.insert_audio_segments([segment_data], channel_id)[0]