from datetime import datetime, timedelta
from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
import json

from core_admin.models import Channel

# Create your models here.


class TranscriptionDetail(models.Model):
    audio_segment = models.OneToOneField('AudioSegments', on_delete=models.CASCADE, related_name="transcription_detail", null=True, blank=True)
    rev_job = models.OneToOneField('RevTranscriptionJob', on_delete=models.CASCADE, related_name="transcription_detail")
    transcript = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        if self.audio_segment:
            return f"Transcription for {self.audio_segment} at {self.created_at}"
        else:
            return f"Transcription at {self.created_at}"

    def clean(self):
        """Validate that audio_segment is set"""
        super().clean()
        if not self.audio_segment:
            raise ValidationError("audio_segment must be set")

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


class TranscriptionQueue(models.Model):
    """Model to track audio segments queued for transcription"""
    audio_segment = models.OneToOneField('AudioSegments', on_delete=models.CASCADE, related_name='transcription_queue')
    is_transcribed = models.BooleanField(default=False, help_text="Whether the transcription has been completed")
    is_analyzed = models.BooleanField(default=False, help_text="Whether the analysis has been completed")
    queued_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    def __str__(self):
        return f"Transcription Queue for {self.audio_segment} - Transcribed: {self.is_transcribed}, Analyzed: {self.is_analyzed}"

class TranscriptionAnalysis(models.Model):
    transcription_detail = models.OneToOneField('TranscriptionDetail', on_delete=models.CASCADE, related_name='analysis')
    summary = models.TextField()
    sentiment = models.CharField(max_length=50)
    general_topics = models.TextField(help_text="General topics identified in the transcript")
    iab_topics = models.TextField(help_text="IAB topics identified in the transcript")
    bucket_prompt = models.TextField(help_text="Bucket prompt for categorization")
    content_type_prompt = models.TextField(null=True, blank=True, help_text="Content type classification result")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Analysis for {self.transcription_detail}"  
    
class GeneralTopic(models.Model):
    """Model to store all general topics with their active/inactive status"""
    topic_name = models.CharField(max_length=255, help_text="Name of the general topic")
    is_active = models.BooleanField(default=True, help_text="Whether this topic should be included in results (True) or ignored (False)")
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name='general_topics', help_text="Channel associated with this topic")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        status = "Active" if self.is_active else "Inactive"
        return f"{self.topic_name} ({status}) - {self.channel}"
    
    class Meta:
        ordering = ['topic_name']
        constraints = [
            models.UniqueConstraint(fields=['topic_name', 'channel'], name='unique_topic_per_channel')
        ]
        indexes = [
            models.Index(fields=['is_active']),
            models.Index(fields=['topic_name']),
            models.Index(fields=['channel']),
        ]
    
class AudioSegments(models.Model):
    """Model to store audio segments with recognition status and title relationships"""
    
    # -------------------------
    # Classification
    # -------------------------
    SEGMENT_TYPE_CHOICES = (
        ('broadcast', 'Broadcast'),
        ('podcast', 'Podcast'),
        ('custom', 'Custom'),
    )

    segment_type = models.CharField(
        max_length=10,
        choices=SEGMENT_TYPE_CHOICES,
        default='broadcast',
        db_index=True
    )
    
    # -------------------------
    # Time
    # -------------------------
    start_time = models.DateTimeField(help_text="Start time of the audio segment")
    end_time = models.DateTimeField(help_text="End time of the audio segment")
    duration_seconds = models.PositiveIntegerField(help_text="Duration in seconds")
    created_at = models.DateTimeField(auto_now_add=True)

    # -------------------------
    # Audio location
    # -------------------------
    file_name = models.CharField(max_length=255)

    file_path = models.CharField(
        max_length=512,
        null=True,
        blank=True,
        help_text="Local file path for broadcast/custom uploads"
    )

    audio_url = models.URLField(
        max_length=2048,
        null=True,
        blank=True,
        help_text="Remote audio URL for podcast or custom URL audio"
    )

    AUDIO_LOCATION_TYPE_CHOICES = (
        ('file_path', 'File path'),
        ('audio_url', 'Audio URL'),
    )
    audio_location_type = models.CharField(
        max_length=20,
        choices=AUDIO_LOCATION_TYPE_CHOICES,
        help_text="Whether audio is served from file_path (local) or audio_url (remote)"
    )

    # -------------------------
    # Podcast
    # -------------------------
    rss_guid = models.CharField(
        max_length=512,
        null=True,
        blank=True,
        unique=True,
        help_text="Unique identifier (GUID) from RSS feed for podcast episodes"
    )

    pub_date = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Publication date of the audio segment (e.g., podcast episode publish date)"
    )

    # -------------------------
    # Recognition / processing
    # -------------------------
    is_recognized = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    is_analysis_completed = models.BooleanField(default=False, help_text="Whether data analysis has been completed for this audio segment")
    is_audio_downloaded = models.BooleanField(default=False, help_text="Whether the audio file has been downloaded")
    is_manually_processed = models.BooleanField(default=False, help_text="Whether the segment was manually transcribed or analyzed")
    is_delete = models.BooleanField(default=False)

    # -------------------------
    # Titles
    # -------------------------
    title = models.CharField(
        max_length=500,
        null=True, 
        blank=True, 
    )
    title_before = models.CharField(
        max_length=500,
        null=True, 
        blank=True, 
    )
    title_after = models.CharField(
        max_length=500,
        null=True, 
        blank=True, 
    )
    metadata_json = models.JSONField(
        null=True, 
        blank=True, 
        help_text="JSON field to store metadata like artists, albums, external IDs, etc. from music recognition data"
    )
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name="audios_segments")
    notes = models.TextField(null=True, blank=True)
    SOURCE_CHOICES = (
        ('system', 'System'),
        ('user', 'User'),
        ('system_merge', 'System Merged'),
        ('user_merged', 'User Merged'),
    )
    source = models.CharField(max_length=15, choices=SOURCE_CHOICES, default='system')
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name='created_audio_segments')

    def __str__(self):
        status = "ACTIVE" if self.is_active else "INACTIVE"
        deleted_status = " [DELETED]" if self.is_delete else ""
        if self.is_recognized and self.title:
            return f"Recognized: {self.title} ({self.start_time} - {self.end_time}) [{status}]{deleted_status}"
        else:
            return f"Unrecognized: {self.start_time} - {self.end_time} ({self.duration_seconds}s) [{status}]{deleted_status}"

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
        
        # Validate that either audio_url or file_path is set, but not both
        if self.audio_url and self.file_path:
            raise ValidationError("Cannot have both audio_url and file_path set. Please provide only one.")
        if not self.audio_url and not self.file_path:
            raise ValidationError("Either audio_url or file_path must be provided.")
        
        # Validate that podcast segments have rss_guid
        if self.segment_type == 'podcast' and not self.rss_guid:
            raise ValidationError("Podcast segments must have rss_guid")

    class Meta:
        ordering = ['start_time']
        indexes = [
            # main API path
            models.Index(fields=['channel', 'start_time', 'end_time']),
        ] 
    
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


class ReportFolder(models.Model):
    """Model to store report folders for organizing saved audio segments"""
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name='report_folders')
    name = models.CharField(max_length=255, help_text="Name of the report folder")
    description = models.TextField(null=True, blank=True, help_text="Optional description of the folder")
    color = models.CharField(max_length=7, default="#3B82F6", help_text="Hex color code for the folder (default: blue)")
    is_public = models.BooleanField(default=False, help_text="Whether the folder is public or private")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.name} ({'Public' if self.is_public else 'Private'})"
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['channel']),
            models.Index(fields=['is_public']),
            models.Index(fields=['created_at']),
        ]


class SavedAudioSegment(models.Model):
    """Model to store saved audio segments in report folders"""
    folder = models.ForeignKey(ReportFolder, on_delete=models.CASCADE, related_name='saved_segments')
    audio_segment = models.ForeignKey(AudioSegments, on_delete=models.CASCADE, related_name='saved_in_folders')
    is_favorite = models.BooleanField(default=False, help_text="Whether this is marked as favorite")
    saved_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.audio_segment.title or 'Untitled'} in {self.folder.name}"
    
    class Meta:
        ordering = ['-saved_at']
        unique_together = ['folder', 'audio_segment']
        indexes = [
            models.Index(fields=['folder']),
            models.Index(fields=['saved_at']),
            models.Index(fields=['is_favorite']),
            models.Index(fields=['folder', 'saved_at']),
        ]


class AudioSegmentInsight(models.Model):
    """Model to store multiple insights for saved audio segments"""
    saved_audio_segment = models.ForeignKey(SavedAudioSegment, on_delete=models.CASCADE, related_name='insights')
    title = models.CharField(max_length=255, help_text="Title of the insight")
    description = models.TextField(help_text="Detailed description of the insight")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.title} - {self.saved_audio_segment.audio_segment.title or 'Untitled'}"
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['saved_audio_segment']),
            models.Index(fields=['created_at']),
        ]