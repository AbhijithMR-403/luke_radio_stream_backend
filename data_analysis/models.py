from datetime import datetime, timedelta
from django.db import models
from django.core.exceptions import ValidationError

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
    unrecognized_audio = models.OneToOneField(UnrecognizedAudio, on_delete=models.CASCADE, related_name="transcription_detail")
    rev_job = models.OneToOneField('RevTranscriptionJob', on_delete=models.CASCADE, related_name="transcription_detail")
    transcript = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Transcription for {self.unrecognized_audio} at {self.created_at}"

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
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.job_id} - {self.job_name} ({self.status})"


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

    class Meta:
        ordering = ['start_time'] 
    