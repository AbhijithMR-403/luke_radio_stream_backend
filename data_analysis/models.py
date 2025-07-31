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
    