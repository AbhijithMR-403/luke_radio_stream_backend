from datetime import datetime, timedelta
from django.db import models
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from encrypted_model_fields.fields import EncryptedTextField
from zoneinfo import ZoneInfo

# Create your models here.
class Channel(models.Model):
    name = models.CharField(max_length=255, blank=True)  # Optional label
    channel_id = models.PositiveIntegerField()
    project_id = models.PositiveIntegerField()
    timezone = models.CharField(
        max_length=50,
        default='UTC',
        help_text='Timezone for the channel (e.g., America/New_York, Europe/London, UTC)'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    is_deleted = models.BooleanField(default=False)  # To soft delete

    def clean(self):
        """Validate the timezone field"""
        super().clean()
        if self.timezone:
            try:
                ZoneInfo(self.timezone)
            except Exception:
                raise ValidationError({'timezone': f'Invalid timezone: {self.timezone}'})

    def __str__(self):
        return f"Channel {self.channel_id} in Project {self.project_id}"

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['channel_id', 'project_id'], name='unique_channel_per_project')
        ]

class GeneralSetting(models.Model):

    # Auth Keys
    openai_api_key = EncryptedTextField(null=True, blank=True)
    openai_org_id = models.CharField(max_length=255)
    acr_cloud_api_key = EncryptedTextField(null=True, blank=True)
    revai_access_token = EncryptedTextField(null=True, blank=True)

    # Prompts
    summarize_transcript_prompt = models.TextField()
    sentiment_analysis_prompt = models.TextField()
    general_topics_prompt = models.TextField()
    iab_topics_prompt = models.TextField()


    updated_at = models.DateTimeField(auto_now=True)

    # --- Added fields for bucket and radio segment classification ---
    bucket_prompt = models.TextField(
        help_text="Prompt describing the definitions and classification rules for wellness buckets.",
        null=True, blank=True
    )
    bucket_definition_error_rate = models.PositiveIntegerField(
        default=80,
        help_text="Minimum accuracy percentage required for bucket classification (e.g., 80).",
        null=True, blank=True
    )
    chatgpt_model = models.CharField(
        max_length=100,
        default="gpt-3.5-turbo",
        help_text="ChatGPT model to use for classification (e.g., gpt-40).",
        null=True, blank=True
    )
    chatgpt_max_tokens = models.PositiveIntegerField(
        default=0,
        help_text="Maximum tokens for ChatGPT response (0 for default).",
        null=True, blank=True
    )
    chatgpt_temperature = models.FloatField(
        default=1.0,
        help_text="ChatGPT temperature parameter.",
        null=True, blank=True
    )
    chatgpt_top_p = models.FloatField(
        default=1.0,
        help_text="ChatGPT top_p parameter.",
        null=True, blank=True
    )
    chatgpt_frequency_penalty = models.FloatField(
        default=0.0,
        help_text="ChatGPT frequency penalty parameter.",
        null=True, blank=True
    )
    chatgpt_presence_penalty = models.FloatField(
        default=0.0,
        help_text="ChatGPT presence penalty parameter.",
        null=True, blank=True
    )
    determine_radio_content_type_prompt = models.TextField(
        help_text="Prompt for determining radio content type from transcript.",
        null=True, blank=True
    )
    content_type_prompt = models.TextField(
        help_text="Prompt for determining general content type from transcript.",
        null=True, blank=True
    )
    # Remove later
    radio_segment_types = models.TextField(
        help_text="Comma-separated list of radio segment/content types.",
        null=True, blank=True
    )
    radio_segment_error_rate = models.PositiveIntegerField(
        default=80,
        help_text="Minimum accuracy percentage required for radio segment classification (e.g., 80).",
        null=True, blank=True
    )

    def __str__(self):
        return "General Settings"


class WellnessBucket(models.Model):
    CATEGORY_CHOICES = [
        ('personal', 'Personal'),
        ('community', 'Community'),
        ('spiritual', 'Spiritual'),
    ]
    
    title = models.CharField(max_length=255)  # eg: "Emotional Wellness"
    description = models.TextField()
    category = models.CharField(
        max_length=20,
        choices=CATEGORY_CHOICES,
        help_text="Category for dashboard classification"
    )

    def __str__(self):
        return f"Bucket {self.id} - {self.title}"

