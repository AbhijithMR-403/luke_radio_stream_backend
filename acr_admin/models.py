from datetime import datetime, timedelta
from django.db import models
from django.core.exceptions import ValidationError
from encrypted_model_fields.fields import EncryptedTextField

# Create your models here.
class Channel(models.Model):
    name = models.CharField(max_length=255, blank=True)  # Optional label
    channel_id = models.PositiveIntegerField()
    project_id = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    is_deleted = models.BooleanField(default=False)  # To soft delete

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
    bucket_id = models.CharField(max_length=20, unique=True, editable=False)  # Auto "bucket_1", "bucket_2", ...
    title = models.CharField(max_length=255)  # eg: "Emotional Wellness"
    description = models.TextField()
    prompt = models.TextField(help_text="Prompt to use when analyzing transcript for this bucket")

    def __str__(self):
        return f"{self.bucket_id} - {self.title}"

    def clean(self):
        if not self.pk and WellnessBucket.objects.count() >= 20:
            raise ValidationError("You cannot have more than 20 wellness buckets.")

    def save(self, *args, **kwargs):
        import re
        # If bucket_id is provided, validate it
        if self.bucket_id:
            match = re.match(r'^bucket_(\d{1,2})$', self.bucket_id)
            if not match:
                raise ValidationError("bucket_id must be in the format 'bucket_N' where N is 1-20.")
            number = int(match.group(1))
            if not (1 <= number <= 20):
                raise ValidationError("bucket_id must be between bucket_1 and bucket_20.")
            # Ensure uniqueness is handled by the model's unique constraint
        else:
            # Auto-generate next available bucket_id
            existing = WellnessBucket.objects.all().order_by('bucket_id')
            existing_ids = {
                int(bucket.bucket_id.split('_')[1])
                for bucket in existing if bucket.bucket_id.startswith("bucket_")
            }
            for i in range(1, 21):
                if i not in existing_ids:
                    self.bucket_id = f"bucket_{i}"
                    break
            else:
                raise ValidationError("Max 20 buckets already created.")
        super().save(*args, **kwargs)

