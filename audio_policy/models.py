from django.db import models
from django.conf import settings

# Create your models here.

class FlagCondition(models.Model):
    """
    Model to define conditions for flagging audio segments.
    """
    name = models.CharField(max_length=255, unique=True, help_text="Name of the flag condition")
    channel = models.OneToOneField(
        'acr_admin.Channel',
        on_delete=models.CASCADE,
        related_name="flag_condition",
        help_text="Channel this condition applies to",
    )
    
    # Text matching fields
    # Store keywords as grouped synonym lists (e.g. [["mom","mum","mummy"]])
    # so the flagging logic can highlight any equivalent variant the user provided.
    transcription_keywords = models.JSONField(default=list, blank=True, help_text="List of keyword groups to match in transcription")
    summary_keywords = models.JSONField(default=list, blank=True, help_text="List of keyword groups to match in summary")
    
    # Sentiment range
    sentiment_min = models.FloatField(null=True, blank=True, help_text="Minimum sentiment score")
    sentiment_max = models.FloatField(null=True, blank=True, help_text="Maximum sentiment score")
    
    # Topic matching
    iab_topics = models.JSONField(default=list, blank=True, help_text="List of IAB topics to match")
    bucket_prompt = models.JSONField(default=list, blank=True, help_text="List of bucket prompts to match")
    general_topics = models.JSONField(default=list, blank=True, help_text="List of general topics to match")
    
    # Metadata
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="created_flag_conditions")
    is_active = models.BooleanField(default=True, help_text="Whether this condition is active")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['-created_at']


class ContentTypeDeactivationRule(models.Model):
    """Model to store content types that should automatically deactivate audio segments"""
    channel = models.ForeignKey(
        'acr_admin.Channel',
        on_delete=models.CASCADE,
        related_name="content_type_deactivation_rules",
        help_text="Channel this rule applies to",
    )
    content_type = models.CharField(
        max_length=255,
        help_text="Content type name that should trigger deactivation (e.g., 'Commercial', 'Advertisement')",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this rule is currently active",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        status = "Active" if self.is_active else "Inactive"
        return f"{self.content_type} ({self.channel.name}) ({status})"

    class Meta:
        ordering = ['channel', 'content_type']
        unique_together = [['channel', 'content_type']]
        indexes = [
            models.Index(fields=['is_active']),
            models.Index(fields=['channel', 'is_active']),
            models.Index(fields=['content_type']),
        ]
