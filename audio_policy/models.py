from django.db import models
from django.conf import settings
from django.core.validators import MinValueValidator, MaxValueValidator

# Create your models here.

class FlagCondition(models.Model):
    """
    Model to define conditions for flagging audio segments.
    """
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
    
    # Sentiment range - supports flexible range matching with lower and upper bounds
    sentiment_min_lower = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Lower bound for minimum sentiment value (0-100)"
    )
    sentiment_min_upper = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Upper bound for minimum sentiment value (0-100)"
    )
    sentiment_max_lower = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Lower bound for maximum sentiment value (0-100)"
    )
    sentiment_max_upper = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Upper bound for maximum sentiment value (0-100)"
    )
    target_sentiments = models.IntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Target sentiment integer value to match (0-100)"
    )
    
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
        return f"Flag Condition for {self.channel.name}"

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
