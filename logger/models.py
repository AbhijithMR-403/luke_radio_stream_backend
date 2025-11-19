from django.db import models
from django.conf import settings

from data_analysis.models import AudioSegments


class AudioSegmentEditLog(models.Model):
    """Stores a history of manual or automatic edits on audio segments."""

    ACTION_SPLIT = "split"
    ACTION_MERGE = "merge"
    ACTION_ADJUST = "adjust"

    ACTION_CHOICES = (
        (ACTION_SPLIT, "Split"),
        (ACTION_MERGE, "Merge"),
        (ACTION_ADJUST, "Adjust"),
    )

    TRIGGER_MANUAL = "manual"
    TRIGGER_AUTOMATIC = "automatic"
    TRIGGER_CHOICES = (
        (TRIGGER_MANUAL, "Manual"),
        (TRIGGER_AUTOMATIC, "Automatic"),
    )

    audio_segment = models.ForeignKey(
        AudioSegments,
        on_delete=models.CASCADE,
        related_name="edit_logs",
        help_text="Primary audio segment affected by this edit",
    )
    affected_segments = models.ManyToManyField(
        AudioSegments,
        related_name="affected_edit_logs",
        blank=True,
        help_text="Other audio segments participating in the edit (e.g., source segments in a merge)",
    )
    action = models.CharField(
        max_length=20,
        choices=ACTION_CHOICES,
        help_text="Type of edit that was performed",
    )
    trigger_type = models.CharField(
        max_length=20,
        choices=TRIGGER_CHOICES,
        help_text="Whether the edit was manual or automatic",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="audio_segment_edit_logs",
        help_text="User who triggered the edit, if any",
    )
    metadata = models.JSONField(
        null=True,
        blank=True,
        help_text="Optional metadata (e.g., involved segment IDs, reasons)",
    )
    notes = models.TextField(
        null=True,
        blank=True,
        help_text="Free-form notes or justification for the edit",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(
                fields=["audio_segment"],
                name="logger_edit_audio_idx",
            ),
            models.Index(
                fields=["action"],
                name="logger_edit_action_idx",
            ),
            models.Index(
                fields=["trigger_type"],
                name="logger_edit_trigger_idx",
            ),
        ]

    def __str__(self):
        user = self.user.email if self.user else "system"
        return f"{self.get_action_display()} ({self.get_trigger_type_display()}) on {self.audio_segment_id} by {user}"
