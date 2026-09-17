from django.conf import settings
from django.db import models

from core_admin.models import Channel


class RecoveredAudioFile(models.Model):
    """A source recording downloaded during audio recovery (e.g. a client-provided
    replacement hour, pulled from a Dropbox link). One of these gets split into
    many AudioSegments - see RecoveredAudioSegment for that connection.

    The download + ACRCloud identification run asynchronously via Celery
    (see audio_recovery.tasks.recover_audio_task) - `status` tracks that job.
    """

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    channel = models.ForeignKey(
        Channel,
        on_delete=models.CASCADE,
        related_name="recovered_audio_files",
    )

    source_url = models.URLField(max_length=2048, null=True, blank=True)

    recorded_at = models.DateTimeField(help_text="UTC start time of the recording")
    duration_seconds = models.PositiveIntegerField(default=0)

    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING, db_index=True)
    celery_task_id = models.CharField(max_length=255, blank=True)
    error = models.TextField(blank=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="recovered_audio_files",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["recorded_at"]

    def __str__(self):
        return f"Recovery for {self.channel} at {self.recorded_at} [{self.status}]"


class RecoveredAudioSegment(models.Model):
    """Connects one AudioSegments row back to the RecoveredAudioFile it was cut from."""

    recovered_audio_file = models.ForeignKey(
        RecoveredAudioFile,
        on_delete=models.CASCADE,
        related_name="segments",
    )
    audio_segment = models.OneToOneField(
        "data_analysis.AudioSegments",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="recovered_from",
    )

    start_sec = models.PositiveIntegerField(help_text="Offset into the source file where this segment starts")
    end_sec = models.PositiveIntegerField(help_text="Offset into the source file where this segment ends")

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["start_sec"]

    def __str__(self):
        return f"Recovery #{self.recovered_audio_file_id} [{self.start_sec}-{self.end_sec}s] -> segment {self.audio_segment_id}"
