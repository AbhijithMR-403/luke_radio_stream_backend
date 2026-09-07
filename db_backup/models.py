from django.conf import settings
from django.db import models


class DBBackup(models.Model):
    """A single database dump produced by pg_dump, run asynchronously by Celery."""

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    class Trigger(models.TextChoices):
        MANUAL = "manual", "Manual"
        SCHEDULED = "scheduled", "Scheduled"

    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PENDING, db_index=True
    )
    trigger = models.CharField(
        max_length=20, choices=Trigger.choices, default=Trigger.MANUAL
    )
    requested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="db_backups",
    )

    file = models.FileField(
        upload_to="backups/", max_length=500, null=True, blank=True
    )
    file_name = models.CharField(max_length=255, blank=True)
    size_bytes = models.BigIntegerField(null=True, blank=True)

    pg_dump_version = models.CharField(max_length=255, blank=True)
    celery_task_id = models.CharField(max_length=255, blank=True)
    error = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"DBBackup #{self.pk} ({self.status})"

    @property
    def duration_seconds(self):
        if self.started_at and self.finished_at:
            return round((self.finished_at - self.started_at).total_seconds(), 2)
        return None
