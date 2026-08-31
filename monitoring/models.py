from django.db import models

from core_admin.models import Channel


class ComponentHealth(models.Model):
    """Latest known status per (component, channel) - upserted, not a growing log."""

    COMPONENT_ACR_INGESTION = "acr_ingestion"
    COMPONENT_PROCESSING_PIPELINE = "processing_pipeline"
    COMPONENT_OPENAI_BILLING = "openai_billing"
    COMPONENT_CHOICES = (
        (COMPONENT_ACR_INGESTION, "ACRCloud Ingestion"),
        (COMPONENT_PROCESSING_PIPELINE, "Processing Pipeline"),
        (COMPONENT_OPENAI_BILLING, "OpenAI Billing"),
    )

    STATUS_HEALTHY = "healthy"
    STATUS_WARNING = "warning"
    STATUS_UNHEALTHY = "unhealthy"
    STATUS_UNKNOWN = "unknown"
    STATUS_CHOICES = (
        (STATUS_HEALTHY, "Healthy"),
        (STATUS_WARNING, "Warning"),
        (STATUS_UNHEALTHY, "Unhealthy"),
        (STATUS_UNKNOWN, "Unknown"),
    )

    component = models.CharField(max_length=32, choices=COMPONENT_CHOICES, db_index=True)
    channel = models.ForeignKey(
        Channel,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="health_records",
        help_text="Null for channel-independent components (e.g. processing_pipeline).",
    )
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_UNKNOWN)

    last_run_at = models.DateTimeField(null=True, blank=True)
    last_success_at = models.DateTimeField(null=True, blank=True)

    segments_received = models.PositiveIntegerField(null=True, blank=True)
    segments_saved = models.PositiveIntegerField(null=True, blank=True)

    expected_window_seconds = models.PositiveIntegerField(null=True, blank=True)
    actual_coverage_seconds = models.PositiveIntegerField(null=True, blank=True)
    coverage_ratio = models.FloatField(null=True, blank=True)

    message = models.CharField(max_length=255, blank=True, default="")
    details = models.JSONField(null=True, blank=True)

    last_alert_sent_at = models.DateTimeField(null=True, blank=True)

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["component", "channel"],
                condition=models.Q(channel__isnull=False),
                name="uq_monitoring_component_channel",
            ),
            models.UniqueConstraint(
                fields=["component"],
                condition=models.Q(channel__isnull=True),
                name="uq_monitoring_component_global",
            ),
        ]
        indexes = [
            models.Index(fields=["component", "channel"]),
            models.Index(fields=["status"]),
        ]

    def __str__(self):
        return f"{self.component}({self.channel_id or '-'}): {self.status}"


class ChannelMonitoringConfig(models.Model):
    """Per-channel expected ACRCloud ingestion volume, used to catch corrupted/degraded feeds."""

    channel = models.OneToOneField(
        Channel,
        on_delete=models.CASCADE,
        related_name="monitoring_config",
    )
    expected_segments_per_hour = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text=(
            "Admin-set minimum expected segments per hour for this channel. Used to detect a "
            "corrupted/degraded ACRCloud feed (e.g. an incident where only 5-15 segments/day "
            "came in instead of the normal volume). Leave blank to skip this check."
        ),
    )
    low_volume_alert_enabled = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"MonitoringConfig({self.channel_id})"


class AlertRecipient(models.Model):
    """Recipients notified (via GHL) when a component's health worsens."""

    email = models.EmailField()
    channel = models.ForeignKey(
        Channel,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="alert_recipients",
        help_text=(
            "Only notified about this channel's components (acr_ingestion, openai_billing). "
            "Leave blank to be notified about every channel plus channel-independent "
            "components (e.g. the processing pipeline backlog)."
        ),
    )
    name = models.CharField(max_length=255, blank=True, default="")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["email", "channel"],
                condition=models.Q(channel__isnull=False),
                name="uq_alertrecipient_email_channel",
            ),
            models.UniqueConstraint(
                fields=["email"],
                condition=models.Q(channel__isnull=True),
                name="uq_alertrecipient_email_global",
            ),
        ]

    def __str__(self):
        return f"{self.email} ({self.channel_id or 'all channels'})"
