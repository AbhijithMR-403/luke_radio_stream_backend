import logging
from datetime import timedelta

from celery import shared_task
from django.utils import timezone

from core_admin.models import GeneralSetting
from data_analysis.tasks import deactivate_channels_without_valid_settings
from monitoring.integrations import validate_openai_api_key
from monitoring.models import ComponentHealth
from monitoring.services import (
    maybe_send_alert,
    record_openai_billing_health,
    record_processing_backlog_health,
)

logger = logging.getLogger(__name__)


@shared_task
def check_system_health():
    """
    Beat-scheduled task (every 15 min) covering stale ACRCloud ingestion and the
    stalled-processing backlog. OpenAI billing is checked separately by
    check_openai_billing (hourly) - it doesn't run on this 15-min cadence at all.
    """
    try:
        _check_stale_ingestion()
    except Exception:
        logger.exception("monitoring: failed staleness check")

    record_processing_backlog_health()


@shared_task
def check_openai_billing():
    """
    Beat-scheduled task (hourly) - validates each channel's OpenAI API key/billing status.
    Kept as its own task, on its own hourly schedule, rather than folded into
    check_system_health, since it makes real (billed) external API calls and has no
    reason to be considered on the 15-min cadence the other checks run on.
    """
    settings_with_key = (
        GeneralSetting.objects.filter(is_active=True)
        .exclude(openai_api_key__isnull=True)
        .exclude(openai_api_key="")
        .select_related("channel")
    )

    for setting in settings_with_key:
        try:
            result = validate_openai_api_key(setting.openai_api_key)
            record_openai_billing_health(setting.channel, result["is_valid"], result["error_message"])
        except Exception:
            logger.exception("monitoring: failed openai billing check for channel %s", setting.channel_id)


def _check_stale_ingestion():
    channels = deactivate_channels_without_valid_settings()
    stale_cutoff = timezone.now() - timedelta(minutes=90)  # ingestion runs hourly; tolerate one delayed run

    for channel in channels:
        row = ComponentHealth.objects.filter(
            component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=channel
        ).first()
        if not row or not row.last_run_at:
            continue  # no run recorded yet - first-run case, not a confirmed failure
        if row.last_run_at >= stale_cutoff:
            continue  # recent enough

        previous_status = row.status
        if row.status != ComponentHealth.STATUS_UNHEALTHY:
            row.status = ComponentHealth.STATUS_UNHEALTHY
            row.message = f"No ingestion run recorded since {row.last_run_at.isoformat()}"
            row.save(update_fields=["status", "message", "updated_at"])
        # Always re-evaluate the alert, even if already unhealthy - otherwise a channel that
        # stays stale indefinitely would get exactly one alert, ever, with no 24h reminder.
        maybe_send_alert(row, previous_status)
