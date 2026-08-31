import logging
from datetime import datetime, timedelta, timezone as dt_timezone

from django.db.models import Q
from django.utils import timezone

from data_analysis.models import AudioSegments
from monitoring.integrations import send_alert_via_ghl
from monitoring.models import AlertRecipient, ChannelMonitoringConfig, ComponentHealth

logger = logging.getLogger(__name__)

# How often to re-notify while a problem persists (the only named, reused threshold -
# every other check below is a single, binary healthy/bad decision at one inline threshold).
ALERT_REMINDER_HOURS = 24

_STATUS_RANK = {
    ComponentHealth.STATUS_HEALTHY: 0,
    ComponentHealth.STATUS_WARNING: 1,
    ComponentHealth.STATUS_UNHEALTHY: 2,
}
_BAD_STATUSES = (ComponentHealth.STATUS_WARNING, ComponentHealth.STATUS_UNHEALTHY)


# ---------------------------------------------------------------------------
# ACRCloud ingestion health
# ---------------------------------------------------------------------------

def record_ingestion_health(channel, segments_data, segments_saved_count, is_today, date_str, run_started_at):
    """Upserts the acr_ingestion ComponentHealth row for `channel`. Never raises."""
    try:
        _record_ingestion_health(channel, segments_data, segments_saved_count, is_today, date_str, run_started_at)
    except Exception:
        logger.exception(
            "monitoring: failed to record ingestion health for channel %s",
            getattr(channel, "id", channel),
        )


def _record_ingestion_health(channel, segments_data, segments_saved_count, is_today, date_str, run_started_at):
    run_started_at = run_started_at or timezone.now()

    previous = ComponentHealth.objects.filter(
        component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=channel
    ).first()
    previous_status = previous.status if previous else ComponentHealth.STATUS_UNKNOWN

    is_valid, reason = _validate_response_shape(segments_data)
    segments_received = len(segments_data["data"]) if is_valid else None
    segments_saved = segments_saved_count or 0

    expected_window_seconds = actual_coverage_seconds = coverage_ratio = None

    if not is_valid:
        # Malformed/unusable API response - always unhealthy, no other checks apply.
        status, message = ComponentHealth.STATUS_UNHEALTHY, f"Invalid ACRCloud response: {reason}"
    else:
        window_start, window_end = _compute_expected_window(is_today, date_str, run_started_at)
        # Re-query what this run actually persisted (by creation time), rather than requiring
        # the caller to pass the segment objects through - keeps the ingestion task's own code
        # untouched beyond a single call site.
        run_segments = list(
            AudioSegments.objects.filter(channel=channel, is_delete=False, created_at__gte=run_started_at)
        )
        expected_window_seconds, actual_coverage_seconds, coverage_ratio = _compute_coverage(
            run_segments, window_start, window_end
        )

        if expected_window_seconds == 0:
            # Legitimate empty window (e.g. the is_today cutoff collapsing near midnight) - not a problem.
            status, message = ComponentHealth.STATUS_HEALTHY, ""
        else:
            # Volume drop (the incident this was built for) is always unhealthy; coverage gaps are a softer warning.
            vol_status, vol_message = _evaluate_volume(channel, segments_saved, expected_window_seconds)
            cov_status, cov_message = _evaluate_coverage(coverage_ratio)
            status, message = _combine_status(vol_status, vol_message, cov_status, cov_message)

    now = timezone.now()
    defaults = {
        "status": status,
        "last_run_at": now,
        "segments_received": segments_received,
        "segments_saved": segments_saved,
        "expected_window_seconds": expected_window_seconds,
        "actual_coverage_seconds": actual_coverage_seconds,
        "coverage_ratio": coverage_ratio,
        "message": message,
    }
    if status == ComponentHealth.STATUS_HEALTHY:
        defaults["last_success_at"] = now

    component_health, _ = ComponentHealth.objects.update_or_create(
        component=ComponentHealth.COMPONENT_ACR_INGESTION,
        channel=channel,
        defaults=defaults,
    )
    maybe_send_alert(component_health, previous_status)


def _validate_response_shape(segments_data):
    """Returns (is_usable, reason)."""
    if not isinstance(segments_data, dict):
        return False, "response is not a dict"
    if not isinstance(segments_data.get("data"), list):
        return False, "response is missing a 'data' list"
    return True, None


def _compute_expected_window(is_today, date_str, run_started_at):
    """Returns (window_start_utc, window_end_utc)."""
    if is_today:
        window_start = run_started_at.replace(hour=0, minute=0, second=0, microsecond=0)
        window_end = run_started_at - timedelta(hours=1)
        if window_end < window_start:
            window_end = window_start
        return window_start, window_end

    if date_str:
        day = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=dt_timezone.utc)
        return day, day + timedelta(days=1)

    return run_started_at, run_started_at


def _compute_coverage(inserted_segments, window_start, window_end):
    """Returns (expected_window_seconds, actual_coverage_seconds, coverage_ratio) clamped to [0, 1]."""
    expected_window_seconds = max(int((window_end - window_start).total_seconds()), 0)

    if not inserted_segments or expected_window_seconds == 0:
        return expected_window_seconds, 0, 0.0

    span_start = max(min(seg.start_time for seg in inserted_segments), window_start)
    span_end = min(max(seg.end_time for seg in inserted_segments), window_end)
    actual_coverage_seconds = max(int((span_end - span_start).total_seconds()), 0)
    coverage_ratio = min(actual_coverage_seconds / expected_window_seconds, 1.0)
    return expected_window_seconds, actual_coverage_seconds, coverage_ratio


def _evaluate_volume(channel, segments_saved, expected_window_seconds):
    """
    Single-threshold, always-unhealthy-on-trip check - this is the incident-detection check
    (a severely corrupted/degraded ACRCloud feed is a hard problem, not a soft warning).
    """
    config = ChannelMonitoringConfig.objects.filter(channel=channel).first()
    if config and config.expected_segments_per_hour and config.low_volume_alert_enabled:
        window_hours = expected_window_seconds / 3600.0
        expected_count = config.expected_segments_per_hour * window_hours
        if expected_count > 0 and segments_saved < expected_count * 0.5:
            return ComponentHealth.STATUS_UNHEALTHY, (
                f"Expected ~{int(expected_count)} segments for this window, received {segments_saved}"
            )
        return ComponentHealth.STATUS_HEALTHY, ""

    if segments_saved == 0:
        return ComponentHealth.STATUS_UNHEALTHY, "No segments received/saved for a non-empty expected window"
    return ComponentHealth.STATUS_HEALTHY, ""


def _evaluate_coverage(coverage_ratio):
    """Single-threshold, always-warning-on-trip check - partial gaps aren't as severe as a volume drop."""
    if coverage_ratio is None or coverage_ratio >= 0.7:
        return ComponentHealth.STATUS_HEALTHY, ""
    return ComponentHealth.STATUS_WARNING, f"Low time-window coverage: {coverage_ratio:.0%}"


def _combine_status(status_a, message_a, status_b, message_b):
    combined_message = "; ".join(m for m in (message_a, message_b) if m)
    if _STATUS_RANK.get(status_b, 0) > _STATUS_RANK.get(status_a, 0):
        return status_b, combined_message
    return status_a, combined_message


# ---------------------------------------------------------------------------
# Processing pipeline backlog
# ---------------------------------------------------------------------------

def record_processing_backlog_health():
    """Upserts the global processing_pipeline ComponentHealth row. Never raises."""
    try:
        _record_processing_backlog_health()
    except Exception:
        logger.exception("monitoring: failed to record processing backlog health")


def _record_processing_backlog_health():
    previous = ComponentHealth.objects.filter(
        component=ComponentHealth.COMPONENT_PROCESSING_PIPELINE, channel__isnull=True
    ).first()
    previous_status = previous.status if previous else ComponentHealth.STATUS_UNKNOWN

    stalled_threshold_hours = 6  # AudioSegments still unanalyzed after this long counts as backlog
    cutoff = timezone.now() - timedelta(hours=stalled_threshold_hours)
    stalled_count = AudioSegments.objects.filter(
        is_analysis_completed=False, is_delete=False, created_at__lt=cutoff
    ).count()

    # Single threshold, always warning on trip - a growing backlog is concerning but processing
    # is still running, unlike a hard outage.
    if stalled_count > 20:
        status = ComponentHealth.STATUS_WARNING
        message = f"{stalled_count} segments unanalyzed for over {stalled_threshold_hours}h"
    else:
        status, message = ComponentHealth.STATUS_HEALTHY, ""

    now = timezone.now()
    defaults = {
        "status": status,
        "last_run_at": now,
        "message": message,
        "details": {"stalled_count": stalled_count, "threshold_hours": stalled_threshold_hours},
    }
    if status == ComponentHealth.STATUS_HEALTHY:
        defaults["last_success_at"] = now

    component_health, _ = ComponentHealth.objects.update_or_create(
        component=ComponentHealth.COMPONENT_PROCESSING_PIPELINE,
        channel=None,
        defaults=defaults,
    )
    maybe_send_alert(component_health, previous_status)


# ---------------------------------------------------------------------------
# OpenAI billing/quota
# ---------------------------------------------------------------------------

def record_openai_billing_health(channel, is_valid, error_message):
    """Upserts the openai_billing ComponentHealth row for `channel`. Never raises."""
    try:
        _record_openai_billing_health(channel, is_valid, error_message)
    except Exception:
        logger.exception("monitoring: failed to record openai billing health for channel %s", channel.id)


def _record_openai_billing_health(channel, is_valid, error_message):
    previous = ComponentHealth.objects.filter(
        component=ComponentHealth.COMPONENT_OPENAI_BILLING, channel=channel
    ).first()
    previous_status = previous.status if previous else ComponentHealth.STATUS_UNKNOWN

    now = timezone.now()
    # Binary check - an API key is either usable or it isn't, no warning tier.
    status = ComponentHealth.STATUS_HEALTHY if is_valid else ComponentHealth.STATUS_UNHEALTHY
    defaults = {
        "status": status,
        "last_run_at": now,
        "message": "" if is_valid else (error_message or "OpenAI API key invalid"),
    }
    if is_valid:
        defaults["last_success_at"] = now

    component_health, _ = ComponentHealth.objects.update_or_create(
        component=ComponentHealth.COMPONENT_OPENAI_BILLING,
        channel=channel,
        defaults=defaults,
    )
    maybe_send_alert(component_health, previous_status)


# ---------------------------------------------------------------------------
# Alerting (delivered via the existing GHL contact/custom-field mechanism)
# ---------------------------------------------------------------------------

def maybe_send_alert(component_health, previous_status):
    """Sends an alert on a worsening transition, or a reminder while unresolved. Never raises."""
    try:
        _maybe_send_alert(component_health, previous_status)
    except Exception:
        logger.exception("monitoring: failed to evaluate/send alert for %s", component_health)


def _maybe_send_alert(component_health, previous_status):
    status = component_health.status

    if previous_status in _BAD_STATUSES and status == ComponentHealth.STATUS_HEALTHY:
        _dispatch_alert(component_health, recovered=True)
        ComponentHealth.objects.filter(pk=component_health.pk).update(last_alert_sent_at=None)
        return

    if status not in _BAD_STATUSES:
        return

    worsened = _STATUS_RANK.get(status, 0) > _STATUS_RANK.get(previous_status, 0)
    reminder_due = (
        component_health.last_alert_sent_at is None
        or timezone.now() - component_health.last_alert_sent_at > timedelta(hours=ALERT_REMINDER_HOURS)
    )
    if worsened or reminder_due:
        _dispatch_alert(component_health, recovered=False)
        ComponentHealth.objects.filter(pk=component_health.pk).update(last_alert_sent_at=timezone.now())


def _dispatch_alert(component_health, recovered):
    channel_label = component_health.channel.name if component_health.channel_id else "system-wide"
    if recovered:
        message = f"[RECOVERED] {component_health.component} ({channel_label}) is healthy again."
    else:
        message = (
            f"[{component_health.status.upper()}] {component_health.component} ({channel_label}): "
            f"{component_health.message or 'no details'}"
        )
    send_status_alert(message, channel=component_health.channel)


def send_status_alert(message, channel=None):
    """
    Delivers `message` to the recipients scoped to `channel`, via GHL. No-op if none configured.

    `channel=None` (channel-independent components, e.g. processing_pipeline) only reaches
    recipients who are themselves global (channel=None). A specific `channel` reaches that
    channel's own recipients plus any global recipients.
    """
    recipients = AlertRecipient.objects.filter(is_active=True)
    if channel is not None:
        recipients = recipients.filter(Q(channel=channel) | Q(channel__isnull=True))
    else:
        recipients = recipients.filter(channel__isnull=True)

    for recipient in recipients:
        try:
            send_alert_via_ghl(email=recipient.email, message=message, name=recipient.name or None)
        except Exception:
            logger.exception("monitoring: failed to send GHL alert to %s", recipient.email)
