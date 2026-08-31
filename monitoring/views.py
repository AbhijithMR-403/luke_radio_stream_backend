from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import permissions, views
from rest_framework.response import Response

from core_admin.models import Channel
from monitoring.models import AlertRecipient, ChannelMonitoringConfig, ComponentHealth
from monitoring.serializers import AlertRecipientSerializer

_STATUS_RANK = {
    ComponentHealth.STATUS_HEALTHY: 0,
    ComponentHealth.STATUS_UNKNOWN: 1,
    ComponentHealth.STATUS_WARNING: 2,
    ComponentHealth.STATUS_UNHEALTHY: 3,
}
_RANK_TO_STATUS = {rank: status for status, rank in _STATUS_RANK.items()}


def _worst(statuses):
    if not statuses:
        return ComponentHealth.STATUS_UNKNOWN
    return _RANK_TO_STATUS[max(_STATUS_RANK.get(s, 0) for s in statuses)]


def _channel_row(row):
    return {
        "channel_id": row.channel_id,
        "channel_name": row.channel.name if row.channel else None,
        "status": row.status,
        "last_run_at": row.last_run_at,
        "last_success_at": row.last_success_at,
        "segments_received": row.segments_received,
        "segments_saved": row.segments_saved,
        "coverage_ratio": row.coverage_ratio,
        "message": row.message,
    }


class HealthCheckView(views.APIView):
    """GET /health/ - lightweight, always-200 status endpoint. No auth required."""

    permission_classes = [permissions.AllowAny]
    authentication_classes = []

    def get(self, request):
        rows = list(ComponentHealth.objects.select_related("channel").all())

        acr_rows = [r for r in rows if r.component == ComponentHealth.COMPONENT_ACR_INGESTION]
        openai_rows = [r for r in rows if r.component == ComponentHealth.COMPONENT_OPENAI_BILLING]
        pipeline_row = next(
            (r for r in rows if r.component == ComponentHealth.COMPONENT_PROCESSING_PIPELINE), None
        )

        components = {
            "acr_ingestion": {
                "status": _worst([r.status for r in acr_rows]),
                "channels": [_channel_row(r) for r in acr_rows],
            },
            "openai_billing": {
                "status": _worst([r.status for r in openai_rows]),
                "channels": [_channel_row(r) for r in openai_rows],
            },
            "processing_pipeline": {
                "status": pipeline_row.status if pipeline_row else ComponentHealth.STATUS_UNKNOWN,
                "last_run_at": pipeline_row.last_run_at if pipeline_row else None,
                "message": pipeline_row.message if pipeline_row else "",
                "details": pipeline_row.details if pipeline_row else None,
            },
        }

        # Worst-of-all-actual-rows, not worst-of-per-group-summaries - an empty group (e.g. no
        # channel has an OpenAI key configured yet) must not drag the overall status to "unknown".
        overall_status = _worst([r.status for r in rows])

        return Response(
            {
                "status": overall_status,
                "generated_at": timezone.now(),
                "components": components,
            },
            status=200,
        )


class ChannelMonitoringSettingsView(views.APIView):
    """
    GET /api/monitoring/channels/<channel_id>/settings/
    PUT /api/monitoring/channels/<channel_id>/settings/

    Manages a channel's expected-volume config (ChannelMonitoringConfig) and its
    alert recipients (AlertRecipient) together, in a single call - both are configured
    per channel and naturally belong together from an admin's point of view.

    PUT body:
    {
        "expected_segments_per_hour": 20,       // null/omit to disable the volume check
        "low_volume_alert_enabled": true,
        "alert_recipients": [
            {"email": "ops@client.com", "name": "Ops Team"},
            {"email": "admin@client.com"}
        ]
    }
    `alert_recipients` fully replaces this channel's recipient list (channel-scoped ones
    only - global recipients, i.e. AlertRecipient rows with channel=null, are untouched
    and not shown here).
    """

    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, channel_id):
        channel = get_object_or_404(Channel, pk=channel_id)
        return Response(self._serialize(channel))

    def put(self, request, channel_id):
        channel = get_object_or_404(Channel, pk=channel_id)

        config, _ = ChannelMonitoringConfig.objects.update_or_create(
            channel=channel,
            defaults={
                "expected_segments_per_hour": request.data.get("expected_segments_per_hour"),
                "low_volume_alert_enabled": request.data.get("low_volume_alert_enabled", True),
            },
        )

        incoming = request.data.get("alert_recipients", [])
        incoming_emails = {r["email"] for r in incoming if r.get("email")}

        # Full replace: drop channel-scoped recipients no longer in the list, upsert the rest.
        AlertRecipient.objects.filter(channel=channel).exclude(email__in=incoming_emails).delete()
        for r in incoming:
            email = r.get("email")
            if not email:
                continue
            AlertRecipient.objects.update_or_create(
                channel=channel,
                email=email,
                defaults={"name": r.get("name", ""), "is_active": r.get("is_active", True)},
            )

        return Response(self._serialize(channel))

    @staticmethod
    def _serialize(channel):
        config = ChannelMonitoringConfig.objects.filter(channel=channel).first()
        recipients = AlertRecipient.objects.filter(channel=channel)
        return {
            "channel_id": channel.id,
            "expected_segments_per_hour": config.expected_segments_per_hour if config else None,
            "low_volume_alert_enabled": config.low_volume_alert_enabled if config else True,
            "alert_recipients": AlertRecipientSerializer(recipients, many=True).data,
        }
