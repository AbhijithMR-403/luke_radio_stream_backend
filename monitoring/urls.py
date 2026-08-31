from django.urls import path

from monitoring.views import ChannelMonitoringSettingsView, HealthCheckView

urlpatterns = [
    path("health/", HealthCheckView.as_view(), name="health-check"),
    path(
        "monitoring/channels/<int:channel_id>/settings/",
        ChannelMonitoringSettingsView.as_view(),
        name="channel-monitoring-settings",
    ),
]
