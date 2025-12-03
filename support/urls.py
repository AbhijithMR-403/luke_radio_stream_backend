from django.urls import path

from .views import (
    SupportTicketListCreateAPIView,
    SupportTicketDetailAPIView,
    SupportTicketRespondAPIView,
    TranscribedAudioStatsView,
    TranscribedAudioStatsCSVView,
)


urlpatterns = [
    path("support-tickets/", SupportTicketListCreateAPIView.as_view(), name="support-ticket-list-create"),
    path("support-tickets/<int:ticket_id>/", SupportTicketDetailAPIView.as_view(), name="support-ticket-detail"),
    path("support-tickets/<int:ticket_id>/respond/", SupportTicketRespondAPIView.as_view(), name="support-ticket-respond"),
    # Transcribed Audio Statistics API (JSON)
    path("transcribed-audio-stats/", TranscribedAudioStatsView.as_view(), name="transcribed_audio_stats"),
    # Transcribed Audio Statistics CSV Export API
    path("transcribed-audio-stats/csv/", TranscribedAudioStatsCSVView.as_view(), name="transcribed_audio_stats_csv"),
]


