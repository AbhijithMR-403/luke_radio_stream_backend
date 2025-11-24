from django.urls import path

from logger.views import AudioSegmentEditLogListView

urlpatterns = [
    path(
        "logger/audio-segments/<int:audio_segment_id>/edit-logs/",
        AudioSegmentEditLogListView.as_view(),
        name="audio-segment-edit-log-list",
    ),
]

