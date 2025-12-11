from django.urls import path

from logger.views import (
    AudioSegmentEditLogListView,
    RevTranscriptionJobLogStatisticsView,
)

urlpatterns = [
    path(
        "logger/audio-segments/<int:audio_segment_id>/edit-logs/",
        AudioSegmentEditLogListView.as_view(),
        name="audio-segment-edit-log-list",
    ),
    path(
        "logger/rev-transcription-job-logs/statistics/",
        RevTranscriptionJobLogStatisticsView.as_view(),
        name="rev-transcription-job-log-statistics",
    ),
]

