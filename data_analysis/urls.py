from django.urls import path
from . import views


urlpatterns = [
    path('audio_segments_with_transcription', views.AudioSegmentsWithTranscriptionView.as_view(), name='audio_segments_with_transcription'),
    path('audio_segments', views.AudioSegments.as_view(), name='audio_segments'),
    path('rev-callback', views.RevCallbackView.as_view(), name='rev-callback'),
    path('download_media/<path:file_path>', views.MediaDownloadView.as_view(), name='download_media'),
    path('transcribe_and_analyze', views.AudioTranscriptionAndAnalysisView.as_view(), name='transcribe_and_analyze'),
    path('transcription_queue_status', views.TranscriptionQueueStatusView.as_view(), name='transcription_queue_status'),
    path('general_topics', views.GeneralTopicsManagementView.as_view(), name='general_topics_management'),
]