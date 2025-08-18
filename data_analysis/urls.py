from django.urls import path
from . import views


urlpatterns = [
    path('audio_segments_with_transcription', views.AudioSegmentsWithTranscriptionView.as_view(), name='audio_segments_with_transcription'),
    path('rev-callback', views.RevCallbackView.as_view(), name='rev-callback'),
    path('download_media/<path:file_path>', views.MediaDownloadView.as_view(), name='download_media'),
]