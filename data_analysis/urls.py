from django.urls import path
from . import views
from .v2 import views as v2_views


urlpatterns = [
    path('audio_segments/update_active_status', views.AudioSegmentBulkIsActiveUpdateView.as_view(), name='audio_segment_update_active_status'),
    path('audio_segments_with_transcription', views.AudioSegmentsWithTranscriptionView.as_view(), name='audio_segments_with_transcription'),
    path('audio_segments', views.AudioSegments.as_view(), name='audio_segments'),
    path('pie_chart', views.PieChartDataView.as_view(), name='pie_chart_data'),
    path('rev-callback', views.RevCallbackView.as_view(), name='rev-callback'),
    path('download_media/<path:file_path>', views.MediaDownloadView.as_view(), name='download_media'),
    path('transcribe_and_analyze', views.AudioTranscriptionAndAnalysisView.as_view(), name='transcribe_and_analyze'),
    # V2 API endpoints
    path('v2/audio-segments/', v2_views.ListAudioSegmentsV2View.as_view(), name='v2_audio_segments'),
    path('v2/filter/options/', v2_views.ContentTypePromptView.as_view(), name='v2_content_type_prompt'),
]