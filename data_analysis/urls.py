from django.urls import path
from . import views


urlpatterns = [
    path('audio_segments/<int:segment_id>', views.AudioSegmentIsActiveUpdateView.as_view(), name='audio_segment_is_active_update'),
    path('segments/create', views.CreateSegmentFromRangeView.as_view(), name='create_segment_manually'),
    path('audio_segments_with_transcription', views.AudioSegmentsWithTranscriptionView.as_view(), name='audio_segments_with_transcription'),
    path('audio_segments', views.AudioSegments.as_view(), name='audio_segments'),
    path('pie_chart', views.PieChartDataView.as_view(), name='pie_chart_data'),
    path('rev-callback', views.RevCallbackView.as_view(), name='rev-callback'),
    path('download_media/<path:file_path>', views.MediaDownloadView.as_view(), name='download_media'),
    path('transcribe_and_analyze', views.AudioTranscriptionAndAnalysisView.as_view(), name='transcribe_and_analyze'),
]