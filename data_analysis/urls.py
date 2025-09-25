from django.urls import path
from . import views


urlpatterns = [
    path('audio_segments_with_transcription', views.AudioSegmentsWithTranscriptionView.as_view(), name='audio_segments_with_transcription'),
    path('audio_segments', views.AudioSegments.as_view(), name='audio_segments'),
    path('rev-callback', views.RevCallbackView.as_view(), name='rev-callback'),
    path('download_media/<path:file_path>', views.MediaDownloadView.as_view(), name='download_media'),
    path('transcribe_and_analyze', views.AudioTranscriptionAndAnalysisView.as_view(), name='transcribe_and_analyze'),

    # Not sure is this api is been used or not
    path('transcription_queue_status', views.TranscriptionQueueStatusView.as_view(), name='transcription_queue_status'),

    
    # Report folder management endpoints
    path('report_folders', views.ReportFolderManagementView.as_view(), name='report_folders'),
    path('report_folders/<int:folder_id>', views.ReportFolderManagementView.as_view(), name='report_folder_detail'),
    
    # Save audio segments to folders
    path('save/audio_segment', views.SaveAudioSegmentView.as_view(), name='save_audio_segment'),
    path('saved/segments/<int:saved_segment_id>', views.SaveAudioSegmentView.as_view(), name='saved_segment_detail'),
    
    # Get folder contents
    path('folders/<int:folder_id>/contents', views.FolderContentsView.as_view(), name='folder_contents'),
    
    # Audio segment insights management
    path('saved/segments/<int:saved_segment_id>/insights', views.AudioSegmentInsightsView.as_view(), name='audio_segment_insights'),
    path('saved/segments/<int:saved_segment_id>/insights/<int:insight_id>', views.AudioSegmentInsightsView.as_view(), name='audio_segment_insight_detail'),
]