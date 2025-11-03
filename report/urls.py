from django.urls import path
from . import views


urlpatterns = [
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

