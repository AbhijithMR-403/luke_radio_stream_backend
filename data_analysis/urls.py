from django.urls import path
from . import views


urlpatterns = [
    path('unrecognized_segments', views.UnrecognizedAudioSegmentsView.as_view(), name='unrecognized_segments'),
    path('rev-callback', views.RevCallbackView.as_view(), name='rev-callback'),
    path('download_media/<path:file_path>', views.MediaDownloadView.as_view(), name='download_media'),
]