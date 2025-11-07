from django.urls import path
from . import views


urlpatterns = [
	path('process_segments', views.ProcessSegmentsView.as_view(), name='audio_merger_process_segments'),
	path('segments/create', views.SplitAudioSegmentView.as_view(), name='split_audio_segment'),
]


