from django.urls import path

from .views import AudioSegmentFilterView, AudioSegmentFilterV3View


urlpatterns = [
    path("prompt/", AudioSegmentFilterView.as_view(), name="filter_audio_segments_prompt"),
    path("v3/audio-segments/", AudioSegmentFilterV3View.as_view(), name="v3_filter_audio_segments_transcribed"),
]
