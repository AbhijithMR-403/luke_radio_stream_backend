from django.urls import path

from .views import AudioSegmentFilterView


urlpatterns = [
    path("prompt/", AudioSegmentFilterView.as_view(), name="v3_filter_audio_segments"),
]
