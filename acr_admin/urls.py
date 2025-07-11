from django.urls import path
from . import views
from .views import ChannelCRUDView, SettingsAndBucketsView
from .views import UnrecognizedAudioSegmentsView
from .views import RevCallbackView

urlpatterns = [
    path('settings', SettingsAndBucketsView.as_view(), name='settings'),
    path('channels', ChannelCRUDView.as_view(), name='channels_crud'),
    path('unrecognized_segments', UnrecognizedAudioSegmentsView.as_view(), name='unrecognized_segments'),
    path('rev-callback', RevCallbackView.as_view(), name='rev-callback'),
]
