from django.urls import path
from .views.channel import ChannelAPIView
from .views.settings_and_buckets import SettingsAndBucketsAPIView

urlpatterns = [
    path('settings', SettingsAndBucketsAPIView.as_view(), name='settings'),
    path('channels', ChannelAPIView.as_view(), name='channels_crud'),
]
