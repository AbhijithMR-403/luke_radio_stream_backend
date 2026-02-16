from django.urls import path
from .views.channel import (
    ChannelAPIView,
    IngestPodcastRSSFeedAPIView,
    RSSFeedTotalDurationAPIView,
    SetChannelDefaultSettingsAPIView,
)
from .views.settings_and_buckets import SettingsAndBucketsAPIView, RevertToVersionAPIView

urlpatterns = [
    path('settings', SettingsAndBucketsAPIView.as_view(), name='settings'),
    path('settings/revert', RevertToVersionAPIView.as_view(), name='settings_revert'),
    path('channels', ChannelAPIView.as_view(), name='channels_crud'),
    path('channels/default-settings', SetChannelDefaultSettingsAPIView.as_view(), name='channel_default_settings'),
    path('channels/rss/reanalyze', IngestPodcastRSSFeedAPIView.as_view(), name='ingest_podcast_rss'),
    path('channels/rss/total-duration', RSSFeedTotalDurationAPIView.as_view(), name='rss_total_duration'),
]
