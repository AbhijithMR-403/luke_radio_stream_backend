from django.urls import path
from . import views
from .views import update_settings_and_buckets, ChannelCRUDView, get_settings_and_buckets

urlpatterns = [
    path('settings', update_settings_and_buckets, name='update_settings_and_buckets'),
    path('settings/get', get_settings_and_buckets, name='get_settings_and_buckets'),
    path('channels', ChannelCRUDView.as_view(), name='channels_crud'),
]
