from django.urls import path
from . import views
from .views import update_settings_and_buckets, ChannelCRUDView

urlpatterns = [
    # path('api/create_channel/', views.create_channel, name='create_channel'),
    path('settings', update_settings_and_buckets, name='update_settings_and_buckets'),
    path('channels', ChannelCRUDView.as_view(), name='channels_crud'),
]
