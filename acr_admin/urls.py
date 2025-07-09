from django.urls import path
from . import views
from .views import ChannelCRUDView, SettingsAndBucketsView

urlpatterns = [
    path('settings', SettingsAndBucketsView.as_view(), name='settings'),
    path('channels', ChannelCRUDView.as_view(), name='channels_crud'),
]
