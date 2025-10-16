from django.urls import path
from . import views

app_name = 'dashboard'

urlpatterns = [
    path('dashboard/stats/', views.DashboardStatsView.as_view(), name='dashboard_stats'),
    path('dashboard/shift-analytics/', views.ShiftAnalyticsView.as_view(), name='shift_analytics'),
    path('dashboard/shift-analytics/v2/', views.ShiftAnalyticsV2View.as_view(), name='shift_analytics_v2'),
    path('dashboard/topic-audio-segments/', views.TopicAudioSegmentsView.as_view(), name='topic_audio_segments'),
    # General Topic Management
    path('general_topics', views.GeneralTopicsManagementView.as_view(), name='general_topics_management'),

]
