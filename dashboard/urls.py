from django.urls import path
from dashboard.v1.views import DashboardStatsView, ShiftAnalyticsView, ShiftAnalyticsV2View, TopicAudioSegmentsView, GeneralTopicsManagementView

app_name = 'dashboard'

urlpatterns = [
    path('dashboard/stats/', DashboardStatsView.as_view(), name='dashboard_stats'),
    path('dashboard/shift-analytics/', ShiftAnalyticsView.as_view(), name='shift_analytics'),
    path('dashboard/shift-analytics/v2/', ShiftAnalyticsV2View.as_view(), name='shift_analytics_v2'),
    path('dashboard/topic-audio-segments/', TopicAudioSegmentsView.as_view(), name='topic_audio_segments'),
    # General Topic Management
    path('general_topics', GeneralTopicsManagementView.as_view(), name='general_topics_management'),

]
