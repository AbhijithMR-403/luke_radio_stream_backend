from django.urls import path
from dashboard.v1.views import DashboardStatsView, ShiftAnalyticsView, ShiftAnalyticsV2View, TopicAudioSegmentsView, GeneralTopicsManagementView
from dashboard.v2.views import SummaryView, BucketCountView

app_name = 'dashboard'

urlpatterns = [
    path('dashboard/stats/', DashboardStatsView.as_view(), name='dashboard_stats'),
    path('dashboard/shift-analytics/', ShiftAnalyticsView.as_view(), name='shift_analytics'),
    path('dashboard/shift-analytics/v2/', ShiftAnalyticsV2View.as_view(), name='shift_analytics_v2'),
    path('dashboard/topic-audio-segments/', TopicAudioSegmentsView.as_view(), name='topic_audio_segments'),
    # General Topic Management
    path('general_topics', GeneralTopicsManagementView.as_view(), name='general_topics_management'),

    # V2 API
    # Summary API
    path('v2/dashboard/summary/', SummaryView.as_view(), name='summary'),
    # Bucket Count API
    path('v2/dashboard/bucket-count/', BucketCountView.as_view(), name='bucket_count'),

]
