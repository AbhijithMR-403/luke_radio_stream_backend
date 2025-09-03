from django.urls import path
from . import views

app_name = 'dashboard'

urlpatterns = [
    path('dashboard/stats/', views.DashboardStatsView.as_view(), name='dashboard_stats'),
]
