from django.urls import path
from .views import (
    ShiftListCreateView,
    ShiftDetailView,
    ActiveShiftsView,
    PredefinedFilterListCreateView,
    PredefinedFilterDetailView,
    PredefinedFilterSchedulesView,
    FilterScheduleListCreateView,
    FilterScheduleDetailView,
)

urlpatterns = [
    # Shift URLs
    path('shifts/', ShiftListCreateView.as_view(), name='shift-list'),
    path('shifts/<int:pk>/', ShiftDetailView.as_view(), name='shift-detail'),
    path('shifts/active/', ActiveShiftsView.as_view(), name='shift-active'),
    
    # Predefined Filter URLs
    path('predefined-filters/', PredefinedFilterListCreateView.as_view(), name='predefined-filter-list'),
    path('predefined-filters/<int:pk>/', PredefinedFilterDetailView.as_view(), name='predefined-filter-detail'),
    path('predefined-filters/<int:pk>/schedules/', PredefinedFilterSchedulesView.as_view(), name='predefined-filter-schedules'),
    
    # Filter Schedule URLs
    path('filter-schedules/', FilterScheduleListCreateView.as_view(), name='filter-schedule-list'),
    path('filter-schedules/<int:pk>/', FilterScheduleDetailView.as_view(), name='filter-schedule-detail'),
]
