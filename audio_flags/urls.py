from django.urls import path

from .views import (
    FlagConditionDetailView,
    FlagConditionListCreateView,
)

urlpatterns = [
    path(
        "custom-flag/",
        FlagConditionListCreateView.as_view(),
        name="flagcondition-list-create",
    ),
    path(
        "custom-flag/<int:pk>/",
        FlagConditionDetailView.as_view(),
        name="flagcondition-detail",
    ),
]

