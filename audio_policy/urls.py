from django.urls import path

from .views import (
    FlagConditionDetailView,
    FlagConditionListCreateView,
    ContentTypeDeactivationRuleListCreateView,
    ContentTypeDeactivationRuleDetailView,
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
    path(
        "content-type-deactivation-rules/",
        ContentTypeDeactivationRuleListCreateView.as_view(),
        name="content_type_deactivation_rule_list_create",
    ),
    path(
        "content-type-deactivation-rules/<int:pk>/",
        ContentTypeDeactivationRuleDetailView.as_view(),
        name="content_type_deactivation_rule_detail",
    ),
]

