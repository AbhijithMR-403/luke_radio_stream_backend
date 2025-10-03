from django.urls import path

from .views import (
    AudioUnrecognizedCategoryDetailView,
    AudioUnrecognizedCategoryListCreateView,
    TitleMappingRuleDetailView,
    TitleMappingRuleListCreateView,
)

urlpatterns = [
    path(
        "unrecognized-categories/",
        AudioUnrecognizedCategoryListCreateView.as_view(),
        name="unrecognized-category-list-create",
    ),
    path(
        "unrecognized-categories/<int:pk>/",
        AudioUnrecognizedCategoryDetailView.as_view(),
        name="unrecognized-category-detail",
    ),
    path(
        "title-mapping-rules/",
        TitleMappingRuleListCreateView.as_view(),
        name="title-mapping-rule-list-create",
    ),
    path(
        "title-mapping-rules/<int:pk>/",
        TitleMappingRuleDetailView.as_view(),
        name="title-mapping-rule-detail",
    ),
]
