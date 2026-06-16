from django.urls import path

from .views import (
    PromptDetailView,
    PromptListCreateView,
    PromptRunExecuteView,
    PromptRunListView,
    PromptRunRetrieveView,
    PromptRunTitleUpdateView,
)


urlpatterns = [
    path("prompts/", PromptListCreateView.as_view(), name="prompt-list-create"),
    path("prompt-runs/", PromptRunListView.as_view(), name="prompt-run-list",),
    path("prompt-runs/<int:pk>/", PromptRunRetrieveView.as_view(), name="prompt-run-detail"),
    path("prompt-runs/<int:pk>/title/", PromptRunTitleUpdateView.as_view(), name="prompt-run-title-update"),
    path("prompts/execute/", PromptRunExecuteView.as_view(), name="prompt-run-execute", ),
    path("prompts/<int:pk>/", PromptDetailView.as_view(), name="prompt-detail"),
]
