from django.urls import path

from .views import (
    PromptDetailView,
    PromptListCreateView,
    PromptRunExecuteView,
    PromptRunListView,
    PromptRunRetrieveView,
)


urlpatterns = [
    path("prompts/", PromptListCreateView.as_view(), name="prompt-list-create"),
    path(
        "prompt-runs/",
        PromptRunListView.as_view(),
        name="prompt-run-list",
    ),
    path("prompt-runs/<int:pk>/", PromptRunRetrieveView.as_view(), name="prompt-run-detail"),
    path("prompts/execute/", PromptRunExecuteView.as_view(), name="prompt-run-execute", ),
    path("prompts/<int:pk>/", PromptDetailView.as_view(), name="prompt-detail"),
]
