from django.urls import path

from .views import PromptDetailView, PromptListCreateView


urlpatterns = [
    path("prompts/", PromptListCreateView.as_view(), name="prompt-list-create"),
    path("prompts/<int:pk>/", PromptDetailView.as_view(), name="prompt-detail"),
]
