from django.urls import path

from .views import OpenRouterModelsAPIView


urlpatterns = [
    path("openrouter/models", OpenRouterModelsAPIView.as_view(), name="openrouter_models"),
]
