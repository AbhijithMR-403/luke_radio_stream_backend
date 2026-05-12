from rest_framework import generics, permissions

from .models import Prompt
from .serializers import PromptSerializer


class PromptListCreateView(generics.ListCreateAPIView):
    serializer_class = PromptSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        queryset = Prompt.objects.all().order_by("-created_at")

        is_active = self.request.query_params.get("is_active")
        if is_active is not None:
            queryset = queryset.filter(is_active=is_active.lower() == "true")

        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(name__icontains=search)

        return queryset


class PromptDetailView(generics.RetrieveUpdateDestroyAPIView):
    serializer_class = PromptSerializer
    permission_classes = [permissions.IsAuthenticated]
    queryset = Prompt.objects.all()
