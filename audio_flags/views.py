from typing import Optional

from rest_framework import generics, permissions

from .models import FlagCondition
from .serializers import FlagConditionSerializer
from .repositories import FlagConditionRepository


class FlagConditionListCreateView(generics.ListCreateAPIView):
    """
    Provides list and create operations for FlagCondition records.
    Supports optional filtering by channel and active status.
    """

    serializer_class = FlagConditionSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        channel_id = self.request.query_params.get("channel_id")
        active_only = self._parse_bool(self.request.query_params.get("active_only"))

        if channel_id:
            queryset = FlagConditionRepository.get_by_channel(
                channel_id=channel_id,
                active_only=active_only,
            )
        else:
            queryset = FlagConditionRepository.get_all()
            if active_only:
                queryset = queryset.filter(is_active=True)

        return queryset.select_related("channel", "created_by")

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user)

    @staticmethod
    def _parse_bool(value: Optional[str]) -> bool:
        return str(value).lower() in {"true", "1", "yes"} if value is not None else False


class FlagConditionDetailView(generics.RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update, or delete a single FlagCondition instance.
    """

    serializer_class = FlagConditionSerializer
    permission_classes = [permissions.IsAuthenticated]
    queryset = FlagCondition.objects.select_related("channel", "created_by")
