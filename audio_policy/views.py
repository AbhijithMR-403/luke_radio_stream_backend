from typing import Optional

from rest_framework import generics, permissions

from .models import FlagCondition, ContentTypeDeactivationRule
from .serializers import FlagConditionSerializer, ContentTypeDeactivationRuleSerializer
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


class ContentTypeDeactivationRuleListCreateView(generics.ListCreateAPIView):
    """
    Provides list and create operations for ContentTypeDeactivationRule records.
    
    Query Parameters:
    - channel_id (optional): Filter by channel ID (integer primary key)
    - is_active (optional): Filter by active status (true/false)
    - search (optional): Search by content type name (case-insensitive partial match)
    - ordering (optional): Order results by field(s), default: "channel,content_type"
    
    Examples:
    - GET /api/content-type-deactivation-rules/?channel_id=1
    - GET /api/content-type-deactivation-rules/?channel_id=1&is_active=true
    - GET /api/content-type-deactivation-rules/?search=Commercial
    - GET /api/content-type-deactivation-rules/?channel_id=1&search=Ad&ordering=-created_at
    """

    serializer_class = ContentTypeDeactivationRuleSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        queryset = ContentTypeDeactivationRule.objects.all()

        # Filter by channel if provided
        channel_id = self.request.query_params.get("channel_id")
        if channel_id:
            try:
                queryset = queryset.filter(channel_id=int(channel_id))
            except (ValueError, TypeError):
                # Invalid channel_id, return empty queryset
                queryset = queryset.none()

        # Filter by active status if provided
        is_active = self.request.query_params.get("is_active")
        if is_active is not None:
            queryset = queryset.filter(is_active=is_active.lower() == "true")

        # Search by content type if provided
        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(content_type__icontains=search)

        # Order by channel and content_type by default
        ordering = self.request.query_params.get("ordering", "channel,content_type")
        queryset = queryset.order_by(ordering)
        
        return queryset.select_related("channel")


class ContentTypeDeactivationRuleDetailView(generics.RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update, or delete a single ContentTypeDeactivationRule instance.
    """

    serializer_class = ContentTypeDeactivationRuleSerializer
    permission_classes = [permissions.IsAuthenticated]
    queryset = ContentTypeDeactivationRule.objects.select_related("channel")
