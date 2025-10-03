from rest_framework import filters, generics

from .models import AudioUnrecognizedCategory, TitleMappingRule
from .serializer import (
    AudioUnrecognizedCategorySerializer,
    TitleMappingRuleSerializer,
)


class AudioUnrecognizedCategoryListCreateView(generics.ListCreateAPIView):
    queryset = AudioUnrecognizedCategory.objects.all()
    serializer_class = AudioUnrecognizedCategorySerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ["name", "description"]
    ordering_fields = ["name", "created_at", "updated_at"]
    ordering = ["name"]


class AudioUnrecognizedCategoryDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = AudioUnrecognizedCategory.objects.all()
    serializer_class = AudioUnrecognizedCategorySerializer


class TitleMappingRuleListCreateView(generics.ListCreateAPIView):
    queryset = TitleMappingRule.objects.select_related("category").all()
    serializer_class = TitleMappingRuleSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ["before_title", "notes", "category__name"]
    ordering_fields = ["updated_at", "created_at", "before_title"]
    ordering = ["-updated_at"]


class TitleMappingRuleDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = TitleMappingRule.objects.select_related("category").all()
    serializer_class = TitleMappingRuleSerializer
