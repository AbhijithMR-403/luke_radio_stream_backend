from rest_framework import filters, generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .models import AudioUnrecognizedCategory, TitleMappingRule
from .serializer import (
    AudioUnrecognizedCategorySerializer,
    TitleMappingRuleSerializer,
)


class AudioUnrecognizedCategoryListCreateView(generics.ListCreateAPIView):
    queryset = AudioUnrecognizedCategory.objects.select_related("channel").all()
    serializer_class = AudioUnrecognizedCategorySerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ["name", "description", "channel__name"]
    ordering_fields = ["name", "created_at", "updated_at"]
    ordering = ["name"]


class AudioUnrecognizedCategoryDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = AudioUnrecognizedCategory.objects.select_related("channel").all()
    serializer_class = AudioUnrecognizedCategorySerializer


class TitleMappingRuleListCreateView(generics.ListCreateAPIView):
    queryset = TitleMappingRule.objects.select_related("category", "category__channel").all()
    serializer_class = TitleMappingRuleSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ["before_title", "after_title", "notes", "category__name", "category__channel__name"]
    ordering_fields = ["updated_at", "created_at", "before_title", "after_title"]
    ordering = ["-updated_at"]


class TitleMappingRuleDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = TitleMappingRule.objects.select_related("category", "category__channel").all()
    serializer_class = TitleMappingRuleSerializer


@api_view(['GET'])
def get_category_titles(request, category_id):
    """
    API endpoint to fetch all title mapping rules from a specific category.
    
    Returns complete details of all title mapping rules associated with the given category.
    """
    try:
        # Check if category exists
        category = AudioUnrecognizedCategory.objects.get(id=category_id, is_active=True)
        
        # Get all active title mapping rules for this category with related data
        title_rules = TitleMappingRule.objects.filter(
            category=category,
        ).select_related('category', 'category__channel').order_by('-updated_at')
        
        # Serialize the rules using the existing serializer
        serializer = TitleMappingRuleSerializer(title_rules, many=True)
        
        return Response({
            'category_id': category.id,
            'category_name': category.name,
            'category_description': category.description,
            'channel_id': category.channel.id if category.channel else None,
            'channel_name': category.channel.name if category.channel else None,
            'title_mapping_rules': serializer.data,
            'count': len(serializer.data)
        }, status=status.HTTP_200_OK)
        
    except AudioUnrecognizedCategory.DoesNotExist:
        return Response({
            'error': 'Category not found or inactive'
        }, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response({
            'error': f'An error occurred: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
