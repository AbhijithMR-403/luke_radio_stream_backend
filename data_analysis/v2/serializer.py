"""
Serializers for data_analysis v2 API endpoints.
Used for request parameter validation.
"""

from rest_framework import serializers
from django.utils import timezone
from datetime import datetime


class ListAudioSegmentsV2QuerySerializer(serializers.Serializer):
    """
    Serializer for validating query parameters for ListAudioSegmentsV2View.
    """
    channel_id = serializers.IntegerField(required=True, help_text="Channel ID to filter segments")
    start_datetime = serializers.CharField(required=True, help_text="Start datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)")
    end_datetime = serializers.CharField(required=True, help_text="End datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)")
    status = serializers.ChoiceField(
        choices=['active', 'inactive', 'both'],
        default='both',
        help_text="Filter by active status - 'active', 'inactive', or 'both'"
    )
    shift_id = serializers.IntegerField(required=False, allow_null=True, help_text="Shift ID to filter segments by shift time windows")
    predefined_filter_id = serializers.IntegerField(required=False, allow_null=True, help_text="PredefinedFilter ID to filter segments by filter schedule time windows")
    content_type = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True,
        help_text="List of strings to filter by content_type_prompt. Can be passed multiple times."
    )
    page = serializers.IntegerField(default=1, min_value=1, help_text="Page number")
    page_size = serializers.IntegerField(default=1, min_value=1, help_text="Hours per page")
    
    def validate(self, attrs):
        """
        Validate the combined attributes.
        """
        # Validate that only one filtering mechanism is used at a time
        if attrs.get('shift_id') and attrs.get('predefined_filter_id'):
            raise serializers.ValidationError({
                'non_field_errors': ['Cannot use both shift_id and predefined_filter_id simultaneously']
            })
        
        # Parse and validate datetime parameters
        start_datetime_str = attrs.get('start_datetime')
        end_datetime_str = attrs.get('end_datetime')
        
        if start_datetime_str:
            start_dt = self._parse_datetime(start_datetime_str)
            if not start_dt:
                raise serializers.ValidationError({
                    'start_datetime': ['Invalid format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS']
                })
            attrs['base_start_dt'] = start_dt
        
        if end_datetime_str:
            end_dt = self._parse_datetime(end_datetime_str)
            if not end_dt:
                raise serializers.ValidationError({
                    'end_datetime': ['Invalid format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS']
                })
            attrs['base_end_dt'] = end_dt
        
        # Validate that end_datetime is after start_datetime
        if 'base_start_dt' in attrs and 'base_end_dt' in attrs:
            if attrs['base_end_dt'] <= attrs['base_start_dt']:
                raise serializers.ValidationError({
                    'end_datetime': ['end_datetime must be after start_datetime']
                })
        
        # Convert status to boolean or None
        status_value = attrs.get('status', 'both')
        if status_value == 'active':
            attrs['status'] = True
        elif status_value == 'inactive':
            attrs['status'] = False
        else:  # 'both'
            attrs['status'] = None
        
        return attrs
    
    def _parse_datetime(self, value):
        """
        Parse datetime string to timezone-aware datetime object.
        
        Args:
            value: String datetime in ISO format or YYYY-MM-DD HH:MM:SS format
            
        Returns:
            timezone-aware datetime object or None if invalid
        """
        if not isinstance(value, str):
            return None
        
        try:
            if 'T' in value:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None
        
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt)
        
        return dt

