from rest_framework import serializers
from django.utils import timezone
from datetime import datetime
from acr_admin.models import WellnessBucket


def parse_datetime_string(value, field_name='datetime'):
    """
    Parse a datetime string to timezone-aware datetime object.
    
    Args:
        value: Datetime string in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        field_name: Name of the field for error messages (default: 'datetime')
    
    Returns:
        timezone-aware datetime object
    
    Raises:
        serializers.ValidationError: If the format is invalid
    """
    try:
        if 'T' in value:
            dt = timezone.make_aware(datetime.fromisoformat(value))
        elif ' ' in value:
            dt = timezone.make_aware(datetime.strptime(value, '%Y-%m-%d %H:%M:%S'))
        else:
            raise serializers.ValidationError(
                f'{field_name} must include time (use YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format)'
            )
        return dt
    except ValueError as e:
        raise serializers.ValidationError(f'Invalid {field_name} format: {str(e)}')


class SummaryQuerySerializer(serializers.Serializer):
    """
    Serializer for validating summary API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    shift_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs


class BucketCountQuerySerializer(serializers.Serializer):
    """
    Serializer for validating bucket count API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    shift_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs


class CategoryBucketCountQuerySerializer(serializers.Serializer):
    """
    Serializer for validating category bucket count API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    category_name = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    shift_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate_category_name(self, value):
        """
        Validate that category_name is one of the valid categories from WellnessBucket model
        """
        # Get valid categories from WellnessBucket.CATEGORY_CHOICES
        valid_categories = [choice[0] for choice in WellnessBucket.CATEGORY_CHOICES]
        if value.lower() not in valid_categories:
            raise serializers.ValidationError(
                f'category_name must be one of: {", ".join(valid_categories)}'
            )
        return value.lower()
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs


class TopicQuerySerializer(serializers.Serializer):
    """
    Serializer for validating topic API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    shift_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    show_all_topics = serializers.BooleanField(required=False, default=False)
    sort_by = serializers.ChoiceField(choices=['count', 'duration'], required=False, default='duration')
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs


class GeneralTopicCountByShiftQuerySerializer(serializers.Serializer):
    """
    Serializer for validating general topic count by shift API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    show_all_topics = serializers.BooleanField(required=False, default=False)
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs


class CSVExportQuerySerializer(serializers.Serializer):
    """
    Serializer for validating CSV export API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    shift_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs


class WordCountQuerySerializer(serializers.Serializer):
    """
    Serializer for validating word count API query parameters
    """
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    channel_id = serializers.IntegerField(required=True, min_value=1)
    shift_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    
    def validate_start_datetime(self, value):
        """
        Validate and parse start_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='start_datetime')
    
    def validate_end_datetime(self, value):
        """
        Validate and parse end_datetime string to timezone-aware datetime
        """
        return parse_datetime_string(value, field_name='end_datetime')
    
    def validate(self, attrs):
        """
        Validate that end_datetime is after start_datetime
        """
        start_dt = attrs.get('start_datetime')
        end_dt = attrs.get('end_datetime')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_datetime': 'end_datetime must be greater than start_datetime'
                })
        
        return attrs

