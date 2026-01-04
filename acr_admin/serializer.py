from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from .models import WellnessBucket, GeneralSetting
from .utils import OpenAIUtils, ACRCloudUtils, RevAIUtils
from rest_framework import serializers
import re

def channel_to_dict(channel):
    return {
        'id': channel.id,
        'name': channel.name,
        'channel_id': channel.channel_id,
        'project_id': channel.project_id,
        'timezone': channel.timezone,
        'created_at': channel.created_at.isoformat() if channel.created_at else None,
        'is_deleted': channel.is_deleted,
    }

def general_setting_to_dict(settings):
    return {
        'id': settings.id,
        'openai_api_key': settings.openai_api_key,
        'openai_org_id': settings.openai_org_id,
        'acr_cloud_api_key': settings.acr_cloud_api_key,
        'revai_access_token': settings.revai_access_token,
        'summarize_transcript_prompt': settings.summarize_transcript_prompt,
        'sentiment_analysis_prompt': settings.sentiment_analysis_prompt,
        'general_topics_prompt': settings.general_topics_prompt,
        'iab_topics_prompt': settings.iab_topics_prompt,
        'created_at': settings.created_at.isoformat() if settings.created_at else None,
        'bucket_prompt': TranscriptionAnalyzer.get_bucket_prompt(),
        'bucket_definition_error_rate': settings.bucket_definition_error_rate,
        'chatgpt_model': settings.chatgpt_model,
        'chatgpt_max_tokens': settings.chatgpt_max_tokens,
        'chatgpt_temperature': settings.chatgpt_temperature,
        'chatgpt_top_p': settings.chatgpt_top_p,
        'determine_radio_content_type_prompt': settings.determine_radio_content_type_prompt,
        'content_type_prompt': settings.content_type_prompt,
        'radio_segment_error_rate': settings.radio_segment_error_rate,
    }

def wellness_bucket_to_dict(bucket):
    return {
        'id': bucket.id,
        'title': bucket.title,
        'description': bucket.description,
        'category': bucket.category,
        'is_deleted': bucket.is_deleted,
    }


class GeneralSettingSerializer(serializers.Serializer):
    """Serializer for validating GeneralSetting fields in request payload"""
    id = serializers.IntegerField(required=False, allow_null=True)
    openai_api_key = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    openai_org_id = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    acr_cloud_api_key = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    revai_access_token = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    summarize_transcript_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    sentiment_analysis_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    general_topics_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    iab_topics_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    bucket_definition_error_rate = serializers.IntegerField(required=False, allow_null=True, min_value=0, max_value=100)
    chatgpt_model = serializers.CharField(max_length=100, required=False, allow_blank=True, allow_null=True)
    chatgpt_max_tokens = serializers.IntegerField(required=False, allow_null=True, min_value=0)
    chatgpt_temperature = serializers.FloatField(required=False, allow_null=True, min_value=0.0, max_value=2.0)
    chatgpt_top_p = serializers.FloatField(required=False, allow_null=True, min_value=0.0, max_value=1.0)
    determine_radio_content_type_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    content_type_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    radio_segment_error_rate = serializers.IntegerField(required=False, allow_null=True, min_value=0, max_value=100)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._openai_email = None
        self._validation_error_occurred = False

    def validate_openai_api_key(self, value):
        """Validate OpenAI API key if provided and extract email for openai_org_id"""
        if self._validation_error_occurred:
            return value
        if value and value.strip():
            is_valid, error_message, email = OpenAIUtils.validate_api_key(value)
            if not is_valid:
                self._validation_error_occurred = True
                raise serializers.ValidationError(error_message or 'Invalid OpenAI API key')
            # Store email in instance variable for later use in validate method
            if email:
                self._openai_email = email
        return value
    
    def validate_acr_cloud_api_key(self, value):
        """Validate ACR Cloud API key if provided"""
        if self._validation_error_occurred:
            return value
        if value and value.strip():
            is_valid, error_message = ACRCloudUtils.validate_api_key(value)
            if not is_valid:
                self._validation_error_occurred = True
                raise serializers.ValidationError(error_message or 'Invalid ACR Cloud API key')
        return value
    
    def validate_revai_access_token(self, value):
        """Validate Rev.ai access token if provided"""
        if self._validation_error_occurred:
            return value
        if value and value.strip():
            is_valid, error_message = RevAIUtils.validate_api_key(value)
            if not is_valid:
                self._validation_error_occurred = True
                raise serializers.ValidationError(error_message or 'Invalid Rev.ai access token')
        return value
    
    def validate_content_type_prompt(self, value):
        """Validate and normalize content_type_prompt to ensure comma-separated values"""
        if self._validation_error_occurred:
            return value
        if value and value.strip():
            # Split by comma and clean up each item
            items = [item.strip() for item in value.split(',') if item.strip()]
            if not items:
                raise serializers.ValidationError('content_type_prompt must contain at least one value separated by commas')
            
            # Check for duplicates (case-insensitive)
            seen = set()
            duplicates = []
            for item in items:
                item_lower = item.lower()
                if item_lower in seen:
                    duplicates.append(item)
                seen.add(item_lower)
            
            if duplicates:
                raise serializers.ValidationError(
                    f'content_type_prompt contains duplicate entries: {", ".join(duplicates)}'
                )
            
            # Validate allowed characters: A-Z, a-z, 0-9, spaces, commas, hyphens, ampersand
            # Pattern allows: letters, numbers, spaces, hyphens, ampersand
            allowed_pattern = re.compile(r'^[A-Za-z0-9\s\-&]+$')
            invalid_items = []
            for item in items:
                if not allowed_pattern.match(item):
                    invalid_items.append(item)
            
            if invalid_items:
                raise serializers.ValidationError(
                    f'content_type_prompt contains invalid characters. Only letters (A-Z, a-z), numbers (0-9), spaces, commas, hyphens, and ampersand are allowed. Invalid entries: {", ".join(invalid_items)}'
                )
            
            # Return normalized version with proper comma separation
            return ', '.join(items)
        return value
    
    def validate(self, attrs):
        """Set openai_org_id from email if API key was validated, and validate chatgpt_model"""
        # If we have an email from API key validation, always update openai_org_id with it
        if self._openai_email and 'openai_api_key' in attrs:
            attrs['openai_org_id'] = self._openai_email
        
        # Validate chatgpt_model if provided
        if 'chatgpt_model' in attrs and attrs.get('chatgpt_model'):
            model = attrs['chatgpt_model'].strip()
            if model:
                # Get API key: use from request if provided, otherwise from database
                api_key = None
                
                # First, try to get from validated attrs (if provided in request)
                if 'openai_api_key' in attrs and attrs.get('openai_api_key'):
                    api_key = attrs['openai_api_key']
                
                # If not in request, try to get from database (active GeneralSetting)
                if not api_key:
                    try:
                        active_setting = GeneralSetting.objects.filter(is_active=True).first()
                        if active_setting and active_setting.openai_api_key:
                            api_key = active_setting.openai_api_key
                    except Exception:
                        pass  # If database query fails, we'll handle it below
                
                if not api_key:
                    raise serializers.ValidationError({
                        'chatgpt_model': 'OpenAI API key is required to validate the model. Please provide openai_api_key in the request or ensure it exists in the database.'
                    })
                
                # Validate the model using the API
                is_valid, error_message = OpenAIUtils.validate_model(model, api_key)
                if not is_valid:
                    self._validation_error_occurred = True
                    raise serializers.ValidationError({
                        'chatgpt_model': error_message or f'Invalid or unavailable model: {model}'
                    })
        
        return attrs


class WellnessBucketSerializer(serializers.Serializer):
    """Serializer for validating WellnessBucket in request payload"""
    id = serializers.IntegerField(required=False, allow_null=True)
    title = serializers.CharField(max_length=255, required=False)
    description = serializers.CharField(required=False)
    category = serializers.ChoiceField(
        choices=WellnessBucket.CATEGORY_CHOICES,
        required=False
    )
    is_deleted = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        """Validate that required fields are present and prevent mutations on delete-only updates"""
        bucket_id = attrs.get('id')
        is_deleted = attrs.get('is_deleted', False)
        
        # If deleting, only id and is_deleted should be present
        if is_deleted:
            if bucket_id:
                # Check for any other fields that shouldn't be present when deleting
                disallowed_fields = ['title', 'description', 'category']
                for field in disallowed_fields:
                    if field in attrs:
                        raise serializers.ValidationError(
                            {field: f'Cannot update {field} when is_deleted is true'}
                        )
                return attrs
            else:
                raise serializers.ValidationError(
                    {'id': 'id is required when is_deleted is true'}
                )
        
        # For new buckets (no id), title, description, and category are required
        if not bucket_id:
            if not attrs.get('title'):
                raise serializers.ValidationError({'title': 'Title is required for new buckets'})
            if not attrs.get('description'):
                raise serializers.ValidationError({'description': 'Description is required for new buckets'})
            if not attrs.get('category'):
                raise serializers.ValidationError({'category': 'Category is required for new buckets'})
        
        return attrs


class SettingsAndBucketsSerializer(serializers.Serializer):
    """Serializer for validating settings and buckets request payload"""
    settings = GeneralSettingSerializer(required=False, allow_null=True)
    buckets = WellnessBucketSerializer(many=True, required=False, allow_empty=True)
    change_reason = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=500)

    def validate_settings(self, value):
        """Handle None or empty dict for settings"""
        if value is None:
            return {}
        return value

    def validate_buckets(self, value):
        """Validate buckets array"""
        if value and len(value) > 20:
            raise serializers.ValidationError('You can only have 20 buckets')
        return value


class GeneralSettingResponseSerializer(serializers.ModelSerializer):
    """Serializer for GeneralSetting response (read-only)"""
    bucket_prompt = serializers.SerializerMethodField()

    class Meta:
        model = GeneralSetting
        fields = [
            'id', 'openai_api_key', 'openai_org_id', 'acr_cloud_api_key', 'revai_access_token',
            'summarize_transcript_prompt', 'sentiment_analysis_prompt', 'general_topics_prompt',
            'iab_topics_prompt', 'bucket_prompt', 'bucket_definition_error_rate',
            'chatgpt_model', 'chatgpt_max_tokens', 'chatgpt_temperature', 'chatgpt_top_p',
            'determine_radio_content_type_prompt', 'content_type_prompt', 'radio_segment_error_rate'
        ]
        read_only_fields = fields

    def get_bucket_prompt(self, obj):
        """Get bucket prompt from TranscriptionAnalyzer"""
        return TranscriptionAnalyzer.get_bucket_prompt()


class WellnessBucketResponseSerializer(serializers.ModelSerializer):
    """Serializer for WellnessBucket response (read-only)"""
    
    class Meta:
        model = WellnessBucket
        fields = ['id', 'title', 'description', 'category', 'is_deleted']
        read_only_fields = fields


class SettingsAndBucketsResponseSerializer(serializers.Serializer):
    """Serializer for SettingsAndBuckets GET response"""
    settings = GeneralSettingResponseSerializer(required=False, allow_null=True)
    buckets = WellnessBucketResponseSerializer(many=True, required=False)
