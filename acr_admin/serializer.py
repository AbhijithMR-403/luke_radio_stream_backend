from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from .models import WellnessBucket
from .utils import OpenAIUtils, ACRCloudUtils, RevAIUtils
from rest_framework import serializers

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
        'updated_at': settings.updated_at.isoformat() if settings.updated_at else None,
        'bucket_prompt': TranscriptionAnalyzer.get_bucket_prompt(),
        'bucket_definition_error_rate': settings.bucket_definition_error_rate,
        'chatgpt_model': settings.chatgpt_model,
        'chatgpt_max_tokens': settings.chatgpt_max_tokens,
        'chatgpt_temperature': settings.chatgpt_temperature,
        'chatgpt_top_p': settings.chatgpt_top_p,
        'chatgpt_frequency_penalty': settings.chatgpt_frequency_penalty,
        'chatgpt_presence_penalty': settings.chatgpt_presence_penalty,
        'determine_radio_content_type_prompt': settings.determine_radio_content_type_prompt,
        'content_type_prompt': settings.content_type_prompt,
        'radio_segment_types': settings.radio_segment_types,
        'radio_segment_error_rate': settings.radio_segment_error_rate,
    }

def wellness_bucket_to_dict(bucket):
    return {
        'id': bucket.id,
        'title': bucket.title,
        'description': bucket.description,
        'category': bucket.category,
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
    chatgpt_frequency_penalty = serializers.FloatField(required=False, allow_null=True, min_value=-2.0, max_value=2.0)
    chatgpt_presence_penalty = serializers.FloatField(required=False, allow_null=True, min_value=-2.0, max_value=2.0)
    determine_radio_content_type_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    content_type_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    radio_segment_types = serializers.CharField(required=False, allow_blank=True, allow_null=True)
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
    
    def validate(self, attrs):
        """Set openai_org_id from email if API key was validated"""
        # If we have an email from API key validation, always update openai_org_id with it
        if self._openai_email and 'openai_api_key' in attrs:
            attrs['openai_org_id'] = self._openai_email
        return attrs


class WellnessBucketSerializer(serializers.Serializer):
    """Serializer for validating WellnessBucket in request payload"""
    id = serializers.IntegerField(required=False, allow_null=True)
    title = serializers.CharField(max_length=255, required=True)
    description = serializers.CharField(required=True)
    category = serializers.ChoiceField(
        choices=WellnessBucket.CATEGORY_CHOICES,
        required=True
    )

    def validate_category(self, value):
        """Validate category is one of the allowed choices"""
        if value not in [choice[0] for choice in WellnessBucket.CATEGORY_CHOICES]:
            raise serializers.ValidationError(
                f"Category must be one of: {', '.join([choice[0] for choice in WellnessBucket.CATEGORY_CHOICES])}"
            )
        return value


class SettingsAndBucketsSerializer(serializers.Serializer):
    """Serializer for validating settings and buckets request payload"""
    settings = GeneralSettingSerializer(required=False, allow_null=True)
    buckets = WellnessBucketSerializer(many=True, required=False, allow_empty=True)

    def validate_settings(self, value):
        """Handle None or empty dict for settings"""
        if value is None:
            return {}
        return value

    def validate_buckets(self, value):
        """Validate buckets array"""
        if value and len(value) > 20:
            raise serializers.ValidationError('You can only have 20 buckets')
        # Category validation is handled by WellnessBucketSerializer (required=True)
        return value
