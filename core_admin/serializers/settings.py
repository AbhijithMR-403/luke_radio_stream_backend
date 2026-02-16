import re
from rest_framework import serializers

from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from ..models import WellnessBucket, GeneralSetting
from ..utils import OpenAIUtils, ACRCloudUtils, RevAIUtils


# ============================================================
# WRITE SERIALIZERS (request validation)
# ============================================================

class GeneralSettingSerializer(serializers.Serializer):
    """
    Serializer for validating GeneralSetting fields in request payload
    """

    id = serializers.IntegerField(required=False, allow_null=True)

    # Auth keys
    openai_api_key = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    openai_org_id = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    acr_cloud_api_key = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    revai_access_token = serializers.CharField(required=False, allow_blank=True, allow_null=True)

    # Prompts
    summarize_transcript_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    sentiment_analysis_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    general_topics_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    iab_topics_prompt = serializers.CharField(required=False, allow_blank=True, allow_null=True)

    # Classification / GPT settings
    bucket_definition_error_rate = serializers.IntegerField(
        required=False, allow_null=True, min_value=0, max_value=100
    )
    chatgpt_model = serializers.CharField(
        max_length=100, required=False, allow_blank=True, allow_null=True
    )
    chatgpt_max_tokens = serializers.IntegerField(
        required=False, allow_null=True, min_value=0
    )
    chatgpt_temperature = serializers.FloatField(
        required=False, allow_null=True, min_value=0.0, max_value=2.0
    )
    chatgpt_top_p = serializers.FloatField(
        required=False, allow_null=True, min_value=0.0, max_value=1.0
    )
    determine_radio_content_type_prompt = serializers.CharField(
        required=False, allow_blank=True, allow_null=True
    )
    content_type_prompt = serializers.CharField(
        required=False, allow_blank=True, allow_null=True
    )
    radio_segment_error_rate = serializers.IntegerField(
        required=False, allow_null=True, min_value=0, max_value=100
    )
    channel_id = serializers.IntegerField(required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._openai_email = None
        self._validation_error_occurred = False

    # -----------------------------
    # API key validations
    # -----------------------------

    def validate_openai_api_key(self, value):
        if self._validation_error_occurred:
            return value

        if value and value.strip():
            result = OpenAIUtils.validate_api_key(value)
            if not result.get("is_valid"):
                self._validation_error_occurred = True
                raise serializers.ValidationError(
                    result.get("error_message") or 'Invalid OpenAI API key'
                )

            email = result.get("email")
            if email:
                self._openai_email = email

        return value

    def validate_acr_cloud_api_key(self, value):
        if self._validation_error_occurred:
            return value

        if value and value.strip():
            result = ACRCloudUtils.validate_api_key(value)
            if not result.get("is_valid"):
                self._validation_error_occurred = True
                raise serializers.ValidationError(
                    result.get("error_message") or 'Invalid ACR Cloud API key'
                )
        return value

    def validate_revai_access_token(self, value):
        if self._validation_error_occurred:
            return value

        if value and value.strip():
            result = RevAIUtils.validate_api_key(value)
            if not result.get("is_valid"):
                self._validation_error_occurred = True
                raise serializers.ValidationError(
                    result.get("error_message") or 'Invalid Rev.ai access token'
                )
        return value

    # -----------------------------
    # Prompt validations
    # -----------------------------

    def validate_content_type_prompt(self, value):
        """
        Normalize and validate comma-separated content types
        """
        if self._validation_error_occurred:
            return value

        if not value or not value.strip():
            return value

        items = [item.strip() for item in value.split(',') if item.strip()]
        if not items:
            raise serializers.ValidationError(
                'content_type_prompt must contain at least one value'
            )

        seen = set()
        duplicates = []
        for item in items:
            key = item.lower()
            if key in seen:
                duplicates.append(item)
            seen.add(key)

        if duplicates:
            raise serializers.ValidationError(
                f'Duplicate content types found: {", ".join(duplicates)}'
            )

        allowed_pattern = re.compile(r'^[A-Za-z0-9\s\-&]+$')
        invalid_items = [item for item in items if not allowed_pattern.match(item)]

        if invalid_items:
            raise serializers.ValidationError(
                'Invalid characters in content_type_prompt. '
                'Allowed: letters, numbers, spaces, hyphens, ampersand. '
                f'Invalid entries: {", ".join(invalid_items)}'
            )

        return ', '.join(items)

    def validate_determine_radio_content_type_prompt(self, value):
        """
        Validate radio segment classification prompt.
        Requires the {{segments}} placeholder and enforces
        instruction-based prompt structure.
        """
        if self._validation_error_occurred:
            return value

        if not value or not value.strip():
            return value

        prompt = value.strip()

        # Enforce required placeholder
        if "{{segments}}" not in prompt:
            raise serializers.ValidationError(
                "Prompt must contain the {{segments}} placeholder."
            )

        # Minimum length check (prevents CSV-only input)
        if len(prompt) < 50:
            raise serializers.ValidationError(
                "Prompt must be a descriptive instruction, not a short list."
            )

        # Require output definition
        if not re.search(r"\b(output|return)\b", prompt, re.IGNORECASE):
            raise serializers.ValidationError(
                "Prompt must explicitly define the expected output format."
            )

        return prompt

    # -----------------------------
    # Cross-field validation
    # -----------------------------

    def validate(self, attrs):
        # Always set openai_org_id from API key email if available
        if self._openai_email and 'openai_api_key' in attrs:
            attrs['openai_org_id'] = self._openai_email

        # Validate ChatGPT model if provided
        model = attrs.get('chatgpt_model')
        if model and model.strip():
            api_key = attrs.get('openai_api_key')

            if not api_key:
                active_setting = GeneralSetting.objects.filter(is_active=True).first()
                if active_setting:
                    api_key = active_setting.openai_api_key

            if not api_key:
                raise serializers.ValidationError({
                    'chatgpt_model': (
                        'OpenAI API key is required to validate the model. '
                        'Provide openai_api_key or ensure it exists in the database.'
                    )
                })

            result = OpenAIUtils.validate_model(model.strip(), api_key)
            if not result.get("is_valid"):
                raise serializers.ValidationError({
                    'chatgpt_model': result.get("error_message") or f'Invalid model: {model}'
                })

        return attrs


class WellnessBucketSerializer(serializers.Serializer):
    """
    Serializer for validating WellnessBucket in request payload
    """

    id = serializers.IntegerField(required=False, allow_null=True)
    title = serializers.CharField(max_length=255, required=False)
    description = serializers.CharField(required=False)
    category = serializers.ChoiceField(
        choices=WellnessBucket.CATEGORY_CHOICES,
        required=False
    )
    is_deleted = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        bucket_id = attrs.get('id')
        is_deleted = attrs.get('is_deleted', False)

        if is_deleted:
            if not bucket_id:
                raise serializers.ValidationError(
                    {'id': 'id is required when deleting a bucket'}
                )

            for field in ('title', 'description', 'category'):
                if field in attrs:
                    raise serializers.ValidationError({
                        field: f'Cannot update {field} when is_deleted is true'
                    })

            return attrs

        if not bucket_id:
            for field in ('title', 'description', 'category'):
                if not attrs.get(field):
                    raise serializers.ValidationError({
                        field: f'{field} is required for new buckets'
                    })

        return attrs


class RevertToVersionSerializer(serializers.Serializer):
    """Serializer for revert-to-version request: channel_id and target_version_number."""

    channel_id = serializers.IntegerField(required=True)
    target_version_number = serializers.IntegerField(required=True, min_value=1)


class SettingsAndBucketsSerializer(serializers.Serializer):
    """
    Serializer for validating settings + buckets payload
    """

    settings = GeneralSettingSerializer(required=False, allow_null=True)
    buckets = WellnessBucketSerializer(many=True, required=False, allow_empty=True)
    change_reason = serializers.CharField(
        required=False, allow_blank=True, allow_null=True, max_length=500
    )

    def validate_settings(self, value):
        return value or {}

    def validate_buckets(self, value):
        if value and len(value) > 20:
            raise serializers.ValidationError('You can only have 20 buckets')
        return value


# ============================================================
# READ SERIALIZERS (response only)
# ============================================================

class GeneralSettingResponseSerializer(serializers.ModelSerializer):
    bucket_prompt = serializers.SerializerMethodField()

    class Meta:
        model = GeneralSetting
        fields = [
            'id',
            'openai_api_key',
            'openai_org_id',
            'acr_cloud_api_key',
            'revai_access_token',
            'summarize_transcript_prompt',
            'sentiment_analysis_prompt',
            'general_topics_prompt',
            'iab_topics_prompt',
            'bucket_prompt',
            'bucket_definition_error_rate',
            'chatgpt_model',
            'chatgpt_max_tokens',
            'chatgpt_temperature',
            'chatgpt_top_p',
            'determine_radio_content_type_prompt',
            'content_type_prompt',
            'radio_segment_error_rate',
            'version',
            'is_active',
            'created_by',
            'created_at',
            'change_reason',
            'parent_version',
            'channel_id',
        ]
        read_only_fields = fields

    def get_bucket_prompt(self, obj):
        return TranscriptionAnalyzer.get_bucket_prompt(obj.channel_id)


class WellnessBucketResponseSerializer(serializers.ModelSerializer):
    class Meta:
        model = WellnessBucket
        fields = ['id', 'title', 'description', 'category', 'is_deleted']
        read_only_fields = fields


class SettingsAndBucketsResponseSerializer(serializers.Serializer):
    settings = GeneralSettingResponseSerializer(required=False, allow_null=True)
    buckets = WellnessBucketResponseSerializer(many=True, required=False)
