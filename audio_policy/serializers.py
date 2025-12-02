from rest_framework import serializers

from .models import FlagCondition, ContentTypeDeactivationRule


class FlagConditionSerializer(serializers.ModelSerializer):
    """
    Serializer for CRUD operations on FlagCondition.
    """

    created_by = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = FlagCondition
        fields = [
            "id",
            "name",
            "channel",
            "transcription_keywords",
            "summary_keywords",
            "sentiment_min",
            "sentiment_max",
            "iab_topics",
            "bucket_prompt",
            "general_topics",
            "is_active",
            "created_by",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["created_at", "updated_at", "created_by"]

    def validate(self, attrs):
        """
        Validate sentiment values:
        - sentiment_min and sentiment_max must be between 0 and 100 (inclusive) if provided
        - sentiment_min must be less than or equal to sentiment_max when both are provided
        """
        sentiment_min = attrs.get("sentiment_min")
        sentiment_max = attrs.get("sentiment_max")
        
        if sentiment_min is not None:
            if sentiment_min < 0 or sentiment_min > 100:
                raise serializers.ValidationError({
                    "sentiment_min": "Sentiment minimum must be between 0 and 100 (inclusive)."
                })
        
        if sentiment_max is not None:
            if sentiment_max < 0 or sentiment_max > 100:
                raise serializers.ValidationError({
                    "sentiment_max": "Sentiment maximum must be between 0 and 100 (inclusive)."
                })
        
        # Validate that sentiment_min <= sentiment_max when both are provided
        if sentiment_min is not None and sentiment_max is not None:
            if sentiment_min > sentiment_max:
                raise serializers.ValidationError({
                    "sentiment_min": "Sentiment minimum must be less than or equal to sentiment maximum."
                })
        
        return attrs


class ContentTypeDeactivationRuleSerializer(serializers.ModelSerializer):
    """
    Serializer for CRUD operations on ContentTypeDeactivationRule.
    """

    class Meta:
        model = ContentTypeDeactivationRule
        fields = [
            "id",
            "channel",
            "content_type",
            "is_active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["created_at", "updated_at"]

