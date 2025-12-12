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
            "channel",
            "transcription_keywords",
            "summary_keywords",
            "sentiment_min_lower",
            "sentiment_min_upper",
            "sentiment_max_lower",
            "sentiment_max_upper",
            "target_sentiments",
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
        - All sentiment fields must be between 0 and 100 (inclusive) if provided
        - sentiment_min_lower <= sentiment_min_upper (when both are provided)
        - sentiment_max_lower <= sentiment_max_upper (when both are provided)
        """
        min_lower = attrs.get("sentiment_min_lower")
        min_upper = attrs.get("sentiment_min_upper")
        max_lower = attrs.get("sentiment_max_lower")
        max_upper = attrs.get("sentiment_max_upper")
        
        # Validate each field is in range 0-100 if provided
        for field_name, value in [
            ("sentiment_min_lower", min_lower),
            ("sentiment_min_upper", min_upper),
            ("sentiment_max_lower", max_lower),
            ("sentiment_max_upper", max_upper),
        ]:
            if value is not None:
                if value < 0 or value > 100:
                    raise serializers.ValidationError({
                        field_name: f"{field_name.replace('_', ' ').title()} must be between 0 and 100 (inclusive)."
                    })
        
        # Validate min_lower <= min_upper (when both are provided)
        if min_lower is not None and min_upper is not None:
            if min_lower > min_upper:
                raise serializers.ValidationError({
                    "sentiment_min_lower": "Sentiment min lower must be less than or equal to sentiment min upper."
                })
        
        # Validate max_lower <= max_upper (when both are provided)
        if max_lower is not None and max_upper is not None:
            if max_lower > max_upper:
                raise serializers.ValidationError({
                    "sentiment_max_lower": "Sentiment max lower must be less than or equal to sentiment max upper."
                })
        
        # Validate target_sentiments if provided
        target_sentiments = attrs.get("target_sentiments")
        if target_sentiments is not None:
            if not isinstance(target_sentiments, int):
                raise serializers.ValidationError({
                    "target_sentiments": "Target sentiments must be an integer."
                })
            if target_sentiments < 0 or target_sentiments > 100:
                raise serializers.ValidationError({
                    "target_sentiments": "Target sentiments must be between 0 and 100 (inclusive)."
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

