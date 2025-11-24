from rest_framework import serializers

from .models import FlagCondition


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

