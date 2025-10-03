from rest_framework import serializers

from .models import AudioUnrecognizedCategory, TitleMappingRule


class AudioUnrecognizedCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = AudioUnrecognizedCategory
        fields = [
            "id",
            "name",
            "description",
            "is_active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class TitleMappingRuleSerializer(serializers.ModelSerializer):
    category_detail = AudioUnrecognizedCategorySerializer(source="category", read_only=True)

    class Meta:
        model = TitleMappingRule
        fields = [
            "id",
            "category",
            "category_detail",
            "before_title",
            "skip_transcription",
            "is_active",
            "notes",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at", "category_detail"]

