from rest_framework import serializers

from .models import Prompt


class PromptSerializer(serializers.ModelSerializer):
    class Meta:
        model = Prompt
        fields = ("id", "name", "content", "is_active", "created_at")
        read_only_fields = ("id", "created_at")
