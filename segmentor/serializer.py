from rest_framework import serializers
from acr_admin.models import Channel

from .models import AudioUnrecognizedCategory, TitleMappingRule


class AudioUnrecognizedCategorySerializer(serializers.ModelSerializer):
    channel_detail = serializers.SerializerMethodField()
    
    class Meta:
        model = AudioUnrecognizedCategory
        fields = [
            "id",
            "name",
            "description",
            "channel",
            "channel_detail",
            "is_active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at", "channel_detail"]
    
    def get_channel_detail(self, obj):
        """Return channel details for display purposes"""
        if obj.channel:
            return {
                "id": obj.channel.id,
                "name": obj.channel.name,
                "channel_id": obj.channel.channel_id,
                "project_id": obj.channel.project_id,
            }
        return None

    def validate(self, data):
        """Validate that name is unique per channel"""
        name = data.get('name')
        channel = data.get('channel')
        
        if name and channel:
            # Check if another category with the same name exists for the same channel
            existing_category = AudioUnrecognizedCategory.objects.filter(
                name=name,
                channel=channel
            ).exclude(pk=self.instance.pk if self.instance else None)
            
            if existing_category.exists():
                raise serializers.ValidationError({
                    'name': f'A category with this name already exists for channel "{channel.name or channel.channel_id}".'
                })
        
        return data


class TitleMappingRuleSerializer(serializers.ModelSerializer):
    category_detail = AudioUnrecognizedCategorySerializer(source="category", read_only=True)

    class Meta:
        model = TitleMappingRule
        fields = [
            "id",
            "category",
            "category_detail",
            "before_title",
            "after_title",
            "skip_transcription",
            "is_active",
            "notes",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at", "category_detail"]
    
    def validate(self, data):
        """Validate that before_title is unique per channel"""
        before_title = data.get('before_title')
        category = data.get('category')
        
        if before_title and category:
            # Check if another rule with the same before_title exists for the same channel
            existing_rule = TitleMappingRule.objects.filter(
                before_title=before_title,
                category__channel=category.channel
            ).exclude(pk=self.instance.pk if self.instance else None)
            
            if existing_rule.exists():
                raise serializers.ValidationError({
                    'before_title': f'A rule with this before_title already exists for channel "{category.channel.name or category.channel.channel_id}".'
                })
        
        return data

