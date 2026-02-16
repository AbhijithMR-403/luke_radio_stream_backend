from rest_framework import serializers
from ..models import Channel
from config.validation import ValidationUtils


class ChannelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Channel
        fields = [
            'id',
            'name',
            'channel_id',
            'project_id',
            'timezone',
            'rss_url',
            'channel_type',
            'rss_start_date',
            'is_active',
            'created_at',
            'is_deleted',
            'is_default_settings',
        ]
        read_only_fields = [
            'id',
            'created_at',
            'is_deleted',
        ]

    def validate_timezone(self, value):
        ValidationUtils.validate_timezone(value)
        return value

    def validate(self, data):
        """
        Validate channel type specific requirements:
        - Podcast: requires rss_url, rss_start_date (can be null), no channel_id/project_id
        - Broadcast: requires channel_id/project_id, no rss_url/rss_start_date
        """
        # Get channel_type from data or existing instance (for updates)
        channel_type = data.get('channel_type')
        if channel_type is None and self.instance:
            channel_type = self.instance.channel_type
        
        # Only validate if channel_type is specified (either in data or instance)
        if not channel_type:
            return data
        
        if channel_type == 'podcast':
            # For creation, rss_url is required
            if not self.instance:  # This is a create operation
                if 'rss_url' not in data or not data.get('rss_url'):
                    raise serializers.ValidationError({
                        'rss_url': 'RSS URL is required for Podcast channels'
                    })
                
                # Ensure rss_start_date field is present (can be None/null)
                if 'rss_start_date' not in data:
                    raise serializers.ValidationError({
                        'rss_start_date': 'rss_start_date field is required for Podcast channels (can be null)'
                    })
            
            # Don't allow channel_id and project_id for podcast channels
            if 'channel_id' in data and data.get('channel_id') is not None:
                raise serializers.ValidationError({
                    'channel_id': 'channel_id is not allowed for Podcast channels'
                })
            
            if 'project_id' in data and data.get('project_id') is not None:
                raise serializers.ValidationError({
                    'project_id': 'project_id is not allowed for Podcast channels'
                })
        
        elif channel_type == 'broadcast':
            # For creation, channel_id and project_id are required
            if not self.instance:  # This is a create operation
                if 'channel_id' not in data or data.get('channel_id') is None:
                    raise serializers.ValidationError({
                        'channel_id': 'channel_id is required for Broadcast channels'
                    })
                
                if 'project_id' not in data or data.get('project_id') is None:
                    raise serializers.ValidationError({
                        'project_id': 'project_id is required for Broadcast channels'
                    })
            
            # Don't allow rss_url and rss_start_date for broadcast channels
            if 'rss_url' in data and data.get('rss_url'):
                raise serializers.ValidationError({
                    'rss_url': 'RSS URL is not allowed for Broadcast channels'
                })
            
            # if 'rss_start_date' in data and data.get('rss_start_date') is not None:
            #     raise serializers.ValidationError({
            #         'rss_start_date': 'RSS start date is not allowed for Broadcast channels'
            #     })
        
        return data


class ChannelPatchSerializer(serializers.Serializer):
    """
    Serializer specifically for PATCH operations.
    Only allows updating: name, is_active, timezone, rss_start_date
    """
    name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    is_active = serializers.BooleanField(required=False)
    timezone = serializers.CharField(max_length=50, required=False)
    rss_start_date = serializers.DateTimeField(required=False)

    def validate_timezone(self, value):
        if value:
            ValidationUtils.validate_timezone(value)
        return value

    def update(self, instance, validated_data):
        """
        Update and return an existing Channel instance.
        """
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        return instance


class SetChannelDefaultSettingsSerializer(serializers.Serializer):
    """Request body for POST /channels/default-settings."""
    channel_id = serializers.IntegerField(required=True)
    is_default_settings = serializers.BooleanField(required=True)
