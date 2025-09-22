from rest_framework import serializers
from .models import RadioUser, UserChannelAssignment, MagicLink
from acr_admin.models import Channel

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = RadioUser
        fields = ['id', 'email', 'name', 'password_set']

class AdminCreateUserSerializer(serializers.Serializer):
    email = serializers.EmailField()
    name = serializers.CharField(max_length=255)

class MagicLinkVerificationSerializer(serializers.Serializer):
    token = serializers.CharField(max_length=64, min_length=64)

class PasswordSetupSerializer(serializers.Serializer):
    token = serializers.CharField(max_length=64, min_length=64)
    password = serializers.CharField(min_length=8)

class LoginSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField()

class ChannelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Channel
        fields = ['id', 'name', 'channel_id', 'project_id']

class UserChannelAssignmentSerializer(serializers.ModelSerializer):
    channel = ChannelSerializer()
    class Meta:
        model = UserChannelAssignment
        fields = ['channel', 'assigned_at']

class AssignChannelSerializer(serializers.Serializer):
    user_id = serializers.IntegerField()
    channel_id = serializers.IntegerField()
