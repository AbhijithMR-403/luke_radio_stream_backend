from rest_framework import serializers
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from .models import RadioUser, UserChannelAssignment, MagicLink
from acr_admin.models import Channel

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = RadioUser
        fields = ['id', 'email', 'name', 'password_set', 'is_admin']

class AdminCreateUserSerializer(serializers.Serializer):
    email = serializers.EmailField()
    name = serializers.CharField(max_length=255)

class MagicLinkVerificationSerializer(serializers.Serializer):
    token = serializers.CharField(max_length=64, min_length=64)

class PasswordSetupSerializer(serializers.Serializer):
    token = serializers.CharField(max_length=64, min_length=64)
    password = serializers.CharField(min_length=8)

class ChannelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Channel
        fields = ['id', 'name', 'channel_id', 'project_id', 'timezone']

class UserChannelAssignmentSerializer(serializers.ModelSerializer):
    channel = ChannelSerializer()
    class Meta:
        model = UserChannelAssignment
        fields = ['channel', 'assigned_at']

class AssignChannelSerializer(serializers.Serializer):
    user_id = serializers.IntegerField()
    channel_id = serializers.IntegerField()

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        # Add custom claims
        token['email'] = user.email
        token['name'] = user.name
        token['is_admin'] = user.is_admin
        token['password_set'] = user.password_set
        return token

    def validate(self, attrs):
        data = super().validate(attrs)
        # Add user information to response
        data['user'] = UserSerializer(self.user).data
        return data
