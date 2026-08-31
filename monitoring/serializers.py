from rest_framework import serializers

from monitoring.models import AlertRecipient


class AlertRecipientSerializer(serializers.ModelSerializer):
    class Meta:
        model = AlertRecipient
        fields = ["id", "email", "name", "channel", "is_active", "created_at"]
        read_only_fields = ["id", "created_at"]
