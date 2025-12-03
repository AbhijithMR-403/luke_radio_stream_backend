from typing import List
from django.db import transaction
from django.utils import timezone
from datetime import datetime
from rest_framework import serializers

from .models import SupportTicket, SupportTicketImage, SupportTicketResponse


class SupportTicketImageSerializer(serializers.ModelSerializer):
    class Meta:
        model = SupportTicketImage
        fields = ["id", "image", "created_at"]
        read_only_fields = ["id", "created_at"]


class SupportTicketResponseSerializer(serializers.ModelSerializer):
    responder_name = serializers.SerializerMethodField()
    responder_email = serializers.SerializerMethodField()

    class Meta:
        model = SupportTicketResponse
        fields = ["id", "message", "created_at", "responder", "responder_name", "responder_email"]
        read_only_fields = ["id", "created_at", "responder", "responder_name", "responder_email"]

    def get_responder_name(self, obj):
        return getattr(obj.responder, "name", getattr(obj.responder, "email", str(obj.responder)))

    def get_responder_email(self, obj):
        return getattr(obj.responder, "email", None)

class SupportTicketSerializer(serializers.ModelSerializer):
    images = SupportTicketImageSerializer(many=True, read_only=True)
    responses = SupportTicketResponseSerializer(many=True, read_only=True)
    upload_images = serializers.ListField(
        child=serializers.ImageField(allow_empty_file=False, use_url=True),
        write_only=True,
        required=False,
        allow_empty=True,
    )

    class Meta:
        model = SupportTicket
        fields = [
            "id",
            "subject",
            "description",
            "created_at",
            "updated_at",
            "images",
            "responses",
            "upload_images",
        ]
        read_only_fields = ["id", "created_at", "updated_at", "images", "responses"]

    def validate_upload_images(self, value: List) -> List:
        if value and len(value) > 8:
            raise serializers.ValidationError("You can upload up to 8 images.")
        return value

    def create(self, validated_data):
        upload_images = validated_data.pop("upload_images", [])
        user = self.context["request"].user
        with transaction.atomic():
            ticket = SupportTicket.objects.create(user=user, **validated_data)
            for img in upload_images[:8]:
                SupportTicketImage.objects.create(ticket=ticket, image=img)
        return ticket


def parse_datetime_string(value, field_name='datetime'):
    """
    Parse a datetime string to timezone-aware datetime object.
    
    Args:
        value: Datetime string in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        field_name: Name of the field for error messages (default: 'datetime')
    
    Returns:
        timezone-aware datetime object
    
    Raises:
        serializers.ValidationError: If the format is invalid
    """
    try:
        if 'T' in value:
            dt = timezone.make_aware(datetime.fromisoformat(value))
        elif ' ' in value:
            dt = timezone.make_aware(datetime.strptime(value, '%Y-%m-%d %H:%M:%S'))
        else:
            raise serializers.ValidationError(
                f'{field_name} must include time (use YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format)'
            )
        return dt
    except ValueError as e:
        raise serializers.ValidationError(f'Invalid {field_name} format: {str(e)}')


class TranscribedAudioQuerySerializer(serializers.Serializer):
    """
    Serializer for validating transcribed audio API query parameters
    """
    start_date = serializers.CharField(required=False, allow_blank=True)
    end_date = serializers.CharField(required=False, allow_blank=True)
    channel_id = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    
    def validate_start_date(self, value):
        """
        Validate and parse start_date string to timezone-aware datetime
        Supports both date (YYYY-MM-DD) and datetime formats
        """
        if not value:
            return None
        try:
            # Try date format first (YYYY-MM-DD)
            if len(value) == 10 and '-' in value:
                dt = datetime.strptime(value, '%Y-%m-%d')
                dt = timezone.make_aware(datetime.combine(dt.date(), datetime.min.time()))
                return dt
            # Try datetime format
            return parse_datetime_string(value, field_name='start_date')
        except ValueError as e:
            raise serializers.ValidationError(f'Invalid start_date format: {str(e)}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format')
    
    def validate_end_date(self, value):
        """
        Validate and parse end_date string to timezone-aware datetime
        Supports both date (YYYY-MM-DD) and datetime formats
        """
        if not value:
            return None
        try:
            # Try date format first (YYYY-MM-DD)
            if len(value) == 10 and '-' in value:
                dt = datetime.strptime(value, '%Y-%m-%d')
                # Set to end of day
                dt = timezone.make_aware(datetime.combine(dt.date(), datetime.max.time()))
                return dt
            # Try datetime format
            return parse_datetime_string(value, field_name='end_date')
        except ValueError as e:
            raise serializers.ValidationError(f'Invalid end_date format: {str(e)}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format')
    
    def validate(self, attrs):
        """
        Validate that end_date is after start_date if both are provided
        """
        start_dt = attrs.get('start_date')
        end_dt = attrs.get('end_date')
        
        if start_dt and end_dt:
            if end_dt <= start_dt:
                raise serializers.ValidationError({
                    'end_date': 'end_date must be greater than start_date'
                })
        
        return attrs

