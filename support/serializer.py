from typing import List

from django.db import transaction
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


