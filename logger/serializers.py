from rest_framework import serializers

from data_analysis.models import AudioSegments, RevTranscriptionJob
from acr_admin.models import Channel
from logger.models import AudioSegmentEditLog


class AudioSegmentReferenceSerializer(serializers.ModelSerializer):
    """Lightweight serializer for related audio segments."""

    class Meta:
        model = AudioSegments
        fields = [
            "id",
            "start_time",
            "end_time",
            "duration_seconds",
            "title",
            "title_before",
            "title_after",
            "is_recognized",
            "source",
        ]


class AudioSegmentEditLogSerializer(serializers.ModelSerializer):
    """Serializer for exposing edit log history for an audio segment."""

    audio_segment = AudioSegmentReferenceSerializer(read_only=True)
    affected_segments = AudioSegmentReferenceSerializer(read_only=True, many=True)
    user = serializers.SerializerMethodField()

    class Meta:
        model = AudioSegmentEditLog
        fields = [
            "id",
            "audio_segment",
            "affected_segments",
            "action",
            "trigger_type",
            "user",
            "metadata",
            "notes",
            "created_at",
        ]
        read_only_fields = fields

    def get_user(self, obj):
        if not obj.user:
            return None
        return {
            "id": obj.user.id,
            "email": obj.user.email,
            "name": getattr(obj.user, "name", None),
        }


class RevTranscriptionJobReferenceSerializer(serializers.ModelSerializer):
    """Lightweight serializer for related Rev transcription job."""
    
    class Meta:
        model = RevTranscriptionJob
        fields = [
            "id",
            "job_id",
            "job_name",
            "status",
            "duration_seconds",
            "created_on",
            "completed_on",
        ]


class ChannelReferenceSerializer(serializers.ModelSerializer):
    """Lightweight serializer for related channel."""
    
    class Meta:
        model = Channel
        fields = [
            "id",
            "name",
        ]


class ChannelDurationStatSerializer(serializers.Serializer):
    """Serializer for channel duration statistics."""
    channel_id = serializers.IntegerField()
    channel_name = serializers.CharField()
    total_duration_seconds = serializers.FloatField()


class DurationStatisticsSerializer(serializers.Serializer):
    """Serializer for duration statistics response."""
    channels = ChannelDurationStatSerializer(many=True)
    grand_total = serializers.FloatField()

