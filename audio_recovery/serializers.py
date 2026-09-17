from rest_framework import serializers

from core_admin.models import Channel
from data_analysis.models import AudioSegments
from audio_recovery.models import RecoveredAudioFile


class RecoverAudioSerializer(serializers.Serializer):
    url = serializers.URLField(help_text="Direct-download audio link (e.g. Dropbox link ending in ?dl=1).")
    channel_id = serializers.PrimaryKeyRelatedField(queryset=Channel.objects.all(), source="channel")
    recorded_at = serializers.DateTimeField(help_text="Real UTC start time of the source recording.")

    def validate_url(self, value):
        active_statuses = [
            RecoveredAudioFile.Status.PENDING,
            RecoveredAudioFile.Status.RUNNING,
            RecoveredAudioFile.Status.SUCCESS,
        ]
        existing = (
            RecoveredAudioFile.objects.filter(source_url=value, status__in=active_statuses)
            .order_by("-created_at")
            .first()
        )
        if existing:
            raise serializers.ValidationError(
                f"This URL already has a recovery job (id={existing.id}) with status '{existing.get_status_display()}'."
            )
        return value


class RecoveredSegmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = AudioSegments
        fields = [
            "id", "start_time", "end_time", "duration_seconds",
            "file_name", "file_path", "is_recognized", "title",
        ]


class RecoveredAudioFileSerializer(serializers.ModelSerializer):
    segments = serializers.SerializerMethodField()

    class Meta:
        model = RecoveredAudioFile
        fields = [
            "id", "status", "channel", "source_url",
            "recorded_at", "duration_seconds", "celery_task_id", "error",
            "created_at", "started_at", "finished_at", "segments",
        ]
        read_only_fields = fields

    def get_segments(self, obj):
        if obj.status != RecoveredAudioFile.Status.SUCCESS:
            return []
        audio_segments = AudioSegments.objects.filter(recovered_from__recovered_audio_file=obj)
        return RecoveredSegmentSerializer(audio_segments, many=True).data
