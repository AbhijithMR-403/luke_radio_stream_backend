from rest_framework import serializers

from core_admin.models import Channel
from data_analysis.models import AudioSegments
from audio_recovery.models import RecoveredAudioFile


class RecoverAudioSerializer(serializers.Serializer):
    url = serializers.URLField(help_text="Direct-download audio link (e.g. Dropbox link ending in ?dl=1).")
    channel_id = serializers.PrimaryKeyRelatedField(queryset=Channel.objects.all(), source="channel")
    recorded_at = serializers.DateTimeField(help_text="Real UTC start time of the source recording.")


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
