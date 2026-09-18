from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from rest_framework import serializers

from core_admin.models import Channel
from data_analysis.models import AudioSegments
from audio_recovery.models import RecoveredAudioFile


def _normalize_dropbox_url(value: str) -> str:
    """Dropbox share links default to `dl=0` (the HTML preview page) - ffmpeg
    needs the raw file, which Dropbox only serves with `dl=1`. Rewrite `dl=0`
    to `dl=1` for dropbox.com links so a pasted share link works as-is."""
    parts = urlsplit(value)
    if not parts.netloc.endswith("dropbox.com"):
        return value

    query = parse_qsl(parts.query, keep_blank_values=True)
    if ("dl", "0") not in query:
        return value

    query = [(k, "1") if (k, v) == ("dl", "0") else (k, v) for k, v in query]
    return urlunsplit(parts._replace(query=urlencode(query)))


class RecoverAudioSerializer(serializers.Serializer):
    url = serializers.URLField(help_text="Direct-download audio link (e.g. Dropbox link ending in ?dl=1).")
    channel_id = serializers.PrimaryKeyRelatedField(queryset=Channel.objects.all(), source="channel")
    recorded_at = serializers.DateTimeField(help_text="Real UTC start time of the source recording.")

    def validate_url(self, value):
        value = _normalize_dropbox_url(value)

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


class RecoveredAudioFileListSerializer(serializers.ModelSerializer):
    """Used for GET /recover/ (list) - no nested segments, so listing many
    jobs doesn't run a query per row."""

    class Meta:
        model = RecoveredAudioFile
        fields = [
            "id", "status", "channel", "source_url",
            "recorded_at", "duration_seconds", "celery_task_id", "error",
            "created_at", "started_at", "finished_at",
        ]
        read_only_fields = fields


class RecoveredAudioFileSerializer(RecoveredAudioFileListSerializer):
    """Used for POST /recover/ and GET /recover/<id>/status/ - includes the
    segments a successful job created."""

    segments = serializers.SerializerMethodField()

    class Meta(RecoveredAudioFileListSerializer.Meta):
        fields = RecoveredAudioFileListSerializer.Meta.fields + ["segments"]
        read_only_fields = fields

    def get_segments(self, obj):
        if obj.status != RecoveredAudioFile.Status.SUCCESS:
            return []
        audio_segments = AudioSegments.objects.filter(recovered_from__recovered_audio_file=obj)
        return RecoveredSegmentSerializer(audio_segments, many=True).data
