from rest_framework import serializers

from data_analysis.models import AudioSegments, TranscriptionDetail

from .models import Prompt, PromptResult, PromptRun


class PromptSerializer(serializers.ModelSerializer):
    class Meta:
        model = Prompt
        fields = ("id", "name", "content", "is_active", "created_at")
        read_only_fields = ("id", "created_at")


class PromptForRunListSerializer(serializers.ModelSerializer):
    """Prompt row for a prompt run list, with optional nested ``PromptResult``."""

    result = serializers.SerializerMethodField()

    class Meta:
        model = Prompt
        fields = ("id", "name", "content", "is_active", "created_at", "result")

    def get_result(self, obj: Prompt):
        by_id: dict | None = self.context.get("prompt_results_by_prompt_id")
        if not by_id:
            return None
        pr = by_id.get(obj.pk)
        if pr is None:
            return None
        return PromptResultReadSerializer(pr, context=self.context).data


class PromptRunExecuteSerializer(serializers.Serializer):
    prompt_ids = serializers.ListField(
        child=serializers.IntegerField(min_value=1),
        allow_empty=False,
    )
    audio_segment_ids = serializers.ListField(
        child=serializers.IntegerField(min_value=1),
        allow_empty=False,
    )
    max_tokens = serializers.IntegerField(default=1000, min_value=0)

    def validate_prompt_ids(self, value: list[int]) -> list[int]:
        if len(value) != len(set(value)):
            raise serializers.ValidationError("prompt_ids must be unique.")
        return value

    def validate_audio_segment_ids(self, value: list[int]) -> list[int]:
        if len(value) != len(set(value)):
            raise serializers.ValidationError("audio_segment_ids must be unique.")
        return value

    def validate(self, attrs: dict) -> dict:
        prompt_ids: list[int] = attrs["prompt_ids"]
        segment_ids: list[int] = attrs["audio_segment_ids"]

        found_prompts = set(
            Prompt.objects.filter(id__in=prompt_ids).values_list("id", flat=True)
        )
        missing_p = sorted(set(prompt_ids) - found_prompts)
        if missing_p:
            raise serializers.ValidationError(
                {"prompt_ids": f"Unknown prompt id(s): {missing_p}"}
            )

        inactive_p = sorted(
            Prompt.objects.filter(
                id__in=prompt_ids, is_active=False
            ).values_list("id", flat=True)
        )
        if inactive_p:
            raise serializers.ValidationError(
                {"prompt_ids": f"Inactive prompt id(s): {inactive_p}"}
            )

        found_seg = set(
            AudioSegments.objects.filter(id__in=segment_ids).values_list("id", flat=True)
        )
        missing_s = sorted(set(segment_ids) - found_seg)
        if missing_s:
            raise serializers.ValidationError(
                {"audio_segment_ids": f"Unknown audio segment id(s): {missing_s}"}
            )

        return attrs


class PromptResultReadSerializer(serializers.ModelSerializer):
    class Meta:
        model = PromptResult
        fields = (
            "id",
            "prompt_id",
            "status",
            "response",
            "error_message",
            "created_at",
        )
        read_only_fields = fields


class SegmentTranscriptSerializer(serializers.ModelSerializer):
    """Transcription row linked to an audio segment (via ``transcription_detail``)."""

    class Meta:
        model = TranscriptionDetail
        fields = ("id", "transcript", "created_at")
        read_only_fields = fields


class AudioSegmentBriefSerializer(serializers.ModelSerializer):
    """Subset of AudioSegments for nested prompt run responses."""

    transcript = SegmentTranscriptSerializer(
        source="transcription_detail",
        read_only=True,
        allow_null=True,
    )

    class Meta:
        model = AudioSegments
        fields = (
            "id",
            "segment_type",
            "start_time",
            "end_time",
            "duration_seconds",
            "file_name",
            "title",
            "channel_id",
            "transcript",
        )
        read_only_fields = fields


class PromptRunListSerializer(serializers.ModelSerializer):
    prompts = serializers.SerializerMethodField()
    audio_segments = AudioSegmentBriefSerializer(many=True, read_only=True)

    class Meta:
        model = PromptRun
        fields = ("id", "created_at", "prompts", "audio_segments")
        read_only_fields = fields

    def get_prompts(self, run: PromptRun):
        by_id = {r.prompt_id: r for r in run.results.all()}
        return PromptForRunListSerializer(
            run.prompts.all(),
            many=True,
            context={**self.context, "prompt_results_by_prompt_id": by_id},
        ).data
