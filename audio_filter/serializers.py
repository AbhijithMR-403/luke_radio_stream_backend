from datetime import date, datetime

from core_admin.models import Channel
from django.utils import timezone
from rest_framework import serializers


class AudioSegmentFilterV3Serializer(serializers.Serializer):
    """
    Query parameters for v3 audio segment filtering.

    Example:
        channel_id=1
        start_datetime=2025-09-10T13:00:00
        end_datetime=2025-09-12T00:00:00
        content_type=Announcer Fundraising output
        content_type=interview
        shift_id=6
        predefined_filter_id=
        status=active
        search_text=speaker
        search_in=transcription
        slot_date=20240123
        slot_date=2025-10-10
        slot_date=2025-10-10T00:00:00+11:00
        slot_index=7
        duration_seconds_min=30
        duration_seconds_max=300
        sentiment_min=20
        sentiment_max=80
    """

    channel_id = serializers.IntegerField(required=True)
    start_datetime = serializers.CharField(required=True)
    end_datetime = serializers.CharField(required=True)
    status = serializers.ChoiceField(
        choices=["active", "inactive"],
        required=False,
        allow_null=True,
    )
    shift_id = serializers.IntegerField(required=False, allow_null=True)
    predefined_filter_id = serializers.IntegerField(required=False, allow_null=True)
    content_type = serializers.ListField(child=serializers.CharField(), required=False, allow_empty=True)
    transcribed_only = serializers.BooleanField(required=False, default=False)
    search_text = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    search_in = serializers.ChoiceField(choices=[
        "transcription",
        "general_topics",
        "iab_topics",
        "bucket_prompt",
        "summary",
        "content_type_prompt",
        "title"], required=False, allow_null=True)
    search_type = serializers.CharField(
        required=False, allow_null=True, allow_blank=True)
    show_flagged_only = serializers.BooleanField(required=False, default=False)
    duration_seconds_min = serializers.IntegerField(required=False, allow_null=True, min_value=0)
    duration_seconds_max = serializers.IntegerField(required=False, allow_null=True, min_value=0)
    sentiment_min = serializers.FloatField(required=False, allow_null=True, min_value=0, max_value=100)
    sentiment_max = serializers.FloatField(required=False, allow_null=True, min_value=0, max_value=100)
    slot_date = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    slot_index = serializers.IntegerField(required=False, allow_null=True, min_value=0, max_value=23)

    def validate(self, attrs):

        if attrs.get("channel_id") is None:
            raise serializers.ValidationError("channel_id is required")
        else:
            channel = Channel.objects.get(id=attrs.get("channel_id"), is_deleted=False)
            if channel is None:
                raise serializers.ValidationError("Channel not found")
            attrs["channel"] = channel
        
        if attrs.get("shift_id") and attrs.get("predefined_filter_id"):
            raise serializers.ValidationError("Cannot use both shift_id and predefined_filter_id simultaneously")

        start_datetime_str = attrs.get("start_datetime")
        end_datetime_str = attrs.get("end_datetime")

        if start_datetime_str:
            start_dt = self._parse_datetime(start_datetime_str)
            if not start_dt:
                raise serializers.ValidationError({
                    "start_datetime": [
                        "Invalid format. Use ISO format (YYYY-MM-DDTHH:MM:SS) "
                        "or YYYY-MM-DD HH:MM:SS"
                    ]
                })
            attrs["start_datetime"] = start_dt

        if end_datetime_str:
            end_dt = self._parse_datetime(end_datetime_str)
            if not end_dt:
                raise serializers.ValidationError({
                    "end_datetime": [
                        "Invalid format. Use ISO format (YYYY-MM-DDTHH:MM:SS) "
                        "or YYYY-MM-DD HH:MM:SS"
                    ]
                })
            attrs["end_datetime"] = end_dt

        if attrs.get("end_datetime") <= attrs.get("start_datetime"):
            raise serializers.ValidationError({
                "end_datetime": ["end_datetime must be after start_datetime"]
            })


        slot_date = (attrs.get("slot_date") or "").strip()
        slot_index = attrs.get("slot_index")

        if slot_date and slot_index is None:
            raise serializers.ValidationError("slot_index is required when slot_date is provided")
        if slot_index is not None and not slot_date:
            raise serializers.ValidationError("slot_date is required when slot_index is provided")

        if slot_date:
            normalized = self._normalize_slot_date(slot_date)
            if not normalized:
                raise serializers.ValidationError(
                    "slot_date must be YYYYMMDD (e.g. 20240123), YYYY-MM-DD (e.g. 2025-10-10), "
                    "or an ISO datetime (e.g. 2025-10-10T00:00:00+11:00)"
                )
            attrs["slot_date"] = normalized
        else:
            attrs["slot_date"] = None

        search_text = attrs.get("search_text")
        search_in = attrs.get("search_in")

        if search_text and not search_in:
            raise serializers.ValidationError("search_in is required when search_text is provided")

        if search_in and not search_text:
            raise serializers.ValidationError("search_text is required when search_in is provided")

        if search_text == "":
            attrs["search_text"] = None
        if search_in == "":
            attrs["search_in"] = None
        if attrs.get("search_type") == "":
            attrs["search_type"] = None

        duration_min = attrs.get("duration_seconds_min")
        duration_max = attrs.get("duration_seconds_max")
        if duration_min is not None and duration_max is not None and duration_min > duration_max:
            raise serializers.ValidationError(
                "duration_seconds_min must be less than or equal to duration_seconds_max"
            )

        sentiment_min = attrs.get("sentiment_min")
        sentiment_max = attrs.get("sentiment_max")
        if sentiment_min is not None and sentiment_max is not None and sentiment_min > sentiment_max:
            raise serializers.ValidationError(
                "sentiment_min must be less than or equal to sentiment_max"
            )

        return attrs

    def _normalize_slot_date(self, value: str) -> str | None:
        """Parse slot_date input and return YYYYMMDD."""
        if not value:
            return None

        if len(value) == 8 and value.isdigit():
            try:
                datetime.strptime(value, "%Y%m%d")
                return value
            except ValueError:
                return None

        parsed_date: date | None = None
        if "T" in value:
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                parsed_date = dt.date()
            except (ValueError, TypeError):
                return None
        else:
            for fmt in ("%Y-%m-%d", "%Y%m%d"):
                try:
                    parsed_date = datetime.strptime(value, fmt).date()
                    break
                except ValueError:
                    continue

        if parsed_date is None:
            return None

        return parsed_date.strftime("%Y%m%d")

    def _parse_datetime(self, value):
        if not isinstance(value, str):
            return None

        try:
            if "T" in value:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return None

        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt)

        return dt


class AudioSegmentFilterV3AnalysisSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    summary = serializers.CharField(allow_null=True)
    sentiment = serializers.CharField(allow_null=True)
    general_topics = serializers.CharField(allow_null=True)
    iab_topics = serializers.CharField(allow_null=True)
    bucket_prompt = serializers.CharField(allow_null=True)
    content_type_prompt = serializers.CharField(allow_null=True)
    created_at = serializers.DateTimeField(allow_null=True)


class AudioSegmentFilterV3SegmentSerializer(serializers.Serializer):
    """Response serializer for v3 audio segment filter results."""

    def to_representation(self, segment):
        try:
            transcription_detail = segment.transcription_detail
        except AttributeError:
            transcription_detail = None

        transcription = None
        analysis = None
        if transcription_detail:
            transcription = {
                "id": transcription_detail.id,
                "transcript": transcription_detail.transcript,
                "created_at": transcription_detail.created_at.isoformat()
                if transcription_detail.created_at
                else None,
                "rev_job_id": transcription_detail.rev_job.job_id
                if transcription_detail.rev_job
                else None,
            }
            try:
                analysis_obj = transcription_detail.analysis
                analysis = AudioSegmentFilterV3AnalysisSerializer(analysis_obj).data
            except AttributeError:
                analysis = None

        return {
            "id": segment.id,
            "start_time": segment.start_time.isoformat() if segment.start_time else None,
            "end_time": segment.end_time.isoformat() if segment.end_time else None,
            "duration_seconds": segment.duration_seconds,
            "file_name": segment.file_name,
            "file_path": segment.file_path,
            "audio_url": segment.audio_url,
            "audio_location_type": segment.audio_location_type,
            "title": segment.title,
            "title_before": segment.title_before,
            "title_after": segment.title_after,
            "is_recognized": segment.is_recognized,
            "is_active": segment.is_active,
            "is_transcribed": transcription_detail is not None,
            "transcription": transcription,
            "analysis": analysis,
            "channel": {
                "id": segment.channel_id,
                "name": segment.channel.name if segment.channel else None,
            },
        }
