from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.db.models import Q
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from audio_filter.serializers import (
    AudioSegmentFilterV3SegmentSerializer,
    AudioSegmentFilterV3Serializer,
)
from data_analysis.models import AudioSegments
from audio_filter.utils import AudioSegmentFilterV3Utils
TRUE_VALUES = {"true", "1", "yes", "y"}
FALSE_VALUES = {"false", "0", "no", "n"}


def _parse_bool(value, field_name):
    if value in (None, ""):
        return False

    normalized_value = str(value).strip().lower()
    if normalized_value in TRUE_VALUES:
        return True
    if normalized_value in FALSE_VALUES:
        return False

    raise ValueError(f"{field_name} must be true or false")


def _parse_positive_int(value, field_name):
    if value in (None, ""):
        return 10

    try:
        parsed_value = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a positive integer")

    if parsed_value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")

    return parsed_value


def _parse_optional_positive_int(value, field_name):
    if value in (None, ""):
        return None

    try:
        parsed_value = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a positive integer")

    if parsed_value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")

    return parsed_value


def _parse_request_datetime(value, field_name):
    if not value:
        raise ValueError(f"{field_name} is required")

    parsed_datetime = parse_datetime(value)
    if not parsed_datetime:
        raise ValueError(
            f"Invalid {field_name} format. Use ISO format or YYYY-MM-DD HH:MM:SS"
        )

    if timezone.is_naive(parsed_datetime):
        parsed_datetime = timezone.make_aware(
            parsed_datetime,
            timezone.get_current_timezone(),
        )

    return parsed_datetime


def _serialize_segment(segment):
    try:
        transcription_detail = segment.transcription_detail
    except AttributeError:
        transcription_detail = None

    transcription = None
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
        "channel": {
            "id": segment.channel_id,
            "name": segment.channel.name if segment.channel else None,
        },
    }


class AudioSegmentFilterView(APIView):
    """
    Filter audio segments by datetime range, title name, and transcription availability.

    Query parameters:
    - start_datetime: Required datetime range start.
    - end_datetime: Required datetime range end.
    - id: Optional audio segment ID.
    - channel_Id: Optional channel ID.
    - title_name: Optional, case-insensitive search across title, title_before, and title_after.
    - transcribed_only: Optional boolean. When true, returns only transcribed segments.
    - limit: Optional positive integer. Defaults to 10 newest segments.
    """

    def get(self, request):
        try:
            start_datetime = _parse_request_datetime(
                request.query_params.get("start_datetime"),
                "start_datetime",
            )
            end_datetime = _parse_request_datetime(
                request.query_params.get("end_datetime"),
                "end_datetime",
            )
        except ValueError as exc:
            return Response(
                {"success": False, "error": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if end_datetime <= start_datetime:
            return Response(
                {"success": False, "error": "end_datetime must be after start_datetime"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            transcribed_only = _parse_bool(
                request.query_params.get("transcribed_only"),
                "transcribed_only",
            )
        except ValueError as exc:
            return Response(
                {"success": False, "error": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )

        title_name = (request.query_params.get("title_name") or "").strip()
        try:
            limit = _parse_positive_int(request.query_params.get("limit"), "limit")
            segment_id = _parse_optional_positive_int(request.query_params.get("id"), "id")
            channel_id = _parse_optional_positive_int(
                request.query_params.get("channel_Id"),
                "channel_Id",
            )
        except ValueError as exc:
            return Response(
                {"success": False, "error": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )

        segments = AudioSegments.objects.filter(
            is_delete=False,
            start_time__gte=start_datetime,
            start_time__lt=end_datetime,
        )

        if segment_id:
            segments = segments.filter(id=segment_id)

        if channel_id:
            segments = segments.filter(channel_id=channel_id)

        if title_name:
            segments = segments.filter(
                Q(title__icontains=title_name)
                | Q(title_before__icontains=title_name)
                | Q(title_after__icontains=title_name)
            )

        if transcribed_only:
            segments = segments.filter(transcription_detail__isnull=False)

        segments = segments.select_related(
            "channel",
            "transcription_detail",
            "transcription_detail__rev_job",
        ).order_by("-start_time")

        total_count = segments.count()
        segments = segments[:limit]

        data = [_serialize_segment(segment) for segment in segments]

        return Response(
            {
                "success": True,
                "filters": {
                    "start_datetime": start_datetime.isoformat(),
                    "end_datetime": end_datetime.isoformat(),
                    "id": segment_id,
                    "channel_Id": channel_id,
                    "title_name": title_name or None,
                    "transcribed_only": transcribed_only,
                    "limit": limit,
                },
                "count": len(data),
                "total_count": total_count,
                "data": data,
            }
        )

class AudioSegmentFilterV3View(APIView):
    serializer_class = AudioSegmentFilterV3Serializer
    def get(self, request):
        serializer = self.serializer_class(data=request.query_params)
        if not serializer.is_valid():
            return Response(
                {"success": False, "error": serializer.errors},
                status=status.HTTP_400_BAD_REQUEST,
            )
        start_datetime = serializer.validated_data["start_datetime"]
        end_datetime = serializer.validated_data["end_datetime"]
        channel = serializer.validated_data["channel"]
        audio_segments = AudioSegmentFilterV3Utils.get_segments(
            channel_id=serializer.validated_data["channel_id"],
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            shift_id=serializer.validated_data.get("shift_id"),
            predefined_filter_id=serializer.validated_data.get("predefined_filter_id"),
        )
        filter_data = {
            "status": serializer.validated_data.get("status"),
            "content_type": serializer.validated_data.get("content_type"),
            "transcribed_only": serializer.validated_data.get("transcribed_only"),
            "slot_date": serializer.validated_data.get("slot_date"),
            "slot_index": serializer.validated_data.get("slot_index"),
            "search_text": serializer.validated_data.get("search_text"),
            "search_in": serializer.validated_data.get("search_in"),
            "search_type": serializer.validated_data.get("search_type"),
            "show_flagged_only": serializer.validated_data.get("show_flagged_only"),
            "duration_seconds_min": serializer.validated_data.get("duration_seconds_min"),
            "duration_seconds_max": serializer.validated_data.get("duration_seconds_max"),
            "sentiment_min": serializer.validated_data.get("sentiment_min"),
            "sentiment_max": serializer.validated_data.get("sentiment_max"),
        }
        audio_segments = AudioSegmentFilterV3Utils.filter_segments(audio_segments, filter_data)

        is_search_mode = filter_data.get("search_text") and filter_data.get("search_in")
        pagination = None
        if not is_search_mode:
            slot_date = serializer.validated_data.get("slot_date")
            slot_index = serializer.validated_data.get("slot_index")
            if slot_date is None or slot_index is None:
                slot_date, slot_index = AudioSegmentFilterV3Utils.find_first_slot_with_data(
                    audio_segments, channel, start_datetime, end_datetime
                )
                if slot_date is None:
                    slot_date, slot_index = AudioSegmentFilterV3Utils.default_slot(
                        channel, start_datetime
                    )
            pagination = AudioSegmentFilterV3Utils.build_slot_pagination(
                audio_segments,
                channel,
                start_datetime,
                end_datetime,
                slot_date,
                slot_index,
            )
            audio_segments = AudioSegmentFilterV3Utils.filter_by_slot(
                audio_segments, channel, slot_date, slot_index
            )

        audio_segments = audio_segments.order_by("start_time")
        data = AudioSegmentFilterV3SegmentSerializer(audio_segments, many=True).data

        response = {
            "success": True,
            "count": len(data),
            "data": data,
        }
        if pagination is not None:
            response["pagination"] = pagination
        return Response(response)