from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from config.validation import TimezoneUtils
from data_analysis.models import AudioSegments
from django.db.models import Case, When, Value, FloatField
from django.db.models.functions import Cast
from shift_analysis.models import Shift, PredefinedFilter
from shift_analysis.utils import get_shift_datetime_filter, get_predefined_filter_datetime_filter
import re

class AudioSegmentFilterV3Utils:

    @staticmethod
    def get_segments(
        channel_id: int,
        start_datetime: datetime,
        end_datetime: datetime,
        shift_id: int | None = None,
        predefined_filter_id: int | None = None,
    ):
        if shift_id:
            return AudioSegmentFilterV3Utils.get_segments_by_shift(
                shift_id, start_datetime, end_datetime
            )
        if predefined_filter_id:
            return AudioSegmentFilterV3Utils.get_segments_by_predefined_filter(
                predefined_filter_id, start_datetime, end_datetime
            )
        return AudioSegmentFilterV3Utils.get_segments_by_channel(
            channel_id, start_datetime, end_datetime
        )

    @staticmethod
    def get_segments_by_channel(channel_id: int, start_datetime: datetime, end_datetime: datetime):
        return AudioSegments.objects.filter(
            is_delete=False,
            channel_id=channel_id,
            start_time__gte=start_datetime,
            start_time__lt=end_datetime,
        ).select_related("channel", "transcription_detail", "transcription_detail__analysis")

    @staticmethod
    def get_segments_by_shift(shift_id: int, start_datetime: datetime, end_datetime: datetime):
        shift = Shift.objects.get(id=shift_id, is_deleted=False)

        if shift is None:
            raise ValueError("Shift not found")
        time_filter = get_shift_datetime_filter(shift, start_datetime, end_datetime)
        return AudioSegments.objects.filter(
            time_filter,
            is_delete=False,
            channel_id=shift.channel_id,
        ).select_related("channel", "transcription_detail", "transcription_detail__analysis")

    @staticmethod
    def get_segments_by_predefined_filter(
        predefined_filter_id: int, start_datetime: datetime, end_datetime: datetime
    ):
        predefined_filter = PredefinedFilter.objects.get(
            id=predefined_filter_id, is_deleted=False
        )
        time_filter = get_predefined_filter_datetime_filter(
            predefined_filter, start_datetime, end_datetime
        )
        return AudioSegments.objects.filter(
            time_filter,
            is_delete=False,
            channel_id=predefined_filter.channel_id,
        ).select_related("channel", "transcription_detail", "transcription_detail__analysis")


    @staticmethod
    def _channel_tz(channel):
        return ZoneInfo(channel.timezone or "UTC")

    @staticmethod
    def slot_window_utc(channel, slot_date: str, slot_index: int):
        tz = AudioSegmentFilterV3Utils._channel_tz(channel)
        local_date = datetime.strptime(slot_date, "%Y%m%d").date()
        slot_start_local = datetime.combine(local_date, time(slot_index, 0), tzinfo=tz)
        slot_end_local = slot_start_local + timedelta(hours=1)
        utc = ZoneInfo("UTC")
        return (
            slot_start_local.astimezone(utc),
            slot_end_local.astimezone(utc),
        )

    @staticmethod
    def filter_by_slot(segments, channel, slot_date: str, slot_index: int):
        start_utc, end_utc = AudioSegmentFilterV3Utils.slot_window_utc(
            channel, slot_date, slot_index
        )
        return segments.filter(start_time__gte=start_utc, start_time__lt=end_utc)

    @staticmethod
    def iter_days_in_range(channel, start_datetime: datetime, end_datetime: datetime):
        tz = AudioSegmentFilterV3Utils._channel_tz(channel)
        start_local = start_datetime.astimezone(tz).date()
        end_local = end_datetime.astimezone(tz).date()
        day = start_local
        while day <= end_local:
            yield day
            day += timedelta(days=1)

    @staticmethod
    def find_first_slot_with_data(segments, channel, start_datetime: datetime, end_datetime: datetime):
        for day in AudioSegmentFilterV3Utils.iter_days_in_range(
            channel, start_datetime, end_datetime
        ):
            slot_date = day.strftime("%Y%m%d")
            for slot_index in range(24):
                if AudioSegmentFilterV3Utils.filter_by_slot(
                    segments, channel, slot_date, slot_index
                ).exists():
                    return slot_date, slot_index
        return None, None

    @staticmethod
    def default_slot(channel, start_datetime: datetime):
        day = start_datetime.astimezone(
            AudioSegmentFilterV3Utils._channel_tz(channel)
        ).date()
        return day.strftime("%Y%m%d"), 0

    @staticmethod
    def build_slot_pagination(
        segments,
        channel,
        start_datetime: datetime,
        end_datetime: datetime,
        slot_date: str,
        slot_index: int,
    ):
        tz = AudioSegmentFilterV3Utils._channel_tz(channel)
        hours = []
        for hour in range(24):
            has_data = AudioSegmentFilterV3Utils.filter_by_slot(
                segments, channel, slot_date, hour
            ).exists()
            hours.append({"slot": hour, "has_data": has_data})

        dates_with_data = []
        for day in AudioSegmentFilterV3Utils.iter_days_in_range(
            channel, start_datetime, end_datetime
        ):
            day_str = day.strftime("%Y%m%d")
            if any(
                AudioSegmentFilterV3Utils.filter_by_slot(segments, channel, day_str, hour).exists()
                for hour in range(24)
            ):
                midnight = datetime.combine(day, time.min, tzinfo=tz)
                dates_with_data.append(
                    TimezoneUtils.convert_to_channel_tz(midnight, channel.timezone)
                )

        return {
            "current_slot": slot_index,
            "slot_date": slot_date,
            "hours": hours,
            "dates_with_data": dates_with_data,
        }

    @staticmethod
    def filter_segments(segments, filter_data: dict):
        if filter_data.get("status"):
            if filter_data.get("status") == "active":
                segments = segments.filter(is_active=True)
            elif filter_data.get("status") == "inactive":
                segments = segments.filter(is_active=False)
        if filter_data.get("content_type"):
            content_types = filter_data.get("content_type")
            escaped_types = [re.escape(ct.strip()) for ct in content_types]
            
            types_pattern = "|".join(escaped_types)

            regex_pattern = f"^({types_pattern})(,|$)"
            segments = segments.filter(
                transcription_detail__analysis__content_type_prompt__iregex=regex_pattern
            )
        if filter_data.get("transcribed_only"):
            segments = segments.filter(transcription_detail__isnull=False)

        duration_min = filter_data.get("duration_seconds_min")
        duration_max = filter_data.get("duration_seconds_max")
        if duration_min is not None or duration_max is not None:
            if duration_min is not None:
                segments = segments.filter(duration_seconds__gte=duration_min)
            if duration_max is not None:
                segments = segments.filter(duration_seconds__lte=duration_max)

        sentiment_min = filter_data.get("sentiment_min")
        sentiment_max = filter_data.get("sentiment_max")

        if sentiment_min is not None or sentiment_max is not None:
            segments = segments.annotate(sentiment_score=Case(
                When(
                    transcription_detail__analysis__sentiment__regex=r'^-?\d+(\.\d+)?$',
                    then=Cast('transcription_detail__analysis__sentiment', FloatField())
                ),
                    default=Value(None),
                output_field=FloatField()
            ))
            if sentiment_min is not None:
                segments = segments.filter(sentiment_score__gte=sentiment_min)
            if sentiment_max is not None:
                segments = segments.filter(sentiment_score__lte=sentiment_max)

        if filter_data.get("search_in"):
            search_in = filter_data.get("search_in")
            search_text = filter_data.get("search_text")
            if search_in == "transcription":
                segments = segments.filter(transcription_detail__transcript__icontains=search_text)
            elif search_in == "general_topics":
                segments = segments.filter(transcription_detail__analysis__general_topics__icontains=search_text)
            elif search_in == "iab_topics":
                segments = segments.filter(transcription_detail__analysis__iab_topics__icontains=search_text)
            elif search_in == "bucket_prompt":
                segments = segments.filter(transcription_detail__analysis__bucket_prompt__icontains=search_text)
            elif search_in == "summary":
                segments = segments.filter(transcription_detail__analysis__summary__icontains=search_text)
            elif search_in == "content_type_prompt":
                segments = segments.filter(transcription_detail__analysis__content_type_prompt__icontains=search_text)
            elif search_in == "title":
                segments = segments.filter(title__icontains=search_text)
        
            
        return segments