from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from audio_policy.models import FlagCondition
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
        shift = Shift.objects.get(id=shift_id, is_active=True)

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


    # Helper functions Flag Conditions
    @staticmethod
    def _flatten_nested_list(nested_list):
        if not nested_list:
            return []
        flattened = []
        for item in nested_list:
            if isinstance(item, list):
                flattened.extend(AudioSegmentFilterV3Utils._flatten_nested_list(item))
            elif item:
                flattened.append(str(item))
        return flattened

    @staticmethod
    def get_active_flag_condition(channel):
        try:
            return FlagCondition.objects.get(channel=channel, is_active=True)
        except FlagCondition.DoesNotExist:
            return None

    @staticmethod
    def _segment_text_fields(segment):
        transcription = {}
        analysis = {}
        try:
            transcription_detail = segment.transcription_detail
        except AttributeError:
            return transcription, analysis

        if not transcription_detail:
            return transcription, analysis

        transcription = {"transcript": transcription_detail.transcript or ""}
        try:
            analysis_obj = transcription_detail.analysis
        except AttributeError:
            return transcription, analysis

        if analysis_obj:
            analysis = {
                "summary": analysis_obj.summary,
                "sentiment": analysis_obj.sentiment,
                "iab_topics": analysis_obj.iab_topics,
                "bucket_prompt": analysis_obj.bucket_prompt,
                "general_topics": analysis_obj.general_topics,
            }
        return transcription, analysis

    @staticmethod
    def evaluate_flag_conditions(segment, flag_condition):
        """
        Evaluate FlagCondition rules for an AudioSegments instance.
        Returns flag entries keyed by condition type.
        """
        flags = {}
        transcription, analysis = AudioSegmentFilterV3Utils._segment_text_fields(segment)

        def build_flag_entry(triggered, message=""):
            return {"flagged": bool(triggered), "message": message}

        def check_keywords(text, keyword_groups):
            if not text or not keyword_groups:
                return False, ""
            text_lower = text.lower()
            matched = []
            for group in keyword_groups:
                if isinstance(group, list):
                    for kw in group:
                        if kw and kw.lower() in text_lower:
                            matched.append(group)
                            break
            if matched:
                display_matches = [", ".join(g) for g in matched[:3]]
                return True, f"Found keywords: {', '.join(display_matches)}"
            return False, ""

        t_trig, t_msg = check_keywords(
            transcription.get("transcript", ""),
            flag_condition.transcription_keywords,
        )
        flags["transcription_keywords"] = build_flag_entry(t_trig, t_msg)

        s_trig, s_msg = check_keywords(
            analysis.get("summary", ""),
            flag_condition.summary_keywords,
        )
        flags["summary_keywords"] = build_flag_entry(s_trig, s_msg)

        sentiment_str = str(analysis.get("sentiment") or "")
        sentiment_value = None
        match = re.search(r"-?\d+(\.\d+)?", sentiment_str)
        if match:
            try:
                sentiment_value = float(match.group())
            except ValueError:
                pass

        triggered = False
        message = ""
        if sentiment_value is not None:
            if (
                flag_condition.target_sentiments is not None
                and sentiment_value == flag_condition.target_sentiments
            ):
                triggered = True
                message = "Matches target sentiment"

            ranges = []
            if (
                flag_condition.sentiment_min_lower is not None
                or flag_condition.sentiment_min_upper is not None
            ):
                ranges.append(
                    (
                        flag_condition.sentiment_min_lower or float("-inf"),
                        flag_condition.sentiment_min_upper or float("inf"),
                    )
                )
            if (
                flag_condition.sentiment_max_lower is not None
                or flag_condition.sentiment_max_upper is not None
            ):
                ranges.append(
                    (
                        flag_condition.sentiment_max_lower or float("-inf"),
                        flag_condition.sentiment_max_upper or float("inf"),
                    )
                )

            for lower, upper in ranges:
                if lower == 0.0 and upper == 100.0:
                    continue
                if lower <= sentiment_value <= upper:
                    triggered = True
                    message = f"Sentiment {sentiment_value} in range [{lower}, {upper}]"
                    break

        flags["sentiment"] = build_flag_entry(triggered, message)

        def check_list_overlap(segment_list_or_str, condition_list, label):
            if not condition_list:
                return False, ""
            target_flat = AudioSegmentFilterV3Utils._flatten_nested_list(condition_list)
            source_val = segment_list_or_str
            if not isinstance(source_val, list):
                source_val = [str(source_val)] if source_val else []
            source_flat = AudioSegmentFilterV3Utils._flatten_nested_list(source_val)
            source_str = " ".join(source_flat).lower()
            matched = [topic for topic in target_flat if topic and topic.lower() in source_str]
            if matched:
                return True, f"Found {label}: {', '.join(matched[:5])}"
            return False, ""

        i_trig, i_msg = check_list_overlap(
            analysis.get("iab_topics"),
            flag_condition.iab_topics,
            "IAB topics",
        )
        flags["iab_topics"] = build_flag_entry(i_trig, i_msg)

        b_trig, b_msg = check_list_overlap(
            analysis.get("bucket_prompt"),
            flag_condition.bucket_prompt,
            "bucket prompts",
        )
        flags["bucket_prompt"] = build_flag_entry(b_trig, b_msg)

        g_trig, g_msg = check_list_overlap(
            analysis.get("general_topics"),
            flag_condition.general_topics,
            "general topics",
        )
        flags["general_topics"] = build_flag_entry(g_trig, g_msg)

        return flags

    @staticmethod
    def build_segment_flags(segment, flag_condition=None):
        """Build flag payload for a segment from FlagCondition only."""
        if not flag_condition:
            return {}
        return AudioSegmentFilterV3Utils.evaluate_flag_conditions(segment, flag_condition)