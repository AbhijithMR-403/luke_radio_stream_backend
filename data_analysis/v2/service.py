"""
Helper functions for AudioSegments V2 API
This module contains all the helper functions for the AudioSegments API to keep views.py clean and organized.
"""

import re
import math
from datetime import datetime, timezone as dt_utc
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict, Any, Tuple

from django.http import JsonResponse
from django.utils import timezone
from django.db.models import Q

from core_admin.models import Channel
from data_analysis.repositories import AudioSegmentDAO
from data_analysis.serializers import AudioSegmentsSerializer
from audio_policy.models import FlagCondition
from config.validation import TimezoneUtils


# ==========================================
# Utility Functions
# ==========================================

def flatten_nested_list(nested_list):
    """
    Flatten a nested list structure into a single list of strings.
    Handles cases like: [["item1"], ["item2"], ["item3", "item4"]] -> ["item1", "item2", "item3", "item4"]
    """
    if not nested_list:
        return []
    
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_nested_list(item))
        elif item:
            flattened.append(str(item))
    return flattened


def parse_dt(value):
    """Parse string to timezone-aware datetime."""
    if isinstance(value, str):
        try:
            if 'T' in value:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    elif isinstance(value, datetime):
        dt = value
    else:
        return None
        
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt)
    return dt


def validate_v2_parameters(request):
    """
    Validate and extract parameters for v2 audio segments API using serializer.
    """
    # Import locally to avoid circular imports if serializer imports models
    from data_analysis.v2.serializer import ListAudioSegmentsV2QuerySerializer
    
    # Handle QueryDict list extraction for content_type
    data = dict(request.GET.items())
    if 'content_type' in request.GET:
        data['content_type'] = request.GET.getlist('content_type')
    
    serializer = ListAudioSegmentsV2QuerySerializer(data=data)
    
    if not serializer.is_valid():
        return None, JsonResponse({
            'success': False,
            'error': 'Validation Error',
            'errors': serializer.errors
        }, status=400)
    
    validated_data = serializer.validated_data
    
    # Map serializer output to the dictionary structure expected by views
    params = {
        'channel_pk': validated_data['channel_id'],
        'base_start_dt': validated_data['base_start_dt'],
        'base_end_dt': validated_data['base_end_dt'],
        'shift_id': validated_data.get('shift_id'),
        'predefined_filter_id': validated_data.get('predefined_filter_id'),
        'page': validated_data.get('page', 1),
        'page_size': validated_data.get('page_size', 1),
        'status': validated_data.get('status'),
        'content_type': validated_data.get('content_type', []),
        'search_text': validated_data.get('search_text'),
        'search_in': validated_data.get('search_in'),
        'show_flagged_only': validated_data.get('show_flagged_only', False)
    }
    
    return params, None


def get_channel_and_shift(params):
    """Get channel, shift, and predefined_filter objects from database."""
    try:
        channel = Channel.objects.get(id=params['channel_pk'], is_deleted=False)
    except Channel.DoesNotExist:
        return None, None, None, JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
    
    shift = None
    if params.get('shift_id'):
        try:
            from shift_analysis.models import Shift
            shift = Shift.objects.get(id=params['shift_id'], is_active=True)
        except Shift.DoesNotExist:
            return None, None, None, JsonResponse({'success': False, 'error': 'Shift not found or inactive'}, status=404)
    
    predefined_filter = None
    if params.get('predefined_filter_id'):
        try:
            from shift_analysis.models import PredefinedFilter
            predefined_filter = PredefinedFilter.objects.get(id=params['predefined_filter_id'], is_active=True, channel=channel)
        except Exception:
            return None, None, None, JsonResponse({'success': False, 'error': 'Predefined filter not found or inactive'}, status=404)
    
    return channel, shift, predefined_filter, None


# ==========================================
# Filtering Logic (Master Query Builder)
# ==========================================

def apply_content_type_filter(query, content_type_list: List[str]):
    """
    Apply content_type filtering using Regex to ensure exact comma-separated value matching.
    """
    if not content_type_list:
        return query
    
    content_type_conditions = Q()
    for content_type_value in content_type_list:
        value = content_type_value.strip()
        if not value:
            continue
            
        escaped_value = re.escape(value)
        # Matches start of string or comma+space, followed by value, followed by comma or end
        # Example: Matches "Announcer" in "Announcer, 95%"
        pattern = rf'(?:^|,\s*){escaped_value}(?:,|$)'
        
        content_type_conditions |= Q(
            transcription_detail__analysis__content_type_prompt__iregex=pattern
        )
    
    # Ensure necessary joins exist
    query = query.filter(
        transcription_detail__analysis__isnull=False
    ).filter(content_type_conditions)
    
    return query.distinct()


def apply_search_filters(base_query, search_text, search_in):
    """Apply text search filters."""
    if not search_text or not search_in:
        return base_query
        
    search_map = {
        'transcription': 'transcription_detail__transcript__icontains',
        'general_topics': 'transcription_detail__analysis__general_topics__icontains',
        'iab_topics': 'transcription_detail__analysis__iab_topics__icontains',
        'bucket_prompt': 'transcription_detail__analysis__bucket_prompt__icontains',
        'summary': 'transcription_detail__analysis__summary__icontains',
        'content_type_prompt': 'transcription_detail__analysis__content_type_prompt__icontains',
        'title': 'title__icontains',
    }
    
    if search_in in search_map:
        kwargs = {search_map[search_in]: search_text}
        return base_query.filter(**kwargs).distinct()
        
    return base_query


def get_segments_queryset(channel, start_dt, end_dt, valid_windows=None, status=None, 
                          content_types=None, search_text=None, search_in=None, 
                          is_last_page=False):
    """
    Master function to build the AudioSegments queryset with all filters applied.
    Replaces repetitive query building in views.
    """
    
    # 1. Start with base filters (Channel, Status, Deleted)
    # Note: AudioSegmentDAO.filter returns a QuerySet, so we can chain it
    base_query = AudioSegmentDAO.filter(
        channel_id=channel.id,
        is_active=status,
        is_delete=False
    )
    
    # 2. Apply Time Filtering (Shift Windows vs Simple Range)
    if valid_windows:
        time_conditions = Q()
        had_intersection = False
        
        for window_start, window_end in valid_windows:
            # Intersect request window with shift window
            intersection_start = max(start_dt, window_start)
            intersection_end = min(end_dt, window_end)
            
            if intersection_start < intersection_end:
                had_intersection = True
                if is_last_page:
                    time_conditions |= Q(start_time__gte=intersection_start, start_time__lte=intersection_end)
                else:
                    time_conditions |= Q(start_time__gte=intersection_start, start_time__lt=intersection_end)
        
        if had_intersection:
            base_query = base_query.filter(time_conditions)
        else:
            return base_query.none()
            
    else:
        # Simple time range
        if is_last_page:
            base_query = base_query.filter(start_time__gte=start_dt, start_time__lte=end_dt)
        else:
            base_query = base_query.filter(start_time__gte=start_dt, start_time__lt=end_dt)

    # 3. Apply Content Type Filter
    if content_types:
        base_query = apply_content_type_filter(base_query, content_types)

    # 4. Apply Search Filter
    if search_text and search_in:
        base_query = apply_search_filters(base_query, search_text, search_in)

    # 5. Optimization
    base_query = base_query.select_related(
        'transcription_detail',
        'transcription_detail__rev_job',
        'transcription_detail__analysis'
    )
    
    return base_query.order_by('start_time')


# ==========================================
# Shift & Time Calculation
# ==========================================

def apply_shift_filtering(base_start_dt, base_end_dt, shift):
    """Apply shift-based time filtering to the datetime range."""
    if not shift:
        return []
    
    from shift_analysis.utils import _build_utc_windows_for_local_day
    
    shift_tz = ZoneInfo(shift.channel.timezone)
    base_start_local = base_start_dt.astimezone(shift_tz)
    base_end_local = base_end_dt.astimezone(shift_tz)
    
    shift_days = [day.strip().lower() for day in shift.days.split(',')] if shift.days else []
    
    valid_windows = []
    current_date = base_start_local.date()
    end_date = base_end_local.date()
    
    while current_date <= end_date:
        current_day_name = current_date.strftime('%A').lower()
        
        if not shift_days or current_day_name in shift_days:
            day_windows = _build_utc_windows_for_local_day(
                shift.start_time, shift.end_time, current_date, shift_tz
            )
            for window_start, window_end in day_windows:
                intersection_start = max(base_start_dt, window_start)
                intersection_end = min(base_end_dt, window_end)
                if intersection_start < intersection_end:
                    valid_windows.append((intersection_start, intersection_end))
        
        current_date += timezone.timedelta(days=1)
    
    return valid_windows


def apply_predefined_filter_filtering(base_start_dt, base_end_dt, predefined_filter):
    """Apply predefined filter-based time filtering."""
    if not predefined_filter:
        return []
    
    from shift_analysis.utils import _build_utc_windows_for_local_day
    from shift_analysis.models import FilterSchedule
    
    filter_tz = ZoneInfo(predefined_filter.channel.timezone)
    base_start_local = base_start_dt.astimezone(filter_tz)
    base_end_local = base_end_dt.astimezone(filter_tz)
    
    valid_windows = []
    current_date = base_start_local.date()
    end_date = base_end_local.date()
    
    while current_date <= end_date:
        day_of_week = current_date.strftime('%A').lower()
        schedules = FilterSchedule.objects.filter(
            predefined_filter=predefined_filter, day_of_week=day_of_week
        )
        
        for schedule in schedules:
            day_windows = _build_utc_windows_for_local_day(
                schedule.start_time, schedule.end_time, current_date, filter_tz
            )
            for window_start, window_end in day_windows:
                intersection_start = max(base_start_dt, window_start)
                intersection_end = min(base_end_dt, window_end)
                if intersection_start < intersection_end:
                    valid_windows.append((intersection_start, intersection_end))
                    
        current_date += timezone.timedelta(days=1)
    
    return valid_windows


def get_page_window_from_pagination_entry(entry):
    """
    Get (start_dt_utc, end_dt_utc, is_last_page) from an available_pages entry.
    Use when the entry has whole_day=True so the view fetches the full day range.
    Returns None if entry is missing or missing required keys.
    """
    if not entry or 'start_time' not in entry or 'end_time' not in entry:
        return None
    from datetime import datetime as dt_class
    try:
        start_s = entry['start_time'].replace('Z', '+00:00') if entry.get('start_time') else None
        end_s = entry['end_time'].replace('Z', '+00:00') if entry.get('end_time') else None
        if not start_s or not end_s:
            return None
        start_dt = dt_class.fromisoformat(start_s)
        end_dt = dt_class.fromisoformat(end_s)
        if start_dt.tzinfo is None or end_dt.tzinfo is None:
            return None
        start_utc = start_dt.astimezone(dt_utc.utc)
        end_utc = end_dt.astimezone(dt_utc.utc)
        is_last_page = entry.get('is_last_page', False)
        return start_utc, end_utc, is_last_page
    except (ValueError, TypeError):
        return None


def calculate_pagination_window(base_start_dt, base_end_dt, page, page_size, search_text, search_in, valid_windows=None):
    """Calculate the current page time window."""
    # If searching, collapse to a single page
    if search_text and search_in:
        if valid_windows:
            return valid_windows[0][0], valid_windows[-1][1], True, None
        return base_start_dt, base_end_dt, True, None

    page_start_offset = (page - 1) * page_size
    current_page_start = base_start_dt + timezone.timedelta(hours=page_start_offset)
    current_page_end = current_page_start + timezone.timedelta(hours=page_size)
    
    total_hours = math.ceil((base_end_dt - base_start_dt).total_seconds() / 3600)
    total_pages_needed = math.ceil(total_hours / page_size)
    is_last_page = page == total_pages_needed
    
    if current_page_start >= base_end_dt:
        return None, None, None, JsonResponse({
            'success': False, 
            'error': f'Page {page} is beyond the available time range'
        }, status=400)
    
    if current_page_end > base_end_dt:
        current_page_end = base_end_dt
        
    return current_page_start, current_page_end, is_last_page, None


# ==========================================
# Flagging Logic
# ==========================================

def check_flag_conditions(segment, flag_condition):
    """
    Check if a segment matches the flag condition criteria.
    Returns a dictionary with flag information.
    """
    flags = {}
    transcription = segment.get('transcription') or {}
    analysis = segment.get('analysis') or {}
    
    def build_flag_entry(triggered, message=''):
        return {'flagged': bool(triggered), 'message': message}

    # Helper for simple keyword checking
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
            display_matches = [', '.join(g) for g in matched[:3]]
            return True, f"Found keywords: {', '.join(display_matches)}"
        return False, ""

    # 1. Transcription
    t_trig, t_msg = check_keywords(transcription.get('transcript', ''), flag_condition.transcription_keywords)
    flags['transcription_keywords'] = build_flag_entry(t_trig, t_msg)

    # 2. Summary
    s_trig, s_msg = check_keywords(analysis.get('summary', ''), flag_condition.summary_keywords)
    flags['summary_keywords'] = build_flag_entry(s_trig, s_msg)

    # 3. Sentiment (Robust Parsing)
    sentiment_str = str(analysis.get('sentiment') or '')
    sentiment_value = None
    match = re.search(r'-?\d+(\.\d+)?', sentiment_str)
    if match:
        try:
            sentiment_value = float(match.group())
        except ValueError:
            pass
            
    triggered = False
    message = ''
    if sentiment_value is not None:
        # Check target match
        if flag_condition.target_sentiments is not None and sentiment_value == flag_condition.target_sentiments:
            triggered = True
            message = "Matches target sentiment"
        
        # Check Ranges
        # Logic: Flag if value is WITHIN the configured risk range
        ranges = []
        if flag_condition.sentiment_min_lower is not None or flag_condition.sentiment_min_upper is not None:
             ranges.append((flag_condition.sentiment_min_lower or float('-inf'), flag_condition.sentiment_min_upper or float('inf')))
        if flag_condition.sentiment_max_lower is not None or flag_condition.sentiment_max_upper is not None:
             ranges.append((flag_condition.sentiment_max_lower or float('-inf'), flag_condition.sentiment_max_upper or float('inf')))
             
        for lower, upper in ranges:
            # Skip default 0-100 ranges which flag everything
            if lower == 0.0 and upper == 100.0:
                continue
            if lower <= sentiment_value <= upper:
                triggered = True
                message = f"Sentiment {sentiment_value} in range [{lower}, {upper}]"
                break
    
    flags['sentiment'] = build_flag_entry(triggered, message)

    # 4. Topics & Prompts (Using flatten_nested_list helper)
    def check_list_overlap(segment_list_or_str, condition_list, label):
        if not condition_list:
            return False, ""
        
        target_flat = flatten_nested_list(condition_list)
        
        source_val = segment_list_or_str
        if not isinstance(source_val, list):
            source_val = [str(source_val)] if source_val else []
        source_flat = flatten_nested_list(source_val)
        source_str = ' '.join(source_flat).lower()
        
        matched = [t for t in target_flat if t and t.lower() in source_str]
        
        if matched:
            return True, f"Found {label}: {', '.join(matched[:5])}"
        return False, ""

    i_trig, i_msg = check_list_overlap(analysis.get('iab_topics'), flag_condition.iab_topics, "IAB topics")
    flags['iab_topics'] = build_flag_entry(i_trig, i_msg)
    
    b_trig, b_msg = check_list_overlap(analysis.get('bucket_prompt'), flag_condition.bucket_prompt, "bucket prompts")
    flags['bucket_prompt'] = build_flag_entry(b_trig, b_msg)
    
    g_trig, g_msg = check_list_overlap(analysis.get('general_topics'), flag_condition.general_topics, "general topics")
    flags['general_topics'] = build_flag_entry(g_trig, g_msg)

    return flags


def apply_flag_conditions_to_segments(segments, channel, shift=None):
    """
    Apply FlagCondition checks AND Shift Duration checks to all segments.
    Adds 'flag' dictionary to each segment.
    """
    # 1. Get Policy Flag Condition
    try:
        flag_condition = FlagCondition.objects.get(channel=channel, is_active=True)
    except FlagCondition.DoesNotExist:
        flag_condition = None

    # 2. Get Duration Threshold from Shift
    duration_threshold = None
    if shift and getattr(shift, 'flag_seconds', None) is not None:
        try:
            duration_threshold = int(shift.flag_seconds)
        except (ValueError, TypeError):
            pass

    # 3. Apply checks
    for seg in segments:
        seg.setdefault('flag', {})
        
        # Apply Duration Flag
        if duration_threshold is not None:
            duration = seg.get('duration_seconds') or 0
            exceeded = bool(duration > duration_threshold)
            msg = f"Duration exceeded limit by {int(duration - duration_threshold)}s" if exceeded else ""
            seg['flag']['duration'] = {'flagged': exceeded, 'message': msg}
            
        # Apply Policy Flags
        if flag_condition:
            policy_flags = check_flag_conditions(seg, flag_condition)
            seg['flag'].update(policy_flags)
    
    return segments


def flag_entry_is_active(flag_entry):
    """Check if a flag entry is active."""
    if not isinstance(flag_entry, dict):
        return False
    return bool(flag_entry.get('flagged')) or bool(flag_entry.get('exceeded'))


def has_active_flag_condition(channel):
    return FlagCondition.objects.filter(channel=channel, is_active=True).exists()


# ==========================================
# Pagination & Response
# ==========================================

def build_pagination_info_v2(base_start_dt, base_end_dt, page, page_size, channel, valid_windows=None, 
                             status=None, content_type_list=None, search_text=None, search_in=None):
    """
    Build pagination info.
    Optimized: Returns 24 pages (one per hour) for the selected day, but only 1 page per other day
    that covers the whole day.
    """
    from datetime import datetime as dt_class
    from config.validation import TimezoneUtils

    # Use datetime in channel tz for .date() logic (convert_to_channel_tz returns ISO str)
    channel_zone = TimezoneUtils.get_channel_timezone_zone(channel.timezone)
    def to_local_dt(dt_utc):
        return dt_utc.astimezone(channel_zone) if channel_zone else dt_utc

    available_pages = []
    total_hours = math.ceil((base_end_dt - base_start_dt).total_seconds() / 3600)
    total_pages_needed = math.ceil(total_hours / page_size)

    # 1. Determine the "Target Day" (The local day of the currently selected page)
    target_page_offset = (page - 1) * page_size
    target_dt_utc = base_start_dt + timezone.timedelta(hours=target_page_offset)
    target_local_dt = to_local_dt(target_dt_utc)
    target_date = target_local_dt.date()

    # Track which dates we have already added an entry for (target day: per-hour; others: one whole-day)
    seen_dates = set()

    # 2. Iterate through all potential pages
    for page_num in range(1, total_pages_needed + 1):
        hour_offset = (page_num - 1) * page_size
        page_start = base_start_dt + timezone.timedelta(hours=hour_offset)
        page_end = page_start + timezone.timedelta(hours=page_size)

        if page_end > base_end_dt:
            page_end = base_end_dt

        is_last_page = (page_num == total_pages_needed)

        # Calculate the local date for this specific page (datetime for .date())
        current_local_dt = to_local_dt(page_start)
        current_date = current_local_dt.date()

        # ------------------------------------------------------------------
        # FILTER LOGIC:
        # A) Target day: include every hour (24 pages, one per hour)
        # B) Other days: include one page per day covering the whole day
        # ------------------------------------------------------------------
        is_target_day = (current_date == target_date)
        is_first_hour_of_other_day = (current_date != target_date and current_date not in seen_dates)

        if is_target_day:
            seen_dates.add(current_date)
            # One page per hour for the selected day
            page_query = get_segments_queryset(
                channel, page_start, page_end, valid_windows, status,
                content_type_list, search_text, search_in, is_last_page
            )
            segment_count = page_query.distinct().count()
            available_pages.append({
                'page': page_num,
                'start_time': TimezoneUtils.convert_to_channel_tz(page_start, channel.timezone),
                'end_time': TimezoneUtils.convert_to_channel_tz(page_end, channel.timezone),
                'has_data': segment_count > 0,
                'segment_count': segment_count,
                'whole_day': False,
                'is_last_page': is_last_page,
            })
        elif is_first_hour_of_other_day:
            seen_dates.add(current_date)
            # One page for the whole day (midnight to midnight in channel tz, clamped to base range)
            day_start_local = dt_class(
                current_date.year, current_date.month, current_date.day, 0, 0, 0, tzinfo=channel_zone
            )
            day_end_local = day_start_local + timezone.timedelta(days=1)
            day_start_utc = day_start_local.astimezone(dt_utc.utc)
            day_end_utc = day_end_local.astimezone(dt_utc.utc)
            day_start_utc = max(day_start_utc, base_start_dt)
            day_end_utc = min(day_end_utc, base_end_dt)
            if day_start_utc >= day_end_utc:
                continue
            page_query = get_segments_queryset(
                channel, day_start_utc, day_end_utc, valid_windows, status,
                content_type_list, search_text, search_in, is_last_page=(day_end_utc >= base_end_dt)
            )
            segment_count = page_query.distinct().count()
            available_pages.append({
                'page': page_num,
                'start_time': TimezoneUtils.convert_to_channel_tz(day_start_utc, channel.timezone),
                'end_time': TimezoneUtils.convert_to_channel_tz(day_end_utc, channel.timezone),
                'has_data': segment_count > 0,
                'segment_count': segment_count,
                'whole_day': True,
                'is_last_page': day_end_utc >= base_end_dt,
            })
        else:
            continue

    return {
        'current_page': page,
        'page_size': page_size,
        'available_pages': available_pages,
        'total_pages': total_pages_needed,
        'time_range': {
            'start': TimezoneUtils.convert_to_channel_tz(base_start_dt, channel.timezone),
            'end': TimezoneUtils.convert_to_channel_tz(base_end_dt, channel.timezone)
        }
    }