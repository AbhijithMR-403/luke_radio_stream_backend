"""
Helper functions for AudioSegments API
This module contains all the helper functions for the AudioSegments API to keep views.py clean and organized.
"""

from django.http import JsonResponse
from django.utils import timezone
from datetime import datetime
from acr_admin.models import Channel
from data_analysis.models import AudioSegments as AudioSegmentsModel
from data_analysis.serializers import AudioSegmentsSerializer
from data_analysis.repositories import AudioSegmentDAO
from audio_policy.models import FlagCondition


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
            # Recursively flatten nested lists
            flattened.extend(flatten_nested_list(item))
        elif item:  # Only add non-empty items
            flattened.append(str(item))
    
    return flattened


def parse_dt(value):
    if isinstance(value, str):
        if 'T' in value:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    else:
        return None
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt)
    return dt



def validate_audio_segments_parameters(request):
    """Validate and extract parameters from the request"""
    channel_pk = request.GET.get('channel_id')
    start_datetime = request.GET.get('start_datetime')
    end_datetime = request.GET.get('end_datetime')
    shift_id = request.GET.get('shift_id')
    predefined_filter_id = request.GET.get('predefined_filter_id')
    
    # Pagination parameters
    page = int(request.GET.get('page', 1))
    page_size = int(request.GET.get('page_size', 1))  # hours per page
    
    # Search parameters
    search_text = request.GET.get('search_text')
    search_in = request.GET.get('search_in')
    
    # Duration filter parameter
    duration = request.GET.get('duration')
    
    # Flagged only parameter
    show_flagged_only = request.GET.get('show_flagged_only', 'false').lower() == 'true'
    
    # Status filter parameter (is_active)
    status = request.GET.get('status')
    
    # Recognition status filter parameter
    recognition_status = request.GET.get('recognition_status')
    
    # Has content filter parameter
    has_content = request.GET.get('has_content')
    
    if not channel_pk:
        return None, JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
    
    # Validate that start_datetime and end_datetime are both provided
    if not start_datetime:
        return None, JsonResponse({'success': False, 'error': 'start_datetime is required'}, status=400)
    
    if not end_datetime:
        return None, JsonResponse({'success': False, 'error': 'end_datetime is required'}, status=400)
    
    # Validate that only one filtering mechanism is used at a time
    if shift_id and predefined_filter_id:
        return None, JsonResponse({'success': False, 'error': 'Cannot use both shift_id and predefined_filter_id simultaneously'}, status=400)
    
    # Validate search parameters
    if search_text and not search_in:
        return None, JsonResponse({'success': False, 'error': 'search_in parameter is required when search_text is provided'}, status=400)
    
    if search_in and not search_text:
        return None, JsonResponse({'success': False, 'error': 'search_text parameter is required when search_in is provided'}, status=400)
    
    # Validate search_in options
    valid_search_options = ['transcription', 'general_topics', 'iab_topics', 'bucket_prompt', 'summary', 'content_type_prompt', 'title']
    if search_in and search_in not in valid_search_options:
        return None, JsonResponse({
            'success': False, 
            'error': f'Invalid search_in option. Must be one of: {", ".join(valid_search_options)}'
        }, status=400)
    
    # Validate duration parameter
    duration_value = None
    if duration:
        try:
            duration_value = int(duration)
            if duration_value < 0:
                return None, JsonResponse({'success': False, 'error': 'duration must be a non-negative integer'}, status=400)
        except (ValueError, TypeError):
            return None, JsonResponse({'success': False, 'error': 'duration must be a valid integer'}, status=400)
    
    # Validate show_flagged_only parameter
    # if show_flagged_only and not shift_id:
    #     return None, JsonResponse({
    #         'success': False, 
    #         'error': 'show_flagged_only requires shift_id to be provided'
    #     }, status=400)
    
    # Validate status parameter (is_active)
    status_value = None
    if status:
        status_lower = status.lower()
        if status_lower == 'true':
            status_value = True
        elif status_lower == 'false':
            status_value = False
        else:
            return None, JsonResponse({
                'success': False,
                'error': 'status must be "true" or "false"'
            }, status=400)
    
    # Validate recognition_status parameter
    recognition_status_value = None
    if recognition_status:
        recognition_status_lower = recognition_status.lower()
        valid_recognition_statuses = ['all', 'recognized', 'unrecognized']
        if recognition_status_lower not in valid_recognition_statuses:
            return None, JsonResponse({
                'success': False,
                'error': f'recognition_status must be one of: {", ".join(valid_recognition_statuses)}'
            }, status=400)
        recognition_status_value = recognition_status_lower
    
    # Validate has_content parameter
    has_content_value = None
    if has_content:
        has_content_lower = has_content.lower()
        if has_content_lower == 'true':
            has_content_value = True
        elif has_content_lower == 'false':
            has_content_value = False
        else:
            return None, JsonResponse({
                'success': False,
                'error': 'has_content must be "true" or "false"'
            }, status=400)
    
    return {
        'channel_pk': channel_pk,
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'shift_id': shift_id,
        'predefined_filter_id': predefined_filter_id,
        'page': page,
        'page_size': page_size,
        'search_text': search_text,
        'search_in': search_in,
        'duration': duration_value,
        'show_flagged_only': show_flagged_only,
        'status': status_value,
        'recognition_status': recognition_status_value,
        'has_content': has_content_value
    }, None


def get_channel_and_shift(params):
    """Get channel, shift, and predefined_filter objects from database"""
    try:
        channel = Channel.objects.get(id=params['channel_pk'], is_deleted=False)
    except Channel.DoesNotExist:
        return None, None, None, JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
    
    shift = None
    if params['shift_id']:
        try:
            from shift_analysis.models import Shift
            shift = Shift.objects.get(id=params['shift_id'], is_active=True)
        except Shift.DoesNotExist:
            return None, None, None, JsonResponse({'success': False, 'error': 'Shift not found or inactive'}, status=404)
    
    predefined_filter = None
    if params['predefined_filter_id']:
        try:
            from shift_analysis.models import PredefinedFilter
            predefined_filter = PredefinedFilter.objects.get(id=params['predefined_filter_id'], is_active=True, channel=channel)
        except:
            return None, None, None, JsonResponse({'success': False, 'error': 'Predefined filter not found or inactive'}, status=404)
    
    return channel, shift, predefined_filter, None


def parse_datetime_parameters(params):
    """Parse and validate datetime parameters"""
    # Both start_datetime and end_datetime are required (validated in validate_audio_segments_parameters)
    base_start_dt = parse_dt(params['start_datetime'])
    if not base_start_dt:
        return None, None, JsonResponse({'success': False, 'error': 'Invalid start_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
    
    base_end_dt = parse_dt(params['end_datetime'])
    if not base_end_dt:
        return None, None, JsonResponse({'success': False, 'error': 'Invalid end_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
    
    # Validate that end_datetime is after start_datetime
    if base_end_dt <= base_start_dt:
        return None, None, JsonResponse({'success': False, 'error': 'end_datetime must be after start_datetime'}, status=400)
    
    return base_start_dt, base_end_dt, None


def apply_shift_filtering(base_start_dt, base_end_dt, shift):
    """Apply shift-based time filtering to the datetime range, considering days_of_week"""
    if not shift:
        return base_start_dt, base_end_dt
    
    # Use the existing utility function from shift_analysis.utils
    from shift_analysis.utils import _build_utc_windows_for_local_day
    from zoneinfo import ZoneInfo
    
    # Get channel timezone (shift no longer has its own timezone)
    shift_tz = ZoneInfo(shift.channel.timezone)
    
    # Convert base times to shift timezone for comparison
    base_start_local = base_start_dt.astimezone(shift_tz)
    base_end_local = base_end_dt.astimezone(shift_tz)
    
    # Parse the shift's days_of_week field
    shift_days = [day.strip().lower() for day in shift.days.split(',')] if shift.days else []
    
    # Create a list to store all valid time windows
    valid_windows = []
    
    # Iterate through each day in the range
    current_date = base_start_local.date()
    end_date = base_end_local.date()
    
    while current_date <= end_date:
        # Check if this day matches any of the shift's specified days
        current_day_name = current_date.strftime('%A').lower()
        
        # If shift has specific days defined, only process matching days
        if shift_days and current_day_name not in shift_days:
            current_date += timezone.timedelta(days=1)
            continue
        
        # Use the utility function to build UTC windows for this day
        day_windows = _build_utc_windows_for_local_day(
            shift.start_time, 
            shift.end_time, 
            current_date, 
            shift_tz
        )
        
        # Filter windows that intersect with our base time range
        for window_start, window_end in day_windows:
            # Find intersection with base time range
            intersection_start = max(base_start_dt, window_start)
            intersection_end = min(base_end_dt, window_end)
            
            if intersection_start < intersection_end:
                valid_windows.append((intersection_start, intersection_end))
        
        current_date += timezone.timedelta(days=1)
    
    return valid_windows


def apply_predefined_filter_filtering(base_start_dt, base_end_dt, predefined_filter):
    """Apply predefined filter-based time filtering to the datetime range"""
    if not predefined_filter:
        return []
    
    # Use the existing utility function from shift_analysis.utils
    from shift_analysis.utils import _build_utc_windows_for_local_day
    from zoneinfo import ZoneInfo
    
    # Get channel timezone
    filter_tz = ZoneInfo(predefined_filter.channel.timezone)
    
    # Convert base times to filter timezone for comparison
    base_start_local = base_start_dt.astimezone(filter_tz)
    base_end_local = base_end_dt.astimezone(filter_tz)
    
    # Create a list to store all valid time windows
    valid_windows = []
    
    # Iterate through each day in the range
    current_date = base_start_local.date()
    end_date = base_end_local.date()
    
    while current_date <= end_date:
        # Get all schedules for this day
        day_of_week = current_date.strftime('%A').lower()
        from shift_analysis.models import FilterSchedule
        schedules = FilterSchedule.objects.filter(
            predefined_filter=predefined_filter,
            day_of_week=day_of_week
        )
        
        for schedule in schedules:
            schedule_windows = _build_utc_windows_for_local_day(
                schedule.start_time,
                schedule.end_time,
                current_date,
                filter_tz
            )
            
            # Filter windows that intersect with our base time range
            for window_start, window_end in schedule_windows:
                # Find intersection with base time range
                intersection_start = max(base_start_dt, window_start)
                intersection_end = min(base_end_dt, window_end)
                
                if intersection_start < intersection_end:
                    valid_windows.append((intersection_start, intersection_end))
        
        current_date += timezone.timedelta(days=1)
    
    return valid_windows


def calculate_pagination_window(base_start_dt, base_end_dt, page, page_size, search_text, search_in, valid_windows=None):
    """Calculate the current page time window"""
    import math
    
    # If searching, collapse to a single page that spans the entire base range
    if search_text and search_in:
        if valid_windows:
            # Use the first valid window for search
            current_page_start = valid_windows[0][0]
            current_page_end = valid_windows[-1][1]
        else:
            current_page_start = base_start_dt
            current_page_end = base_end_dt
        page = 1  # force single page
        is_last_page = True  # Search always covers the full range
    else:
        page_start_offset = (page - 1) * page_size
        current_page_start = base_start_dt + timezone.timedelta(hours=page_start_offset)
        current_page_end = current_page_start + timezone.timedelta(hours=page_size)
        
        # Calculate total pages needed to determine if this is the last page
        total_hours = math.ceil((base_end_dt - base_start_dt).total_seconds() / 3600)
        total_pages_needed = math.ceil(total_hours / page_size)
        is_last_page = page == total_pages_needed
        
        # Ensure current page doesn't exceed the base_end_dt
        if current_page_start >= base_end_dt:
            return None, None, None, JsonResponse({
                'success': False, 
                'error': f'Page {page} is beyond the available time range'
            }, status=400)
        
        if current_page_end > base_end_dt:
            current_page_end = base_end_dt
    
    return current_page_start, current_page_end, is_last_page, None


def build_base_query(channel, current_page_start, current_page_end, valid_windows=None, is_last_page=False, duration=None, status=None, recognition_status=None, has_content=None):
    """Build the base query with filter conditions using AudioSegmentDAO"""
    from django.db import models
    
    # Apply time window filtering
    if valid_windows:
        # Use AudioSegmentDAO.filter() to get base queryset with all filters applied
        # Time filtering will be applied separately using window Q objects
        base_query = AudioSegmentDAO.filter(
            channel_id=channel.id,
            is_active=status,
            recognition_status=recognition_status,
            has_content=has_content,
            is_delete=False
        )
        
        # Apply duration filter if provided
        if duration is not None:
            base_query = base_query.filter(duration_seconds__gte=duration)
        # Use shift-based filtering, constrained to current page window
        time_conditions = models.Q()
        had_intersection = False
        for window_start, window_end in valid_windows:
            # Intersect each shift window with the current page window
            intersection_start = max(current_page_start, window_start)
            intersection_end = min(current_page_end, window_end)
            if intersection_start < intersection_end:
                had_intersection = True
                # Use inclusive end time for the last page to capture segments at exact end time
                if is_last_page:
                    time_conditions |= models.Q(
                        start_time__gte=intersection_start,
                        start_time__lte=intersection_end
                    )
                else:
                    time_conditions |= models.Q(
                        start_time__gte=intersection_start,
                        start_time__lt=intersection_end
                    )
        
        if had_intersection:
            base_query = base_query.filter(time_conditions)
        else:
            # No overlap between page window and any valid shift window → empty queryset
            return base_query.none()
    else:
        # Simple time range filtering - use AudioSegmentDAO with start_time/end_time
        if is_last_page:
            base_query = AudioSegmentDAO.filter(
                channel_id=channel.id,
                start_time=current_page_start,
                end_time=current_page_end,
                is_active=status,
                recognition_status=recognition_status,
                has_content=has_content,
                is_delete=False
            )
            # Override end_time filter to use lte instead of lt for last page
            base_query = base_query.filter(
                start_time__gte=current_page_start,
                start_time__lte=current_page_end
            )
        else:
            base_query = AudioSegmentDAO.filter(
                channel_id=channel.id,
                start_time=current_page_start,
                end_time=current_page_end,
                is_active=status,
                recognition_status=recognition_status,
                has_content=has_content,
                is_delete=False
            )
        
        # Apply duration filter if provided
        if duration is not None:
            base_query = base_query.filter(duration_seconds__gte=duration)
    
    # Add prefetch_related for optimized data loading (AudioSegmentDAO already has select_related)
    base_query = base_query.prefetch_related(
        'transcription_detail__rev_job',
        'transcription_detail__analysis'
    )
    
    return base_query


def apply_search_filters(base_query, search_text, search_in):
    """Apply search filters to the query"""
    if not search_text or not search_in:
        return base_query
    filter_applied = False
    
    if search_in == 'transcription':
        # Search in transcription text
        base_query = base_query.filter(
            transcription_detail__transcript__icontains=search_text
        )
        filter_applied = True
    elif search_in == 'general_topics':
        # Search in general topics
        base_query = base_query.filter(
            transcription_detail__analysis__general_topics__icontains=search_text
        )
        filter_applied = True
    elif search_in == 'iab_topics':
        # Search in IAB topics
        base_query = base_query.filter(
            transcription_detail__analysis__iab_topics__icontains=search_text
        )
        filter_applied = True
    elif search_in == 'bucket_prompt':
        # Search in bucket prompt
        base_query = base_query.filter(
            transcription_detail__analysis__bucket_prompt__icontains=search_text
        )
        filter_applied = True
    elif search_in == 'summary':
        # Search in summary
        base_query = base_query.filter(
            transcription_detail__analysis__summary__icontains=search_text
        )
        filter_applied = True
    elif search_in == 'content_type_prompt':
        # Search in content type prompt
        base_query = base_query.filter(
            transcription_detail__analysis__content_type_prompt__icontains=search_text
        )
        filter_applied = True
    elif search_in == 'title':
        # Search in title
        base_query = base_query.filter(
            title__icontains=search_text
        )
        filter_applied = True
    
    # Ensure no duplicate rows from JOINs when counting or listing
    if filter_applied:
        base_query = base_query.distinct()
    return base_query


def build_pagination_info(base_start_dt, base_end_dt, page, page_size, search_text, search_in, channel, valid_windows=None, duration=None, status=None, recognition_status=None, has_content=None):
    """Build pagination information for the response"""
    import math
    from config.validation import TimezoneUtils
    
    available_pages = []
    total_hours = math.ceil((base_end_dt - base_start_dt).total_seconds() / 3600)
    
    if search_text and search_in:
        # Single page covering the entire range when searching
        if valid_windows:
            start_time = valid_windows[0][0]
            end_time = valid_windows[-1][1]
        else:
            start_time = base_start_dt
            end_time = base_end_dt
            
        # Use AudioSegmentDAO.filter() as base, then apply time windows and search
        from django.db import models
        
        search_query = AudioSegmentDAO.filter(
            channel_id=channel.id,
            is_active=status,
            recognition_status=recognition_status,
            has_content=has_content,
            is_delete=False
        )
        
        # Apply duration filter if provided
        if duration is not None:
            search_query = search_query.filter(duration_seconds__gte=duration)
        
        # Apply time window filtering
        if valid_windows:
            time_conditions = models.Q()
            for window_start, window_end in valid_windows:
                time_conditions |= models.Q(
                    start_time__gte=window_start,
                    start_time__lt=window_end
                )
            search_query = search_query.filter(time_conditions)
        else:
            search_query = search_query.filter(
                start_time__gte=start_time,
                start_time__lt=end_time
            )
        
        # Apply search filters
        search_query = apply_search_filters(search_query, search_text, search_in)
        # Count distinct segments to avoid duplicates due to JOINs
        segment_count = search_query.distinct().count()
        
        available_pages.append({
            'page': 1,
            'start_time': TimezoneUtils.convert_to_channel_tz(start_time, channel.timezone),
            'end_time': TimezoneUtils.convert_to_channel_tz(end_time, channel.timezone),
            'has_data': segment_count > 0,
            'segment_count': segment_count
        })
        computed_page_size = total_hours or 1
    
        return {
            'current_page': 1,
            'page_size': computed_page_size,
            'available_pages': available_pages,
            'total_pages': 1,
            'time_range': {
                'start': TimezoneUtils.convert_to_channel_tz(start_time, channel.timezone),
                'end': TimezoneUtils.convert_to_channel_tz(end_time, channel.timezone)
            }
        }
    else:
        # Calculate total pages needed to cover the entire time range
        total_pages_needed = math.ceil(total_hours / page_size)
        
        for page_num in range(1, total_pages_needed + 1):
            hour_offset = (page_num - 1) * page_size
            page_start = base_start_dt + timezone.timedelta(hours=hour_offset)
            page_end = page_start + timezone.timedelta(hours=page_size)
            
            # For the last page, ensure we include all remaining time up to base_end_dt
            if page_end > base_end_dt:
                page_end = base_end_dt
            
            # Count segments for this page
            if valid_windows:
                # Use shift-based filtering
                from django.db import models
                time_conditions = models.Q()
                had_intersection = False
                for window_start, window_end in valid_windows:
                    # Check if this window intersects with the current page
                    intersection_start = max(page_start, window_start)
                    intersection_end = min(page_end, window_end)
                    
                    if intersection_start < intersection_end:
                        had_intersection = True
                        # Use inclusive end time for the last page to capture segments at exact end time
                        is_last_page = page_num == total_pages_needed
                        if is_last_page:
                            time_conditions |= models.Q(
                                start_time__gte=intersection_start,
                                start_time__lte=intersection_end
                            )
                        else:
                            time_conditions |= models.Q(
                                start_time__gte=intersection_start,
                                start_time__lt=intersection_end
                            )
                
                # Use AudioSegmentDAO.filter() as base, then apply time windows
                page_query = AudioSegmentDAO.filter(
                    channel_id=channel.id,
                    is_active=status,
                    recognition_status=recognition_status,
                    has_content=has_content,
                    is_delete=False
                )
                
                # Apply duration filter if provided
                if duration is not None:
                    page_query = page_query.filter(duration_seconds__gte=duration)
                
                if had_intersection:
                    page_query = page_query.filter(time_conditions)
                else:
                    # No overlap between this page window and any valid shift window
                    segment_count = 0
                    has_data = False
                    available_pages.append({
                        'page': page_num,
                        'start_time': TimezoneUtils.convert_to_channel_tz(page_start, channel.timezone),
                        'end_time': TimezoneUtils.convert_to_channel_tz(page_end, channel.timezone),
                        'has_data': has_data,
                        'segment_count': segment_count
                    })
                    continue
            else:
                # Use AudioSegmentDAO.filter() as base, then apply time range and duration
                is_last_page = page_num == total_pages_needed
                
                page_query = AudioSegmentDAO.filter(
                    channel_id=channel.id,
                    start_time=page_start,
                    end_time=page_end,
                    is_active=status,
                    recognition_status=recognition_status,
                    has_content=has_content,
                    is_delete=False
                )
                
                # For last page, override end_time filter to use lte instead of lt
                if is_last_page:
                    page_query = page_query.filter(
                        start_time__gte=page_start,
                        start_time__lte=page_end
                    )
                
                # Apply duration filter if provided
                if duration is not None:
                    page_query = page_query.filter(duration_seconds__gte=duration)
            
            # Apply search filters if they exist (for both window and non-window cases)
            if search_text and search_in:
                page_query = apply_search_filters(page_query, search_text, search_in)
            
            # Count distinct segments to avoid duplicates due to JOINs
            segment_count = page_query.distinct().count()
            has_data = segment_count > 0
            
            available_pages.append({
                'page': page_num,
                'start_time': TimezoneUtils.convert_to_channel_tz(page_start, channel.timezone),
                'end_time': TimezoneUtils.convert_to_channel_tz(page_end, channel.timezone),
                'has_data': has_data,
                'segment_count': segment_count
            })
        
        return {
            'current_page': page,
            'page_size': page_size,
            'available_pages': available_pages,
            'total_pages': len(available_pages),
            'time_range': {
                'start': TimezoneUtils.convert_to_channel_tz(base_start_dt, channel.timezone),
                'end': TimezoneUtils.convert_to_channel_tz(base_end_dt, channel.timezone)
            }
        }


def check_flag_conditions(segment, flag_condition):
    """
    Check if a segment matches the flag condition criteria.
    Returns a dictionary with flag information for each condition type.
    
    Args:
        segment: Dictionary containing segment data (from serialize_segments_data)
        flag_condition: FlagCondition model instance
    
    Returns:
        Dictionary with flag information, e.g.:
        {
            'transcription_keywords': {'flagged': True, 'message': '...'},
            'summary_keywords': {'flagged': False, 'message': ''},
            'sentiment': {'flagged': True, 'message': '...'},
            ...
        }
    """
    def build_flag_entry(triggered=False, message=''):
        return {
            'flagged': bool(triggered),
            'message': message or ''
        }
    
    flags = {}
    transcription = segment.get('transcription') if isinstance(segment.get('transcription'), dict) else {}
    analysis = segment.get('analysis') if isinstance(segment.get('analysis'), dict) else {}
    
    # Check transcription keywords
    if flag_condition.transcription_keywords:
        transcript = transcription.get('transcript') or ''
        transcript_lower = transcript.lower()
        matched_groups = []
        
        for keyword_group in flag_condition.transcription_keywords:
            if isinstance(keyword_group, list):
                # Check if any keyword in the group matches
                for keyword in keyword_group:
                    if keyword and keyword.lower() in transcript_lower:
                        matched_groups.append(keyword_group)
                        break
        
        if matched_groups:
            flags['transcription_keywords'] = build_flag_entry(
                True,
                f"Found matching keywords in transcription: {', '.join([', '.join(group) for group in matched_groups[:3]])}"
            )
        else:
            flags['transcription_keywords'] = build_flag_entry(False, '')
    
    # Check summary keywords
    if flag_condition.summary_keywords:
        summary = analysis.get('summary') or ''
        summary_lower = summary.lower()
        matched_groups = []
        
        for keyword_group in flag_condition.summary_keywords:
            if isinstance(keyword_group, list):
                # Check if any keyword in the group matches
                for keyword in keyword_group:
                    if keyword and keyword.lower() in summary_lower:
                        matched_groups.append(keyword_group)
                        break
        
        if matched_groups:
            flags['summary_keywords'] = build_flag_entry(
                True,
                f"Found matching keywords in summary: {', '.join([', '.join(group) for group in matched_groups[:3]])}"
            )
        else:
            flags['summary_keywords'] = build_flag_entry(False, '')
    
    # Check sentiment range (flag when sentiment is WITHIN the configured range)
    if flag_condition.sentiment_min is not None or flag_condition.sentiment_max is not None:
        sentiment_str = analysis.get('sentiment') or ''
        sentiment_value = None
        
        # Try to parse sentiment as float
        try:
            sentiment_value = float(sentiment_str)
        except (ValueError, TypeError):
            # If not a number, try to extract number from string
            import re
            numbers = re.findall(r'-?\d+\.?\d*', sentiment_str)
            if numbers:
                try:
                    sentiment_value = float(numbers[0])
                except (ValueError, TypeError):
                    pass
        
        triggered = False
        message = ''
        
        if sentiment_value is not None:
            meets_min = flag_condition.sentiment_min is None or sentiment_value >= flag_condition.sentiment_min
            meets_max = flag_condition.sentiment_max is None or sentiment_value <= flag_condition.sentiment_max
            triggered = meets_min and meets_max
            if triggered:
                range_msg = []
                if flag_condition.sentiment_min is not None:
                    range_msg.append(f">= {flag_condition.sentiment_min}")
                if flag_condition.sentiment_max is not None:
                    range_msg.append(f"<= {flag_condition.sentiment_max}")
                message = f"Sentiment {sentiment_value} is within configured range ({' and '.join(range_msg)})"
            else:
                message = f"Sentiment {sentiment_value} is outside configured range"
        elif sentiment_str:
            message = f"Sentiment '{sentiment_str}' cannot be compared to numeric range"
        
        flags['sentiment'] = build_flag_entry(triggered, message)
    
    # Check IAB topics
    if flag_condition.iab_topics:
        # Flatten nested lists from flag_condition
        flag_topics_flat = flatten_nested_list(flag_condition.iab_topics)
        
        iab_topics = analysis.get('iab_topics') or ''
        # Handle both string and list cases for segment's iab_topics
        if isinstance(iab_topics, list):
            iab_topics_flat = flatten_nested_list(iab_topics)
            iab_topics_lower = ' '.join(iab_topics_flat).lower()
        else:
            iab_topics_lower = str(iab_topics).lower()
        
        matched_topics = []
        
        for topic in flag_topics_flat:
            if topic and topic.lower() in iab_topics_lower:
                matched_topics.append(topic)
        
        if matched_topics:
            flags['iab_topics'] = build_flag_entry(
                True,
                f"Found matching IAB topics: {', '.join(matched_topics[:5])}"
            )
        else:
            flags['iab_topics'] = build_flag_entry(False, '')
    
    # Check bucket prompt
    if flag_condition.bucket_prompt:
        # Flatten nested lists from flag_condition
        flag_prompts_flat = flatten_nested_list(flag_condition.bucket_prompt)
        
        bucket_prompt = analysis.get('bucket_prompt') or ''
        # Handle both string and list cases for segment's bucket_prompt
        if isinstance(bucket_prompt, list):
            bucket_prompt_flat = flatten_nested_list(bucket_prompt)
            bucket_prompt_lower = ' '.join(bucket_prompt_flat).lower()
        else:
            bucket_prompt_lower = str(bucket_prompt).lower()
        
        matched_prompts = []
        
        for prompt in flag_prompts_flat:
            if prompt and prompt.lower() in bucket_prompt_lower:
                matched_prompts.append(prompt)
        
        if matched_prompts:
            flags['bucket_prompt'] = build_flag_entry(
                True,
                f"Found matching bucket prompts: {', '.join(matched_prompts[:5])}"
            )
        else:
            flags['bucket_prompt'] = build_flag_entry(False, '')
    
    # Check general topics
    if flag_condition.general_topics:
        # Flatten nested lists from flag_condition
        flag_topics_flat = flatten_nested_list(flag_condition.general_topics)
        
        general_topics = analysis.get('general_topics') or ''
        # Handle both string and list cases for segment's general_topics
        if isinstance(general_topics, list):
            general_topics_flat = flatten_nested_list(general_topics)
            general_topics_lower = ' '.join(general_topics_flat).lower()
        else:
            general_topics_lower = str(general_topics).lower()
        
        matched_topics = []
        
        for topic in flag_topics_flat:
            if topic and topic.lower() in general_topics_lower:
                matched_topics.append(topic)
        
        if matched_topics:
            flags['general_topics'] = build_flag_entry(
                True,
                f"Found matching general topics: {', '.join(matched_topics[:5])}"
            )
        else:
            flags['general_topics'] = build_flag_entry(False, '')
    
    return flags


def apply_flag_conditions_to_segments(segments, channel):
    """
    Apply FlagCondition checks to all segments and add flag information.
    Merges with existing flags if they exist.
    
    Args:
        segments: List of segment dictionaries
        channel: Channel model instance
    
    Returns:
        List of segments with flag information added
    """
    # Get FlagCondition for the channel if it exists and is active
    try:
        flag_condition = FlagCondition.objects.get(channel=channel, is_active=True)
    except FlagCondition.DoesNotExist:
        # No flag condition configured, return segments as-is
        return segments
    
    # Apply flag conditions to each segment
    for seg in segments:
        flag_info = check_flag_conditions(seg, flag_condition)
        
        # Merge with existing flags if they exist
        if 'flag' in seg:
            seg['flag'].update(flag_info)
        else:
            seg['flag'] = flag_info
    
    return segments


def flag_entry_is_active(flag_entry):
    """
    Helper to determine if a flag entry is active.
    Supports both the new 'flagged' key and legacy 'exceeded' key.
    """
    if not isinstance(flag_entry, dict):
        return False
    if 'flagged' in flag_entry:
        return bool(flag_entry['flagged'])
    return bool(flag_entry.get('exceeded'))


def has_active_flag_condition(channel):
    """
    Check if a channel has an active FlagCondition configured.
    
    Args:
        channel: Channel model instance
    
    Returns:
        bool: True if channel has an active FlagCondition, False otherwise
    """
    try:
        FlagCondition.objects.get(channel=channel, is_active=True)
        return True
    except FlagCondition.DoesNotExist:
        return False
