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
    if show_flagged_only and not shift_id:
        return None, JsonResponse({
            'success': False, 
            'error': 'show_flagged_only requires shift_id to be provided'
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
        'show_flagged_only': show_flagged_only
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


def build_base_query(channel, current_page_start, current_page_end, valid_windows=None, is_last_page=False, duration=None):
    """Build the base query with filter conditions"""
    # Build filter conditions for current page
    if valid_windows:
        # Use shift-based filtering, constrained to current page window
        from django.db import models
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
        filter_conditions = {
            'channel': channel,
        }
    else:
        # Use inclusive end time for the last page to capture segments at exact end time
        if is_last_page:
            filter_conditions = {
                'channel': channel,
                'start_time__gte': current_page_start,
                'start_time__lte': current_page_end
            }
        else:
            filter_conditions = {
                'channel': channel,
                'start_time__gte': current_page_start,
                'start_time__lt': current_page_end
            }
        time_conditions = None
    
    # Add duration filter if provided
    if duration is not None:
        filter_conditions['duration_seconds__gte'] = duration
    
    # Build the base query with optimized joins
    base_query = AudioSegmentsModel.objects.filter(**filter_conditions).select_related(
        'channel'
    ).prefetch_related(
        'transcription_detail__rev_job',
        'transcription_detail__analysis'
    )
    
    if valid_windows:
        if had_intersection:
            base_query = base_query.filter(time_conditions)
        else:
            # No overlap between page window and any valid shift window → empty queryset
            return base_query.none()
    elif time_conditions:
        base_query = base_query.filter(time_conditions)
    
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


def build_pagination_info(base_start_dt, base_end_dt, page, page_size, search_text, search_in, channel, valid_windows=None, duration=None):
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
            
        # Count segments for this search
        search_query = AudioSegmentsModel.objects.filter(channel=channel)
        if valid_windows:
            from django.db import models
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
        
        # Apply duration filter if provided
        if duration is not None:
            search_query = search_query.filter(duration_seconds__gte=duration)
        
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
                
                page_filter = {
                    'channel': channel,
                }
                # Add duration filter if provided
                if duration is not None:
                    page_filter['duration_seconds__gte'] = duration
                
                page_query = AudioSegmentsModel.objects.filter(**page_filter)
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
                # Use inclusive end time for the last page to capture segments at exact end time
                is_last_page = page_num == total_pages_needed
                if is_last_page:
                    page_filter = {
                        'channel': channel,
                        'start_time__gte': page_start,
                        'start_time__lte': page_end
                    }
                else:
                    page_filter = {
                        'channel': channel,
                        'start_time__gte': page_start,
                        'start_time__lt': page_end
                    }
                # Add duration filter if provided
                if duration is not None:
                    page_filter['duration_seconds__gte'] = duration
                
                page_query = AudioSegmentsModel.objects.filter(**page_filter)
            
            # Apply search filters if they exist
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
