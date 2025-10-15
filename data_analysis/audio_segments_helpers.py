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
    
    # Pagination parameters
    page = int(request.GET.get('page', 1))
    page_size = int(request.GET.get('page_size', 1))  # hours per page
    
    # Search parameters
    search_text = request.GET.get('search_text')
    search_in = request.GET.get('search_in')
    
    if not channel_pk:
        return None, JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
    
    # Validate search parameters
    if search_text and not search_in:
        return None, JsonResponse({'success': False, 'error': 'search_in parameter is required when search_text is provided'}, status=400)
    
    if search_in and not search_text:
        return None, JsonResponse({'success': False, 'error': 'search_text parameter is required when search_in is provided'}, status=400)
    
    # Validate search_in options
    valid_search_options = ['transcription', 'general_topics', 'iab_topics', 'bucket_prompt', 'summary', 'title']
    if search_in and search_in not in valid_search_options:
        return None, JsonResponse({
            'success': False, 
            'error': f'Invalid search_in option. Must be one of: {", ".join(valid_search_options)}'
        }, status=400)
    
    return {
        'channel_pk': channel_pk,
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'shift_id': shift_id,
        'page': page,
        'page_size': page_size,
        'search_text': search_text,
        'search_in': search_in
    }, None


def get_channel_and_shift(params):
    """Get channel and shift objects from database"""
    try:
        channel = Channel.objects.get(id=params['channel_pk'], is_deleted=False)
    except Channel.DoesNotExist:
        return None, None, JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
    
    shift = None
    if params['shift_id']:
        try:
            from shift_analysis.models import Shift
            shift = Shift.objects.get(id=params['shift_id'], is_active=True)
        except Shift.DoesNotExist:
            return None, None, JsonResponse({'success': False, 'error': 'Shift not found or inactive'}, status=404)
    
    return channel, shift, None


def parse_datetime_parameters(params):
    """Parse and validate datetime parameters"""
    if params['start_datetime']:
        base_start_dt = parse_dt(params['start_datetime'])
        if not base_start_dt:
            return None, None, JsonResponse({'success': False, 'error': 'Invalid start_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
    else:
        # Default to today if no start_datetime provided
        base_start_dt = timezone.make_aware(datetime.combine(timezone.now().date(), datetime.min.time()))
    
    if params['end_datetime']:
        base_end_dt = parse_dt(params['end_datetime'])
        if not base_end_dt:
            return None, None, JsonResponse({'success': False, 'error': 'Invalid end_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
    else:
        # Default to 7 days from base_start_dt if no end_datetime provided
        base_end_dt = base_start_dt + timezone.timedelta(days=7)
    
    # Ensure we don't exceed 7 days limit
    max_end_dt = base_start_dt + timezone.timedelta(days=7)
    if base_end_dt > max_end_dt:
        base_end_dt = max_end_dt
    
    return base_start_dt, base_end_dt, None


def apply_shift_filtering(base_start_dt, base_end_dt, shift):
    """Apply shift-based time filtering to the datetime range"""
    if not shift:
        return base_start_dt, base_end_dt
    
    # Use the existing utility function from shift_analysis.utils
    from shift_analysis.utils import _build_utc_windows_for_local_day
    from zoneinfo import ZoneInfo
    
    # Get shift timezone
    shift_tz = ZoneInfo(shift.timezone)
    
    # Convert base times to shift timezone for comparison
    base_start_local = base_start_dt.astimezone(shift_tz)
    base_end_local = base_end_dt.astimezone(shift_tz)
    
    # Create a list to store all valid time windows
    valid_windows = []
    
    # Iterate through each day in the range
    current_date = base_start_local.date()
    end_date = base_end_local.date()
    
    while current_date <= end_date:
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


def calculate_pagination_window(base_start_dt, base_end_dt, page, page_size, search_text, search_in, valid_windows=None):
    """Calculate the current page time window"""
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
    else:
        page_start_offset = (page - 1) * page_size
        current_page_start = base_start_dt + timezone.timedelta(hours=page_start_offset)
        current_page_end = current_page_start + timezone.timedelta(hours=page_size)
        
        # Ensure current page doesn't exceed the base_end_dt
        if current_page_start >= base_end_dt:
            return None, None, JsonResponse({
                'success': False, 
                'error': f'Page {page} is beyond the available time range'
            }, status=400)
        
        if current_page_end > base_end_dt:
            current_page_end = base_end_dt
    
    return current_page_start, current_page_end, None


def build_base_query(channel, current_page_start, current_page_end, valid_windows=None):
    """Build the base query with filter conditions"""
    # Build filter conditions for current page
    if valid_windows:
        # Use shift-based filtering
        from django.db import models
        time_conditions = models.Q()
        for window_start, window_end in valid_windows:
            time_conditions |= models.Q(
                start_time__gte=window_start,
                start_time__lt=window_end
            )
        filter_conditions = {
            'channel': channel,
        }
    else:
        filter_conditions = {
            'channel': channel,
            'start_time__gte': current_page_start,
            'start_time__lt': current_page_end
        }
        time_conditions = None
    
    # Build the base query with optimized joins
    base_query = AudioSegmentsModel.objects.filter(**filter_conditions).select_related(
        'channel'
    ).prefetch_related(
        'transcription_detail__rev_job',
        'transcription_detail__analysis'
    )
    
    if time_conditions:
        base_query = base_query.filter(time_conditions)
    
    return base_query


def apply_search_filters(base_query, search_text, search_in):
    """Apply search filters to the query"""
    if not search_text or not search_in:
        return base_query
    
    if search_in == 'transcription':
        # Search in transcription text
        base_query = base_query.filter(
            transcription_detail__transcript__icontains=search_text
        )
    elif search_in == 'general_topics':
        # Search in general topics
        base_query = base_query.filter(
            transcription_detail__analysis__general_topics__icontains=search_text
        )
    elif search_in == 'iab_topics':
        # Search in IAB topics
        base_query = base_query.filter(
            transcription_detail__analysis__iab_topics__icontains=search_text
        )
    elif search_in == 'bucket_prompt':
        # Search in bucket prompt
        base_query = base_query.filter(
            transcription_detail__analysis__bucket_prompt__icontains=search_text
        )
    elif search_in == 'summary':
        # Search in summary
        base_query = base_query.filter(
            transcription_detail__analysis__summary__icontains=search_text
        )
    elif search_in == 'title':
        # Search in title
        base_query = base_query.filter(
            title__icontains=search_text
        )
    
    return base_query


def build_pagination_info(base_start_dt, base_end_dt, page, page_size, search_text, search_in, channel, valid_windows=None):
    """Build pagination information for the response"""
    available_pages = []
    total_hours = int((base_end_dt - base_start_dt).total_seconds() / 3600)
    
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
        
        # Apply search filters
        search_query = apply_search_filters(search_query, search_text, search_in)
        segment_count = search_query.count()
        
        available_pages.append({
            'page': 1,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
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
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            }
        }
    else:
        for hour_offset in range(0, total_hours, page_size):
            page_num = (hour_offset // page_size) + 1
            page_start = base_start_dt + timezone.timedelta(hours=hour_offset)
            page_end = page_start + timezone.timedelta(hours=page_size)
            
            # Ensure page_end doesn't exceed base_end_dt
            if page_end > base_end_dt:
                page_end = base_end_dt
            
            # Count segments for this page
            if valid_windows:
                # Use shift-based filtering
                from django.db import models
                time_conditions = models.Q()
                for window_start, window_end in valid_windows:
                    # Check if this window intersects with the current page
                    intersection_start = max(page_start, window_start)
                    intersection_end = min(page_end, window_end)
                    
                    if intersection_start < intersection_end:
                        time_conditions |= models.Q(
                            start_time__gte=intersection_start,
                            start_time__lt=intersection_end
                        )
                
                page_filter = {
                    'channel': channel,
                }
                page_query = AudioSegmentsModel.objects.filter(**page_filter)
                if time_conditions:
                    page_query = page_query.filter(time_conditions)
            else:
                page_filter = {
                    'channel': channel,
                    'start_time__gte': page_start,
                    'start_time__lt': page_end
                }
                page_query = AudioSegmentsModel.objects.filter(**page_filter)
            
            # Apply search filters if they exist
            if search_text and search_in:
                page_query = apply_search_filters(page_query, search_text, search_in)
            
            segment_count = page_query.count()
            has_data = segment_count > 0
            
            available_pages.append({
                'page': page_num,
                'start_time': page_start.isoformat(),
                'end_time': page_end.isoformat(),
                'has_data': has_data,
                'segment_count': segment_count
            })
        
        return {
            'current_page': page,
            'page_size': page_size,
            'available_pages': available_pages,
            'total_pages': len(available_pages),
            'time_range': {
                'start': base_start_dt.isoformat(),
                'end': base_end_dt.isoformat()
            }
        }
