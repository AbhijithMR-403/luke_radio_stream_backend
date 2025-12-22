"""
Service module for data_analysis v2 API endpoints.
Contains helper functions for filtering and validation.
"""

import re
import math
from django.http import JsonResponse
from django.db.models import Q
from django.utils import timezone
from typing import List, Optional, Tuple
from data_analysis.repositories import AudioSegmentDAO


def apply_content_type_filter(query, content_type_list: List[str]):
    """
    Apply content_type filtering to a queryset based on content_type_prompt field.
    
    Args:
        query: Django QuerySet of AudioSegments
        content_type_list: List of strings to filter by. Examples:
            - Empty list: No filtering (returns query as-is)
            - ['Announcer']: Filter for segments with "Announcer, 95%" in content_type_prompt
            - ['Station ID']: Filter for segments with "Station ID, 90%" in content_type_prompt
            - ['Announcer', 'Station ID']: Filter for segments matching any of the provided types
    
    Returns:
        Filtered QuerySet
        
    Logic:
        - Empty list: No filtering
        - Values must appear as a separate comma-separated value in content_type_prompt
        - Matches "Announcer, 95%" but NOT "Announcer Fundraising Output, 90%"
    """
    if not content_type_list:
        return query
    
    # Build Q objects for each content type in the list
    content_type_conditions = Q()
    
    for content_type_value in content_type_list:
        # Match if value appears as a separate comma-separated value
        # Pattern: "Announcer, 95%" or "Station ID, 90%" or "..., Station ID, ..."
        # This will match "Announcer, 95%" but NOT "Announcer Fundraising Output, 90%"
        value = content_type_value.strip()
        if not value:
            continue
            
        # Escape special regex characters in the content_type_value
        escaped_value = re.escape(value)
        
        # Match: value followed by comma (with optional spaces) - ensures it's a separate value
        # Pattern: Match value at start of string OR after comma, followed by comma
        # This ensures we match "Announcer, 95%" but not "Announcer Fundraising Output, 90%"
        # The pattern (?:^|,\s*) means: start of string OR comma followed by optional spaces
        pattern = rf'(?:^|,\s*){escaped_value}\s*,'
        
        content_type_conditions |= Q(
            transcription_detail__analysis__content_type_prompt__iregex=pattern
        )
    
    # Apply the filter - segments must have transcription_detail and analysis
    # Ensure we have the necessary joins for the filter to work
    query = query.filter(
        transcription_detail__isnull=False,
        transcription_detail__analysis__isnull=False,
        transcription_detail__analysis__content_type_prompt__isnull=False
    ).filter(content_type_conditions)
    
    return query.distinct()


def apply_search_filters(base_query, search_text, search_in):
    """
    Apply search filters to the query.
    
    Args:
        base_query: Django QuerySet of AudioSegments
        search_text: Text to search for
        search_in: Field to search in - must be one of: 'transcription', 'general_topics', 
                   'iab_topics', 'bucket_prompt', 'summary', 'content_type_prompt', 'title'
    
    Returns:
        Filtered QuerySet with distinct() applied if filter was applied
    """
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


def validate_v2_parameters(request):
    """
    Validate and extract parameters for v2 audio segments API using serializer.
    
    Args:
        request: Django HttpRequest object
        
    Returns:
        Tuple of (params_dict, error_response):
            - params_dict: Dictionary of validated parameters or None if error
            - error_response: JsonResponse with error or None if valid
    """
    from data_analysis.v2.serializer import ListAudioSegmentsV2QuerySerializer
    
    # Prepare data for serializer - QueryDict needs special handling for list fields
    # Convert QueryDict to regular dict, handling multiple values for content_type
    data = dict(request.GET.items())
    # For content_type, get all values using getlist
    if 'content_type' in request.GET:
        data['content_type'] = request.GET.getlist('content_type')
    
    serializer = ListAudioSegmentsV2QuerySerializer(data=data)
    
    if not serializer.is_valid():
        # Format serializer errors for response
        errors = serializer.errors
        error_messages = []
        
        for field, field_errors in errors.items():
            if isinstance(field_errors, list):
                for error in field_errors:
                    if field == 'non_field_errors':
                        error_messages.append(str(error))
                    else:
                        error_messages.append(f"{field}: {str(error)}")
            else:
                error_messages.append(f"{field}: {str(field_errors)}")
        
        error_message = '; '.join(error_messages) if error_messages else 'Validation error'
        return None, JsonResponse({
            'success': False,
            'error': error_message,
            'errors': errors
        }, status=400)
    
    # Get validated data
    validated_data = serializer.validated_data
    
    # Convert to format expected by view (with channel_pk instead of channel_id)
    params = {
        'channel_pk': validated_data['channel_id'],
        'start_datetime': validated_data['start_datetime'],
        'end_datetime': validated_data['end_datetime'],
        'shift_id': validated_data.get('shift_id'),
        'predefined_filter_id': validated_data.get('predefined_filter_id'),
        'page': validated_data.get('page', 1),
        'page_size': validated_data.get('page_size', 1),
        'status': validated_data.get('status'),  # Already converted to True/False/None
        'content_type': validated_data.get('content_type', []),  # List of strings
        'base_start_dt': validated_data['base_start_dt'],
        'base_end_dt': validated_data['base_end_dt'],
        'search_text': validated_data.get('search_text'),
        'search_in': validated_data.get('search_in'),
        'show_flagged_only': validated_data.get('show_flagged_only', False)
    }
    
    return params, None


def build_pagination_info_v2(base_start_dt, base_end_dt, page, page_size, channel, valid_windows=None, status=None, content_type_list=None, search_text=None, search_in=None):
    """
    Build pagination information for v2 API with content_type filtering support.
    
    This is a v2-specific version that applies content_type filtering to segment counts.
    
    Args:
        base_start_dt: Start datetime for the entire range
        base_end_dt: End datetime for the entire range
        page: Current page number
        page_size: Hours per page
        channel: Channel object
        valid_windows: List of (start, end) tuples for shift/predefined filter windows
        status: Filter by active status (True/False/None)
        content_type_list: List of content type strings to filter by
        
    Returns:
        Dictionary with pagination information including accurate segment counts
    """
    from config.validation import TimezoneUtils
    from django.db import models
    
    available_pages = []
    total_hours = math.ceil((base_end_dt - base_start_dt).total_seconds() / 3600)
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
                is_delete=False
            )
            
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
            # Use AudioSegmentDAO.filter() as base, then apply time range
            is_last_page = page_num == total_pages_needed
            
            page_query = AudioSegmentDAO.filter(
                channel_id=channel.id,
                start_time=page_start,
                end_time=page_end,
                is_active=status,
                is_delete=False
            )
            
            # For last page, override end_time filter to use lte instead of lt
            if is_last_page:
                page_query = page_query.filter(
                    start_time__gte=page_start,
                    start_time__lte=page_end
                )
        
        # Apply content_type filtering if provided
        if content_type_list:
            page_query = apply_content_type_filter(page_query, content_type_list)
        
        # Apply search filters if provided
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

