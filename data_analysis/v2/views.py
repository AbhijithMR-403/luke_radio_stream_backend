"""
V2 API views for data_analysis app.
"""

import json
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from data_analysis.v2.service import (
    validate_v2_parameters,
    apply_content_type_filter,
    build_pagination_info_v2,
    apply_search_filters
)
from data_analysis.audio_segments_helpers import (
    get_channel_and_shift,
    apply_shift_filtering,
    apply_predefined_filter_filtering,
    calculate_pagination_window,
    build_base_query,
    apply_flag_conditions_to_segments,
    has_active_flag_condition,
    flag_entry_is_active
)
from data_analysis.serializers import AudioSegmentsSerializer
from data_analysis.repositories import AudioSegmentDAO
from acr_admin.models import GeneralSetting


class ListAudioSegmentsV2View(APIView):
    """
    V2 API endpoint for listing audio segments with comprehensive filtering.
    
    Query Parameters:
    - channel_id (required): Channel ID to filter segments
    - start_datetime (required): Start datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)
    - end_datetime (required): End datetime filter
    - status (optional): Filter by active status - 'active', 'inactive', or 'both' (default: 'both')
    - shift_id (optional): Shift ID to filter segments by shift time windows
    - predefined_filter_id (optional): PredefinedFilter ID to filter segments by filter schedule time windows
    - content_type (optional): List of strings to filter by content_type_prompt. Can be passed multiple times.
      Examples:
        - Not provided: No filtering (all segments)
        - content_type=announcer: Filter for announcers only
        - content_type=Station%20ID: Filter for segments with "Station ID" in content_type_prompt
        - content_type=Announcer&content_type=Station%20ID: Filter for segments matching any of the provided types
    - page (optional): Page number (default: 1)
    - page_size (optional): Hours per page (default: 1)
    - search_text (optional): Text to search for
    - search_in (optional): Field to search in - must be one of: 'transcription', 'general_topics', 'iab_topics', 'bucket_prompt', 'summary', 'content_type_prompt', 'title'
    - show_flagged_only (optional): When set to 'true', returns only segments that have triggered flag thresholds
    
    Flagging:
    - All segments are automatically checked against FlagCondition for the channel (if configured and active)
    - Flag conditions check: transcription_keywords, summary_keywords, sentiment range, iab_topics, bucket_prompt, general_topics
    - Flag information is included in the 'flag' field of each segment
    - When shift_id is provided with flag_seconds, duration flags are also checked
    - When show_flagged_only is set to 'true', only segments matching any flag condition are returned
    
    Example URLs:
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1&page_size=1
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&shift_id=1&content_type=announcer
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&search_text=music&search_in=transcription
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&show_flagged_only=true
    """
    
    def get(self, request, *args, **kwargs):
        try:
            # Step 1: Validate and extract parameters
            params, error_response = validate_v2_parameters(request)
            if error_response:
                # Convert JsonResponse to DRF Response
                error_data = json.loads(error_response.content.decode('utf-8'))
                return Response(error_data, status=error_response.status_code)
            
            # Step 2: Get channel, shift, and predefined_filter objects
            channel, shift, predefined_filter, error_response = get_channel_and_shift(params)
            if error_response:
                # Convert JsonResponse to DRF Response
                error_data = json.loads(error_response.content.decode('utf-8'))
                return Response(error_data, status=error_response.status_code)
            
            # Step 3: Apply shift or predefined_filter filtering if provided
            valid_windows = None
            if shift:
                valid_windows = apply_shift_filtering(
                    params['base_start_dt'], 
                    params['base_end_dt'], 
                    shift
                )
                if not valid_windows:
                    # No valid time windows for this shift in the given range
                    from config.validation import TimezoneUtils
                    
                    return Response({
                        'success': True,
                        'data': [],
                        'has_data': False,
                        'pagination': {
                            'current_page': params['page'],
                            'page_size': params['page_size'],
                            'available_pages': [],
                            'total_pages': 0,
                            'time_range': {
                                'start': TimezoneUtils.convert_to_channel_tz(
                                    params['base_start_dt'], 
                                    channel.timezone
                                ),
                                'end': TimezoneUtils.convert_to_channel_tz(
                                    params['base_end_dt'], 
                                    channel.timezone
                                )
                            }
                        }
                    })
            elif predefined_filter:
                valid_windows = apply_predefined_filter_filtering(
                    params['base_start_dt'], 
                    params['base_end_dt'], 
                    predefined_filter
                )
                if not valid_windows:
                    # No valid time windows for this predefined filter in the given range
                    from config.validation import TimezoneUtils
                    
                    return Response({
                        'success': True,
                        'data': [],
                        'has_data': False,
                        'pagination': {
                            'current_page': params['page'],
                            'page_size': params['page_size'],
                            'available_pages': [],
                            'total_pages': 0,
                            'time_range': {
                                'start': TimezoneUtils.convert_to_channel_tz(
                                    params['base_start_dt'], 
                                    channel.timezone
                                ),
                                'end': TimezoneUtils.convert_to_channel_tz(
                                    params['base_end_dt'], 
                                    channel.timezone
                                )
                            }
                        }
                    })
            
            # Step 3.5: Handle show_flagged_only mode - skip pagination and return only flagged segments
            if params.get('show_flagged_only'):
                # Check if we have a way to flag (either shift with flag_seconds or FlagCondition)
                has_flag_condition = has_active_flag_condition(channel)
                
                if shift:
                    # Check if shift has flag_seconds configured
                    if getattr(shift, 'flag_seconds', None) is None and not has_flag_condition:
                        return Response({
                            'success': False,
                            'error': 'Shift does not have flag_seconds configured and no FlagCondition found. Cannot filter flagged segments.'
                        }, status=status.HTTP_400_BAD_REQUEST)
                elif not has_flag_condition:
                    # No shift and no FlagCondition
                    return Response({
                        'success': False,
                        'error': 'No FlagCondition found for this channel. Cannot filter flagged segments.'
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                # Use full time range (no pagination)
                current_page_start = params['base_start_dt']
                current_page_end = params['base_end_dt']
                is_last_page = True
                
                # Build base query for entire time range
                base_query = build_base_query(
                    channel, 
                    current_page_start, 
                    current_page_end, 
                    valid_windows, 
                    is_last_page, 
                    None,  # duration (not used in v2)
                    params.get('status'), 
                    None,  # recognition_status (not used in v2)
                    None   # has_content (not used in v2)
                )
                
                # Apply search filters if provided
                if params.get('search_text') and params.get('search_in'):
                    base_query = apply_search_filters(base_query, params['search_text'], params['search_in'])
                
                # Apply content_type filtering if provided
                if params.get('content_type'):
                    base_query = apply_content_type_filter(base_query, params['content_type'])
                
                # Execute the query
                db_segments = base_query.order_by('start_time')
                
                # Serialize segments
                all_segments = AudioSegmentsSerializer.serialize_segments_data(db_segments, channel.timezone)
                
                # Add flags to all segments (duration and FlagCondition)
                threshold = None
                if shift and getattr(shift, 'flag_seconds', None) is not None:
                    try:
                        threshold = int(shift.flag_seconds)
                    except Exception:
                        threshold = None
                
                # Apply duration flags if threshold is set
                if threshold is not None:
                    for seg in all_segments:
                        duration = seg.get('duration_seconds') or 0
                        exceeded = bool(duration > threshold)
                        message = f"Duration exceeded limit by {int(duration - threshold)} seconds" if exceeded else ""
                        seg.setdefault('flag', {})
                        seg['flag']['duration'] = {
                            'flagged': exceeded,
                            'message': message
                        }
                
                # Apply FlagCondition flags
                all_segments = apply_flag_conditions_to_segments(all_segments, channel)
                
                # Filter to only flagged segments (duration or FlagCondition)
                if threshold is not None:
                    flagged_segments = []
                    for seg in all_segments:
                        has_flag = False
                        
                        # Check duration flag
                        if flag_entry_is_active(seg.get('flag', {}).get('duration')):
                            has_flag = True
                        
                        # Check FlagCondition flags
                        for flag_key, flag_data in seg.get('flag', {}).items():
                            if flag_key != 'duration' and flag_entry_is_active(flag_data):
                                has_flag = True
                                break
                        
                        if has_flag:
                            flagged_segments.append(seg)
                    
                    all_segments = flagged_segments
                else:
                    # If no duration threshold, filter by FlagCondition flags only
                    flagged_segments = []
                    for seg in all_segments:
                        for flag_key, flag_data in seg.get('flag', {}).items():
                            if flag_entry_is_active(flag_data):
                                flagged_segments.append(seg)
                                break
                    all_segments = flagged_segments
                
                # Build response without pagination
                response_data = AudioSegmentsSerializer.build_response(all_segments, channel)
                from config.validation import TimezoneUtils
                response_data['pagination'] = {
                    'current_page': 1,
                    'page_size': params['page_size'],
                    'available_pages': [],
                    'total_pages': 0,
                    'time_range': {
                        'start': TimezoneUtils.convert_to_channel_tz(params['base_start_dt'], channel.timezone),
                        'end': TimezoneUtils.convert_to_channel_tz(params['base_end_dt'], channel.timezone)
                    }
                }
                response_data['has_data'] = len(all_segments) > 0
                
                return Response(response_data)
            
            # Step 4: Calculate pagination window
            current_page_start, current_page_end, is_last_page, error_response = calculate_pagination_window(
                params['base_start_dt'], 
                params['base_end_dt'], 
                params['page'], 
                params['page_size'], 
                params.get('search_text'),
                params.get('search_in'),
                valid_windows
            )
            if error_response:
                error_data = json.loads(error_response.content.decode('utf-8'))
                return Response(error_data, status=error_response.status_code)
            
            # Step 5: Build base query
            base_query = build_base_query(
                channel, 
                current_page_start, 
                current_page_end, 
                valid_windows, 
                is_last_page, 
                None,  # duration (not used in v2)
                params.get('status'), 
                None,  # recognition_status (not used in v2)
                None   # has_content (not used in v2)
            )
            
            # Step 6: Apply search filters if provided
            if params.get('search_text') and params.get('search_in'):
                base_query = apply_search_filters(base_query, params['search_text'], params['search_in'])
            
            # Step 7: Apply content_type filtering if provided
            if params.get('content_type'):
                base_query = apply_content_type_filter(base_query, params['content_type'])
            
            # Step 8: Execute the final query with ordering
            db_segments = base_query.order_by('start_time')
            
            # Step 9: Serialize segments
            all_segments = AudioSegmentsSerializer.serialize_segments_data(db_segments, channel.timezone)
            
            # Step 10: Add flags to all segments (duration and FlagCondition)
            threshold = None
            if shift and getattr(shift, 'flag_seconds', None) is not None:
                try:
                    threshold = int(shift.flag_seconds)
                except Exception:
                    threshold = None
            
            # Apply duration flags if threshold is set
            if threshold is not None:
                for seg in all_segments:
                    duration = seg.get('duration_seconds') or 0
                    exceeded = bool(duration > threshold)
                    message = f"Duration exceeded limit by {int(duration - threshold)} seconds" if exceeded else ""
                    seg.setdefault('flag', {})
                    seg['flag']['duration'] = {
                        'flagged': exceeded,
                        'message': message
                    }
            
            # Apply FlagCondition flags
            all_segments = apply_flag_conditions_to_segments(all_segments, channel)
            
            # Step 11: Build the complete response using serializer
            response_data = AudioSegmentsSerializer.build_response(all_segments, channel)
            
            # Step 12: Build pagination information with all filters
            response_data['pagination'] = build_pagination_info_v2(
                params['base_start_dt'], 
                params['base_end_dt'], 
                params['page'], 
                params['page_size'],
                channel, 
                valid_windows, 
                params.get('status'), 
                params.get('content_type', []),
                params.get('search_text'),
                params.get('search_in')
            )
            
            # Step 13: Add has_data flag for current page
            current_page_has_data = len(all_segments) > 0
            response_data['has_data'] = current_page_has_data
            
            return Response(response_data)
            
        except Exception as e:
            import traceback
            return Response({
                'success': False, 
                'error': str(e),
                'traceback': traceback.format_exc() if __debug__ else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ContentTypePromptView(APIView):
    """
    V2 API endpoint for getting content_type_prompt from GeneralSetting.
    
    Returns:
    - content_type_prompt: The prompt text from GeneralSetting
    - search_in: List of search_in options with their labels
    
    Example URL:
    - /api/v2/content-type-prompt/
    """
    
    def get(self, request, *args, **kwargs):
        try:
            # Get GeneralSetting object
            settings_obj = GeneralSetting.objects.first()
            
            if not settings_obj:
                return Response({
                    'success': False,
                    'error': 'GeneralSetting not found'
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Define search_in options with labels as mentioned in views.py line 439-440
            search_in = [
                {
                    'value': 'transcription',
                    'label': 'Transcription'
                },
                {
                    'value': 'general_topics',
                    'label': 'General Topics'
                },
                {
                    'value': 'iab_topics',
                    'label': 'IAB Topics'
                },
                {
                    'value': 'bucket_prompt',
                    'label': 'Bucket Prompt'
                },
                {
                    'value': 'summary',
                    'label': 'Summary'
                },
                {
                    'value': 'content_type_prompt',
                    'label': 'Content Type Prompt'
                },
                {
                    'value': 'title',
                    'label': 'Title'
                }
            ]
            
            # Convert comma-separated content_type_prompt to a list
            content_type_prompt_list = []
            if settings_obj.content_type_prompt:
                content_type_prompt_list = [
                    item.strip() 
                    for item in settings_obj.content_type_prompt.split(',') 
                    if item.strip()
                ]
            
            return Response({
                'success': True,
                'data': {
                    'content_type_prompt': content_type_prompt_list,
                    'search_in': search_in
                }
            })
            
        except Exception as e:
            import traceback
            return Response({
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc() if __debug__ else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

