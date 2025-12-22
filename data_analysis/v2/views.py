"""
V2 API views for data_analysis app.
"""

from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from data_analysis.v2.service import (
    validate_v2_parameters,
    apply_content_type_filter,
    build_pagination_info_v2
)
from data_analysis.audio_segments_helpers import (
    get_channel_and_shift,
    apply_shift_filtering,
    apply_predefined_filter_filtering,
    calculate_pagination_window,
    build_base_query
)
from data_analysis.serializers import AudioSegmentsSerializer
from data_analysis.repositories import AudioSegmentDAO
from acr_admin.models import GeneralSetting


@method_decorator(csrf_exempt, name='dispatch')
class ListAudioSegmentsV2View(View):
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
    
    Example URLs:
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1&page_size=1
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&shift_id=1&content_type=announcer
    - /api/v2/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&predefined_filter_id=1&content_type=Station%20ID
    """
    
    def get(self, request, *args, **kwargs):
        try:
            # Step 1: Validate and extract parameters
            params, error_response = validate_v2_parameters(request)
            if error_response:
                return error_response
            
            # Step 2: Get channel, shift, and predefined_filter objects
            channel, shift, predefined_filter, error_response = get_channel_and_shift(params)
            if error_response:
                return error_response
            
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
                    
                    return JsonResponse({
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
                    
                    return JsonResponse({
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
            
            # Step 4: Calculate pagination window
            current_page_start, current_page_end, is_last_page, error_response = calculate_pagination_window(
                params['base_start_dt'], 
                params['base_end_dt'], 
                params['page'], 
                params['page_size'], 
                None,  # search_text (not used in v2)
                None,  # search_in (not used in v2)
                valid_windows
            )
            if error_response:
                return error_response
            
            # Step 5: Build base query
            base_query = build_base_query(
                channel, 
                current_page_start, 
                current_page_end, 
                valid_windows, 
                is_last_page, 
                None,  # duration (not used in v2)
                params['status'], 
                None,  # recognition_status (not used in v2)
                None   # has_content (not used in v2)
            )
            
            # Step 6: Apply content_type filtering if provided
            if params['content_type']:
                base_query = apply_content_type_filter(base_query, params['content_type'])
            
            # Step 7: Execute the final query with ordering
            db_segments = base_query.order_by('start_time')
            
            # Step 8: Serialize segments
            all_segments = AudioSegmentsSerializer.serialize_segments_data(db_segments, channel.timezone)
            
            # Step 9: Build the complete response using serializer
            response_data = AudioSegmentsSerializer.build_response(all_segments, channel)
            
            # Step 10: Build pagination information with content_type filtering
            response_data['pagination'] = build_pagination_info_v2(
                params['base_start_dt'], 
                params['base_end_dt'], 
                params['page'], 
                params['page_size'],
                channel, 
                valid_windows, 
                params['status'], 
                params.get('content_type', [])  # Apply content_type filter to pagination counts
            )
            
            # Step 11: Add has_data flag for current page
            current_page_has_data = len(all_segments) > 0
            response_data['has_data'] = current_page_has_data
            
            return JsonResponse(response_data)
            
        except Exception as e:
            import traceback
            return JsonResponse({
                'success': False, 
                'error': str(e),
                'traceback': traceback.format_exc() if __debug__ else None
            }, status=500)


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

