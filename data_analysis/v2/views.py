"""
V2 API views for data_analysis app.
"""

import json
import traceback
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.core.cache import cache

from data_analysis.services.custom_audio_service import CustomAudioService
from data_analysis.v2.serializer import CustomAudioDownloadSerializer
from data_analysis.models import SavedAudioSegment
from core_admin.repositories import GeneralSettingService


from data_analysis.v2.service import (
    validate_v2_parameters,
    get_channel_and_shift,
    apply_shift_filtering,
    apply_predefined_filter_filtering,
    calculate_pagination_window,
    get_segments_queryset,
    apply_flag_conditions_to_segments,
    build_pagination_info_v2,
    has_active_flag_condition,
    flag_entry_is_active
)

from data_analysis.serializers import AudioSegmentsSerializer
from config.validation import TimezoneUtils

# Cache timeout for list audio segments (seconds). Same as dashboard v2 views.
AUDIO_SEGMENTS_V2_CACHE_TIMEOUT = 300


def _audio_segments_v2_cache_key(params, channel):
    """Build a stable cache key from request params and channel."""
    start_iso = params['base_start_dt'].isoformat() if params.get('base_start_dt') else ""
    end_iso = params['base_end_dt'].isoformat() if params.get('base_end_dt') else ""
    content_type_str = ",".join(sorted(params.get('content_type') or []))
    status_val = params.get('status')
    status_str = "active" if status_val is True else ("inactive" if status_val is False else "both")
    return "data_analysis:v2:audio_segments:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s" % (
        getattr(channel, 'id', params.get('channel_pk')),
        start_iso,
        end_iso,
        params.get('shift_id') or "",
        params.get('predefined_filter_id') or "",
        params.get('page', 1),
        params.get('page_size', 1),
        status_str,
        content_type_str,
        (params.get('search_text') or "").strip(),
        params.get('search_in') or "",
        "1" if params.get('show_flagged_only') else "0",
    )


class ListAudioSegmentsV2View(APIView):
    """
    V2 API endpoint for listing audio segments with comprehensive filtering.
    """
    
    def get(self, request, *args, **kwargs):
        try:
            params, error_response = validate_v2_parameters(request)
            if error_response:
                # Convert JsonResponse to DRF Response
                return Response(json.loads(error_response.content), status=error_response.status_code)
            
            channel, shift, predefined_filter, error_response = get_channel_and_shift(params)
            if error_response:
                return Response(json.loads(error_response.content), status=error_response.status_code)

            cache_key = _audio_segments_v2_cache_key(params, channel)
            cached = cache.get(cache_key)
            if cached is not None:
                return Response(cached)
            
            valid_windows = None
            if shift:
                valid_windows = apply_shift_filtering(params['base_start_dt'], params['base_end_dt'], shift)
                if not valid_windows:
                    return self._return_empty_response(params, channel, cache_key)
            elif predefined_filter:
                valid_windows = apply_predefined_filter_filtering(params['base_start_dt'], params['base_end_dt'], predefined_filter)
                if not valid_windows:
                    return self._return_empty_response(params, channel, cache_key)

            is_flagged_mode = params.get('show_flagged_only')
            is_search_mode = params.get('search_text') and params.get('search_in')
            is_podcast = getattr(channel, 'channel_type', None) == 'podcast'
            
            skip_pagination = is_flagged_mode or is_search_mode or is_podcast

            # Pre-check for Flagged Mode
            if is_flagged_mode:
                has_condition = has_active_flag_condition(channel)
                has_shift_flag = shift and getattr(shift, 'flag_seconds', None) is not None
                
                if not (has_condition or has_shift_flag):
                    return Response({
                        'success': False,
                        'error': 'No FlagCondition or Shift Duration limit configured. Cannot filter flagged segments.'
                    }, status=status.HTTP_400_BAD_REQUEST)

            if skip_pagination:
                # Query the full requested date range
                current_page_start = params['base_start_dt']
                current_page_end = params['base_end_dt']
                is_last_page = True
            else:
                # Calculate the specific hour window for the requested page
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
                    return Response(json.loads(error_response.content), status=error_response.status_code)

            db_segments = get_segments_queryset(
                channel=channel,
                start_dt=current_page_start,
                end_dt=current_page_end,
                valid_windows=valid_windows,
                status=params.get('status'),
                content_types=params.get('content_type'),
                search_text=params.get('search_text'),
                search_in=params.get('search_in'),
                is_last_page=is_last_page
            )

            all_segments = AudioSegmentsSerializer.serialize_segments_data(db_segments, channel.timezone)
            
            # Apply Flags (Policy + Shift Duration)
            all_segments = apply_flag_conditions_to_segments(all_segments, channel, shift)

            # If Flagged Mode, filter the list in Python
            if is_flagged_mode:
                all_segments = [
                    seg for seg in all_segments 
                    if any(flag_entry_is_active(f_data) for f_data in seg.get('flag', {}).values())
                ]

            response_data = AudioSegmentsSerializer.build_response(all_segments, channel)
            if skip_pagination:
                # Return simplified pagination info for full-list modes
                response_data['pagination'] = {
                    'current_page': 1,
                    'page_size': params['page_size'],
                    'available_pages': [], 
                    'total_pages': 1,
                    'time_range': {
                        'start': TimezoneUtils.convert_to_channel_tz(params['base_start_dt'], channel.timezone),
                        'end': TimezoneUtils.convert_to_channel_tz(params['base_end_dt'], channel.timezone)
                    }
                }
            else:
                # Calculate accurate available pages / counts using the helper
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
            
            response_data['has_data'] = len(all_segments) > 0

            cache.set(cache_key, response_data, timeout=AUDIO_SEGMENTS_V2_CACHE_TIMEOUT)
            return Response(response_data)

        except Exception as e:
            # Catch-all for unexpected errors
            return Response({
                'success': False, 
                'error': str(e),
                'traceback': traceback.format_exc() if __debug__ else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _return_empty_response(self, params, channel, cache_key=None):
        """Helper to return consistent empty response structure."""
        response_data = {
            'success': True,
            'data': [],
            'has_data': False,
            'pagination': {
                'current_page': params['page'],
                'page_size': params['page_size'],
                'available_pages': [],
                'total_pages': 0,
                'time_range': {
                    'start': TimezoneUtils.convert_to_channel_tz(params['base_start_dt'], channel.timezone),
                    'end': TimezoneUtils.convert_to_channel_tz(params['base_end_dt'], channel.timezone)
                }
            }
        }
        if cache_key is not None:
            cache.set(cache_key, response_data, timeout=AUDIO_SEGMENTS_V2_CACHE_TIMEOUT)
        return Response(response_data)


class ContentTypePromptView(APIView):
    """
    V2 API endpoint for getting content_type_prompt from GeneralSetting.
    
    Query Parameters:
    - channel_id (required): Channel ID to get settings for
    
    Returns:
    - content_type_prompt: The prompt text from GeneralSetting
    - search_in: List of search_in options with their labels
    
    Example URL:
    - /api/v2/content-type-prompt/?channel_id=1
    """
    
    def get(self, request, *args, **kwargs):
        try:
            # Validate channel_id parameter
            channel_id = request.query_params.get('channel_id')
            if not channel_id:
                return Response({
                    'success': False,
                    'error': 'channel_id is required'
                }, status=status.HTTP_400_BAD_REQUEST)
                        
            # Get GeneralSetting object for the specified channel
            settings_obj = GeneralSettingService.get_active_setting(channel=channel_id, include_buckets=False)
            
            if not settings_obj:
                return Response({
                    'success': False,
                    'error': f'GeneralSetting not found for channel {channel_id}'
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


class DownloadCustomAudioV2View(APIView):
    """
    V2 API endpoint to upload and save custom audio file.
    Saves to custom_audio/{date}/{filename}. Max size 100 MB; path traversal is blocked.
    Pass either folder_id or channel_id (not both). A folder belongs to a channel; passing folder_id uses that channel and links the segment to the folder.
    If folder_id is passed, the new segment is linked to that folder via SavedAudioSegment after insertion.
    """
    def post(self, request):
        serializer = CustomAudioDownloadSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                {'success': False, 'errors': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        data = serializer.validated_data
        folder_id = data.get("folder_id")
        try:
            audio_info = CustomAudioService.insert_custom_audio_segment(
                file=data["file"],
                channel_id=data["channel_id"],
                title=data["title"],
                notes=data.get("notes"),
                recorded_at=data.get("recorded_at"),
            )
            if folder_id is not None:
                SavedAudioSegment.objects.get_or_create(
                    folder_id=folder_id,
                    audio_segment_id=audio_info["segment_id"],
                    defaults={"is_favorite": False},
                )
            return Response({
                'success': True,
                **audio_info,
            }, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response(
                {'success': False, 'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

