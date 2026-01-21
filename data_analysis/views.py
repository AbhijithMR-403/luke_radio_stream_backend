from datetime import datetime
import json
import os
import time
from urllib.parse import unquote, urlparse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.http import FileResponse, JsonResponse
from django.utils import timezone
from decouple import config
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAdminUser
from django.conf import settings
import re

from core_admin.models import Channel
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel, TranscriptionDetail, TranscriptionQueue
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.tasks import analyze_transcription_task
from data_analysis.serializers import AudioSegmentsSerializer, AudioSegmentBulkUpdateRequestSerializer

# Import helper functions from the separate module
from .audio_segments_helpers import (
    validate_audio_segments_parameters,
    get_channel_and_shift,
    parse_datetime_parameters,
    apply_shift_filtering,
    apply_predefined_filter_filtering,
    calculate_pagination_window,
    build_base_query,
    apply_search_filters,
    build_pagination_info,
    check_flag_conditions,
    apply_flag_conditions_to_segments,
    flag_entry_is_active,
    has_active_flag_condition
)

# Create your views here.
# Parse datetimes (accept ISO or 'YYYY-MM-DD HH:MM:SS')
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





@method_decorator(csrf_exempt, name='dispatch')
class AudioSegmentBulkIsActiveUpdateView(APIView):
    permission_classes = [IsAdminUser]

    def patch(self, request, *args, **kwargs):
        try:
            # Parse request data
            if hasattr(request, 'data'):
                payload = request.data
            else:
                try:
                    payload = json.loads(request.body)
                except json.JSONDecodeError:
                    return Response({'success': False, 'error': 'Invalid JSON body'}, status=status.HTTP_400_BAD_REQUEST)

            # Validate request data using serializer
            serializer = AudioSegmentBulkUpdateRequestSerializer(data=payload)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'error': 'Validation failed',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            validated_data = serializer.validated_data
            segment_ids = validated_data['segment_ids']
            is_active_value = validated_data['is_active']

            # Fetch all segments at once
            segments = AudioSegmentsModel.objects.filter(id__in=segment_ids)
            found_ids = set(segments.values_list('id', flat=True))
            not_found_ids = set(segment_ids) - found_ids

            if not_found_ids:
                return Response({
                    'success': False,
                    'error': f'Audio segments not found: {sorted(not_found_ids)}'
                }, status=status.HTTP_404_NOT_FOUND)

            # Bulk update all segments
            updated_count = segments.update(
                is_active=is_active_value,
                is_manually_processed=True
            )

            # Fetch updated segments for response (refresh from database)
            updated_segments = AudioSegmentsModel.objects.filter(id__in=segment_ids).values('id', 'is_active', 'start_time', 'end_time')

            # Build response
            response_data = {
                'success': True,
                'message': f'Successfully updated {updated_count} audio segment(s)',
                'data': {
                    'updated_count': updated_count,
                    'is_active': is_active_value,
                    'segments': [
                        {
                            'segment_id': seg['id'],
                            'is_active': seg['is_active'],
                            'start_time': seg['start_time'].isoformat() if seg['start_time'] else None,
                            'end_time': seg['end_time'].isoformat() if seg['end_time'] else None
                        }
                        for seg in updated_segments
                    ]
                }
            }

            return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({'success': False, 'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class PieChartDataView(View):
    """
    Returns pie chart data for audio segments within a datetime range.

    Query Parameters:
    - start_datetime (required): ISO or 'YYYY-MM-DD HH:MM:SS'
    - channel_id (optional): Filter by channel

    Response:
    {
        "success": true,
        "data": [
            { "title": "Some Title" | "undefined", "value": 123 }
        ],
        "count": 10
    }
    """
    def get(self, request, *args, **kwargs):
        try:
            start_param = request.GET.get('start_datetime')
            channel_pk = request.GET.get('channel_id')

            if not start_param:
                return JsonResponse({
                    'success': False,
                    'error': 'start_datetime is required'
                }, status=400)

            start_dt = parse_dt(start_param)
            # Define a strict 60-minute window starting from start_dt
            window_end = start_dt + timezone.timedelta(minutes=60)
            # If end_param is provided and extends before window_end, still enforce 60 minutes window
            # If end_param extends beyond, ignore the extra (clip to 60 minutes)

            if not start_dt:
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid datetime format. Use ISO or YYYY-MM-DD HH:MM:SS'
                }, status=400)

            filter_conditions = {
                'start_time__lte': window_end,
                'end_time__gte': start_dt,
            }

            if channel_pk:
                try:
                    channel = Channel.objects.get(id=channel_pk, is_deleted=False)
                    filter_conditions['channel'] = channel
                except Channel.DoesNotExist:
                    return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)

            segments = (
                AudioSegmentsModel.objects
                .filter(**filter_conditions)
                .only('id', 'duration_seconds', 'title', 'start_time', 'end_time', 'is_recognized', 'is_active', 'metadata_json')
                .order_by('start_time')
            )

            data = []
            total_accumulated = 0
            for seg in segments:
                # compute overlap within the 60-minute window
                overlap_start = max(seg.start_time, start_dt)
                overlap_end = min(seg.end_time, window_end)
                overlap_seconds = int(max(0, (overlap_end - overlap_start).total_seconds()))
                
                if overlap_seconds > 0 and total_accumulated < 3600:
                    remaining = 3600 - total_accumulated
                    to_add = min(overlap_seconds, remaining)
                    if to_add > 0:
                        # Calculate position within the 60-minute window (0-3600 seconds)
                        segment_start_position = int((overlap_start - start_dt).total_seconds())
                        segment_end_position = int((overlap_end - start_dt).total_seconds())
                        
                        # Determine category based on the 4 types
                        category = self._get_segment_category(seg)
                        
                        data.append({
                            'title': seg.title if seg.title else 'undefined',
                            'value': [segment_start_position, segment_end_position],  # [start_pos, end_pos] in seconds from window start
                            'category': category,
                            'start_time': seg.start_time.isoformat() if seg.start_time else None,
                            'end_time': seg.end_time.isoformat() if seg.end_time else None,
                            'created_by': seg.source,
                            'overlap_start': overlap_start.isoformat(),
                            'overlap_end': overlap_end.isoformat(),
                        })
                        total_accumulated += to_add
                    if total_accumulated >= 3600:
                        break

            return JsonResponse({'success': True, 'data': data, 'count': len(data), 'window': {
                'start': start_dt.isoformat(),
                'end': window_end.isoformat()
            }})

        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def _get_segment_category(self, segment):
        """
        Categorize audio segment into 4 types:
        1. music: metadata_json contains {"source": "music", ...}
        2. unrecognised_with_content: is_recognized=False AND is_active=True
        3. unrecognised_not_active: is_recognized=False AND is_active=False
        4. recognised_not_music: is_recognized=True AND (metadata_json is null OR doesn't have source="music")
        """
        # Check if it's music
        if segment.metadata_json and segment.metadata_json.get('source') == 'music':
            return 'music'
        
        # Check if it's recognized
        if segment.is_recognized:
            return 'recognised_not_music'
        
        # Unrecognized segments
        if segment.is_active and segment.is_analysis_completed:
            return 'unrecognised_with_content'
        elif not segment.is_analysis_completed and segment.is_active:
            return 'unrecognised_active_without_content'
        else:
            return 'unrecognised_not_active'


@method_decorator(csrf_exempt, name='dispatch')
class AudioSegmentsWithTranscriptionView(View):
    def get(self, request, *args, **kwargs):
        try:
            channel_pk = request.GET.get('channel_id')
            date = request.GET.get('date')
            start_time = request.GET.get('start_time')
            end_time = request.GET.get('end_time')
            
            if not channel_pk:
                return JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
            
            try:
                channel = Channel.objects.get(id=channel_pk, is_deleted=False)
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
            
            # Parse the date to filter segments
            if date:
                try:
                    # Try YYYYMMDD format first (e.g., 20250802)
                    if len(date) == 8 and date.isdigit():
                        date_obj = datetime.strptime(date, '%Y%m%d').date()
                    else:
                        # Try YYYY-MM-DD format (e.g., 2025-08-02)
                        date_obj = datetime.strptime(date, '%Y-%m-%d').date()
                except ValueError:
                    return JsonResponse({'success': False, 'error': 'Invalid date format. Use YYYYMMDD or YYYY-MM-DD'}, status=400)
            else:
                # If no date provided, use today's date
                date_obj = timezone.now().date()
            
            # Build filter conditions
            filter_conditions = {'channel': channel}
            
            # Add date filter if provided
            if date:
                # Create timezone-aware datetime range for the entire date
                date_start = timezone.make_aware(datetime.combine(date_obj, datetime.min.time()))
                date_end = timezone.make_aware(datetime.combine(date_obj, datetime.max.time()))
                filter_conditions['start_time__gte'] = date_start
                filter_conditions['start_time__lt'] = date_end
            
            # Add start_time filter if provided
            if start_time:
                try:
                    # Parse start_time in HH:MM:SS format
                    start_time_obj = datetime.strptime(start_time, '%H:%M:%S').time()
                    start_datetime = timezone.make_aware(datetime.combine(date_obj, start_time_obj))
                    filter_conditions['start_time__gte'] = start_datetime
                except ValueError:
                    return JsonResponse({'success': False, 'error': 'Invalid start_time format. Use HH:MM:SS'}, status=400)
            
            # Add end_time filter if provided
            if end_time:
                try:
                    # Parse end_time in HH:MM:SS format
                    end_time_obj = datetime.strptime(end_time, '%H:%M:%S').time()
                    end_datetime = timezone.make_aware(datetime.combine(date_obj, end_time_obj))
                    filter_conditions['start_time__lte'] = end_datetime
                except ValueError:
                    return JsonResponse({'success': False, 'error': 'Invalid end_time format. Use HH:MM:SS'}, status=400)
            
            # Fetch segments from database for the specified filters with optimized queries
            db_segments = AudioSegmentsModel.objects.filter(**filter_conditions).select_related(
                'channel'
            ).prefetch_related(
                'transcription_detail__rev_job',
                'transcription_detail__analysis'
            ).order_by('start_time')
            
            # Convert database objects to dictionary format with transcription and analysis data
            all_segments = []
            for segment in db_segments:
                segment_data = {
                    'id': segment.id,
                    'start_time': segment.start_time,
                    'end_time': segment.end_time,
                    'duration_seconds': segment.duration_seconds,
                    'is_recognized': segment.is_recognized,
                    'is_active': segment.is_active,
                    'file_name': segment.file_name,
                    'file_path': segment.file_path,
                    'title': segment.title,
                    'title_before': segment.title_before,
                    'title_after': segment.title_after,
                    'notes': segment.notes,
                    'created_at': segment.created_at.isoformat() if segment.created_at else None,
                    'is_analysis_completed': segment.is_analysis_completed,
                    'is_audio_downloaded': segment.is_audio_downloaded
                }
                
                # Use prefetched transcription detail data (no database query needed)
                try:
                    transcription_detail = segment.transcription_detail
                    segment_data['transcription'] = {
                        'id': transcription_detail.id,
                        'transcript': transcription_detail.transcript,
                        'created_at': transcription_detail.created_at.isoformat() if transcription_detail.created_at else None,
                        'rev_job_id': transcription_detail.rev_job.job_id if transcription_detail.rev_job else None
                    }
                    
                    # Use prefetched analysis data (no database query needed)
                    try:
                        analysis = transcription_detail.analysis
                        segment_data['analysis'] = {
                            'id': analysis.id,
                            'summary': analysis.summary,
                            'sentiment': analysis.sentiment,
                            'general_topics': analysis.general_topics,
                            'iab_topics': analysis.iab_topics,
                            'bucket_prompt': analysis.bucket_prompt,
                            'content_type_prompt': analysis.content_type_prompt,
                            'created_at': analysis.created_at.isoformat() if analysis.created_at else None
                        }
                    except AttributeError:
                        # No analysis found (prefetched data doesn't have analysis)
                        segment_data['analysis'] = None
                        
                except AttributeError:
                    # No transcription detail found (prefetched data doesn't have transcription_detail)
                    segment_data['transcription'] = None
                    segment_data['analysis'] = None
                
                all_segments.append(segment_data)
            
            # Count recognized and unrecognized segments
            total_recognized = sum(1 for segment in all_segments if segment["is_recognized"])
            total_unrecognized = sum(1 for segment in all_segments if not segment["is_recognized"])
            
            # Count segments with transcription and analysis
            total_with_transcription = sum(1 for segment in all_segments if segment.get("transcription") is not None)
            total_with_analysis = sum(1 for segment in all_segments if segment.get("analysis") is not None)
            
            result = {
                "segments": all_segments,
                "total_segments": len(all_segments),
                "total_recognized": total_recognized,
                "total_unrecognized": total_unrecognized,
                "total_with_transcription": total_with_transcription,
                "total_with_analysis": total_with_analysis
            }
            
            return JsonResponse({
                'success': True,
                'data': result,
                'channel_info': {
                    'channel_id': channel.channel_id,
                    'project_id': channel.project_id,
                    'channel_name': channel.name
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)


@method_decorator(csrf_exempt, name='dispatch')
class AudioSegments(View):
    """
    AudioSegments API with search functionality, pagination, shift filtering, and predefined filter support
    
    Query Parameters:
    - channel_id (required): Channel ID to filter segments
    - start_datetime (required): Start datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)
    - end_datetime (required): End datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)
    - shift_id (optional): Shift ID to filter segments by shift time windows
    - predefined_filter_id (optional): PredefinedFilter ID to filter segments by filter schedule time windows
    - duration (optional): Minimum duration in seconds - only segments with duration >= this value will be returned
    - show_flagged_only (optional): When set to 'true', returns only segments that have triggered flag thresholds (requires shift_id)
    - status (optional): Filter by active status - must be 'true' or 'false' (filters by is_active field)
    - recognition_status (optional): Filter by recognition status - must be one of: 'all', 'recognized', 'unrecognized'
    - has_content (optional): Filter by transcription content - must be 'true' or 'false' (True = segments with transcription_detail, False = segments without)
    
    Pagination Parameters:
    - page (optional): Page number (default: 1)
    - page_size (optional): Hours per page (default: 1)
    
    Search Parameters:
    - search_text (optional): Text to search for
    - search_in (optional): Field to search in - must be one of: 'transcription', 'general_topics', 'iab_topics', 'bucket_prompt', 'summary', 'content_type_prompt', 'title'
    
    Flagging:
    - All segments are automatically checked against FlagCondition for the channel (if configured and active)
    - Flag conditions check: transcription_keywords, summary_keywords, sentiment range, iab_topics, bucket_prompt, general_topics
    - Flag information is included in the 'flag' field of each segment
    - When shift_id is provided with flag_seconds, duration flags are also checked
    - When show_flagged_only is set to 'true', only segments matching any flag condition are returned
    
    Note: If search_text is provided, search_in must also be provided with a valid option.
    When shift_id or predefined_filter_id is provided, segments are filtered to only include those within the time windows.
    Cannot use both shift_id and predefined_filter_id simultaneously.
    When duration is provided, only segments with duration_seconds >= duration will be included in the results.
    When show_flagged_only is set to 'true', pagination is disabled and only segments with flags (duration threshold triggered or FlagCondition matches) are returned.
    show_flagged_only requires either: (1) shift_id with flag_seconds configured, or (2) an active FlagCondition for the channel.
    
    Example URLs:
    - /api/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1&page_size=1
    - /api/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&search_text=music&search_in=transcription&page=2
    - /api/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-08&page=1
    - /api/audio-segments/?channel_id=1&shift_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1
    - /api/audio-segments/?channel_id=1&predefined_filter_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1
    - /api/audio-segments/?channel_id=1&duration=30&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1
    - /api/audio-segments/?channel_id=1&shift_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&show_flagged_only=true
    - /api/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&status=true&recognition_status=recognized&has_content=true
    """
    def get(self, request, *args, **kwargs):
        try:
            # Step 1: Validate and extract parameters
            params, error_response = validate_audio_segments_parameters(request)
            if error_response:
                return error_response
            
            # Step 2: Get channel, shift, and predefined_filter objects
            channel, shift, predefined_filter, error_response = get_channel_and_shift(params)
            if error_response:
                return error_response
            
            # Step 3: Parse datetime parameters
            base_start_dt, base_end_dt, error_response = parse_datetime_parameters(params)
            if error_response:
                return error_response
            
            # Step 4: Apply shift or predefined_filter filtering if provided
            valid_windows = None
            if shift:
                valid_windows = apply_shift_filtering(base_start_dt, base_end_dt, shift)
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
                                'start': TimezoneUtils.convert_to_channel_tz(base_start_dt, channel.timezone),
                                'end': TimezoneUtils.convert_to_channel_tz(base_end_dt, channel.timezone)
                            }
                        }
                    })
            elif predefined_filter:
                valid_windows = apply_predefined_filter_filtering(base_start_dt, base_end_dt, predefined_filter)
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
                                'start': TimezoneUtils.convert_to_channel_tz(base_start_dt, channel.timezone),
                                'end': TimezoneUtils.convert_to_channel_tz(base_end_dt, channel.timezone)
                            }
                        }
                    })
            
            # Handle show_flagged_only mode - skip pagination and return only flagged segments
            if params.get('show_flagged_only'):
                # Check if we have a way to flag (either shift with flag_seconds or FlagCondition)
                has_flag_condition = has_active_flag_condition(channel)
                
                if shift:
                    # Check if shift has flag_seconds configured
                    if getattr(shift, 'flag_seconds', None) is None and not has_flag_condition:
                        from config.validation import TimezoneUtils
                        return JsonResponse({
                            'success': False,
                            'error': 'Shift does not have flag_seconds configured and no FlagCondition found. Cannot filter flagged segments.'
                        }, status=400)
                elif not has_flag_condition:
                    # No shift and no FlagCondition
                    from config.validation import TimezoneUtils
                    return JsonResponse({
                        'success': False,
                        'error': 'No FlagCondition found for this channel. Cannot filter flagged segments.'
                    }, status=400)
                
                # Use full time range (no pagination)
                current_page_start = base_start_dt
                current_page_end = base_end_dt
                is_last_page = True
                
                # Build base query for entire time range
                base_query = build_base_query(
                    channel, current_page_start, current_page_end, valid_windows, is_last_page, 
                    params['duration'], params.get('status'), params.get('recognition_status'), params.get('has_content')
                )
                
                # Apply search filters if provided
                base_query = apply_search_filters(base_query, params['search_text'], params['search_in'])
                
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
                    'page_size': None,  # No pagination in flagged-only mode
                    'available_pages': [],
                    'total_pages': 1,
                    'time_range': {
                        'start': TimezoneUtils.convert_to_channel_tz(base_start_dt, channel.timezone),
                        'end': TimezoneUtils.convert_to_channel_tz(base_end_dt, channel.timezone)
                    },
                    'flagged_only_mode': True
                }
                response_data['has_data'] = len(all_segments) > 0
                
                return JsonResponse(response_data)
            
            # Step 5: Calculate pagination window (normal mode)
            current_page_start, current_page_end, is_last_page, error_response = calculate_pagination_window(
                base_start_dt, base_end_dt, params['page'], params['page_size'], 
                params['search_text'], params['search_in'], valid_windows
            )
            if error_response:
                return error_response
            
            # Step 6: Build base query
            base_query = build_base_query(
                channel, current_page_start, current_page_end, valid_windows, is_last_page, 
                params['duration'], params.get('status'), params.get('recognition_status'), params.get('has_content')
            )
            
            # Step 7: Apply search filters
            base_query = apply_search_filters(base_query, params['search_text'], params['search_in'])
            
            # Step 8: Execute the final query with ordering
            db_segments = base_query.order_by('start_time')
            
            # Step 9: Use serializer to convert database objects to response format
            all_segments = AudioSegmentsSerializer.serialize_segments_data(db_segments, channel.timezone)
            
            # Add per-segment flags (duration and FlagCondition)
            # Add duration flag when shift context with flag_seconds is available
            if shift and getattr(shift, 'flag_seconds', None) is not None:
                try:
                    threshold = int(shift.flag_seconds)
                except Exception:
                    threshold = None
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
            
            # Apply FlagCondition flags to all segments
            all_segments = apply_flag_conditions_to_segments(all_segments, channel)

            # Step 10: Build the complete response using serializer
            response_data = AudioSegmentsSerializer.build_response(all_segments, channel)
            
            # Step 11: Build pagination information
            response_data['pagination'] = build_pagination_info(
                base_start_dt, base_end_dt, params['page'], params['page_size'],
                params['search_text'], params['search_in'], channel, valid_windows, params['duration'],
                params.get('status'), params.get('recognition_status'), params.get('has_content')
            )
            
            # Step 12: Add has_data flag for current page
            current_page_has_data = len(all_segments) > 0
            response_data['has_data'] = current_page_has_data
            
            return JsonResponse(response_data)
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)


@method_decorator(csrf_exempt, name='dispatch')
class AudioTranscriptionAndAnalysisView(View):
    def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            segment_id = data.get('segment_id')
            
            if not segment_id:
                return JsonResponse({'success': False, 'error': 'segment_id is required'}, status=400)
            
            # Check if segment exists
            try:
                segment = AudioSegmentsModel.objects.get(id=segment_id)
            except AudioSegmentsModel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Audio segment not found'}, status=404)
            
            # Check if transcription already exists
            try:
                existing_transcription = TranscriptionDetail.objects.get(audio_segment=segment)
                
                # Get the analysis data if it exists
                analysis_data = None
                try:
                    analysis = existing_transcription.analysis
                    analysis_data = {
                        'summary': analysis.summary,
                        'sentiment': analysis.sentiment,
                        'general_topics': analysis.general_topics,
                        'iab_topics': analysis.iab_topics,
                        'bucket_prompt': analysis.bucket_prompt,
                        'content_type_prompt': analysis.content_type_prompt,
                        'created_at': analysis.created_at.isoformat()
                    }
                except:
                    analysis_data = None
                
                return JsonResponse({
                    'success': True,
                    'message': 'Transcription and analysis already exist for this segment',
                    'data': {
                        'transcription': {
                            'id': existing_transcription.id,
                            'transcript': existing_transcription.transcript,
                            'created_at': existing_transcription.created_at.isoformat(),
                            'rev_job_id': existing_transcription.rev_job.job_id
                        },
                        'analysis': analysis_data,
                        'status': 'already_exists'
                    }
                })
            except TranscriptionDetail.DoesNotExist:
                pass  # Continue with transcription
            
            # Check if already queued for transcription
            try:
                existing_queue = TranscriptionQueue.objects.get(audio_segment=segment)
                
                # Check if it's been more than 120 seconds since queued
                time_since_queued = timezone.now() - existing_queue.queued_at
                if time_since_queued.total_seconds() < 120:
                    return JsonResponse({
                        'success': False, 
                        'error': f'Audio segment was queued recently. Please wait {120 - int(time_since_queued.total_seconds())} more seconds before trying again.',
                        'queue_id': existing_queue.id,
                        'queued_at': existing_queue.queued_at.isoformat(),
                        'seconds_remaining': 120 - int(time_since_queued.total_seconds()),
                        'status': 'recently_queued'
                    }, status=400)
                # If >= 120 seconds have passed, continue with transcription process below
                    
            except TranscriptionQueue.DoesNotExist:
                pass  # Continue with queuing
            
            # Check if audio file exists, if not download it
            if not segment.file_path or not os.path.exists(segment.file_path):
                try:
                    # Download the audio file first
                    from data_analysis.services.audio_download import ACRCloudAudioDownloader
                    
                    # Get project_id and channel_id from the segment's channel
                    project_id = segment.channel.project_id
                    channel_id = segment.channel.channel_id
                    
                    # Ensure the directory exists for the file path
                    file_dir = os.path.dirname(segment.file_path)
                    if file_dir:
                        os.makedirs(file_dir, exist_ok=True)
                    
                    # Download the audio file
                    media_url = ACRCloudAudioDownloader.download_audio(
                        project_id=project_id,
                        channel_id=channel_id,
                        start_time=segment.start_time,
                        duration_seconds=segment.duration_seconds,
                        filepath=segment.file_path
                    )
                    
                    # Update the segment's is_audio_downloaded flag
                    segment.is_audio_downloaded = True
                    segment.is_manually_processed = True
                    segment.save()
                                        
                except Exception as download_error:
                    return JsonResponse({
                        'success': False, 
                        'error': f'Failed to download audio file: {str(download_error)}'
                    }, status=500)
            
            # Create or update transcription queue entry
            try:
                # Check if we're updating an existing queue entry (after 40 seconds)
                existing_queue = TranscriptionQueue.objects.filter(audio_segment=segment).first()
                if existing_queue:
                    # Update existing queue entry
                    existing_queue.queued_at = timezone.now()
                    existing_queue.is_transcribed = False
                    existing_queue.is_analyzed = False
                    existing_queue.completed_at = None
                    existing_queue.save()
                    queue_entry = existing_queue
                else:
                    # Create new queue entry
                    queue_entry = TranscriptionQueue.objects.create(
                        audio_segment=segment,
                        is_transcribed=False,
                        is_analyzed=False
                    )
            except Exception as db_error:
                return JsonResponse({
                    'success': False, 
                    'error': f'Failed to create/update transcription queue entry: {str(db_error)}'
                }, status=500)
            
            # Call transcription function immediately
            try:
                # Ensure file_path is properly formatted for the transcription service
                media_path = "/api/"+segment.file_path
                if not media_path:
                    return JsonResponse({
                        'success': False, 
                        'error': 'File path is empty or null'
                    }, status=400)
                
                # Ensure the path starts with '/' for validation
                # if not media_path.startswith('/'):
                #     media_path = '/' + media_path
                
                # Additional validation - ensure it's a valid file path
                if '..' in media_path or media_path.startswith('//'):
                    return JsonResponse({
                        'success': False, 
                        'error': 'Invalid file path format'
                    }, status=400)
                
                
                # Call the transcription service
                transcription_job = RevAISpeechToText.create_transcription_job(media_path)
                job_id = transcription_job.get('id')
                
                if not job_id:
                    return JsonResponse({
                        'success': False, 
                        'error': 'Failed to create transcription job'
                    }, status=500)
                
                # Save the job to database
                rev_job = RevTranscriptionJob.objects.create(
                    job_id=job_id,
                    job_name=f"Transcription for segment {segment_id}",
                    media_url=f"{config('PUBLIC_BASE_URL')}{segment.file_path}",
                    status='in_progress',
                    job_type='async',
                    language='en',
                    created_on=timezone.now(),
                    audio_segment=segment
                )
                
                
            except Exception as transcription_error:
                # Delete the queue entry if transcription fails
                queue_entry.delete()
                return JsonResponse({
                    'success': False, 
                    'error': f'Failed to start transcription: {str(transcription_error)}'
                }, status=500)
            
            # Return success immediately after queuing
            return JsonResponse({
                'success': True,
                'message': 'Audio segment queued for transcription and analysis',
                'data': {
                    'queue_id': queue_entry.id,
                    'segment_id': segment_id,
                    'rev_job_id': job_id,
                    'queued_at': queue_entry.queued_at.isoformat(),
                    'status': 'queued'
                }
            })
                
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class RevCallbackView(View):
    def post(self, request, *args, **kwargs):
        data = json.loads(request.body)
        # Extract job data from callback
        job_data = data.get('job', {})
        
        # Parse datetime fields
        created_on = None
        completed_on = None
        
        if job_data.get('created_on'):
            try:
                created_on = datetime.fromisoformat(job_data['created_on'].replace('Z', '+00:00'))
            except ValueError:
                created_on = timezone.now()
        
        if job_data.get('completed_on'):
            try:
                completed_on = datetime.fromisoformat(job_data['completed_on'].replace('Z', '+00:00'))
            except ValueError:
                completed_on = None
        
        # Create or update RevTranscriptionJob record
        job, created = RevTranscriptionJob.objects.update_or_create(
            job_id=job_data['id'],
            defaults={
                'job_name': job_data.get('name', ''),
                'media_url': job_data.get('media_url', ''),
                'status': job_data.get('status', ''),
                'created_on': created_on,
                'completed_on': completed_on,
                'job_type': job_data.get('type', 'async'),
                'language': job_data.get('language', 'en'),
                'strict_custom_vocabulary': job_data.get('strict_custom_vocabulary', False),
                'duration_seconds': job_data.get('duration_seconds'),
                'failure': job_data.get('failure'),
                'failure_detail': job_data.get('failure_detail'),
            }
        )
        

        action = 'created' if created else 'updated'

        if job.status == 'transcribed':
            try:
                media_url = job_data.get('media_url')
                parsed_url = urlparse(media_url)
                analyze_transcription_task.delay(job.pk, parsed_url.path, media_url)
            except Exception as e:
                return JsonResponse({'success': False, 'error': str(e)}, status=400)
        return JsonResponse({'success': True, 'action': action, 'job_id': job.job_id})


@method_decorator(csrf_exempt, name='dispatch')
class MediaDownloadView(View):
    def get(self, request, file_path, *args, **kwargs):
        # Remove any leading slashes and decode URL encoding
        file_path = unquote(file_path.lstrip('/'))
        
        # Prevent directory traversal
        if '..' in file_path or file_path.startswith('/'):
            return JsonResponse({'success': False, 'error': 'Invalid file path'}, status=400)
        abs_file_path = os.path.join(file_path)
        if not os.path.exists(abs_file_path):
            return JsonResponse({'success': False, 'error': 'File not found'}, status=404)
        try:
            response = FileResponse(open(abs_file_path, 'rb'), as_attachment=True, filename=os.path.basename(file_path))
            return response
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
