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

from acr_admin.models import Channel
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel, TranscriptionDetail, TranscriptionAnalysis, TranscriptionQueue, GeneralTopic, ReportFolder, SavedAudioSegment, AudioSegmentInsight
from data_analysis.services.audio_segments import AudioSegments
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.tasks import analyze_transcription_task
from data_analysis.serializers import AudioSegmentsSerializer
from data_analysis.services.segment_range_service import create_segment_download_and_queue

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
class AudioSegmentIsActiveUpdateView(APIView):
    permission_classes = [IsAdminUser]

    def patch(self, request, segment_id, *args, **kwargs):
        return self._update_is_active(request, segment_id)

    def _update_is_active(self, request, segment_id):
        try:
            payload = request.data if hasattr(request, 'data') else json.loads(request.body)
            if not isinstance(payload, dict):
                return Response({'success': False, 'error': 'Expected JSON object body'}, status=status.HTTP_400_BAD_REQUEST)

            if 'is_active' not in payload:
                return Response({'success': False, 'error': 'is_active is required'}, status=status.HTTP_400_BAD_REQUEST)

            is_active_value = bool(payload.get('is_active'))

            try:
                segment = AudioSegmentsModel.objects.get(id=segment_id)
            except AudioSegmentsModel.DoesNotExist:
                return Response({'success': False, 'error': 'Audio segment not found'}, status=status.HTTP_404_NOT_FOUND)

            segment.is_active = is_active_value
            segment.is_manually_processed = True
            segment.save(update_fields=['is_active', 'is_manually_processed'])

            return Response({
                'success': True,
                'message': 'Audio segment updated',
                'data': {
                    'segment_id': segment.id,
                    'is_active': segment.is_active,
                    'start_time': segment.start_time.isoformat(),
                    'end_time': segment.end_time.isoformat()
                }
            })
        except json.JSONDecodeError:
            return Response({'success': False, 'error': 'Invalid JSON body'}, status=status.HTTP_400_BAD_REQUEST)
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
                .only('id', 'duration_seconds', 'title', 'start_time', 'end_time')
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
                        data.append({
                            'title': seg.title if seg.title else 'undefined',
                            'value': to_add,
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

class CreateSegmentFromRangeView(APIView):
    permission_classes = [IsAdminUser]
    def post(self, request, *args, **kwargs):
        try:
            payload = request.data if hasattr(request, 'data') else json.loads(request.body)
            
            # Handle new request body structure
            if isinstance(payload, dict):
                # New structure: {channel_id, segment_id?, is_active?, split_segments: [...]}
                channel_id = payload.get('channel_id')
                segment_id = payload.get('segment_id')
                is_active = payload.get('is_active')
                split_segments = payload.get('split_segments', [])
                
                if not channel_id:
                    return Response({'success': False, 'error': 'channel_id is required'}, status=status.HTTP_400_BAD_REQUEST)
                
                # Validate channel exists
                try:
                    channel = Channel.objects.get(id=channel_id, is_deleted=False)
                except Channel.DoesNotExist:
                    return Response({'success': False, 'error': 'Channel not found'}, status=status.HTTP_400_BAD_REQUEST)
                
                results = []
                errors = []
                updated_segments = []
                
                # Handle segment update if segment_id and is_active are provided
                if segment_id is not None and is_active is not None:
                    print(is_active)
                    try:
                        segment = AudioSegmentsModel.objects.get(id=segment_id, channel=channel)
                        segment.is_active = bool(is_active)
                        segment.save()
                        print(segment.is_active)
                        updated_segments.append({
                            'segment_id': segment.id,
                            'is_active': segment.is_active,
                            'start_time': segment.start_time.isoformat(),
                            'end_time': segment.end_time.isoformat()
                        })
                    except AudioSegmentsModel.DoesNotExist:
                        errors.append({'segment_id': segment_id, 'error': 'Segment not found'})
                    except Exception as e:
                        errors.append({'segment_id': segment_id, 'error': str(e)})
                
                # Handle split segments creation
                for idx, segment_data in enumerate(split_segments):
                    try:
                        from_ts = segment_data.get('from')
                        to_ts = segment_data.get('to')
                        title = segment_data.get('title')
                        title_before = segment_data.get('title_before')
                        title_after = segment_data.get('title_after')
                        transcribe = bool(segment_data.get('transcribe', True))

                        if not from_ts or not to_ts:
                            raise ValueError('from and to are required (ISO or YYYY-MM-DD HH:MM:SS)')

                        start_dt = parse_dt(from_ts)
                        end_dt = parse_dt(to_ts)
                        if not start_dt or not end_dt:
                            raise ValueError('Invalid datetime format for from/to')
                        if end_dt <= start_dt:
                            raise ValueError('to must be after from')

                        result = create_segment_download_and_queue(
                            channel,
                            start_dt,
                            end_dt,
                            user=request.user if getattr(request, 'user', None) and request.user.is_authenticated else None,
                            title=title,
                            title_before=title_before,
                            title_after=title_after,
                            transcribe=transcribe
                        )

                        created_segment = result['segment']
                        queue_entry = result['queue']
                        rev_job = result['rev_job']
                        media_url = result['media_url']

                        results.append({
                            'segment_id': created_segment.id,
                            'queue_id': queue_entry.id if queue_entry else None,
                            'rev_job_id': rev_job.job_id if rev_job else None,
                            'media_url': media_url,
                            'start_time': created_segment.start_time.isoformat(),
                            'end_time': created_segment.end_time.isoformat(),
                            'duration_seconds': created_segment.duration_seconds
                        })
                    except Exception as item_err:
                        errors.append({'index': idx, 'error': str(item_err)})

                return Response({
                    'success': True,
                    'message': 'Request processed',
                    'data': {
                        'updated_segments': updated_segments,
                        'created_segments': results,
                        'errors': errors
                    }
                })
            
            # Handle legacy list format for backward compatibility
            elif isinstance(payload, list):
                results = []
                errors = []
                for idx, item in enumerate(payload):
                    try:
                        channel_pk = item.get('channel_id')
                        from_ts = item.get('from')
                        to_ts = item.get('to')
                        title = item.get('title')
                        title_before = item.get('title_before')
                        title_after = item.get('title_after')
                        transcribe = bool(item.get('transcribe', True))

                        if not channel_pk:
                            raise ValueError('channel_id is required')
                        if not from_ts or not to_ts:
                            raise ValueError('from and to are required (ISO or YYYY-MM-DD HH:MM:SS)')

                        try:
                            channel = Channel.objects.get(id=channel_pk, is_deleted=False)
                        except Channel.DoesNotExist:
                            raise ValueError('Channel not found')

                        start_dt = parse_dt(from_ts)
                        end_dt = parse_dt(to_ts)
                        if not start_dt or not end_dt:
                            raise ValueError('Invalid datetime format for from/to')
                        if end_dt <= start_dt:
                            raise ValueError('to must be after from')

                        result = create_segment_download_and_queue(
                            channel,
                            start_dt,
                            end_dt,
                            user=request.user if getattr(request, 'user', None) and request.user.is_authenticated else None,
                            title=title,
                            title_before=title_before,
                            title_after=title_after,
                            transcribe=transcribe
                        )

                        created_segment = result['segment']
                        queue_entry = result['queue']
                        rev_job = result['rev_job']
                        media_url = result['media_url']

                        results.append({
                            'segment_id': created_segment.id,
                            'queue_id': queue_entry.id if queue_entry else None,
                            'rev_job_id': rev_job.job_id if rev_job else None,
                            'media_url': media_url,
                            'start_time': created_segment.start_time.isoformat(),
                            'end_time': created_segment.end_time.isoformat(),
                            'duration_seconds': created_segment.duration_seconds
                        })
                    except Exception as item_err:
                        errors.append({'index': idx, 'error': str(item_err)})

                return Response({
                    'success': True,
                    'message': 'Batch processed',
                    'data': {
                        'created': results,
                        'errors': errors
                    }
                })
            else:
                return Response({'success': False, 'error': 'Expected a JSON object or list'}, status=status.HTTP_400_BAD_REQUEST)

        except json.JSONDecodeError:
            return Response({'success': False, 'error': 'Invalid JSON data'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'success': False, 'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
    AudioSegments API with search functionality and pagination
    
    Query Parameters:
    - channel_id (required): Channel ID to filter segments
    - start_datetime (optional): Start datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)
    - end_datetime (optional): End datetime filter (ISO format or YYYY-MM-DD HH:MM:SS)
    
    Pagination Parameters:
    - page (optional): Page number (default: 1)
    - page_size (optional): Hours per page (default: 1)
    
    Search Parameters:
    - search_text (optional): Text to search for
    - search_in (optional): Field to search in - must be one of: 'transcription', 'general_topics', 'iab_topics', 'bucket_prompt', 'summary', 'title'
    
    Note: If search_text is provided, search_in must also be provided with a valid option.
    Maximum time range is 7 days from start_datetime.
    
    Example URLs:
    - /api/audio-segments/?channel_id=1&start_datetime=2025-01-01&page=1&page_size=1
    - /api/audio-segments/?channel_id=1&search_text=music&search_in=transcription&page=2
    - /api/audio-segments/?channel_id=1&start_datetime=2025-01-01&end_datetime=2025-01-02&page=1
    """
    def get(self, request, *args, **kwargs):
        try:
            channel_pk = request.GET.get('channel_id')
            start_datetime = request.GET.get('start_datetime')
            end_datetime = request.GET.get('end_datetime')
            
            # Pagination parameters
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 1))  # hours per page
            
            # Search parameters
            search_text = request.GET.get('search_text')
            search_in = request.GET.get('search_in')
            
            if not channel_pk:
                return JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
            
            # Validate search parameters
            if search_text and not search_in:
                return JsonResponse({'success': False, 'error': 'search_in parameter is required when search_text is provided'}, status=400)
            
            if search_in and not search_text:
                return JsonResponse({'success': False, 'error': 'search_text parameter is required when search_in is provided'}, status=400)
            
            # Validate search_in options
            valid_search_options = ['transcription', 'general_topics', 'iab_topics', 'bucket_prompt', 'summary', 'title']
            if search_in and search_in not in valid_search_options:
                return JsonResponse({
                    'success': False, 
                    'error': f'Invalid search_in option. Must be one of: {", ".join(valid_search_options)}'
                }, status=400)
            
            try:
                channel = Channel.objects.get(id=channel_pk, is_deleted=False)
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
            
            # Parse datetime parameters
            if start_datetime:
                base_start_dt = parse_dt(start_datetime)
                if not base_start_dt:
                    return JsonResponse({'success': False, 'error': 'Invalid start_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
            else:
                # Default to today if no start_datetime provided
                base_start_dt = timezone.make_aware(datetime.combine(timezone.now().date(), datetime.min.time()))
            
            if end_datetime:
                base_end_dt = parse_dt(end_datetime)
                if not base_end_dt:
                    return JsonResponse({'success': False, 'error': 'Invalid end_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
            else:
                # Default to 7 days from base_start_dt if no end_datetime provided
                base_end_dt = base_start_dt + timezone.timedelta(days=7)
            
            # Validate 7 days limit
            time_diff = base_end_dt - base_start_dt
            if time_diff > timezone.timedelta(days=7):
                return JsonResponse({
                    'success': False, 
                    'error': 'Time range cannot exceed 7 days. Please adjust start_datetime and end_datetime parameters.'
                }, status=400)
            
            # Ensure we don't exceed 7 days limit
            max_end_dt = base_start_dt + timezone.timedelta(days=7)
            if base_end_dt > max_end_dt:
                base_end_dt = max_end_dt
            
            # Calculate pagination time window
            page_start_offset = (page - 1) * page_size
            current_page_start = base_start_dt + timezone.timedelta(hours=page_start_offset)
            current_page_end = current_page_start + timezone.timedelta(hours=page_size)
            
            # Ensure current page doesn't exceed the base_end_dt
            if current_page_start >= base_end_dt:
                return JsonResponse({
                    'success': False, 
                    'error': f'Page {page} is beyond the available time range'
                }, status=400)
            
            if current_page_end > base_end_dt:
                current_page_end = base_end_dt
            
            # Build filter conditions for current page
            filter_conditions = {
                'channel': channel,
                'start_time__gte': current_page_start,
                'start_time__lt': current_page_end
            }
            
            # Build the base query with optimized joins
            base_query = AudioSegmentsModel.objects.filter(**filter_conditions).select_related(
                'channel'
            ).prefetch_related(
                'transcription_detail__rev_job',
                'transcription_detail__analysis'
            )
            
            # Apply search filters if search parameters are provided
            if search_text and search_in:
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
            
            # Execute the final query with ordering
            db_segments = base_query.order_by('start_time')
            
            # Use serializer to convert database objects to response format
            all_segments = AudioSegmentsSerializer.serialize_segments_data(db_segments)
            
            # Build the complete response using serializer
            response_data = AudioSegmentsSerializer.build_response(all_segments, channel)
            
            # Generate pagination information
            available_pages = []
            total_hours = int((base_end_dt - base_start_dt).total_seconds() / 3600)
            
            for hour_offset in range(0, total_hours, page_size):
                page_num = (hour_offset // page_size) + 1
                page_start = base_start_dt + timezone.timedelta(hours=hour_offset)
                page_end = page_start + timezone.timedelta(hours=page_size)
                
                # Ensure page_end doesn't exceed base_end_dt
                if page_end > base_end_dt:
                    page_end = base_end_dt
                
                # Count segments for this page (without search filters for simplicity)
                page_filter = {
                    'channel': channel,
                    'start_time__gte': page_start,
                    'start_time__lt': page_end
                }
                
                # Apply search filters if they exist
                page_query = AudioSegmentsModel.objects.filter(**page_filter)
                if search_text and search_in:
                    if search_in == 'transcription':
                        page_query = page_query.filter(
                            transcription_detail__transcript__icontains=search_text
                        )
                    elif search_in == 'general_topics':
                        page_query = page_query.filter(
                            transcription_detail__analysis__general_topics__icontains=search_text
                        )
                    elif search_in == 'iab_topics':
                        page_query = page_query.filter(
                            transcription_detail__analysis__iab_topics__icontains=search_text
                        )
                    elif search_in == 'bucket_prompt':
                        page_query = page_query.filter(
                            transcription_detail__analysis__bucket_prompt__icontains=search_text
                        )
                    elif search_in == 'summary':
                        page_query = page_query.filter(
                            transcription_detail__analysis__summary__icontains=search_text
                        )
                    elif search_in == 'title':
                        page_query = page_query.filter(
                            title__icontains=search_text
                        )
                
                segment_count = page_query.count()
                has_data = segment_count > 0
                
                available_pages.append({
                    'page': page_num,
                    'start_time': page_start.isoformat(),
                    'end_time': page_end.isoformat(),
                    'has_data': has_data,
                    'segment_count': segment_count
                })
            
            # Add pagination information to response
            response_data['pagination'] = {
                'current_page': page,
                'page_size': page_size,
                'available_pages': available_pages,
                'total_pages': len(available_pages),
                'time_range': {
                    'start': base_start_dt.isoformat(),
                    'end': base_end_dt.isoformat()
                }
            }
            
            # Add has_data flag for current page
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
                else:
                    # More than 120 seconds have passed, allow calling transcription again
                    print(f"More than 120 seconds passed since last queue. Allowing transcription call again for segment {segment_id}")
                    # Continue with the transcription process below
                    
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
                    
                    print(f"Successfully downloaded audio for segment {segment_id} to {segment.file_path}")
                    
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
                    print(f"Updated existing TranscriptionQueue entry: {queue_entry.id}")
                else:
                    # Create new queue entry
                    queue_entry = TranscriptionQueue.objects.create(
                        audio_segment=segment,
                        is_transcribed=False,
                        is_analyzed=False
                    )
                    print(f"Successfully created TranscriptionQueue entry: {queue_entry.id}")
            except Exception as db_error:
                print(f"Database error creating/updating TranscriptionQueue: {str(db_error)}")
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
                
                print(f"Calling transcription service with media_path: {media_path}")
                
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
                
                print(f"Successfully created RevTranscriptionJob: {rev_job.job_id}")
                
            except Exception as transcription_error:
                print(f"Transcription service error: {str(transcription_error)}")
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
        print(f'RevTranscriptionJob {action}: {job.job_id} - {job.status}')

        if job.status == 'transcribed':
            try:
                parsed_url = urlparse(job_data.get('media_url'))
                analyze_transcription_task.delay(job.pk, parsed_url.path)
            except Exception as e:
                print(e)
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

@method_decorator(csrf_exempt, name='dispatch')
class ReportFolderManagementView(View):
    """API to manage report folders (create, list, update, delete)"""
    
    def get(self, request, *args, **kwargs):
        """Get all report folders with their saved segments count"""
        try:
            folders = ReportFolder.objects.select_related('channel').prefetch_related('saved_segments').all()
            
            folders_data = []
            for folder in folders:
                folders_data.append({
                    'id': folder.id,
                    'name': folder.name,
                    'description': folder.description,
                    'color': folder.color,
                    'is_public': folder.is_public,
                    'channel': {
                        'id': folder.channel.id,
                        'name': folder.channel.name,
                        'channel_id': folder.channel.channel_id,
                        'project_id': folder.channel.project_id,
                    },
                    'saved_segments_count': folder.saved_segments.count(),
                    'created_at': folder.created_at.isoformat(),
                    'updated_at': folder.updated_at.isoformat()
                })
            
            return JsonResponse({
                'success': True,
                'data': {
                    'folders': folders_data,
                    'total_count': len(folders_data)
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def post(self, request, *args, **kwargs):
        """Create a new report folder"""
        try:
            data = json.loads(request.body)
            
            name = data.get('name')
            description = data.get('description', '')
            color = data.get('color', '#3B82F6')
            is_public = data.get('is_public', True)
            channel_id = data.get('channel_id')
            
            if not name:
                return JsonResponse({'success': False, 'error': 'name is required'}, status=400)
            if not channel_id:
                return JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
            try:
                channel = Channel.objects.get(id=channel_id, is_deleted=False)
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
            
            # Validate color format
            if not color.startswith('#') or len(color) != 7:
                return JsonResponse({'success': False, 'error': 'color must be a valid hex color (e.g., #3B82F6)'}, status=400)
            
            folder = ReportFolder.objects.create(
                channel=channel,
                name=name,
                description=description,
                color=color,
                is_public=is_public
            )
            
            return JsonResponse({
                'success': True,
                'message': 'Report folder created successfully',
                'data': {
                    'id': folder.id,
                    'name': folder.name,
                    'description': folder.description,
                    'color': folder.color,
                    'is_public': folder.is_public,
                    'channel': {
                        'id': folder.channel.id,
                        'name': folder.channel.name,
                        'channel_id': folder.channel.channel_id,
                        'project_id': folder.channel.project_id,
                    },
                    'saved_segments_count': 0,
                    'created_at': folder.created_at.isoformat(),
                    'updated_at': folder.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def put(self, request, folder_id, *args, **kwargs):
        """Update an existing report folder"""
        try:
            data = json.loads(request.body)
            
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            # Update fields if provided
            if 'name' in data:
                folder.name = data['name']
            if 'description' in data:
                folder.description = data['description']
            if 'color' in data:
                color = data['color']
                if not color.startswith('#') or len(color) != 7:
                    return JsonResponse({'success': False, 'error': 'color must be a valid hex color (e.g., #3B82F6)'}, status=400)
                folder.color = color
            if 'is_public' in data:
                folder.is_public = data['is_public']
            if 'channel_id' in data:
                try:
                    channel = Channel.objects.get(id=data['channel_id'], is_deleted=False)
                except Channel.DoesNotExist:
                    return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
                folder.channel = channel
            
            folder.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Report folder updated successfully',
                'data': {
                    'id': folder.id,
                    'name': folder.name,
                    'description': folder.description,
                    'color': folder.color,
                    'is_public': folder.is_public,
                    'channel': {
                        'id': folder.channel.id,
                        'name': folder.channel.name,
                        'channel_id': folder.channel.channel_id,
                        'project_id': folder.channel.project_id,
                    },
                    'saved_segments_count': folder.saved_segments.count(),
                    'created_at': folder.created_at.isoformat(),
                    'updated_at': folder.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def delete(self, request, folder_id, *args, **kwargs):
        """Delete a report folder and all its saved segments"""
        try:
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            folder_name = folder.name
            folder.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Report folder "{folder_name}" deleted successfully'
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class SaveAudioSegmentView(View):
    """API to save audio segments to report folders with insights"""
    
    def post(self, request, *args, **kwargs):
        """Save an audio segment to a report folder"""
        try:
            data = json.loads(request.body)
            
            folder_id = data.get('folder_id')
            audio_segment_id = data.get('audio_segment_id')
            is_favorite = data.get('is_favorite', False)
            
            if not folder_id:
                return JsonResponse({'success': False, 'error': 'folder_id is required'}, status=400)
            
            if not audio_segment_id:
                return JsonResponse({'success': False, 'error': 'audio_segment_id is required'}, status=400)
            
            # Validate folder exists
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            # Validate audio segment exists
            try:
                audio_segment = AudioSegmentsModel.objects.get(id=audio_segment_id)
            except AudioSegmentsModel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Audio segment not found'}, status=404)
            
            # Check if already saved in this folder
            existing_save = SavedAudioSegment.objects.filter(
                folder=folder, 
                audio_segment=audio_segment
            ).first()
            
            if existing_save:
                return JsonResponse({
                    'success': False, 
                    'error': 'Audio segment is already saved in this folder'
                }, status=400)
            
            # Create the saved audio segment
            saved_segment = SavedAudioSegment.objects.create(
                folder=folder,
                audio_segment=audio_segment,
                is_favorite=is_favorite
            )
            
            return JsonResponse({
                'success': True,
                'message': 'Audio segment saved to folder successfully',
                'data': {
                    'id': saved_segment.id,
                    'folder_id': folder.id,
                    'folder_name': folder.name,
                    'audio_segment_id': audio_segment.id,
                    'audio_segment_title': audio_segment.title or 'Untitled',
                    'is_favorite': is_favorite,
                    'saved_at': saved_segment.saved_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def delete(self, request, saved_segment_id, *args, **kwargs):
        """Remove an audio segment from a folder"""
        try:
            try:
                saved_segment = SavedAudioSegment.objects.get(id=saved_segment_id)
            except SavedAudioSegment.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Saved audio segment not found'}, status=404)
            
            folder_name = saved_segment.folder.name
            audio_segment_title = saved_segment.audio_segment.title or 'Untitled'
            saved_segment.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'"{audio_segment_title}" removed from folder "{folder_name}"'
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class FolderContentsView(View):
    """API to retrieve saved audio segments from a specific folder"""
    
    def get(self, request, folder_id, *args, **kwargs):
        """Get all saved audio segments in a folder with full details"""
        try:
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            # Get saved segments with related data
            saved_segments = SavedAudioSegment.objects.filter(
                folder=folder
            ).select_related(
                'audio_segment__channel'
            ).prefetch_related(
                'audio_segment__transcription_detail__rev_job',
                'audio_segment__transcription_detail__analysis',
                'insights'
            ).order_by('-saved_at')
            
            segments_data = []
            for saved_segment in saved_segments:
                audio_segment = saved_segment.audio_segment
                
                # Build segment data with transcription and analysis
                segment_data = {
                    'saved_segment_id': saved_segment.id,
                    'audio_segment_id': audio_segment.id,
                    'start_time': audio_segment.start_time.isoformat(),
                    'end_time': audio_segment.end_time.isoformat(),
                    'duration_seconds': audio_segment.duration_seconds,
                    'is_recognized': audio_segment.is_recognized,
                    'title': audio_segment.title,
                    'title_before': audio_segment.title_before,
                    'title_after': audio_segment.title_after,
                    'file_name': audio_segment.file_name,
                    'file_path': audio_segment.file_path,
                    'channel_name': audio_segment.channel.name,
                    'channel_id': audio_segment.channel.channel_id,
                    'is_favorite': saved_segment.is_favorite,
                    'saved_at': saved_segment.saved_at.isoformat(),
                    'updated_at': saved_segment.updated_at.isoformat()
                }
                
                # Add transcription data if available
                try:
                    transcription_detail = audio_segment.transcription_detail
                    segment_data['transcription'] = {
                        'id': transcription_detail.id,
                        'transcript': transcription_detail.transcript,
                        'created_at': transcription_detail.created_at.isoformat(),
                        'rev_job_id': transcription_detail.rev_job.job_id if transcription_detail.rev_job else None
                    }
                    
                    # Add analysis data if available
                    try:
                        analysis = transcription_detail.analysis
                        segment_data['analysis'] = {
                            'id': analysis.id,
                            'summary': analysis.summary,
                            'sentiment': analysis.sentiment,
                            'general_topics': analysis.general_topics,
                            'iab_topics': analysis.iab_topics,
                            'bucket_prompt': analysis.bucket_prompt,
                            'created_at': analysis.created_at.isoformat()
                        }
                    except AttributeError:
                        segment_data['analysis'] = None
                        
                except AttributeError:
                    segment_data['transcription'] = None
                    segment_data['analysis'] = None
                
                # Add insights data
                insights_data = []
                for insight in saved_segment.insights.all():
                    insights_data.append({
                        'id': insight.id,
                        'title': insight.title,
                        'description': insight.description,
                        'created_at': insight.created_at.isoformat(),
                        'updated_at': insight.updated_at.isoformat()
                    })
                segment_data['insights'] = insights_data
                
                segments_data.append(segment_data)
            
                return JsonResponse({
                'success': True,
                'data': {
                    'folder': {
                        'id': folder.id,
                        'name': folder.name,
                        'description': folder.description,
                        'color': folder.color,
                        'is_public': folder.is_public,
                        'created_at': folder.created_at.isoformat(),
                        'channel': {
                            'id': folder.channel.id,
                            'name': folder.channel.name,
                            'channel_id': folder.channel.channel_id,
                            'project_id': folder.channel.project_id,
                        }
                    },
                    'saved_segments': segments_data,
                    'total_count': len(segments_data)
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class AudioSegmentInsightsView(View):
    """API to manage insights for saved audio segments"""
    
    def get(self, request, saved_segment_id, *args, **kwargs):
        """Get all insights for a saved audio segment"""
        try:
            try:
                saved_segment = SavedAudioSegment.objects.get(id=saved_segment_id)
            except SavedAudioSegment.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Saved audio segment not found'}, status=404)
            
            insights = AudioSegmentInsight.objects.filter(
                saved_audio_segment=saved_segment
            ).order_by('-created_at')
            
            insights_data = []
            for insight in insights:
                insights_data.append({
                    'id': insight.id,
                    'title': insight.title,
                    'description': insight.description,
                    'created_at': insight.created_at.isoformat(),
                    'updated_at': insight.updated_at.isoformat()
                })
            
            return JsonResponse({
                'success': True,
                'data': {
                    'saved_segment_id': saved_segment.id,
                    'audio_segment_title': saved_segment.audio_segment.title or 'Untitled',
                    'folder_name': saved_segment.folder.name,
                    'insights': insights_data,
                    'total_count': len(insights_data)
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def post(self, request, saved_segment_id, *args, **kwargs):
        """Create a new insight for a saved audio segment"""
        try:
            data = json.loads(request.body)
            
            title = data.get('title')
            description = data.get('description')
            
            if not title:
                return JsonResponse({'success': False, 'error': 'title is required'}, status=400)
            
            if not description:
                return JsonResponse({'success': False, 'error': 'description is required'}, status=400)
            
            try:
                saved_segment = SavedAudioSegment.objects.get(id=saved_segment_id)
            except SavedAudioSegment.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Saved audio segment not found'}, status=404)
            
            insight = AudioSegmentInsight.objects.create(
                saved_audio_segment=saved_segment,
                title=title,
                description=description
            )
            
            return JsonResponse({
                'success': True,
                'message': 'Insight created successfully',
                'data': {
                    'id': insight.id,
                    'title': insight.title,
                    'description': insight.description,
                    'saved_segment_id': saved_segment.id,
                    'audio_segment_title': saved_segment.audio_segment.title or 'Untitled',
                    'folder_name': saved_segment.folder.name,
                    'created_at': insight.created_at.isoformat(),
                    'updated_at': insight.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def put(self, request, saved_segment_id, insight_id, *args, **kwargs):
        """Update an existing insight"""
        try:
            data = json.loads(request.body)
            
            try:
                insight = AudioSegmentInsight.objects.get(
                    id=insight_id,
                    saved_audio_segment_id=saved_segment_id
                )
            except AudioSegmentInsight.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Insight not found'}, status=404)
            
            # Update fields if provided
            if 'title' in data:
                insight.title = data['title']
            if 'description' in data:
                insight.description = data['description']
            
            insight.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Insight updated successfully',
                'data': {
                    'id': insight.id,
                    'title': insight.title,
                    'description': insight.description,
                    'saved_segment_id': insight.saved_audio_segment.id,
                    'audio_segment_title': insight.saved_audio_segment.audio_segment.title or 'Untitled',
                    'folder_name': insight.saved_audio_segment.folder.name,
                    'created_at': insight.created_at.isoformat(),
                    'updated_at': insight.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def delete(self, request, saved_segment_id, insight_id, *args, **kwargs):
        """Delete an insight"""
        try:
            try:
                insight = AudioSegmentInsight.objects.get(
                    id=insight_id,
                    saved_audio_segment_id=saved_segment_id
                )
            except AudioSegmentInsight.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Insight not found'}, status=404)
            
            insight_title = insight.title
            audio_segment_title = insight.saved_audio_segment.audio_segment.title or 'Untitled'
            insight.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Insight "{insight_title}" deleted from "{audio_segment_title}"'
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

