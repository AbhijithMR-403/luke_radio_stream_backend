from datetime import datetime
import json
import os

from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAdminUser

from core_admin.models import Channel
from core_admin.repositories import GeneralSettingService
from data_analysis.models import (
    AudioSegments as AudioSegmentsModel,
)
from data_analysis.services.audio_download import ACRCloudAudioDownloader
from data_analysis.services.segment_range_service import create_segment_download_and_queue
from data_analysis.serializers import AudioSegmentsSerializer


def _parse_dt(value):
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


def _create_merged_segment(channel, start_dt, end_dt, source_segment_ids=None):
    """Create a merged segment with source='user_merged' and download audio"""
    duration_seconds = int((end_dt - start_dt).total_seconds())
    
    # Build file_name and file_path
    start_time_str = start_dt.strftime("%Y%m%d%H%M%S")
    file_name = f"audio_{channel.project_id}_{channel.channel_id}_{start_time_str}_{duration_seconds}.mp3"
    start_date = start_dt.strftime("%Y%m%d")
    file_path = f"media/{start_date}/{file_name}"
    
    # Ensure the directory exists
    media_dir = os.path.join(os.getcwd(), "media", start_date)
    os.makedirs(media_dir, exist_ok=True)
    
    # Create segment payload with source='user_merged'
    segment_payload = {
        'start_time': start_dt,
        'end_time': end_dt,
        'duration_seconds': duration_seconds,
        'is_recognized': False,
        'is_active': False,
        'file_name': file_name,
        'file_path': file_path,
        'channel': channel,
        'source': 'user_merged',
        'title_before': 'UNKNOWN',
        'title_after': 'UNKNOWN',
    }
    if source_segment_ids:
        segment_payload['notes'] = f"Merged from segments: {','.join(map(str, source_segment_ids))}"
    
    # Check if segment with same file_path already exists
    existing_segment = AudioSegmentsModel.objects.filter(file_path=file_path).first()
    if existing_segment:
        # Update existing segment
        for key, value in segment_payload.items():
            setattr(existing_segment, key, value)
        existing_segment.save()
        created_segment = existing_segment
    else:
        # Create new segment
        created_segment = AudioSegmentsModel.insert_single_audio_segment(segment_payload)
    
    settings = GeneralSettingService.get_active_setting(channel=channel, include_buckets=False)
    if not settings or not settings.acr_cloud_api_key:
        raise ValueError("ACRCloud API key not configured for channel")
    # Download audio
    media_url = ACRCloudAudioDownloader.download_audio(
        api_key=settings.acr_cloud_api_key,
        project_id=channel.project_id,
        channel_id=channel.channel_id,
        start_time=start_dt,
        duration_seconds=duration_seconds,
        filepath=file_path
    )
    created_segment.is_audio_downloaded = True
    created_segment.save()
    
    return created_segment, media_url


@method_decorator(csrf_exempt, name='dispatch')
class ProcessSegmentsView(APIView):
    permission_classes = [IsAdminUser]
    def post(self, request, *args, **kwargs):
        try:
            payload = request.data if hasattr(request, 'data') else json.loads(request.body)
            if not isinstance(payload, dict):
                return Response({'success': False, 'error': 'Expected JSON object body'}, status=status.HTTP_400_BAD_REQUEST)

            segment_ids = payload.get('segment_ids')
            channel_id = payload.get('channel_id')
            start_datetime = payload.get('start_datetime')
            end_datetime = payload.get('end_datetime')

            # Enforce mutual exclusivity
            if (segment_ids and (start_datetime or end_datetime)) or (not segment_ids and not (channel_id and start_datetime and end_datetime)):
                return Response({'success': False, 'error': 'Provide either segment_ids[] OR channel_id+start_datetime+end_datetime, not both'}, status=status.HTTP_400_BAD_REQUEST)

            results = []
            errors = []

            # Case 1: Process a list of existing segment IDs (find min start_time and max end_time)
            if isinstance(segment_ids, list) and len(segment_ids) > 0:
                # Require at least 2 segment IDs
                if len(segment_ids) < 2:
                    return Response({'success': False, 'error': 'At least 2 segment_ids are required'}, status=status.HTTP_400_BAD_REQUEST)
                
                # Fetch all segments that exist
                db_segments = (
                    AudioSegmentsModel.objects
                    .filter(id__in=segment_ids)
                    .select_related('channel')
                    .only('id', 'start_time', 'end_time', 'duration_seconds', 'file_path', 'channel__id', 'channel__project_id', 'channel__channel_id', 'channel__timezone')
                )
                found_segments = list(db_segments)
                
                if not found_segments:
                    return Response({'success': False, 'error': 'No valid segment_ids found'}, status=status.HTTP_400_BAD_REQUEST)

                # Validate all segments are from the same channel
                unique_channels = set(seg.channel.id for seg in found_segments)
                if len(unique_channels) > 1:
                    return Response({
                        'success': False,
                        'error': f'All segments must be from the same channel. Found segments from {len(unique_channels)} different channels.',
                        'channels': list(unique_channels)
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Find the smallest start_time and greatest end_time from all segments
                combined_start = min(seg.start_time for seg in found_segments)
                combined_end = max(seg.end_time for seg in found_segments)
                channel = found_segments[0].channel
                combined_duration = int((combined_end - combined_start).total_seconds())
                
                if combined_duration > 3600:
                    return Response({'success': False, 'error': 'Maximum allowed duration is 3600 seconds (1 hour)'}, status=status.HTTP_400_BAD_REQUEST)

                try:
                    source_ids = [s.id for s in found_segments]
                    created_segment, media_url = _create_merged_segment(
                        channel,
                        combined_start,
                        combined_end,
                        source_segment_ids=source_ids
                    )
                    
                    # Fetch the created segment with all relations for serialization
                    segment_for_response = AudioSegmentsModel.objects.select_related(
                        'channel'
                    ).prefetch_related(
                        'transcription_detail__rev_job',
                        'transcription_detail__analysis'
                    ).get(id=created_segment.id)
                    
                    # Serialize using the same format as GET endpoint
                    serialized_segments = AudioSegmentsSerializer.serialize_segments_data(
                        [segment_for_response],
                        channel_tz=channel.timezone
                    )
                    
                    results = serialized_segments
                except Exception as e:
                    errors.append({'error': str(e)})
                    results = []

                return Response({
                    'success': True,
                    'data': {
                        'segments': results,
                        'errors': errors
                    }
                })

            # Case 2: Create a new segment from UTC time range and process
            elif channel_id and start_datetime and end_datetime:
                try:
                    channel = Channel.objects.get(id=channel_id, is_deleted=False)
                except Channel.DoesNotExist:
                    return Response({'success': False, 'error': 'Channel not found'}, status=status.HTTP_400_BAD_REQUEST)

                start_dt = _parse_dt(start_datetime)
                end_dt = _parse_dt(end_datetime)
                if not start_dt or not end_dt:
                    return Response({'success': False, 'error': 'Invalid datetime format for start_datetime/end_datetime'}, status=status.HTTP_400_BAD_REQUEST)
                if end_dt <= start_dt:
                    return Response({'success': False, 'error': 'end_datetime must be after start_datetime'}, status=status.HTTP_400_BAD_REQUEST)

                duration_seconds = int((end_dt - start_dt).total_seconds())
                if duration_seconds > 3600:
                    return Response({'success': False, 'error': 'Maximum allowed duration is 3600 seconds (1 hour)'}, status=status.HTTP_400_BAD_REQUEST)

                try:
                    created_segment, media_url = _create_merged_segment(
                        channel,
                        start_dt,
                        end_dt,
                        source_segment_ids=None
                    )
                    
                    # Fetch the created segment with all relations for serialization
                    segment_for_response = AudioSegmentsModel.objects.select_related(
                        'channel'
                    ).prefetch_related(
                        'transcription_detail__rev_job',
                        'transcription_detail__analysis'
                    ).get(id=created_segment.id)
                    
                    # Serialize using the same format as GET endpoint
                    serialized_segments = AudioSegmentsSerializer.serialize_segments_data(
                        [segment_for_response],
                        channel_tz=channel.timezone
                    )
                    
                    results = serialized_segments
                except Exception as e:
                    errors.append({'error': str(e)})
                    results = []
            else:
                return Response({'success': False, 'error': 'Provide either segment_ids[] or channel_id + start_datetime + end_datetime'}, status=status.HTTP_400_BAD_REQUEST)

            return Response({
                'success': True,
                'data': {
                    'segments': results,
                    'errors': errors
                }
            })
        except json.JSONDecodeError:
            return Response({'success': False, 'error': 'Invalid JSON body'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'success': False, 'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SplitAudioSegmentView(APIView):
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
                    try:
                        segment = AudioSegmentsModel.objects.get(id=segment_id, channel=channel)
                        segment.is_active = bool(is_active)
                        segment.save()
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

                        start_dt = _parse_dt(from_ts)
                        end_dt = _parse_dt(to_ts)
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
                        # Set is_active and is_recognized based on transcribe parameter
                        if transcribe:
                            created_segment.is_active = True
                            created_segment.is_recognized = False
                        else:
                            created_segment.is_active = False
                            created_segment.is_recognized = False
                        created_segment.save()
                        
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

                        start_dt = _parse_dt(from_ts)
                        end_dt = _parse_dt(to_ts)
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
                        # Set is_active and is_recognized based on transcribe parameter
                        if transcribe:
                            created_segment.is_active = True
                            created_segment.is_recognized = False
                        else:
                            created_segment.is_active = False
                            created_segment.is_recognized = False
                        created_segment.save()
                        
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

