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

from acr_admin.models import Channel
from data_analysis.models import (
    AudioSegments as AudioSegmentsModel,
)
from data_analysis.services.audio_download import ACRCloudAudioDownloader
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
    """Create a merged segment with source='merged' and download audio"""
    duration_seconds = int((end_dt - start_dt).total_seconds())
    
    # Build file_name and file_path
    start_time_str = start_dt.strftime("%Y%m%d%H%M%S")
    file_name = f"audio_{channel.project_id}_{channel.channel_id}_{start_time_str}_{duration_seconds}.mp3"
    start_date = start_dt.strftime("%Y%m%d")
    file_path = f"media/{start_date}/{file_name}"
    
    # Ensure the directory exists
    media_dir = os.path.join(os.getcwd(), "media", start_date)
    os.makedirs(media_dir, exist_ok=True)
    
    # Create segment payload with source='merged'
    segment_payload = {
        'start_time': start_dt,
        'end_time': end_dt,
        'duration_seconds': duration_seconds,
        'is_recognized': False,
        'is_active': True,
        'file_name': file_name,
        'file_path': file_path,
        'channel': channel,
        'source': 'merged',
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
    
    # Download audio
    media_url = ACRCloudAudioDownloader.download_audio(
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

            # Case 1: Process a list of existing segment IDs (cluster by <=2s gaps and segregate)
            if isinstance(segment_ids, list) and len(segment_ids) > 0:
                # Require at least 2 segment IDs
                if len(segment_ids) < 2:
                    return Response({'success': False, 'error': 'At least 2 segment_ids are required'}, status=status.HTTP_400_BAD_REQUEST)
                
                # Fetch all segments; preserve only found IDs, report not found
                db_segments = (
                    AudioSegmentsModel.objects
                    .filter(id__in=segment_ids)
                    .select_related('channel')
                    .only('id', 'start_time', 'end_time', 'duration_seconds', 'file_path', 'channel__id', 'channel__project_id', 'channel__channel_id', 'channel__timezone')
                )
                found_by_id = {s.id: s for s in db_segments}
                for sid in segment_ids:
                    if sid not in found_by_id:
                        errors.append({'segment_id': sid, 'error': 'Segment not found'})
                if not found_by_id:
                    return Response({'success': False, 'error': 'No valid segment_ids found'}, status=status.HTTP_400_BAD_REQUEST)

                # Validate all segments are from the same channel
                unique_channels = set(seg.channel.id for seg in found_by_id.values())
                if len(unique_channels) > 1:
                    return Response({
                        'success': False,
                        'error': f'All segments must be from the same channel. Found segments from {len(unique_channels)} different channels.',
                        'channels': list(unique_channels)
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Validate channels and sort by time
                segments_sorted = sorted(found_by_id.values(), key=lambda s: s.start_time)

                # Cluster by <=2s gap (all segments are already from same channel)
                def cluster_by_gap(segs):
                    groups = []
                    current = []
                    for seg in segs:
                        if not current:
                            current = [seg]
                            continue
                        prev = current[-1]
                        gap = (seg.start_time - prev.end_time).total_seconds()
                        if gap <= 2:
                            current.append(seg)
                        else:
                            groups.append(current)
                            current = [seg]
                    if current:
                        groups.append(current)
                    return groups

                all_groups = cluster_by_gap(segments_sorted)

                # Build groups metadata for response
                groups_meta = []
                for grp in all_groups:
                    grp_sorted = sorted(grp, key=lambda s: s.start_time)
                    gaps = []
                    for i in range(1, len(grp_sorted)):
                        gaps.append((grp_sorted[i].start_time - grp_sorted[i-1].end_time).total_seconds())
                    groups_meta.append({
                        'segment_ids': [s.id for s in grp_sorted],
                        'channel_id': grp_sorted[0].channel.id if grp_sorted and grp_sorted[0].channel else None,
                        'start_time': grp_sorted[0].start_time.isoformat() if grp_sorted else None,
                        'end_time': grp_sorted[-1].end_time.isoformat() if grp_sorted else None,
                        'total_duration_seconds': int((grp_sorted[-1].end_time - grp_sorted[0].start_time).total_seconds()) if grp_sorted else 0,
                        'gaps_seconds': gaps,
                    })

                # Only allow one audio download at a time
                if len(all_groups) != 1:
                    return Response({
                        'success': False,
                        'error': 'Multiple groups detected. Submit one group at a time.',
                        'data': { 'groups': groups_meta }
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Create one combined segment for the single group and download its audio
                grp = sorted(all_groups[0], key=lambda s: s.start_time)
                combined_start = grp[0].start_time
                combined_end = grp[-1].end_time
                channel = grp[0].channel
                combined_duration = int((combined_end - combined_start).total_seconds())
                if combined_duration > 3600:
                    return Response({'success': False, 'error': 'Maximum allowed duration is 3600 seconds (1 hour)'}, status=status.HTTP_400_BAD_REQUEST)

                try:
                    source_ids = [s.id for s in grp]
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

