from datetime import datetime
import json
import os
from urllib.parse import unquote, urlparse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.http import FileResponse, JsonResponse
from django.utils import timezone

from acr_admin.models import Channel
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel, TranscriptionDetail, TranscriptionAnalysis
from data_analysis.services.audio_segments import AudioSegments
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.tasks import analyze_transcription_task

# Create your views here.


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
            
            # Fetch segments from database for the specified filters
            db_segments = AudioSegmentsModel.objects.filter(**filter_conditions).order_by('start_time')
            
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
                
                # Fetch transcription details regardless of is_active flag
                try:
                    transcription_detail = TranscriptionDetail.objects.get(audio_segment=segment)
                    segment_data['transcription'] = {
                        'id': transcription_detail.id,
                        'transcript': transcription_detail.transcript,
                        'created_at': transcription_detail.created_at.isoformat() if transcription_detail.created_at else None,
                        'rev_job_id': transcription_detail.rev_job.job_id if transcription_detail.rev_job else None
                    }
                    
                    # Fetch analysis data regardless of is_analysis_completed flag
                    try:
                        analysis = TranscriptionAnalysis.objects.get(transcription_detail=transcription_detail)
                        segment_data['analysis'] = {
                            'id': analysis.id,
                            'summary': analysis.summary,
                            'sentiment': analysis.sentiment,
                            'general_topics': analysis.general_topics,
                            'iab_topics': analysis.iab_topics,
                            'bucket_prompt': analysis.bucket_prompt,
                            'created_at': analysis.created_at.isoformat() if analysis.created_at else None
                        }
                    except TranscriptionAnalysis.DoesNotExist:
                        # No analysis found
                        segment_data['analysis'] = None
                        
                except TranscriptionDetail.DoesNotExist:
                    # No transcription detail found
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
    def get(self, request, *args, **kwargs):
        try:
            channel_pk = request.GET.get('channel_id')
            start_datetime = request.GET.get('start_datetime')
            end_datetime = request.GET.get('end_datetime')
            
            if not channel_pk:
                return JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
            
            try:
                channel = Channel.objects.get(id=channel_pk, is_deleted=False)
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
            
            # Build filter conditions
            filter_conditions = {'channel': channel}
            
            # Parse and add start_datetime filter if provided
            if start_datetime:
                try:
                    # Try ISO format first (e.g., 2025-08-02T10:30:00)
                    if 'T' in start_datetime:
                        start_dt = datetime.fromisoformat(start_datetime.replace('Z', '+00:00'))
                    else:
                        # Try YYYY-MM-DD HH:MM:SS format
                        start_dt = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
                    
                    # Make timezone-aware if not already
                    if timezone.is_naive(start_dt):
                        start_dt = timezone.make_aware(start_dt)
                    
                    filter_conditions['start_time__gte'] = start_dt
                except ValueError:
                    return JsonResponse({'success': False, 'error': 'Invalid start_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
            
            # Parse and add end_datetime filter if provided
            if end_datetime:
                try:
                    # Try ISO format first (e.g., 2025-08-02T10:30:00)
                    if 'T' in end_datetime:
                        end_dt = datetime.fromisoformat(end_datetime.replace('Z', '+00:00'))
                    else:
                        # Try YYYY-MM-DD HH:MM:SS format
                        end_dt = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')
                    
                    # Make timezone-aware if not already
                    if timezone.is_naive(end_dt):
                        end_dt = timezone.make_aware(end_dt)
                    
                    filter_conditions['start_time__lte'] = end_dt
                except ValueError:
                    return JsonResponse({'success': False, 'error': 'Invalid end_datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS'}, status=400)
            
            # If no datetime filters provided, use today's date as default
            if not start_datetime and not end_datetime:
                today_start = timezone.make_aware(datetime.combine(timezone.now().date(), datetime.min.time()))
                today_end = timezone.make_aware(datetime.combine(timezone.now().date(), datetime.max.time()))
                filter_conditions['start_time__gte'] = today_start
                filter_conditions['start_time__lt'] = today_end
            
            # Fetch segments from database for the specified filters
            db_segments = AudioSegmentsModel.objects.filter(**filter_conditions).order_by('start_time')
            
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
                    'is_audio_downloaded': segment.is_audio_downloaded,
                    'metadata_json': segment.metadata_json
                }
                
                # Fetch transcription details regardless of is_active flag
                try:
                    transcription_detail = TranscriptionDetail.objects.get(audio_segment=segment)
                    segment_data['transcription'] = {
                        'id': transcription_detail.id,
                        'transcript': transcription_detail.transcript,
                        'created_at': transcription_detail.created_at.isoformat() if transcription_detail.created_at else None,
                        'rev_job_id': transcription_detail.rev_job.job_id if transcription_detail.rev_job else None
                    }
                    
                    # Fetch analysis data regardless of is_analysis_completed flag
                    try:
                        analysis = TranscriptionAnalysis.objects.get(transcription_detail=transcription_detail)
                        segment_data['analysis'] = {
                            'id': analysis.id,
                            'summary': analysis.summary,
                            'sentiment': analysis.sentiment,
                            'general_topics': analysis.general_topics,
                            'iab_topics': analysis.iab_topics,
                            'bucket_prompt': analysis.bucket_prompt,
                            'created_at': analysis.created_at.isoformat() if analysis.created_at else None
                        }
                    except TranscriptionAnalysis.DoesNotExist:
                        # No analysis found
                        segment_data['analysis'] = None
                        
                except TranscriptionDetail.DoesNotExist:
                    # No transcription detail found
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

