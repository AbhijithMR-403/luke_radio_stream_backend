from datetime import datetime
import json
import os
from urllib.parse import unquote, urlparse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.http import FileResponse, JsonResponse
from django.utils import timezone

from acr_admin.models import Channel
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel
from data_analysis.services.audio_segments import AudioSegments
from data_analysis.tasks import analyze_transcription_task, bulk_download_audio_task

# Create your views here.


@method_decorator(csrf_exempt, name='dispatch')
class UnrecognizedAudioSegmentsView(View):
    def get(self, request, *args, **kwargs):
        try:
            channel_pk = request.GET.get('channel_id')
            date = request.GET.get('date')
            hour_offset = int(request.GET.get('hour_offset', 0))
            if not channel_pk:
                return JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
            try:
                channel = Channel.objects.get(id=channel_pk, is_deleted=False)
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
            data = AudioSegments.fetch_data(int(channel.project_id), int(channel.channel_id), date)
            unrecognized = AudioSegments.find_unrecognized_segments(data, hour_offset=hour_offset, date=date)
            val_path = bulk_download_audio_task.delay(channel.project_id, channel.channel_id, unrecognized) 
            return JsonResponse({'success': True, 'unrecognized_segments': unrecognized})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)


@method_decorator(csrf_exempt, name='dispatch')
class SeparatedAudioSegmentsView(View):
    def get(self, request, *args, **kwargs):
        try:
            channel_pk = request.GET.get('channel_id')
            date = request.GET.get('date')
            hour = request.GET.get('hour')  # New parameter for specific hour
            
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
            
            # Convert date_obj back to string format for the API call
            date_string = date_obj.strftime('%Y%m%d')
            
            # Determine which hours to fetch
            if hour is not None:
                # Fetch only the specified hour
                try:
                    hour_int = int(hour)
                    if hour_int < 0 or hour_int > 23:
                        return JsonResponse({'success': False, 'error': 'Hour must be between 0 and 23'}, status=400)
                    hours_to_fetch = [hour_int]
                except ValueError:
                    return JsonResponse({'success': False, 'error': 'Invalid hour format. Use 0-23'}, status=400)
            else:
                # Default to hour 0 if no hour specified
                hours_to_fetch = [0]
            
            # Fetch data for the entire date once
            try:
                data = AudioSegments.fetch_data(
                    project_id=int(channel.project_id), 
                    channel_id=int(channel.channel_id), 
                    date=date_string,
                    hours=hours_to_fetch
                )
                
                # Process and get segments for the date (already filtered by hours)
                all_segments = AudioSegments.get_audio_segments_with_recognition_status(
                    data=data,
                    channel=channel,
                )
                
                # Add hour information to each segment
                for segment in all_segments:
                    if 'start_time' in segment and segment['start_time']:
                        segment['hour'] = segment['start_time'].hour
                
            except Exception as e:
                print(f"Error fetching data: {str(e)}")
                all_segments = []
            
            # Create timezone-aware datetime range for the selected date
            start_of_day = timezone.make_aware(datetime.combine(date_obj, datetime.min.time()))
            end_of_day = timezone.make_aware(datetime.combine(date_obj, datetime.max.time()))
            
            # Now fetch segments from database for the specified date range and hours
            # Filter by specific hour(s) - now always a single hour (either specified or default 0)
            hour_start = timezone.make_aware(datetime.combine(date_obj, datetime.min.time().replace(hour=hours_to_fetch[0])))
            hour_end = timezone.make_aware(datetime.combine(date_obj, datetime.max.time().replace(hour=hours_to_fetch[0])))
            
            db_segments = AudioSegmentsModel.objects.filter(
                channel=channel,
                is_active=True,
                start_time__gte=hour_start,
                start_time__lt=hour_end
            ).order_by('start_time')
            
            # Convert database objects to dictionary format
            all_segments = []
            for segment in db_segments:
                segment_data = {
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
                    'created_at': segment.created_at.isoformat() if segment.created_at else None
                }
                all_segments.append(segment_data)
            
            # Separate recognized and unrecognized segments
            recognized_segments = [segment for segment in all_segments if segment["is_recognized"]]
            unrecognized_segments = [segment for segment in all_segments if not segment["is_recognized"]]
            
            result = {
                "recognized": recognized_segments,
                "unrecognized": unrecognized_segments,
                "total_recognized": len(recognized_segments),
                "total_unrecognized": len(unrecognized_segments),
                "total_segments": len(all_segments)
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
