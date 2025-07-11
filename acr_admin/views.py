from urllib.parse import urlparse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from .models import Channel, GeneralSetting, WellnessBucket, RevTranscriptionJob
import json
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from .serializer import channel_to_dict
from .serializer import general_setting_to_dict, wellness_bucket_to_dict
from .utils import ACRCloudUtils
from .utils import UnrecognizedAudioTimestamps
from django.http import StreamingHttpResponse
from .utils import AudioDownloader
from datetime import datetime
from .utils import RevAISpeechToText



@method_decorator(csrf_exempt, name='dispatch')
class SettingsAndBucketsView(View):
    def get(self, request, *args, **kwargs):
        try:
            settings_obj = GeneralSetting.objects.first()
            settings_data = general_setting_to_dict(settings_obj) if settings_obj else None
            buckets = WellnessBucket.objects.all()
            buckets_data = [wellness_bucket_to_dict(b) for b in buckets]
            return JsonResponse({'success': True, 'settings': settings_data, 'buckets': buckets_data})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

    def put(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            settings_data = data.get('settings', {})
            settings_obj, _ = GeneralSetting.objects.get_or_create(id=settings_data.get('id', 1))
            for field in [
                'openai_api_key', 'openai_org_id', 'arc_cloud_api_key', 'revai_access_token',
                'summarize_transcript_prompt', 'sentiment_analysis_prompt', 'general_topics_prompt', 'iab_topics_prompt']:
                if field in settings_data:
                    setattr(settings_obj, field, settings_data[field])
            settings_obj.save()

            buckets = data.get('buckets', [])
            bucket_ids_in_payload = set()
            for bucket in buckets:
                bucket_id = bucket.get('bucket_id')
                wb, _ = WellnessBucket.objects.get_or_create(bucket_id=bucket_id)
                wb.title = bucket.get('title', '')
                wb.description = bucket.get('description', '')
                wb.prompt = bucket.get('prompt', '')
                wb.save()
                bucket_ids_in_payload.add(wb.bucket_id)

            return JsonResponse({'success': True, 'settings_id': settings_obj.id, 'bucket_ids': list(bucket_ids_in_payload)})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)


@method_decorator(csrf_exempt, name='dispatch')
class ChannelCRUDView(View):
    def get(self, request, *args, **kwargs):
        # List or retrieve
        channel_id = request.GET.get('channel_id')
        if channel_id:
            try:
                channel = Channel.objects.get(channel_id=channel_id, is_deleted=False)
                return JsonResponse({'success': True, 'channel': channel_to_dict(channel)})
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
        else:
            channels = Channel.objects.filter(is_deleted=False)
            return JsonResponse({'success': True, 'channels': [channel_to_dict(c) for c in channels]})

    def post(self, request, *args, **kwargs):
        # Create
        try:
            data = json.loads(request.body)
            # Always validate project_id and channel_id first
            result, status = ACRCloudUtils.get_channel_name_by_id(data['project_id'], data['channel_id'])
            if status is not None:
                return JsonResponse({'success': False, **result}, status=status)
            # Use provided name if present, else use fetched name
            name = data.get('name', '') or result
            channel = Channel.objects.create(
                name=name,
                channel_id=data['channel_id'],
                project_id=data['project_id']
            )
            return JsonResponse({'success': True, 'channel': channel_to_dict(channel)})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

    def put(self, request, *args, **kwargs):
        # Edit
        try:
            data = json.loads(request.body)
            channel_id = data.get('id')
            if not channel_id:
                return JsonResponse({'success': False, 'error': 'channel id required'}, status=400)
            channel = Channel.objects.get(id=channel_id, is_deleted=False)
            for field in ['channel_id', 'name', 'project_id']:
                if field in data:
                    setattr(channel, field, data[field])
            channel.save()
            return JsonResponse({'success': True, 'channel': channel_to_dict(channel)})
        except Channel.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

    def delete(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            channel_id = data.get('id')
            if not channel_id:
                return JsonResponse({'success': False, 'error': 'channel id required'}, status=400)
            channel = Channel.objects.get(id=channel_id)
            channel.delete()
            return JsonResponse({'success': True})
        except Channel.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)


@method_decorator(csrf_exempt, name='dispatch')
class UnrecognizedAudioSegmentsView(View):
    def get(self, request, *args, **kwargs):
        try:
            project_id = request.GET.get('project_id')
            channel_id = request.GET.get('channel_id')
            date = request.GET.get('date')
            hour_offset = int(request.GET.get('hour_offset', 0))
            if not project_id or not channel_id:
                return JsonResponse({'success': False, 'error': 'project_id and channel_id are required'}, status=400)
            data = UnrecognizedAudioTimestamps.fetch_data(int(project_id), int(channel_id), date)
            unrecognized = UnrecognizedAudioTimestamps.find_unrecognized_segments(data, hour_offset=hour_offset, date=date)
            print(unrecognized[0].get("start_time"), "_---------", unrecognized[0].get("duration_seconds"))
            val_path = AudioDownloader.bulk_download_audio(project_id, channel_id, unrecognized)
            return JsonResponse({'success': True, 'unrecognized_segments': unrecognized, 'val_path': val_path})
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
                created_on = datetime.now()
        
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
                RevAISpeechToText.get_transcript_by_job_id(job, parsed_url.path)
            except Exception as e:
                print(e)
                return JsonResponse({'success': False, 'error': str(e)}, status=400)
        return JsonResponse({'success': True, 'action': action, 'job_id': job.job_id})
