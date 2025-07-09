from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from .models import Channel, GeneralSetting, WellnessBucket
import json
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from .serializer import channel_to_dict


def create_channel(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            channel = Channel.objects.create(
                name=data.get('name', ''),
                channel_id=data['channel_id'],
                project_id=data['project_id']
            )
            return JsonResponse({'success': True, 'id': channel.id})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)
    return JsonResponse({'error': 'POST request required'}, status=405)


@csrf_exempt
@require_http_methods(["PUT"])
def update_settings_and_buckets(request):
    try:
        print("----------------")
        print(request)
        data = json.loads(request.body)
        print(data)
        # Update or create GeneralSetting (assuming only one row)
        settings_data = data.get('settings', {})
        settings_obj, _ = GeneralSetting.objects.get_or_create(id=settings_data.get('id', 1))
        for field in [
            'openai_api_key', 'openai_org_id', 'arc_cloud_api_key', 'revai_access_token',
            'summarize_transcript_prompt', 'sentiment_analysis_prompt', 'general_topics_prompt', 'iab_topics_prompt']:
            if field in settings_data:
                setattr(settings_obj, field, settings_data[field])
        settings_obj.save()

        # Upsert WellnessBuckets
        buckets = data.get('buckets', [])
        bucket_ids_in_payload = set()
        for bucket in buckets:
            bucket_id = bucket.get('bucket_id')
            # if bucket_id:
            wb, _ = WellnessBucket.objects.get_or_create(bucket_id=bucket_id)
            # else:
            #     # Generate a new bucket_id (e.g., 'bucket_N')
            #     last = WellnessBucket.objects.order_by('-id').first()
            #     next_id = f"bucket_{(last.id + 1) if last else 1}"
            #     wb = WellnessBucket(bucket_id=next_id)
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
            channel = Channel.objects.create(
                name=data.get('name', ''),
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
        # Soft delete
        try:
            data = json.loads(request.body)
            channel_id = data.get('id')
            if not channel_id:
                return JsonResponse({'success': False, 'error': 'channel id required'}, status=400)
            channel = Channel.objects.get(id=channel_id, is_deleted=False)
            channel.is_deleted = True
            channel.save()
            return JsonResponse({'success': True})
        except Channel.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

