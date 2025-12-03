from urllib.parse import urlparse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.core.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .models import Channel, GeneralSetting, WellnessBucket
from .utils import ACRCloudUtils
from .serializer import (
    channel_to_dict, 
    general_setting_to_dict, 
    wellness_bucket_to_dict,
    SettingsAndBucketsSerializer
)
from config.validation import ValidationUtils
import json
from django.views import View
from django.utils.decorators import method_decorator
from django.conf import settings
from urllib.parse import unquote


class SettingsAndBucketsView(APIView):
    def get(self, request, *args, **kwargs):
        try:
            settings_obj = GeneralSetting.objects.first()
            settings_data = general_setting_to_dict(settings_obj) if settings_obj else None
            buckets = WellnessBucket.objects.all()
            buckets_data = [wellness_bucket_to_dict(b) for b in buckets]
            return Response({'success': True, 'settings': settings_data, 'buckets': buckets_data})
        except Exception as e:
            return Response({'success': False, 'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

    def put(self, request, *args, **kwargs):
        # Validate request payload using serializer FIRST - all validation happens here
        serializer = SettingsAndBucketsSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                {'success': False, 'error': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Only proceed if validation passes - use validated_data exclusively
        try:
            validated_data = serializer.validated_data
            settings_data = validated_data.get('settings') or {}
            buckets = validated_data.get('buckets', [])

            # Get or create settings object
            settings_id = settings_data.get('id', 1)
            settings_obj, _ = GeneralSetting.objects.get_or_create(id=settings_id)

            # Update settings fields from validated data only
            # List of all fields in GeneralSetting
            general_setting_fields = [
                'openai_api_key', 'openai_org_id', 'acr_cloud_api_key', 'revai_access_token',
                'summarize_transcript_prompt', 'sentiment_analysis_prompt', 'general_topics_prompt', 'iab_topics_prompt',
                'bucket_definition_error_rate', 'chatgpt_model', 'chatgpt_max_tokens',
                'chatgpt_temperature', 'chatgpt_top_p', 'chatgpt_frequency_penalty', 'chatgpt_presence_penalty',
                'determine_radio_content_type_prompt', 'content_type_prompt', 'radio_segment_types', 'radio_segment_error_rate'
            ]
            for field in general_setting_fields:
                if field in settings_data:
                    setattr(settings_obj, field, settings_data[field])
            settings_obj.save()

            # Process buckets from validated data only
            bucket_ids_in_payload = set()
            for bucket in buckets:
                bucket_id = bucket.get('id')
                if bucket_id:
                    # Update existing bucket
                    try:
                        wb = WellnessBucket.objects.get(id=bucket_id)
                    except WellnessBucket.DoesNotExist:
                        return Response(
                            {'success': False, 'error': f'Bucket with id {bucket_id} not found'}, 
                            status=status.HTTP_404_NOT_FOUND
                        )
                else:
                    # Create new bucket
                    wb = WellnessBucket()
                
                # Use validated data only - all fields are already validated
                wb.title = bucket.get('title', '')
                wb.description = bucket.get('description', '')
                wb.category = bucket.get('category')  # Required and validated by serializer
                wb.save()
                bucket_ids_in_payload.add(wb.id)

            # Delete buckets that are not present in the payload
            existing_bucket_ids = set(WellnessBucket.objects.values_list('id', flat=True))
            buckets_to_delete = existing_bucket_ids - bucket_ids_in_payload
            if buckets_to_delete:
                WellnessBucket.objects.filter(id__in=buckets_to_delete).delete()

            buckets_data = [wellness_bucket_to_dict(b) for b in WellnessBucket.objects.all()]
            return Response({
                'success': True, 
                'settings': general_setting_to_dict(settings_obj), 
                'buckets': buckets_data
            })
        except Exception as e:
            return Response(
                {'success': False, 'error': str(e)}, 
                status=status.HTTP_400_BAD_REQUEST
            )


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
            
            # Validate timezone if provided
            timezone = data.get('timezone', 'UTC')
            if timezone:
                ValidationUtils.validate_timezone(timezone)
            
            channel = Channel.objects.create(
                name=name,
                channel_id=data['channel_id'],
                project_id=data['project_id'],
                timezone=timezone
            )
            return JsonResponse({'success': True, 'channel': channel_to_dict(channel)})
        except ValidationError as ve:
            return JsonResponse({'success': False, 'error': str(ve)}, status=400)
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
            
            # Validate timezone if provided
            if 'timezone' in data:
                ValidationUtils.validate_timezone(data['timezone'])
            
            # Handle name update - if empty, fetch from ACR Cloud API
            if 'name' in data:
                name = data['name']
                if not name or name.strip() == '':
                    # Fetch name from ACR Cloud API using existing channel's project_id and channel_id
                    result, status = ACRCloudUtils.get_channel_name_by_id(channel.project_id, channel.channel_id)
                    if status is not None:
                        return JsonResponse({'success': False, **result}, status=status)
                    name = result or channel.name  # Use fetched name or keep existing if API fails
                setattr(channel, 'name', name)
            
            # Only allow updating name and timezone (not channel_id or project_id)
            if 'timezone' in data:
                setattr(channel, 'timezone', data['timezone'])
            
            channel.save()
            return JsonResponse({'success': True, 'channel': channel_to_dict(channel)})
        except Channel.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
        except ValidationError as ve:
            return JsonResponse({'success': False, 'error': str(ve)}, status=400)
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


