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
from .service import SettingsAndBucketsService
from config.validation import ValidationUtils
import json
from django.views import View
from django.utils.decorators import method_decorator


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
        """
        Update settings and buckets.
        All validation is handled by SettingsAndBucketsSerializer.
        Business logic is handled by SettingsAndBucketsService.
        """
        # Step 1: Validate request payload using serializer - ALL field validation happens here
        serializer = SettingsAndBucketsSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                {'success': False, 'error': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Step 2: Use service layer to handle business logic with validated data
        try:
            validated_data = serializer.validated_data
            result = SettingsAndBucketsService.update_settings_and_buckets(validated_data)
            
            return Response({
                'success': True,
                'settings': result['settings'],
                'buckets': result['buckets']
            })
        except ValidationError as ve:
            # Handle validation errors from service layer
            return Response(
                {'success': False, 'error': str(ve)},
                status=status.HTTP_400_BAD_REQUEST
            )
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
            channel = Channel.objects.get(id=channel_id, is_deleted=False)
            channel.is_deleted = True
            channel.save()
            return JsonResponse({'success': True})
        except Channel.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)


