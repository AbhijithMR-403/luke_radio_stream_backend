from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.core.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAdminUser

from .models import Channel, GeneralSetting, WellnessBucket
from .utils import ACRCloudUtils
from .serializer import (
    channel_to_dict, 
    general_setting_to_dict, 
    wellness_bucket_to_dict,
    SettingsAndBucketsSerializer,
    SettingsAndBucketsResponseSerializer
)
from .service import SettingsAndBucketsService
from .repositories import GeneralSettingService
from config.validation import ValidationUtils
import json
from django.views import View
from django.utils.decorators import method_decorator


class SettingsAndBucketsView(APIView):
    permission_classes = [IsAdminUser]
    def get(self, request, *args, **kwargs):
        try:
            # Get the active settings version
            settings_obj = GeneralSetting.objects.filter(is_active=True).first()
            
            # Get buckets for the active settings (non-deleted only)
            if settings_obj:
                buckets = settings_obj.wellness_buckets.filter(is_deleted=False)
            else:
                buckets = WellnessBucket.objects.none()
            
            # Use serializer for response
            response_data = {
                'settings': settings_obj,
                'buckets': buckets
            }
            serializer = SettingsAndBucketsResponseSerializer(response_data)
            
            return Response({'success': True, **serializer.data})
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

    def post(self, request, *args, **kwargs):
        """
        Create a new version of settings and buckets.
        All validation is handled by SettingsAndBucketsSerializer.
        Business logic is handled by GeneralSettingService.create_new_version.
        """
        # Step 1: Validate request payload using serializer - ALL field validation happens here
        serializer = SettingsAndBucketsSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                {'success': False, 'error': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Step 2: Use repository service to create new version with validated data
        try:
            validated_data = serializer.validated_data
            settings_data = validated_data.get('settings') or {}
            buckets_data = validated_data.get('buckets', [])
            change_reason = validated_data.get('change_reason')
            user = request.user if request.user.is_authenticated else None
            
            # Create new version using repository method
            new_setting = GeneralSettingService.create_new_version(
                settings_data=settings_data,
                buckets_data=buckets_data,
                user=user,
                change_reason=change_reason
            )
            
            # Get buckets for the new version
            buckets = new_setting.wellness_buckets.filter(is_deleted=False)
            buckets_data = [wellness_bucket_to_dict(b) for b in buckets]
            
            return Response({
                'success': True,
                'settings': general_setting_to_dict(new_setting),
                'buckets': buckets_data
            }, status=status.HTTP_201_CREATED)
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


