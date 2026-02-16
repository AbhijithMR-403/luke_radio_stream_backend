from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAdminUser
from django.core.exceptions import ValidationError

from ..models import GeneralSetting, WellnessBucket
from core_admin.serializers.settings import (
    SettingsAndBucketsSerializer,
    SettingsAndBucketsResponseSerializer,
    RevertToVersionSerializer,
)
from ..repositories import GeneralSettingService


class SettingsAndBucketsAPIView(APIView):
    permission_classes = [IsAdminUser]

    def get(self, request, *args, **kwargs):
        try:
            channel_id = request.query_params.get('channel_id')

            if channel_id is not None and channel_id != '' and  not channel_id.isdigit():
                raise ValidationError('channel_id must be a valid integer')

            settings_obj = GeneralSetting.objects.filter(is_active=True, channel_id=channel_id).first()
            buckets = None
            if settings_obj:
                buckets = settings_obj.wellness_buckets.filter(is_deleted=False)

            serializer = SettingsAndBucketsResponseSerializer({
                'settings': settings_obj,
                'buckets': buckets
            })

            return Response(
                {'success': True, **serializer.data},
                status=status.HTTP_200_OK
            )

        except ValidationError as ve:
            return Response(
                {'success': False, 'error': ve},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'success': False, 'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

    def post(self, request, *args, **kwargs):
        serializer = SettingsAndBucketsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data
        user = request.user if request.user.is_authenticated else None

        try:
            # Get active setting to merge with incoming settings
            incoming_settings = validated.get('settings') or {}
            channel_id = validated.get('settings').get('channel_id')
            active_setting = GeneralSetting.objects.filter(is_active=True, channel_id=channel_id).first()
            
            # Merge incoming settings with active setting values
            # Missing keys in incoming_settings will use values from active_setting
            if active_setting:
                # Get all concrete model fields (excluding versioning fields)
                excluded_fields = {
                    'id', 'version', 'is_active', 'created_at', 'created_by', 
                    'parent_version', 'change_reason'
                }
                
                # Start with active setting values
                merged_settings = {}
                # Use concrete_fields to get only actual database columns
                for field in GeneralSetting._meta.concrete_fields:
                    field_name = field.name
                    if field_name not in excluded_fields:
                        # Get the value from active setting
                        merged_settings[field_name] = getattr(active_setting, field_name, None)
                
                # Override with incoming settings (only provided keys)
                merged_settings.update(incoming_settings)
            else:
                # No active setting, use incoming settings as-is
                merged_settings = incoming_settings
                print(f"merged_settings: {merged_settings}")
            
            new_setting = GeneralSettingService.create_new_version(
                settings_data=merged_settings,
                buckets_data=validated.get('buckets', []),
                user=user,
                change_reason=validated.get('change_reason'),
            )

            buckets = new_setting.wellness_buckets.filter(is_deleted=False)

            response_serializer = SettingsAndBucketsResponseSerializer({
                'settings': new_setting,
                'buckets': buckets
            })

            return Response(
                {'success': True, **response_serializer.data},
                status=status.HTTP_201_CREATED
            )

        except ValidationError as ve:
            return Response(
                {'success': False, 'error': str(ve)},
                status=status.HTTP_400_BAD_REQUEST
            )

        except Exception as e:
            return Response(
                {'success': False, 'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )


class RevertToVersionAPIView(APIView):
    """Revert channel settings to a specific historical version by creating a new version cloned from it."""
    permission_classes = [IsAdminUser]

    def post(self, request, *args, **kwargs):
        serializer = RevertToVersionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        user = request.user if request.user.is_authenticated else None

        try:
            new_setting = GeneralSettingService.revert_to_version(
                channel_id=data['channel_id'],
                target_version_number=data['target_version_number'],
                user=user,
            )
            buckets = new_setting.wellness_buckets.filter(is_deleted=False)
            response_serializer = SettingsAndBucketsResponseSerializer({
                'settings': new_setting,
                'buckets': buckets,
            })
            return Response(
                {'success': True, **response_serializer.data},
                status=status.HTTP_201_CREATED,
            )
        except ValidationError as ve:
            return Response(
                {'success': False, 'error': str(ve)},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            return Response(
                {'success': False, 'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST,
            )
