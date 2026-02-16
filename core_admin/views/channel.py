from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAdminUser
from django.core.exceptions import ValidationError
from django.db import transaction
from datetime import datetime

from ..models import Channel
from ..serializers import ChannelSerializer, ChannelPatchSerializer, SetChannelDefaultSettingsSerializer
from ..utils import ACRCloudUtils, channel_has_complete_settings
from ..repositories import GeneralSettingService
from rss_ingestion.tasks import ingest_podcast_rss_feed_task
from rss_ingestion.service import RSSIngestionService


class ChannelAPIView(APIView):

    def get(self, request, *args, **kwargs):
        channel_id = request.query_params.get('channel_id')

        if channel_id:
            channel = Channel.objects.filter(
                channel_id=channel_id,
                is_deleted=False
            ).first()

            if not channel:
                return Response(
                    {'success': False, 'error': 'Channel not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            serializer = ChannelSerializer(channel)
            return Response({'success': True, 'channel': serializer.data})

        channels = Channel.objects.filter(is_deleted=False)
        serializer = ChannelSerializer(channels, many=True)
        return Response({'success': True, 'channels': serializer.data})

    def post(self, request, *args, **kwargs):
        serializer = ChannelSerializer(data=request.data)
        serializer.is_valid()
        if serializer.errors:
            return Response(
                {'success': False, 'error': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST
            )

        validated = serializer.validated_data
        replicate_default_settings = validated.pop('replicate_default_settings', False)
        channel_type = validated['channel_type']

        # Fetch name from ACR Cloud if not provided (only for broadcast channels)
        if channel_type == 'broadcast':
            result, status_code = ACRCloudUtils.get_channel_name_by_id(
                validated['project_id'],
                validated['channel_id']
            )
            if status_code:
                return Response(
                    {'success': False, **result},
                    status=status_code
                )
            validated['name'] = validated.get('name') or result

        # Custom Audio: only name is used; require and strip it
        if channel_type == 'custom_audio':
            name = (validated.get('name') or '').strip()
            if not name:
                return Response(
                    {'success': False, 'error': 'Name is required for Custom Audio channels'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            validated['name'] = name

        # Validate RSS feed URL for podcast channels
        if channel_type == 'podcast':
            rss_url = validated.get('rss_url')
            if rss_url:
                rss_service = RSSIngestionService(rss_url).fetch()
                if rss_service.status != 200:
                    return Response(
                        {'success': False, 'error': 'The podcast RSS URL is not valid'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

        # Create channel with all validated data in a transaction
        # This protects against partial writes if ACR Cloud fails
        try:
            with transaction.atomic():
                channel = Channel.objects.create(**validated)
        except ValidationError as e:
            return Response(
                {'success': False, 'error': str(e.message_dict) if hasattr(e, 'message_dict') else str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Replicate settings from default channel if requested
        if replicate_default_settings:
            default_channel = Channel.objects.filter(
                is_default_settings=True, is_deleted=False
            ).first()
            if not default_channel:
                return Response(
                    {
                        'success': False,
                        'error': 'No default settings channel is set. Set a channel as default first, or leave "replicate_default_settings" false to create with empty settings.',
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            try:
                GeneralSettingService.transfer_settings(
                    default_channel.id, channel.id, getattr(request, 'user', None)
                )
            except ValidationError as e:
                return Response(
                    {'success': False, 'error': str(e)},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        # Trigger RSS ingestion for podcast channels
        if channel_type == 'podcast':
            ingest_podcast_rss_feed_task.delay(channel.id)

        return Response(
            {'success': True, 'channel': ChannelSerializer(channel).data},
            status=status.HTTP_201_CREATED
        )

    def patch(self, request, *args, **kwargs):  # Changed from put to patch
        channel_id = request.data.get('id')
        if not channel_id:
            return Response({'success': False, 'error': 'id required'}, status=400)

        channel = Channel.objects.filter(id=channel_id, is_deleted=False).first()
        if not channel:
            return Response({'success': False, 'error': 'Channel not found'}, status=404)

        # Use the patch-specific serializer
        serializer = ChannelPatchSerializer(channel, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)

        # Save will trigger model validation
        try:
            with transaction.atomic():
                channel = serializer.save()
        except ValidationError as e:
            if getattr(e, 'message_dict', None):
                first_message = next(
                    (str(m) for msgs in e.message_dict.values() for m in (msgs if isinstance(msgs, list) else [msgs])),
                    str(e)
                )
            else:
                first_message = str(e)
            return Response(
                {'success': False, 'error': first_message},
                status=status.HTTP_400_BAD_REQUEST
            )

        return Response({'success': True, 'channel': ChannelSerializer(channel).data})

    def delete(self, request, *args, **kwargs):
        channel_id = request.data.get('id')
        if not channel_id:
            return Response(
                {'success': False, 'error': 'channel id required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        channel = Channel.objects.filter(
            id=channel_id,
            is_deleted=False
        ).first()

        if not channel:
            return Response(
                {'success': False, 'error': 'Channel not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        channel.is_deleted = True
        channel.save()

        return Response(status=status.HTTP_204_NO_CONTENT)


class SetChannelDefaultSettingsAPIView(APIView):
    """Set or unset a channel as default. Only one channel can be default; requires complete active settings to set."""

    def post(self, request, *args, **kwargs):
        serializer = SetChannelDefaultSettingsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        channel = Channel.objects.filter(id=data['channel_id'], is_deleted=False).first()
        if not channel:
            return Response({'success': False, 'error': 'Channel not found'}, status=status.HTTP_404_NOT_FOUND)

        if data['is_default_settings']:
            ok, missing = channel_has_complete_settings(channel)
            if not ok:
                err = (
                    'Channel has no active settings. Create and activate settings first.'
                    if not missing else
                    'Cannot set as default: required settings fields are missing.'
                )
                body = {'success': False, 'error': err}
                if missing:
                    body['missing_fields'] = missing
                return Response(body, status=status.HTTP_400_BAD_REQUEST)
            channel.is_default_settings = True
            channel.save(update_fields=['is_default_settings'])
        else:
            channel.is_default_settings = False
            channel.save(update_fields=['is_default_settings'])

        return Response({'success': True, 'channel': ChannelSerializer(channel).data})


class IngestPodcastRSSFeedAPIView(APIView):
    """API endpoint to manually trigger RSS feed ingestion for a podcast channel."""

    def post(self, request, *args, **kwargs):
        channel_id = request.data.get('channel_id')
        if not channel_id:
            return Response(
                {'success': False, 'error': 'channel_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        channel = Channel.objects.filter(
            id=channel_id,
            is_deleted=False
        ).first()

        if not channel:
            return Response(
                {'success': False, 'error': 'Channel not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        if channel.channel_type != 'podcast':
            return Response(
                {'success': False, 'error': 'RSS feed ingestion is only available for podcast channels'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Trigger the RSS feed ingestion task
        ingest_podcast_rss_feed_task.delay(channel.id)

        return Response(
            {'success': True, 'message': f'RSS feed ingestion task queued for channel: {channel.name}'},
            status=status.HTTP_202_ACCEPTED
        )


class RSSFeedTotalDurationAPIView(APIView):
    """API endpoint to get the total duration of an RSS feed."""
    permission_classes = [IsAdminUser]

    def post(self, request, *args, **kwargs):
        rss_url = request.data.get('rss_url')
        rss_start_date = request.data.get('rss_start_date')

        if not rss_url:
            return Response(
                {'success': False, 'error': 'rss_url is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Parse rss_start_date if provided
        parsed_start_date = None
        if rss_start_date:
            try:
                parsed_start_date = datetime.fromisoformat(rss_start_date.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                return Response(
                    {'success': False, 'error': 'Invalid rss_start_date format. Use ISO format: 2000-01-01T00:00:00Z'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Fetch and validate RSS feed
        rss_service = RSSIngestionService(rss_url).fetch()

        if rss_service.status != 200:
            return Response(
                {'success': False, 'error': 'The RSS URL is not valid'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Get total duration
        total_duration_seconds = rss_service.get_total_duration_seconds(rss_start_date=parsed_start_date)

        return Response({
            'success': True,
            'total_duration_seconds': total_duration_seconds,
            'rss_url': rss_url,
            'rss_start_date': rss_start_date
        })
