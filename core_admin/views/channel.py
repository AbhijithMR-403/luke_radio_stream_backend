from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.core.exceptions import ValidationError
from django.db import transaction

from ..models import Channel
from ..serializers import ChannelSerializer
from ..utils import ACRCloudUtils
from rss_ingestion.tasks import ingest_podcast_rss_feed_task


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
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data
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

        # Trigger RSS ingestion for podcast channels
        if channel_type == 'podcast':
            ingest_podcast_rss_feed_task.delay(channel.id)

        return Response(
            {'success': True, 'channel': ChannelSerializer(channel).data},
            status=status.HTTP_201_CREATED
        )

    def put(self, request, *args, **kwargs):
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

        serializer = ChannelSerializer(
            channel,
            data=request.data,
            partial=True
        )
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data

        # Define allowed updatable fields
        allowed_fields = {'name', 'timezone', 'is_active'}
        
        # Check if any disallowed fields were provided
        disallowed_fields = set(validated.keys()) - allowed_fields
        if disallowed_fields:
            return Response(
                {'success': False, 'error': f'Fields not allowed to update: {", ".join(disallowed_fields)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Filter to only allowed fields
        updatable_data = {k: v for k, v in validated.items() if k in allowed_fields}

        # Handle empty name → fetch from ACR Cloud (only for broadcast channels)
        if 'name' in updatable_data:
            name = updatable_data['name']
            if not name or not name.strip():
                # For podcast channels, name cannot be empty
                if channel.channel_type == 'podcast':
                    return Response(
                        {'success': False, 'error': 'Name is required for Podcast channels'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                # Only fetch from ACR Cloud for broadcast channels
                elif channel.channel_type == 'broadcast' and channel.project_id and channel.channel_id:
                    result, status_code = ACRCloudUtils.get_channel_name_by_id(
                        channel.project_id,
                        channel.channel_id
                    )
                    if status_code:
                        return Response(
                            {'success': False, **result},
                            status=status_code
                        )
                    updatable_data['name'] = result or channel.name  # Fallback to existing name
                else:
                    # For broadcast channels without ACR Cloud data, keep existing name
                    updatable_data['name'] = channel.name

        # Update channel attributes
        for attr, value in updatable_data.items():
            setattr(channel, attr, value)

        # Save will trigger model validation
        # Wrap in transaction to protect against partial writes if ACR Cloud fails
        try:
            with transaction.atomic():
                channel.save()
        except ValidationError as e:
            return Response(
                {'success': False, 'error': str(e.message_dict) if hasattr(e, 'message_dict') else str(e)},
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
