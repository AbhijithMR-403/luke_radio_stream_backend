from __future__ import annotations

from datetime import datetime
from typing import Optional

from django.db.models import QuerySet

from core_admin.models import Channel
from data_analysis.models import AudioSegments


class AudioSegmentDAO:
    """Data Access Object for filtering AudioSegments with transcription details and analysis."""

    @staticmethod
    def filter(
        *,
        channel: int | Channel | None = None,
        report_folder_id: int| None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> QuerySet[AudioSegments]:
        """
        Optimized filter method for AudioSegments with maximum database performance.
        
        This method supports filtering by either channel or report_folder_id (or both).
        It is designed to use database indexes efficiently:
        - Uses ['channel', 'start_time'] index when channel and start_time are provided
        - Uses ['channel', 'is_delete'] index for is_delete filter
        - Uses ['channel', 'is_active'] index for is_active filter
        - Uses ['folder'] index on SavedAudioSegment when report_folder_id is provided
        
        Automatically applies filters:
        - is_active=True (only active segments)
        - is_delete=False (excludes deleted segments)
        
        Automatically includes related data from:
        - transcription_detail (OneToOneField)
        - transcription_detail.analysis (OneToOneField via TranscriptionDetail)
        - channel (ForeignKey) - included when report_folder_id is used or when needed
        
        Args:
            channel: Filter by channel ID or Channel object (optional, but at least one of channel or report_folder_id must be provided)
            report_folder_id: Filter by report folder ID (optional, but at least one of channel or report_folder_id must be provided)
            start_time: Filter segments with start_time >= start_time
            end_time: Filter segments with start_time <= end_time (uses start_time index for optimization)
        
        Returns:
            QuerySet[AudioSegments]: Optimized QuerySet with related transcription_detail and analysis data prefetched
        
        Examples:
            # Filter by channel and time range
            segments = AudioSegmentDAO.filter(
                channel=1,
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 31)
            )
            
            # Filter by channel only
            segments = AudioSegmentDAO.filter(channel=1)
            
        """
        # Validate that at least one filter is provided
        if channel is None and report_folder_id is None:
            raise ValueError("At least one of 'channel' or 'report_folder_id' must be provided")
        
        # Start with base queryset
        qs = AudioSegments.objects.all()
        
        # Apply channel filter if provided
        if channel is not None:
            # Extract channel ID if Channel object is provided
            channel_id = channel.id if isinstance(channel, Channel) else channel
            # This uses the ['channel', 'start_time'] composite index efficiently
            qs = qs.filter(channel_id=channel_id)
        
        # Apply report_folder_id filter if provided
        if report_folder_id is not None:
            # Filter through SavedAudioSegment relationship using report_folder_id
            # This uses the ['folder'] index on SavedAudioSegment efficiently
            qs = qs.filter(saved_in_folders__folder_id=report_folder_id)
        
        # Apply is_delete filter early
        qs = qs.filter(is_delete=False)
        
        # Apply start_time filter (uses ['channel', 'start_time'] index when channel is provided)
        if start_time is not None:
            qs = qs.filter(start_time__gte=start_time)
        
        # Apply end_time filter using start_time for better index usage
        # This ensures we use the ['channel', 'start_time'] index instead of end_time
        if end_time is not None:
            qs = qs.filter(start_time__lte=end_time)
        
        # Apply is_active filter (uses ['channel', 'is_active'] index when channel is provided)
        qs = qs.filter(is_active=True)
        
        # Optimize with select_related for ForeignKey and OneToOne relationships
        # Channel is always included since at least one of channel or report_folder_id must be provided
        qs = qs.select_related(
            'channel',
            'transcription_detail',
            'transcription_detail__analysis'
        )
        
        # Use distinct() when report_folder_id is used to avoid duplicates from the join
        if report_folder_id is not None:
            qs = qs.distinct()
        
        # Order by start_time (uses the same ['channel', 'start_time'] index when channel is provided)
        qs = qs.order_by('start_time')
        
        return qs

