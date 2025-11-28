from __future__ import annotations

from datetime import datetime
from typing import Optional

from django.db.models import QuerySet

from acr_admin.models import Channel
from data_analysis.models import AudioSegments


class AudioSegmentDAO:
    """Data Access Object for filtering AudioSegments with transcription details and analysis."""

    @staticmethod
    def filter(
        *,
        channel: int | Channel,
        start_time: datetime | None,
        end_time: Optional[datetime] = None,
    ) -> QuerySet[AudioSegments]:
        """
        Optimized filter method for AudioSegments with maximum database performance.
        
        This method is designed to use database indexes efficiently:
        - Uses ['channel', 'start_time'] index when channel and start_time are provided
        - Uses ['channel', 'is_delete'] index for is_delete filter
        - Uses ['channel', 'is_active'] index for is_active filter
        
        Automatically applies filters:
        - is_active=True (only active segments)
        - is_delete=False (excludes deleted segments)
        
        Automatically includes related data from:
        - transcription_detail (OneToOneField)
        - transcription_detail.analysis (OneToOneField via TranscriptionDetail)
        
        Args:
            channel: Filter by channel ID or Channel object (required)
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
            
            # Filter by channel and start_time only
            segments = AudioSegmentDAO.filter(
                channel=1,
                start_time=datetime(2024, 1, 1)
            )
        """
        # Extract channel ID if Channel object is provided
        channel_id = channel.id if isinstance(channel, Channel) else channel
        
        # Start with channel filter first for optimal index usage
        # This uses the ['channel', 'start_time'] composite index efficiently
        qs = AudioSegments.objects.filter(channel_id=channel_id)
        
        # Apply is_delete filter early (uses ['channel', 'is_delete'] index)
        qs = qs.filter(is_delete=False)
        
        # Apply start_time filter (uses ['channel', 'start_time'] index)
        if start_time is not None:
            qs = qs.filter(start_time__gte=start_time)
        
        # Apply end_time filter using start_time for better index usage
        # This ensures we use the ['channel', 'start_time'] index instead of end_time
        if end_time is not None:
            qs = qs.filter(start_time__lte=end_time)
        
        # Apply is_active filter (uses ['channel', 'is_active'] index)
        qs = qs.filter(is_active=True)
        
        # Optimize with select_related for ForeignKey and OneToOne relationships
        # This fetches transcription_detail and transcription_detail.analysis in a single query
        qs = qs.select_related(
            'transcription_detail',
            'transcription_detail__analysis'
        )
        
        # Order by start_time (uses the same ['channel', 'start_time'] index)
        qs = qs.order_by('start_time')
        
        return qs

