from typing import Optional, List, Dict, Any
from django.db.models import QuerySet, Sum, Q
from django.core.exceptions import ValidationError
from datetime import datetime

from logger.models import AudioSegmentEditLog
from data_analysis.models import AudioSegments, RevTranscriptionJob
from core_admin.models import Channel


class AudioSegmentEditLogDAO:
    """Data Access Object for AudioSegmentEditLog model"""
    
    @staticmethod
    def get_by_id(edit_log_id: int) -> Optional[AudioSegmentEditLog]:
        """Get an edit log by ID"""
        try:
            return AudioSegmentEditLog.objects.get(id=edit_log_id)
        except AudioSegmentEditLog.DoesNotExist:
            return None
    
    @staticmethod
    def get_all() -> QuerySet[AudioSegmentEditLog]:
        """Get all edit logs"""
        return AudioSegmentEditLog.objects.all()
    
    @staticmethod
    def get_by_audio_segment(audio_segment_id: int) -> QuerySet[AudioSegmentEditLog]:
        """Get all edit logs for a specific audio segment"""
        return AudioSegmentEditLog.objects.filter(audio_segment_id=audio_segment_id)
    
    @staticmethod
    def get_by_action(action: str) -> QuerySet[AudioSegmentEditLog]:
        """Get all edit logs by action type"""
        return AudioSegmentEditLog.objects.filter(action=action)
    
    @staticmethod
    def get_by_trigger_type(trigger_type: str) -> QuerySet[AudioSegmentEditLog]:
        """Get all edit logs by trigger type"""
        return AudioSegmentEditLog.objects.filter(trigger_type=trigger_type)
    
    @staticmethod
    def get_by_user(user_id: int) -> QuerySet[AudioSegmentEditLog]:
        """Get all edit logs by user"""
        return AudioSegmentEditLog.objects.filter(user_id=user_id)
    
    @staticmethod
    def create(
        audio_segment: AudioSegments,
        action: str,
        trigger_type: str,
        user=None,
        metadata: Optional[Dict[str, Any]] = None,
        notes: Optional[str] = None,
        affected_segments: Optional[List[AudioSegments]] = None
    ) -> AudioSegmentEditLog:
        """Create a new edit log"""
        edit_log = AudioSegmentEditLog(
            audio_segment=audio_segment,
            action=action,
            trigger_type=trigger_type,
            user=user,
            metadata=metadata,
            notes=notes
        )
        edit_log.full_clean()
        edit_log.save()
        
        if affected_segments:
            edit_log.affected_segments.set(affected_segments)
        
        return edit_log
    
    @staticmethod
    def update(edit_log_id: int, **kwargs) -> Optional[AudioSegmentEditLog]:
        """Update an edit log"""
        edit_log = AudioSegmentEditLogDAO.get_by_id(edit_log_id)
        if not edit_log:
            return None
        
        for key, value in kwargs.items():
            if hasattr(edit_log, key):
                setattr(edit_log, key, value)
        
        edit_log.full_clean()
        edit_log.save()
        return edit_log
    
    @staticmethod
    def delete(edit_log_id: int) -> bool:
        """Delete an edit log"""
        edit_log = AudioSegmentEditLogDAO.get_by_id(edit_log_id)
        if not edit_log:
            return False
        
        edit_log.delete()
        return True
    
    @staticmethod
    def filter_by_date_range(start_date, end_date) -> QuerySet[AudioSegmentEditLog]:
        """Filter edit logs by date range"""
        return AudioSegmentEditLog.objects.filter(
            created_at__gte=start_date,
            created_at__lte=end_date
        )


class RevTranscriptionJobLogDAO:
    """Data Access Object for RevTranscriptionJob statistics"""
    
    @staticmethod
    def get_duration_totals_by_channel(
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        channel_ids: Optional[List[int]] = None,
        audio_segment_start_time: Optional[datetime] = None,
        audio_segment_end_time: Optional[datetime] = None,
        rev_transcription_job_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get total duration_seconds for each channel from RevTranscriptionJob.
        
        Args:
            start_time: Optional start time filter (for job's created_on)
            end_time: Optional end time filter (for job's created_on)
            channel_ids: Optional list of channel IDs to filter by
            audio_segment_start_time: Optional start time filter for audio_segment's start_time
            audio_segment_end_time: Optional end time filter for audio_segment's start_time
            rev_transcription_job_ids: Optional list of RevTranscriptionJob IDs to filter by
        
        Returns:
            List of dictionaries with channel_id, channel_name, and total_duration_seconds
        """
        queryset = RevTranscriptionJob.objects.all()
        
        # Filter out jobs without audio_segment (they don't have a channel)
        queryset = queryset.filter(audio_segment__isnull=False)
        
        # Apply job time filters if provided
        if start_time:
            queryset = queryset.filter(created_on__gte=start_time)
        if end_time:
            queryset = queryset.filter(created_on__lte=end_time)
        
        # Apply channel filter if provided
        if channel_ids:
            queryset = queryset.filter(audio_segment__channel_id__in=channel_ids)
        
        # Apply audio segment start_time filter if provided
        if audio_segment_start_time:
            queryset = queryset.filter(audio_segment__start_time__gte=audio_segment_start_time)
        if audio_segment_end_time:
            queryset = queryset.filter(audio_segment__start_time__lte=audio_segment_end_time)
        
        # Apply RevTranscriptionJob filter if provided
        if rev_transcription_job_ids:
            queryset = queryset.filter(id__in=rev_transcription_job_ids)
        
        # Aggregate by channel
        results = queryset.values(
            'audio_segment__channel_id', 
            'audio_segment__channel__name'
        ).annotate(
            total_duration_seconds=Sum('duration_seconds')
        ).order_by('audio_segment__channel_id')
        
        return [
            {
                'channel_id': item['audio_segment__channel_id'],
                'channel_name': item['audio_segment__channel__name'],
                'total_duration_seconds': item['total_duration_seconds'] or 0.0,
            }
            for item in results
        ]
    
    @staticmethod
    def get_grand_total_duration(
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        channel_ids: Optional[List[int]] = None,
        audio_segment_start_time: Optional[datetime] = None,
        audio_segment_end_time: Optional[datetime] = None,
        rev_transcription_job_ids: Optional[List[int]] = None,
    ) -> float:
        """
        Get grand total duration_seconds across all channels from RevTranscriptionJob.
        
        Args:
            start_time: Optional start time filter (for job's created_on)
            end_time: Optional end time filter (for job's created_on)
            channel_ids: Optional list of channel IDs to filter by
            audio_segment_start_time: Optional start time filter for audio_segment's start_time
            audio_segment_end_time: Optional end time filter for audio_segment's start_time
            rev_transcription_job_ids: Optional list of RevTranscriptionJob IDs to filter by
        
        Returns:
            Total duration in seconds (float)
        """
        queryset = RevTranscriptionJob.objects.all()
        
        # Filter out jobs without audio_segment (they don't have a channel)
        queryset = queryset.filter(audio_segment__isnull=False)
        
        # Apply job time filters if provided
        if start_time:
            queryset = queryset.filter(created_on__gte=start_time)
        if end_time:
            queryset = queryset.filter(created_on__lte=end_time)
        
        # Apply channel filter if provided
        if channel_ids:
            queryset = queryset.filter(audio_segment__channel_id__in=channel_ids)
        
        # Apply audio segment start_time filter if provided
        if audio_segment_start_time:
            queryset = queryset.filter(audio_segment__start_time__gte=audio_segment_start_time)
        if audio_segment_end_time:
            queryset = queryset.filter(audio_segment__start_time__lte=audio_segment_end_time)
        
        # Apply RevTranscriptionJob filter if provided
        if rev_transcription_job_ids:
            queryset = queryset.filter(id__in=rev_transcription_job_ids)
        
        # Get total sum
        result = queryset.aggregate(total=Sum('duration_seconds'))
        return result['total'] or 0.0
    
    @staticmethod
    def get_duration_statistics(
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        channel_ids: Optional[List[int]] = None,
        audio_segment_start_time: Optional[datetime] = None,
        audio_segment_end_time: Optional[datetime] = None,
        rev_transcription_job_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Get complete duration statistics including per-channel totals and grand total from RevTranscriptionJob.
        
        Args:
            start_time: Optional start time filter (for job's created_on)
            end_time: Optional end time filter (for job's created_on)
            channel_ids: Optional list of channel IDs to filter by
            audio_segment_start_time: Optional start time filter for audio_segment's start_time
            audio_segment_end_time: Optional end time filter for audio_segment's start_time
            rev_transcription_job_ids: Optional list of RevTranscriptionJob IDs to filter by
        
        Returns:
            Dictionary with:
            - channels: List of per-channel totals
            - grand_total: Total across all channels
        """
        channels = RevTranscriptionJobLogDAO.get_duration_totals_by_channel(
            start_time=start_time,
            end_time=end_time,
            channel_ids=channel_ids,
            audio_segment_start_time=audio_segment_start_time,
            audio_segment_end_time=audio_segment_end_time,
            rev_transcription_job_ids=rev_transcription_job_ids
        )
        grand_total = RevTranscriptionJobLogDAO.get_grand_total_duration(
            start_time=start_time,
            end_time=end_time,
            channel_ids=channel_ids,
            audio_segment_start_time=audio_segment_start_time,
            audio_segment_end_time=audio_segment_end_time,
            rev_transcription_job_ids=rev_transcription_job_ids
        )
        
        return {
            'channels': channels,
            'grand_total': grand_total,
        }

