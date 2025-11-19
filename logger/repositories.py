from typing import Optional, List, Dict, Any
from django.db.models import QuerySet
from django.core.exceptions import ValidationError

from logger.models import AudioSegmentEditLog
from data_analysis.models import AudioSegments


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

