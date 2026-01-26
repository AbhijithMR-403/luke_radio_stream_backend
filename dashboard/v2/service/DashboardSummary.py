from __future__ import annotations

from typing import Optional, List, Dict, Iterable, Any, Tuple
from datetime import datetime
from django.db.models import Q

from data_analysis.models import AudioSegments, ReportFolder, SavedAudioSegment
from data_analysis.repositories import AudioSegmentDAO
from audio_policy.models import FlagCondition
from shift_analysis.models import Shift


class ShiftNotFound(Exception):
    """Raised when a requested shift is not found."""
    pass


class ReportFolderNotFound(Exception):
    """Raised when a requested report folder is not found."""
    pass


class SummaryService:

    @staticmethod
    def _get_sentiment_score_from_segment(segment: AudioSegments) -> Optional[float]:
        """
        Extract and parse sentiment score from an audio segment.
        
        Note: Assumes segment has transcription_detail and analysis (guaranteed by query filters).
        
        Args:
            segment: AudioSegments instance to extract sentiment from
        
        Returns:
            Parsed sentiment score as float, or None if sentiment is missing or invalid
        """
        transcription_detail = segment.transcription_detail
        analysis = transcription_detail.analysis
        if not analysis.sentiment:
            return None
        
        return SummaryService._parse_sentiment_score(analysis.sentiment)

    @staticmethod
    def _parse_sentiment_score(value: Any) -> Optional[float]:
        if isinstance(value, str):
            value = value.strip().rstrip('%')

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def get_average_sentiment(
        audio_segments: Iterable[AudioSegments],
    ) -> Optional[float]:
        total_weighted_sentiment = 0.0
        total_duration = 0.0
        
        for segment in audio_segments:
            score = SummaryService._get_sentiment_score_from_segment(segment)
            if score is None:
                continue
            
            duration_seconds = float(segment.duration_seconds) if segment.duration_seconds else 0.0
            if duration_seconds > 0:
                total_weighted_sentiment += score * duration_seconds
                total_duration += duration_seconds
        if total_duration > 0:
            return round(total_weighted_sentiment / total_duration, 3)
        
        return None

    @staticmethod
    def get_low_sentiment_percentage(
        audio_segments: Iterable[AudioSegments],
        min_lower: Optional[float] = None,
        min_upper: Optional[float] = None,
    ) -> Optional[float]:
        """
        Calculate percentage of segments with low sentiment (within range).
        
        Args:
            audio_segments: List of AudioSegments with transcription_detail and analysis loaded
            min_lower: Lower bound of low sentiment range (inclusive)
            min_upper: Upper bound of low sentiment range (inclusive)
        
        Returns:
            Percentage of low sentiment segments (rounded to 2 decimal places) or None if no data
        """
        # If no range is provided, return None
        if min_lower is None and min_upper is None:
            return None
        
        low_sentiment_weighted_duration = 0
        total_duration = 0
        
        for segment in audio_segments:
            score = SummaryService._get_sentiment_score_from_segment(segment)
            if score is None:
                continue
            
            # Get duration from audio segment
            duration_seconds = segment.duration_seconds or 0
            
            if duration_seconds > 0:
                total_duration += duration_seconds
                # Check if score is within the low sentiment range
                meets_min_lower = min_lower is None or score >= min_lower
                meets_min_upper = min_upper is None or score <= min_upper
                if meets_min_lower and meets_min_upper:
                    low_sentiment_weighted_duration += duration_seconds
        
        if total_duration > 0:
            percentage = (low_sentiment_weighted_duration / total_duration) * 100
            return round(percentage, 2)
        
        return None

    @staticmethod
    def get_high_sentiment_percentage(
        audio_segments: Iterable[AudioSegments],
        max_lower: Optional[float] = None,
        max_upper: Optional[float] = None,
    ) -> Optional[float]:
        """
        Calculate percentage of segments with high sentiment (within range).
        
        Args:
            audio_segments: List of AudioSegments with transcription_detail and analysis loaded
            max_lower: Lower bound of high sentiment range (inclusive)
            max_upper: Upper bound of high sentiment range (inclusive)
        
        Returns:
            Percentage of high sentiment segments (rounded to 2 decimal places) or None if no data
        """
        # If no range is provided, return None
        if max_lower is None and max_upper is None:
            return None
        
        high_sentiment_weighted_duration = 0
        total_duration = 0
        
        for segment in audio_segments:
            score = SummaryService._get_sentiment_score_from_segment(segment)
            if score is None:
                continue
            
            # Get duration from audio segment
            duration_seconds = segment.duration_seconds or 0
            
            if duration_seconds > 0:
                total_duration += duration_seconds
                # Check if score is within the high sentiment range
                meets_max_lower = max_lower is None or score >= max_lower
                meets_max_upper = max_upper is None or score <= max_upper
                if meets_max_lower and meets_max_upper:
                    high_sentiment_weighted_duration += duration_seconds
        
        if total_duration > 0:
            percentage = (high_sentiment_weighted_duration / total_duration) * 100
            return round(percentage, 2)
        
        return None

    @staticmethod
    def get_per_day_average_sentiments(
        audio_segments: Iterable[AudioSegments],
    ) -> list[dict[str, float | str]]:
        """
        Calculate average sentiment per day using duration-weighted formula.
        
        Args:
            audio_segments: List of AudioSegments with transcription_detail and analysis loaded
        
        Returns:
            List of dictionaries with 'date' and 'average_sentiment' keys
            Format: [{'date': 'DD/MM/YYYY', 'average_sentiment': float}, ...]
        """
        # Group segments by date
        daily_data: Dict[str, Dict[str, float]] = {}
        
        for segment in audio_segments:
            score = SummaryService._get_sentiment_score_from_segment(segment)
            if score is None:
                continue
            
            # Get transcription_detail for date extraction
            transcription_detail = segment.transcription_detail
            created_at = transcription_detail.created_at
            if not created_at:
                continue
            
            # Convert to date string in DD/MM/YYYY format
            date_key = created_at.date().strftime('%d/%m/%Y')
            
            # Initialize daily data if not exists
            if date_key not in daily_data:
                daily_data[date_key] = {
                    'total_weighted_sentiment': 0,
                    'total_duration': 0
                }
            
            # Get duration from audio segment
            duration_seconds = segment.duration_seconds or 0
            
            if duration_seconds > 0:
                daily_data[date_key]['total_weighted_sentiment'] += score * duration_seconds
                daily_data[date_key]['total_duration'] += duration_seconds
        
        # Calculate average sentiment for each day
        result = []
        for date_str, data in sorted(daily_data.items()):
            if data['total_duration'] > 0:
                avg_sentiment = round(
                    data['total_weighted_sentiment'] / data['total_duration'],
                    3
                )
                result.append({
                    'date': date_str,
                    'average_sentiment': avg_sentiment
                })
        
        return result

    @staticmethod
    def _get_sentiment_thresholds(channel_id: int) -> Dict[str, float]:
        """
        Get sentiment thresholds from FlagCondition for a given channel.
        
        Args:
            channel_id: Channel ID to get thresholds for
        
        Returns:
            Dictionary containing:
            - target_sentiment_score: Target sentiment score (default: 75)
            - sentiment_min_lower: Lower bound for low sentiment (default: 0.0)
            - sentiment_min_upper: Upper bound for low sentiment (default: 20.0)
            - sentiment_max_lower: Lower bound for high sentiment (default: 80.0)
            - sentiment_max_upper: Upper bound for high sentiment (default: 100.0)
        """
        # Default values: low sentiment 0-20, high sentiment 80-100
        target_sentiment_score = 75.0
        sentiment_min_lower = 0.0
        sentiment_min_upper = 20.0
        sentiment_max_lower = 80.0
        sentiment_max_upper = 100.0
        
        try:
            flag_condition = FlagCondition.objects.get(channel_id=channel_id)
            if flag_condition.target_sentiments is not None:
                target_sentiment_score = float(flag_condition.target_sentiments)
            if flag_condition.sentiment_min_lower is not None:
                sentiment_min_lower = float(flag_condition.sentiment_min_lower)
            if flag_condition.sentiment_min_upper is not None:
                sentiment_min_upper = float(flag_condition.sentiment_min_upper)
            if flag_condition.sentiment_max_lower is not None:
                sentiment_max_lower = float(flag_condition.sentiment_max_lower)
            if flag_condition.sentiment_max_upper is not None:
                sentiment_max_upper = float(flag_condition.sentiment_max_upper)
        except FlagCondition.DoesNotExist:
            # If no flag condition exists, use default values
            pass
        
        return {
            'target_sentiment_score': target_sentiment_score,
            'sentiment_min_lower': sentiment_min_lower,
            'sentiment_min_upper': sentiment_min_upper,
            'sentiment_max_lower': sentiment_max_lower,
            'sentiment_max_upper': sentiment_max_upper
        }

    @staticmethod
    def _build_empty_summary_response(
        channel_id: Optional[int],
        total_talk_break: int = 0
    ) -> Dict[str, Any]:
        """
        Build an empty summary response structure with no sentiment data.
        
        Args:
            channel_id: Channel ID to get thresholds for (if None, uses defaults)
            total_talk_break: Total talk break count (default: 0)
        
        Returns:
            Dictionary containing empty summary structure with thresholds
        """
        if channel_id is not None:
            thresholds = SummaryService._get_sentiment_thresholds(channel_id)
        else:
            # Use defaults if no channel_id provided
            thresholds = {
                'target_sentiment_score': 75.0,
                'sentiment_min_lower': 0.0,
                'sentiment_min_upper': 20.0,
                'sentiment_max_lower': 80.0,
                'sentiment_max_upper': 100.0
            }
        
        return {
            'average_sentiment': None,
            'target_sentiment_score': thresholds['target_sentiment_score'],
            'low_sentiment': None,
            'high_sentiment': None,
            'per_day_average_sentiments': [],
            'thresholds': {
                'target_sentiment_score': thresholds['target_sentiment_score'],
                'low_sentiment_range': {
                    'min_lower': thresholds['sentiment_min_lower'],
                    'min_upper': thresholds['sentiment_min_upper']
                },
                'high_sentiment_range': {
                    'max_lower': thresholds['sentiment_max_lower'],
                    'max_upper': thresholds['sentiment_max_upper']
                }
            },
            'analyzed_segment_count': 0,
            'total_talk_break': total_talk_break
        }

    @staticmethod
    def _get_audio_segments_by_channel(
        *,
        channel_id: int,
        start_dt: datetime,
        end_dt: datetime,
        shift_id: Optional[int] = None,
    ) -> Tuple[List[AudioSegments], int]:
        """
        Get audio segments filtered by channel with datetime and optional shift filtering.
        
        Args:
            channel_id: Channel ID to filter by
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            shift_id: Optional shift ID to filter by
        
        Returns:
            Tuple of (audio_segments, total_talk_break)
            - audio_segments: List of AudioSegments with transcription and analysis
            - total_talk_break: Count of segments with transcription
        
        Raises:
            ShiftNotFound: If shift_id is provided but the shift doesn't exist
        """
        # Build Q object for filtering
        if shift_id is not None:
            shift = Shift.objects.filter(id=shift_id, channel_id=channel_id).first()
            if not shift:
                raise ShiftNotFound(f"Shift with id {shift_id} not found for channel {channel_id}")
            # Get Q object from shift's get_datetime_filter method
            # This already filters by the shift's time windows within the datetime range
            q_object = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
        else:
            # No shift filter - use datetime range filter
            q_object = Q(start_time__gte=start_dt, start_time__lt=end_dt)
        
        # Use filter_with_q for optimized filtering with Q object
        # This method automatically handles select_related for transcription_detail and analysis
        audio_segments_query = AudioSegmentDAO.filter_with_q(
            q_objects=q_object,
            channel_id=channel_id,
            is_active=True,
            is_delete=False,
            has_content=True  # Ensures transcription_detail exists
        ).filter(
            transcription_detail__analysis__isnull=False  # Also ensure analysis exists
        )
        
        # Get all audio segments with transcription and analysis
        audio_segments = list(audio_segments_query)
        
        # Count total talk break (segments with transcription, is_active=True, is_delete=False)
        # This uses the same datetime/shift filter but doesn't require analysis
        total_talk_break_query = AudioSegmentDAO.filter_with_q(
            q_objects=q_object,
            channel_id=channel_id,
            is_active=True,
            is_delete=False,
            has_content=True  # Ensures transcription_detail exists
        )
        total_talk_break = total_talk_break_query.count()
        
        return audio_segments, total_talk_break

    @staticmethod
    def _get_audio_segments_by_report_folder(
        *,
        report_folder_id: int,
        start_dt: datetime,
        end_dt: datetime,
        shift_id: Optional[int] = None,
    ) -> Tuple[List[AudioSegments], int, int]:
        """
        Get audio segments filtered by report folder with datetime and optional shift filtering.
        
        Args:
            report_folder_id: Report folder ID to filter by
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            shift_id: Optional shift ID to filter by
        
        Returns:
            Tuple of (audio_segments, total_talk_break, channel_id)
            - audio_segments: List of AudioSegments with transcription and analysis
            - total_talk_break: Count of segments with transcription
            - channel_id: Channel ID from the report folder
        
        Raises:
            ReportFolderNotFound: If the report folder doesn't exist
            ShiftNotFound: If shift_id is provided but the shift doesn't exist
        """
        report_folder = ReportFolder.objects.select_related('channel').filter(id=report_folder_id).first()
        if not report_folder:
            raise ReportFolderNotFound(f"Report folder with id {report_folder_id} not found")
        
        # Get channel_id from the folder for FlagCondition lookup and shift validation
        channel_id = report_folder.channel.id
        
        # Get all saved audio segments in this folder
        saved_segments = SavedAudioSegment.objects.filter(
            folder=report_folder
        ).select_related(
            'audio_segment',
            'audio_segment__transcription_detail',
            'audio_segment__transcription_detail__analysis'
        ).prefetch_related(
            'audio_segment__channel'
        )
        
        # Get audio segments from saved segments
        audio_segment_ids = [saved.audio_segment.id for saved in saved_segments]
        
        # Filter by datetime range and other conditions
        base_query = AudioSegments.objects.filter(
            id__in=audio_segment_ids,
            start_time__gte=start_dt,
            start_time__lt=end_dt,
            is_active=True,
            is_delete=False,
            transcription_detail__isnull=False,
            transcription_detail__analysis__isnull=False
        ).select_related(
            'channel',
            'transcription_detail',
            'transcription_detail__analysis'
        )
        
        # Apply shift filter if provided
        if shift_id is not None:
            shift = Shift.objects.filter(id=shift_id, channel_id=channel_id).first()
            if not shift:
                raise ShiftNotFound(f"Shift with id {shift_id} not found for channel {channel_id}")
            # Get Q object from shift's get_datetime_filter method
            q_object = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
            # Apply shift filter to base query
            base_query = base_query.filter(q_object)
        
        # Get all audio segments with transcription and analysis
        audio_segments = list(base_query)
        
        # Count total talk break (segments with transcription, is_active=True, is_delete=False)
        total_talk_break = SavedAudioSegment.objects.filter(
            folder=report_folder,
            audio_segment__start_time__gte=start_dt,
            audio_segment__start_time__lt=end_dt,
            audio_segment__is_active=True,
            audio_segment__is_delete=False,
            audio_segment__transcription_detail__isnull=False
        ).count()
        
        return audio_segments, total_talk_break, channel_id

    @staticmethod
    def get_summary_data(
        *,
        channel_id: Optional[int] = None,
        start_dt: datetime,
        end_dt: datetime,
        shift_id: Optional[int] = None,
        report_folder_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get complete summary data including all sentiment metrics.
        
        This method handles:
        - Filtering audio segments by datetime, channel (or report folder), and optionally shift
        - Getting sentiment thresholds from FlagCondition (or defaults)
        - Calculating all sentiment metrics
        - Building the complete response data
        
        Args:
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            shift_id: Optional shift ID to filter by
            report_folder_id: Optional report folder ID to filter by (required if channel_id not provided)
        
        Returns:
            Dictionary containing all summary data including:
            - average_sentiment
            - target_sentiment_score
            - low_sentiment_percentage
            - high_sentiment_percentage
            - per_day_average_sentiments
            - thresholds
            - analyzed_segment_count
            - total_talk_break
        """
        # Handle report folder filtering
        if report_folder_id is not None:
            try:
                audio_segments, total_talk_break, channel_id = SummaryService._get_audio_segments_by_report_folder(
                    report_folder_id=report_folder_id,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    shift_id=shift_id
                )
            except (ReportFolderNotFound, ShiftNotFound) as e:
                # If report folder or shift doesn't exist, return empty result with error response
                if isinstance(e, ReportFolderNotFound):
                    error_response = SummaryService._build_empty_summary_response(
                        channel_id=None,
                        total_talk_break=0
                    )
                else:  # ShiftNotFound
                    # For shift not found, we need to get channel_id from the report folder
                    report_folder = ReportFolder.objects.select_related('channel').filter(id=report_folder_id).first()
                    if report_folder:
                        channel_id = report_folder.channel.id
                        # Count total talk break even when shift doesn't exist
                        total_talk_break = SavedAudioSegment.objects.filter(
                            folder=report_folder,
                            audio_segment__start_time__gte=start_dt,
                            audio_segment__start_time__lt=end_dt,
                            audio_segment__is_active=True,
                            audio_segment__is_delete=False,
                            audio_segment__transcription_detail__isnull=False
                        ).count()
                        error_response = SummaryService._build_empty_summary_response(
                            channel_id=channel_id,
                            total_talk_break=total_talk_break
                        )
                    else:
                        error_response = SummaryService._build_empty_summary_response(
                            channel_id=None,
                            total_talk_break=0
                        )
                return error_response
        else:
            # Original channel-based filtering logic
            if channel_id is None:
                raise ValueError("Either channel_id or report_folder_id must be provided")
            
            try:
                audio_segments, total_talk_break = SummaryService._get_audio_segments_by_channel(
                    channel_id=channel_id,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    shift_id=shift_id
                )
            except ShiftNotFound:
                # If shift doesn't exist, return empty result with error response
                audio_segments = []
                # Count total talk break even when shift doesn't exist
                total_talk_break_query = AudioSegmentDAO.filter_with_q(
                    q_objects=Q(start_time__gte=start_dt, start_time__lt=end_dt),
                    channel_id=channel_id,
                    is_active=True,
                    is_delete=False,
                    has_content=True
                )
                total_talk_break = total_talk_break_query.count()
                
                # Build empty summary response with thresholds from FlagCondition
                error_response = SummaryService._build_empty_summary_response(
                    channel_id=channel_id,
                    total_talk_break=total_talk_break
                )
                return error_response
        
        # Get sentiment thresholds from FlagCondition
        thresholds = SummaryService._get_sentiment_thresholds(channel_id)
        target_sentiment_score = thresholds['target_sentiment_score']
        sentiment_min_lower = thresholds['sentiment_min_lower']
        sentiment_min_upper = thresholds['sentiment_min_upper']
        sentiment_max_lower = thresholds['sentiment_max_lower']
        sentiment_max_upper = thresholds['sentiment_max_upper']
        
        # Calculate all sentiment metrics
        average_sentiment = SummaryService.get_average_sentiment(audio_segments)
        low_sentiment_percentage = SummaryService.get_low_sentiment_percentage(
            audio_segments,
            min_lower=sentiment_min_lower,
            min_upper=sentiment_min_upper
        )
        high_sentiment_percentage = SummaryService.get_high_sentiment_percentage(
            audio_segments,
            max_lower=sentiment_max_lower,
            max_upper=sentiment_max_upper
        )
        per_day_average_sentiments = SummaryService.get_per_day_average_sentiments(
            audio_segments
        )
        
        # Build and return summary data
        return {
            'average_sentiment': average_sentiment,
            'target_sentiment_score': target_sentiment_score,
            'low_sentiment': low_sentiment_percentage,
            'high_sentiment': high_sentiment_percentage,
            'per_day_average_sentiments': per_day_average_sentiments,
            'thresholds': {
                'target_sentiment_score': target_sentiment_score,
                'low_sentiment_range': {
                    'min_lower': sentiment_min_lower,
                    'min_upper': sentiment_min_upper
                },
                'high_sentiment_range': {
                    'max_lower': sentiment_max_lower,
                    'max_upper': sentiment_max_upper
                }
            },
            'analyzed_segment_count': len(audio_segments),
            'total_talk_break': total_talk_break
        }

