from __future__ import annotations

from typing import Optional, List, Dict, Iterable, Any, Tuple
from datetime import datetime
from collections import defaultdict
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
    def calculate_all_metrics(
        audio_segments: Iterable[AudioSegments],
        thresholds: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Calculates all metrics in a single pass over the data.
        
        Args:
            audio_segments: Iterable of AudioSegments with transcription_detail and analysis loaded
            thresholds: Dictionary containing sentiment threshold values:
                - sentiment_min_lower: Lower bound for low sentiment
                - sentiment_min_upper: Upper bound for low sentiment
                - sentiment_max_lower: Lower bound for high sentiment
                - sentiment_max_upper: Upper bound for high sentiment
        
        Returns:
            Dictionary containing:
            - average_sentiment: Average sentiment score (weighted by duration)
            - low_sentiment: Percentage of low sentiment segments
            - high_sentiment: Percentage of high sentiment segments
            - per_day_average_sentiments: List of daily average sentiments
            - analyzed_segment_count: Count of segments analyzed
        """
        # Initialize counters
        total_duration = 0.0
        total_weighted_sentiment = 0.0
        low_sent_duration = 0.0
        high_sent_duration = 0.0
        analyzed_count = 0
        
        # Grouping for daily data
        daily_stats = defaultdict(lambda: {"weighted_sent": 0.0, "duration": 0.0})

        for segment in audio_segments:
            analyzed_count += 1
            # 1. Safe extraction
            score = SummaryService._get_sentiment_score_from_segment(segment)
            if score is None:
                continue
            
            duration = float(segment.duration_seconds or 0)
            if duration <= 0:
                continue

            # 2. Global Totals

            total_duration += duration
            total_weighted_sentiment += score * duration

            # 3. Threshold Percentages
            if thresholds['sentiment_min_lower'] <= score <= thresholds['sentiment_min_upper']:
                low_sent_duration += duration
            
            if thresholds['sentiment_max_lower'] <= score <= thresholds['sentiment_max_upper']:
                high_sent_duration += duration

            # 4. Daily Grouping
            transcription_detail = segment.transcription_detail
            created_at = transcription_detail.created_at
            if not created_at:
                continue
            
            date_key = created_at.strftime('%d/%m/%Y')
            daily_stats[date_key]["weighted_sent"] += score * duration
            daily_stats[date_key]["duration"] += duration

        # Finalize Calculations
        avg_sentiment = round(total_weighted_sentiment / total_duration, 3) if total_duration > 0 else None
        
        low_pct = round((low_sent_duration / total_duration) * 100, 2) if total_duration > 0 else None
        high_pct = round((high_sent_duration / total_duration) * 100, 2) if total_duration > 0 else None

        per_day = [
            {
                "date": d, 
                "average_sentiment": round(stats["weighted_sent"] / stats["duration"], 3)
            }
            for d, stats in sorted(daily_stats.items()) if stats["duration"] > 0
        ]

        return {
            "average_sentiment": avg_sentiment,
            "low_sentiment": low_pct,
            "high_sentiment": high_pct,
            "per_day_average_sentiments": per_day,
            "analyzed_segment_count": analyzed_count
        }

    @staticmethod
    def _parse_sentiment_score(value: Any) -> Optional[float]:
        if isinstance(value, str):
            value = value.strip().rstrip('%')

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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
        
        # Filter audio segments directly using reverse lookup from SavedAudioSegment
        # This avoids loading IDs into memory and uses a single efficient JOIN query
        base_query = AudioSegments.objects.filter(
            saved_in_folders__folder=report_folder,
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
        ).distinct()  # Use distinct() to avoid duplicates if a segment is saved multiple times
        
        # Build base query for total_talk_break (same filters but doesn't require analysis)
        total_talk_break_query = AudioSegments.objects.filter(
            saved_in_folders__folder=report_folder,
            start_time__gte=start_dt,
            start_time__lt=end_dt,
            is_active=True,
            is_delete=False,
            transcription_detail__isnull=False
        ).distinct()
        
        # Apply shift filter if provided
        if shift_id is not None:
            shift = Shift.objects.filter(id=shift_id, channel_id=channel_id).first()
            if not shift:
                raise ShiftNotFound(f"Shift with id {shift_id} not found for channel {channel_id}")
            # Get Q object from shift's get_datetime_filter method
            q_object = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
            # Apply shift filter to both queries
            base_query = base_query.filter(q_object)
            total_talk_break_query = total_talk_break_query.filter(q_object)
        
        # Get all audio segments with transcription and analysis
        audio_segments = list(base_query)
        
        # Count total talk break using the same filters (including shift if applicable)
        total_talk_break = total_talk_break_query.count()
        
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
                        # Count total talk break even when shift doesn't exist using reverse lookup
                        # Note: No shift filter applied since shift doesn't exist
                        total_talk_break = AudioSegments.objects.filter(
                            saved_in_folders__folder=report_folder,
                            start_time__gte=start_dt,
                            start_time__lt=end_dt,
                            is_active=True,
                            is_delete=False,
                            transcription_detail__isnull=False
                        ).distinct().count()
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
        
        # Calculate all metrics in a single pass
        metrics = SummaryService.calculate_all_metrics(audio_segments, thresholds)
        
        # Build and return summary data
        return {
            'average_sentiment': metrics['average_sentiment'],
            'target_sentiment_score': target_sentiment_score,
            'low_sentiment': metrics['low_sentiment'],
            'high_sentiment': metrics['high_sentiment'],
            'per_day_average_sentiments': metrics['per_day_average_sentiments'],
            'thresholds': {
                'target_sentiment_score': target_sentiment_score,
                'low_sentiment_range': {
                    'min_lower': thresholds['sentiment_min_lower'],
                    'min_upper': thresholds['sentiment_min_upper']
                },
                'high_sentiment_range': {
                    'max_lower': thresholds['sentiment_max_lower'],
                    'max_upper': thresholds['sentiment_max_upper']
                }
            },
            'analyzed_segment_count': metrics['analyzed_segment_count'],
            'total_talk_break': total_talk_break
        }

