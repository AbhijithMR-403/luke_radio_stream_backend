from __future__ import annotations

from typing import Optional, List, Dict, Iterable, Any
from datetime import datetime
from django.db.models import QuerySet, Q

from data_analysis.models import AudioSegments
from data_analysis.repositories import AudioSegmentDAO
from audio_policy.models import FlagCondition
from shift_analysis.models import Shift


class SummaryService:
    """
    Service class for calculating overall sentiment analysis summary statistics.
    Provides methods for average sentiment, low/high sentiment percentages, and per-day averages.
    
    All methods accept a list of AudioSegments with transcription_detail and analysis preloaded.
    """

    @staticmethod
    def _parse_sentiment_score(sentiment_value: Any) -> Optional[float]:
        if not sentiment_value:
            return None
        
        try:
            if isinstance(sentiment_value, (int, float)):
                return float(sentiment_value)
            if isinstance(sentiment_value, str):
                sentiment_value = sentiment_value.strip()
                if not sentiment_value:
                    return None
                return float(sentiment_value)
        except (ValueError, TypeError):
            import re
            numbers = re.findall(r'-?\d+\.?\d*', str(sentiment_value))
            if numbers:
                try:
                    return float(numbers[0])
                except (ValueError, TypeError):
                    pass
        return None

    @staticmethod
    def get_average_sentiment(
        audio_segments: Iterable[AudioSegments],
    ) -> Optional[float]:
        total_weighted_sentiment = 0.0
        total_duration = 0.0
        
        for segment in audio_segments:
            try:
                transcription_detail = segment.transcription_detail
                if not transcription_detail:
                    continue
                
                analysis = transcription_detail.analysis
                if not analysis or not analysis.sentiment:
                    continue
                
                score = SummaryService._parse_sentiment_score(analysis.sentiment)
                if score is None:
                    continue
                
                duration_seconds = float(segment.duration_seconds) if segment.duration_seconds else 0.0
                if duration_seconds > 0:
                    total_weighted_sentiment += score * duration_seconds
                    total_duration += duration_seconds
            except (AttributeError, TypeError, ValueError):
                continue
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
            # Check if segment has transcription_detail and analysis
            if not hasattr(segment, 'transcription_detail') or not segment.transcription_detail:
                continue
            
            transcription_detail = segment.transcription_detail
            if not hasattr(transcription_detail, 'analysis') or not transcription_detail.analysis:
                continue
            
            analysis = transcription_detail.analysis
            if not analysis.sentiment:
                continue
            
            score = SummaryService._parse_sentiment_score(analysis.sentiment)
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
            # Check if segment has transcription_detail and analysis
            if not hasattr(segment, 'transcription_detail') or not segment.transcription_detail:
                continue
            
            transcription_detail = segment.transcription_detail
            if not hasattr(transcription_detail, 'analysis') or not transcription_detail.analysis:
                continue
            
            analysis = transcription_detail.analysis
            if not analysis.sentiment:
                continue
            
            score = SummaryService._parse_sentiment_score(analysis.sentiment)
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
    ) -> List[Dict[str, any]]:
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
            # Check if segment has transcription_detail and analysis
            if not hasattr(segment, 'transcription_detail') or not segment.transcription_detail:
                continue
            
            transcription_detail = segment.transcription_detail
            if not hasattr(transcription_detail, 'analysis') or not transcription_detail.analysis:
                continue
            
            analysis = transcription_detail.analysis
            if not analysis.sentiment:
                continue
            
            # Get the date from transcription_detail created_at
            created_at = transcription_detail.created_at
            if created_at is None:
                continue
            
            # Convert to date string in DD/MM/YYYY format
            date_key = created_at.date().strftime('%d/%m/%Y')
            
            # Initialize daily data if not exists
            if date_key not in daily_data:
                daily_data[date_key] = {
                    'total_weighted_sentiment': 0,
                    'total_duration': 0
                }
            
            score = SummaryService._parse_sentiment_score(analysis.sentiment)
            if score is None:
                continue
            
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
    def get_summary_data(
        *,
        channel_id: int,
        start_dt: datetime,
        end_dt: datetime,
        user,
        shift_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get complete summary data including all sentiment metrics.
        
        This method handles:
        - Filtering audio segments by datetime, channel, and optionally shift
        - Getting user sentiment preferences (or defaults)
        - Calculating all sentiment metrics
        - Building the complete response data
        
        Args:
            channel_id: Channel ID to filter by
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            user: User object to get preferences for
            shift_id: Optional shift ID to filter by
        
        Returns:
            Dictionary containing all summary data including:
            - average_sentiment
            - target_sentiment_score
            - low_sentiment_percentage
            - high_sentiment_percentage
            - per_day_average_sentiments
            - thresholds
            - segment_count
            - total_talk_break
        """
        # Build Q object for filtering
        if shift_id is not None:
            # Use shift's get_datetime_filter which already handles datetime range
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                # Get Q object from shift's get_datetime_filter method
                # This already filters by the shift's time windows within the datetime range
                q_object = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
            except Shift.DoesNotExist:
                # If shift doesn't exist, return empty result
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
                # Get default sentiment ranges from FlagCondition
                # Default values: low sentiment 0-20, high sentiment 80-100
                target_sentiment_score = 75
                sentiment_min_lower = 0.0
                sentiment_min_upper = 20.0
                sentiment_max_lower = 80.0
                sentiment_max_upper = 100.0
                
                try:
                    flag_condition = FlagCondition.objects.get(channel_id=channel_id)
                    if flag_condition.target_sentiments is not None:
                        target_sentiment_score = flag_condition.target_sentiments
                    if flag_condition.sentiment_min_lower is not None:
                        sentiment_min_lower = flag_condition.sentiment_min_lower
                    if flag_condition.sentiment_min_upper is not None:
                        sentiment_min_upper = flag_condition.sentiment_min_upper
                    if flag_condition.sentiment_max_lower is not None:
                        sentiment_max_lower = flag_condition.sentiment_max_lower
                    if flag_condition.sentiment_max_upper is not None:
                        sentiment_max_upper = flag_condition.sentiment_max_upper
                except FlagCondition.DoesNotExist:
                    # If no flag condition exists, use default values
                    pass
                
                return {
                    'average_sentiment': None,
                    'target_sentiment_score': target_sentiment_score,
                    'low_sentiment': None,
                    'high_sentiment': None,
                    'per_day_average_sentiments': [],
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
                    'segment_count': 0,
                    'total_talk_break': total_talk_break
                }
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
        
        # Get sentiment range from FlagCondition
        # Default values: low sentiment 0-20, high sentiment 80-100
        target_sentiment_score = 75
        sentiment_min_lower = 0.0
        sentiment_min_upper = 20.0
        sentiment_max_lower = 80.0
        sentiment_max_upper = 100.0
        
        try:
            flag_condition = FlagCondition.objects.get(channel_id=channel_id)
            if flag_condition.target_sentiments is not None:
                target_sentiment_score = flag_condition.target_sentiments
            if flag_condition.sentiment_min_lower is not None:
                sentiment_min_lower = flag_condition.sentiment_min_lower
            if flag_condition.sentiment_min_upper is not None:
                sentiment_min_upper = flag_condition.sentiment_min_upper
            if flag_condition.sentiment_max_lower is not None:
                sentiment_max_lower = flag_condition.sentiment_max_lower
            if flag_condition.sentiment_max_upper is not None:
                sentiment_max_upper = flag_condition.sentiment_max_upper
        except FlagCondition.DoesNotExist:
            # If no flag condition exists, use default values
            pass
        
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

