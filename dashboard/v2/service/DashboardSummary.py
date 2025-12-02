from __future__ import annotations

from typing import Optional, List, Dict, Iterable, Any
from datetime import datetime
from django.db.models import QuerySet, Q

from data_analysis.models import AudioSegments
from data_analysis.repositories import AudioSegmentDAO
from dashboard.models import UserSentimentPreference
from shift_analysis.models import Shift


class SummaryService:
    """
    Service class for calculating overall sentiment analysis summary statistics.
    Provides methods for average sentiment, low/high sentiment percentages, and per-day averages.
    
    All methods accept a list of AudioSegments with transcription_detail and analysis preloaded.
    """

    @staticmethod
    def _parse_sentiment_score(sentiment_value: str) -> Optional[int]:
        """
        Parse sentiment value from string to integer.
        
        Args:
            sentiment_value: Sentiment value as string
        
        Returns:
            Integer sentiment score or None if parsing fails
        """
        if not sentiment_value:
            return None
        
        try:
            if isinstance(sentiment_value, str):
                sentiment_value = sentiment_value.strip()
            return int(sentiment_value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def get_average_sentiment(
        audio_segments: Iterable[AudioSegments],
    ) -> Optional[float]:
        """
        Calculate average sentiment using duration-weighted formula.
        Formula: Average Sentiment = Sum of (Score × Duration) / Total Duration
        
        Args:
            audio_segments: List of AudioSegments with transcription_detail and analysis loaded
        
        Returns:
            Average sentiment score (rounded to 3 decimal places) or None if no data
        """
        total_weighted_sentiment = 0
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
                # Calculate weighted sentiment: sentiment_score * duration
                total_weighted_sentiment += score * duration_seconds
                total_duration += duration_seconds
        
        if total_duration > 0:
            return round(total_weighted_sentiment / total_duration, 3)
        
        return None

    @staticmethod
    def get_low_sentiment_percentage(
        audio_segments: Iterable[AudioSegments],
        threshold: int = 20,
    ) -> Optional[float]:
        """
        Calculate percentage of segments with low sentiment (below threshold).
        
        Args:
            audio_segments: List of AudioSegments with transcription_detail and analysis loaded
            threshold: Sentiment threshold (default: 70)
        
        Returns:
            Percentage of low sentiment segments (rounded to 2 decimal places) or None if no data
        """
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
                if score < threshold:
                    low_sentiment_weighted_duration += duration_seconds
        
        if total_duration > 0:
            percentage = (low_sentiment_weighted_duration / total_duration) * 100
            return round(percentage, 2)
        
        return None

    @staticmethod
    def get_high_sentiment_percentage(
        audio_segments: Iterable[AudioSegments],
        threshold: int = 80,
    ) -> Optional[float]:
        """
        Calculate percentage of segments with high sentiment (above threshold).
        
        Args:
            audio_segments: List of AudioSegments with transcription_detail and analysis loaded
            threshold: Sentiment threshold (default: 90)
        
        Returns:
            Percentage of high sentiment segments (rounded to 2 decimal places) or None if no data
        """
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
                if score > threshold:
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
                return {
                    'average_sentiment': None,
                    'target_sentiment_score': 75,
                    'low_sentiment': None,
                    'high_sentiment': None,
                    'per_day_average_sentiments': [],
                    'thresholds': {
                        'target_sentiment_score': 75,
                        'low_sentiment_threshold': 20,
                        'high_sentiment_threshold': 80
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
        
        # Get sentiment preferences from UserSentimentPreference
        # Default values if no preference exists
        target_sentiment_score = 75
        low_sentiment_threshold = 20
        high_sentiment_threshold = 80
        
        try:
            sentiment_preference = UserSentimentPreference.objects.get(user=user)
            target_sentiment_score = sentiment_preference.target_sentiment_score
            low_sentiment_threshold = sentiment_preference.low_sentiment_score
            high_sentiment_threshold = sentiment_preference.high_sentiment_score
        except UserSentimentPreference.DoesNotExist:
            # If no preference exists, use default values
            pass
        
        # Calculate all sentiment metrics
        average_sentiment = SummaryService.get_average_sentiment(audio_segments)
        low_sentiment_percentage = SummaryService.get_low_sentiment_percentage(
            audio_segments,
            threshold=low_sentiment_threshold
        )
        high_sentiment_percentage = SummaryService.get_high_sentiment_percentage(
            audio_segments,
            threshold=high_sentiment_threshold
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
                'low_sentiment_threshold': low_sentiment_threshold,
                'high_sentiment_threshold': high_sentiment_threshold
            },
            'segment_count': len(audio_segments),
            'total_talk_break': total_talk_break
        }

