from __future__ import annotations

from typing import Optional, List, Dict, Iterable
from data_analysis.models import AudioSegments


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

