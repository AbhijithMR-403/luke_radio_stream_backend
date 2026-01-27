from typing import Dict, List, Tuple, Optional, Any, Iterable
from datetime import datetime
from django.db.models import QuerySet, Q
from django.utils import timezone
from collections import defaultdict

from data_analysis.models import TranscriptionAnalysis, GeneralTopic, AudioSegments


class TopicService:
    """
    Service class for getting top topics by duration and count
    """

    @staticmethod
    def _parse_sentiment_score(value: Any) -> Optional[float]:
        """
        Parse sentiment score from various formats.
        
        Args:
            value: Sentiment value (can be string, int, float, or None)
        
        Returns:
            Parsed sentiment score as float, or None if invalid
        """
        if isinstance(value, str):
            value = value.strip().rstrip('%')

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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
        
        return TopicService._parse_sentiment_score(analysis.sentiment)

    @staticmethod
    def get_average_sentiment(
        audio_segments: Iterable[AudioSegments],
    ) -> Optional[float]:
        """
        Calculate average sentiment score weighted by duration.
        
        Args:
            audio_segments: Iterable of AudioSegments with transcription_detail and analysis loaded
        
        Returns:
            Average sentiment score (rounded to 3 decimal places) or None if no valid data
        """
        total_weighted_sentiment = 0.0
        total_duration = 0.0
        
        for segment in audio_segments:
            score = TopicService._get_sentiment_score_from_segment(segment)
            if score is None:
                continue
            
            duration = float(segment.duration_seconds or 0)
            if duration <= 0:
                continue
            
            total_weighted_sentiment += score * duration
            total_duration += duration
        
        if total_duration > 0:
            return round(total_weighted_sentiment / total_duration, 3)
        
        return None

    @staticmethod
    def _parse_topics_from_text(topics_text) -> List[str]:
        """
        Parse topics from the general_topics text field.
        
        Expected format: "1. Funding  \n2. Listener Support  \n3. Radio Station  \n..."
        Each line can be:
        - "1. Topic Name" (with period and space)
        - "1 Topic Name" (with space but no period)
        - "Topic Name" (no number prefix)
        
        Args:
            topics_text: Text containing topics, one per line with optional numbering.
                        Can be None, "undefined", or empty string.
            
        Returns:
            List of topic names (cleaned and stripped)
        """
        # Handle None, undefined, or empty values
        if not topics_text:
            return []
        
        # Convert to string if not already
        if not isinstance(topics_text, str):
            topics_text = str(topics_text)
        
        # Handle the string "undefined" (case-insensitive)
        if topics_text.strip().lower() in ['undefined', 'null', 'none']:
            return []
        
        topics = []
        # Split by newline to get individual topic lines
        topic_lines = topics_text.split('\n')
        
        for line in topic_lines:
            # Strip leading/trailing whitespace from the line
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Skip lines that are just "undefined" or similar
            if line.lower() in ['undefined', 'null', 'none']:
                continue
            
            # Check if line starts with a number (numbered format)
            if line and line[0].isdigit():
                # Handle format: "1. Topic Name" or "1 Topic Name"
                if '. ' in line:
                    # Format: "1. Topic Name" - split on ". " and take the part after
                    topic = line.split('. ', 1)[1].strip()
                elif ' ' in line:
                    # Format: "1 Topic Name" - split on first space and take the part after
                    topic = line.split(' ', 1)[1].strip()
                else:
                    # Just a number, skip it
                    continue
            else:
                # No number prefix, use the line as-is
                topic = line.strip()
            
            # Skip if topic is "undefined" or similar after parsing
            if topic.lower() in ['undefined', 'null', 'none']:
                continue
            
            # Only add non-empty topics
            if topic:
                topics.append(topic)
        
        return topics

    @staticmethod
    def _get_active_topic_names() -> set:
        """
        Get set of active topic names from GeneralTopic model (case-insensitive).
        
        Returns:
            Set of active topic names in lowercase
        """
        active_topics = GeneralTopic.objects.filter(is_active=True).values_list('topic_name', flat=True)
        return {topic.lower() for topic in active_topics}

    @staticmethod
    def _filter_active_topics(topics: List[str], active_topic_names: set) -> List[str]:
        """
        Filter topics to only include active ones.
        
        Args:
            topics: List of topic names
            active_topic_names: Set of active topic names in lowercase
            
        Returns:
            List of active topic names
        """
        return [topic for topic in topics if topic.lower() in active_topic_names]

    @staticmethod
    def _filter_out_generaltopic_topics(topics: List[str], generaltopic_names: set) -> List[str]:
        """
        Filter topics to exclude ones that are in GeneralTopic model.
        
        Args:
            topics: List of topic names
            generaltopic_names: Set of GeneralTopic names in lowercase (all topics in GeneralTopic, regardless of active status)
            
        Returns:
            List of topic names that are NOT in GeneralTopic
        """
        return [topic for topic in topics if topic.lower() not in generaltopic_names]

    @staticmethod
    def _get_all_generaltopic_names() -> set:
        """
        Get set of ALL topic names from GeneralTopic model (case-insensitive), regardless of active status.
        
        Returns:
            Set of all GeneralTopic names in lowercase
        """
        all_topics = GeneralTopic.objects.all().values_list('topic_name', flat=True)
        return {topic.lower() for topic in all_topics}

    @staticmethod
    def get_topics_with_both_metrics(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int = None,
        report_folder_id: int = None,
        shift_id: int = None,
        limit: int = 1000,
        show_all_topics: bool = False
    ) -> Dict:
        """
        Get all topics with both count and duration metrics calculated in a single pass.
        This ensures accurate data for both metrics for all topics.
        
        Args:
            start_dt: Start datetime
            end_dt: End datetime
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
            shift_id: Optional shift ID to filter by
            limit: Number of top topics to return (default: 1000)
            show_all_topics: If True, show all topics. If False, exclude topics that are in GeneralTopic model (default: False)
            
        Returns:
            Dictionary with topics containing both count and duration
        """
        # Validate filter inputs
        if channel_id is None and report_folder_id is None:
            raise ValueError("Either channel_id or report_folder_id must be provided")
        
        # Handle report folder case - get channel_id from folder
        if report_folder_id is not None:
            from data_analysis.models import ReportFolder
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                # If report folder doesn't exist, return empty result
                return {
                    'top_topics': [],
                    'total_topics': 0,
                    'filters': {
                        'start_datetime': start_dt.isoformat(),
                        'end_datetime': end_dt.isoformat(),
                        'channel_id': None,
                        'report_folder_id': report_folder_id,
                        'shift_id': shift_id,
                        'limit': limit
                    }
                }
        
        # Get GeneralTopic names (only if we need to filter them out)
        generaltopic_names = None if show_all_topics else TopicService._get_all_generaltopic_names()
        
        # Ensure datetimes are timezone-aware in UTC for shift filtering
        from zoneinfo import ZoneInfo
        
        if start_dt.tzinfo is None:
            start_dt = timezone.make_aware(start_dt)
        if end_dt.tzinfo is None:
            end_dt = timezone.make_aware(end_dt)
        
        # Convert to UTC if not already
        utc_start = start_dt.astimezone(ZoneInfo("UTC"))
        utc_end = end_dt.astimezone(ZoneInfo("UTC"))
        
        # Build query for TranscriptionAnalysis
        query = Q(
            transcription_detail__audio_segment__channel_id=channel_id,
            transcription_detail__audio_segment__start_time__gte=utc_start,
            transcription_detail__audio_segment__start_time__lt=utc_end,
            transcription_detail__audio_segment__is_delete=False
        )
        
        # Add report_folder_id filter if provided
        if report_folder_id is not None:
            query &= Q(transcription_detail__audio_segment__saved_in_folders__folder_id=report_folder_id)
        
        if shift_id:
            from shift_analysis.models import Shift
            try:
                shift = Shift.objects.get(id=shift_id)
                # Use the shift's get_datetime_filter method to properly filter by shift time windows
                shift_filter = shift.get_datetime_filter(utc_start, utc_end)
                # Apply shift filter to audio segments
                query &= Q(transcription_detail__audio_segment__in=AudioSegments.objects.filter(
                    shift_filter,
                    channel_id=channel_id
                ))
            except Shift.DoesNotExist:
                pass
        
        # Get analyses with general_topics
        analyses = TranscriptionAnalysis.objects.filter(
            query,
            general_topics__isnull=False
        ).exclude(
            general_topics=''
        ).exclude(
            general_topics__iexact='undefined'
        ).select_related(
            'transcription_detail__audio_segment'
        )
        
        # Use distinct() when report_folder_id is used to avoid duplicates from the join
        if report_folder_id is not None:
            analyses = analyses.distinct()
        
        # Track both metrics in a single pass
        # Use dictionaries to track segments and their durations per topic
        topic_duration_segments = defaultdict(dict)  # topic_name -> {audio_segment_id: duration}
        topic_count_segments = defaultdict(set)  # topic_name -> set of audio_segment_ids
        
        for analysis in analyses:
            if not analysis.general_topics:
                continue
            
            # Get audio segment
            audio_segment = None
            audio_segment_id = None
            if analysis.transcription_detail and analysis.transcription_detail.audio_segment:
                audio_segment = analysis.transcription_detail.audio_segment
                audio_segment_id = audio_segment.id
                duration = audio_segment.duration_seconds or 0
            else:
                continue
            
            # Parse topics
            topics = TopicService._parse_topics_from_text(analysis.general_topics)
            
            # Filter topics based on show_all_topics flag
            if show_all_topics:
                # Show all topics (no filtering)
                filtered_topics = topics
            else:
                # Exclude topics that are in GeneralTopic model
                filtered_topics = TopicService._filter_out_generaltopic_topics(topics, generaltopic_names)
            
            # Calculate both metrics for each topic
            for topic in filtered_topics:
                # Track duration: store segment duration only once per topic per segment
                if audio_segment_id not in topic_duration_segments[topic]:
                    topic_duration_segments[topic][audio_segment_id] = duration
                
                # Track count: add segment only once per topic
                if audio_segment_id not in topic_count_segments[topic]:
                    topic_count_segments[topic].add(audio_segment_id)
        
        # Calculate final metrics from the dictionaries
        topic_durations = {}
        topic_counts = {}
        for topic_name in set(topic_duration_segments.keys()) | set(topic_count_segments.keys()):
            # Sum durations from unique segments
            topic_durations[topic_name] = sum(topic_duration_segments.get(topic_name, {}).values())
            # Count unique segments
            topic_counts[topic_name] = len(topic_count_segments.get(topic_name, set()))
        
        # Format results with both metrics
        results = []
        for topic_name in set(topic_durations.keys()) | set(topic_counts.keys()):
            duration_seconds = int(topic_durations.get(topic_name, 0))
            count = topic_counts.get(topic_name, 0)
            
            results.append({
                'topic_name': topic_name,
                'count': count,
                'total_duration_seconds': duration_seconds,
                'total_duration_formatted': TopicService._format_duration(duration_seconds)
            })
        
        # Sort by duration (descending) as default
        results.sort(key=lambda x: x['total_duration_seconds'], reverse=True)
        
        # Apply limit
        limited_results = results[:limit]
        
        return {
            'top_topics': limited_results,
            'total_topics': len(results),
            'filters': {
                'start_datetime': start_dt.isoformat(),
                'end_datetime': end_dt.isoformat(),
                'channel_id': channel_id,
                'report_folder_id': report_folder_id,
                'shift_id': shift_id,
                'limit': limit
            }
        }

    @staticmethod
    def _format_duration(seconds: int) -> str:
        """
        Format duration in seconds to human-readable string.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted string like "1h 30m 15s" or "45m 30s" or "30s"
        """
        if seconds < 60:
            return f"{seconds}s"
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if secs > 0:
            parts.append(f"{secs}s")
        
        return " ".join(parts) if parts else "0s"

    @staticmethod
    def get_general_topic_counts_by_shift(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int = None,
        report_folder_id: int = None,
        show_all_topics: bool = False
    ) -> Dict:
        """
        Get count of general topics grouped by shift.
        
        Args:
            start_dt: Start datetime
            end_dt: End datetime
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
            show_all_topics: If True, show all topics. If False, only show topics that are in GeneralTopic model (default: False)
            
        Returns:
            Dictionary with general topic counts grouped by shift
        """
        from shift_analysis.models import Shift
        from shift_analysis.utils import filter_segments_by_shift
        from zoneinfo import ZoneInfo
        
        # Validate filter inputs
        if channel_id is None and report_folder_id is None:
            raise ValueError("Either channel_id or report_folder_id must be provided")
        
        # Handle report folder case - get channel_id from folder
        if report_folder_id is not None:
            from data_analysis.models import ReportFolder
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                # If report folder doesn't exist, return empty result
                return {
                    'shifts': [],
                    'total_shifts': 0,
                    'filters': {
                        'start_datetime': start_dt.isoformat(),
                        'end_datetime': end_dt.isoformat(),
                        'channel_id': None,
                        'report_folder_id': report_folder_id,
                        'show_all_topics': show_all_topics
                    }
                }
        
        # Get all active shifts for the channel
        active_shifts = Shift.objects.filter(
            channel_id=channel_id,
            is_active=True
        )
        
        # Get GeneralTopic names if we need to filter
        if show_all_topics:
            generaltopic_names = None
        else:
            # Only count topics that are in GeneralTopic model
            generaltopic_names = TopicService._get_all_generaltopic_names()
        
        # Convert to UTC for shift filtering
        if start_dt.tzinfo is None:
            start_dt = timezone.make_aware(start_dt)
        if end_dt.tzinfo is None:
            end_dt = timezone.make_aware(end_dt)
        
        utc_start = start_dt.astimezone(ZoneInfo("UTC"))
        utc_end = end_dt.astimezone(ZoneInfo("UTC"))
        
        # Dictionary to store results: {shift_id: {shift_name: str, topics: {topic_name: count}}}
        shift_results = {}
        
        # Process each shift
        for shift in active_shifts:
            try:
                # Get audio segments for this shift
                filtered_segments = filter_segments_by_shift(shift.id, utc_start, utc_end)
                
                # Further filter by channel and date range
                shift_segments = filtered_segments.filter(
                    channel_id=channel_id,
                    is_delete=False,
                    is_active=True
                )
                
                # Add report_folder_id filter if provided
                if report_folder_id is not None:
                    shift_segments = shift_segments.filter(
                        saved_in_folders__folder_id=report_folder_id
                    )
                
                shift_segments = shift_segments.select_related(
                    'transcription_detail',
                    'transcription_detail__analysis'
                )
                
                # Use distinct() when report_folder_id is used to avoid duplicates from the join
                if report_folder_id is not None:
                    shift_segments = shift_segments.distinct()
                
                # Convert queryset to list to ensure related data is loaded
                # This is needed for both sentiment calculation and getting segment IDs
                segments_list = list(shift_segments)
                segment_ids = [seg.id for seg in segments_list]
                
                # Calculate average sentiment for this shift
                average_sentiment = TopicService.get_average_sentiment(segments_list)
                
                if not segment_ids:
                    # No segments for this shift, add empty result
                    shift_results[shift.id] = {
                        'shift_id': shift.id,
                        'shift_name': shift.name,
                        'topics': {},
                        'average_sentiment': average_sentiment
                    }
                    continue
                
                # Get transcription analyses for these segments (for topic counting)
                analyses = TranscriptionAnalysis.objects.filter(
                    transcription_detail__audio_segment_id__in=segment_ids,
                    general_topics__isnull=False
                ).exclude(
                    general_topics=''
                ).exclude(
                    general_topics__iexact='undefined'
                ).select_related(
                    'transcription_detail__audio_segment'
                )
                
                # Count topics for this shift
                topic_counts = defaultdict(int)
                
                for analysis in analyses:
                    if not analysis.general_topics:
                        continue
                    
                    # Parse topics
                    topics = TopicService._parse_topics_from_text(analysis.general_topics)
                    
                    # Filter topics based on show_all_topics flag
                    if show_all_topics:
                        # Show all topics (no filtering)
                        filtered_topics = topics
                    else:
                        # Only count topics that are in GeneralTopic model
                        filtered_topics = [topic for topic in topics if topic.lower() in generaltopic_names]
                    
                    # Count each topic
                    for topic in filtered_topics:
                        topic_counts[topic] += 1
                
                # Store results for this shift
                shift_results[shift.id] = {
                    'shift_id': shift.id,
                    'shift_name': shift.name,
                    'topics': dict(topic_counts),
                    'average_sentiment': average_sentiment
                }
                
            except Exception as e:
                # If shift filtering fails, add empty result
                shift_results[shift.id] = {
                    'shift_id': shift.id,
                    'shift_name': shift.name,
                    'topics': {},
                    'average_sentiment': None,
                    'error': str(e)
                }
        
        # Format response
        shifts_data = []
        for shift_id, shift_data in shift_results.items():
            # Convert topics dict to list of objects
            topics_list = [
                {
                    'topic_name': topic_name,
                    'count': count
                }
                for topic_name, count in sorted(shift_data['topics'].items(), key=lambda x: x[1], reverse=True)
            ]
            
            shift_result = {
                'shift_id': shift_data['shift_id'],
                'shift_name': shift_data['shift_name'],
                'topics': topics_list,
                'total_topics': len(topics_list),
                'total_count': sum(shift_data['topics'].values()),
                'average_sentiment': shift_data.get('average_sentiment')
            }
            
            if 'error' in shift_data:
                shift_result['error'] = shift_data['error']
            
            shifts_data.append(shift_result)
        
        return {
            'shifts': shifts_data,
            'total_shifts': len(shifts_data),
            'filters': {
                'start_datetime': start_dt.isoformat(),
                'end_datetime': end_dt.isoformat(),
                'channel_id': channel_id,
                'report_folder_id': report_folder_id,
                'show_all_topics': show_all_topics
            }
        }


