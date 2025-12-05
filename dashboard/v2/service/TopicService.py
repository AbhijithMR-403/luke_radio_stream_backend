from typing import Dict, List, Tuple
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
        channel_id: int,
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
            channel_id: Channel ID to filter by
            shift_id: Optional shift ID to filter by
            limit: Number of top topics to return (default: 1000)
            show_all_topics: If True, show all topics. If False, exclude topics that are in GeneralTopic model (default: False)
            
        Returns:
            Dictionary with topics containing both count and duration
        """
        # Get GeneralTopic names (only if we need to filter them out)
        generaltopic_names = None if show_all_topics else TopicService._get_all_generaltopic_names()
        
        # Build query for TranscriptionAnalysis
        query = Q(
            transcription_detail__audio_segment__channel_id=channel_id,
            transcription_detail__audio_segment__start_time__gte=start_dt,
            transcription_detail__audio_segment__start_time__lt=end_dt,
            transcription_detail__audio_segment__is_delete=False
        )
        
        if shift_id:
            from shift_analysis.models import Shift
            try:
                shift = Shift.objects.get(id=shift_id)
                query &= Q(
                    transcription_detail__audio_segment__start_time__gte=shift.start_time,
                    transcription_detail__audio_segment__start_time__lt=shift.end_time
                )
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


