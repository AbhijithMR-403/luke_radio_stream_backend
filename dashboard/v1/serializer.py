from rest_framework import serializers
from data_analysis.models import TranscriptionDetail, TranscriptionAnalysis, AudioSegments, GeneralTopic
from core_admin.models import Channel
from django.db.models import Avg, Count, Q
from django.utils import timezone
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from shift_analysis.utils import filter_segments_by_predefined_filter
from collections import defaultdict

class DashboardStatsSerializer(serializers.Serializer):
    dashboardStats = serializers.DictField()
    topicsDistribution = serializers.ListField()
    topTopicsRanking = serializers.ListField()
    sentimentData = serializers.ListField()
    dateRange = serializers.DictField(required=False)
    bucketRankings = serializers.DictField(required=False)


def _build_date_filter(start_date_or_datetime, end_date_or_datetime):
    """
    Build datetime filter for database queries - accepts datetime formats only
    
    Args:
        start_date_or_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        end_date_or_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
    
    Returns:
        tuple: (date_filter, start_dt, end_dt) or (Q(), None, None)
    """
    date_filter = Q()
    start_dt = None
    end_dt = None
    
    if start_date_or_datetime and end_date_or_datetime:
        try:
            # Parse datetime - handle both T and space separators; reject date-only
            if 'T' in start_date_or_datetime:
                start_dt = timezone.make_aware(datetime.fromisoformat(start_date_or_datetime))
            elif ' ' in start_date_or_datetime:
                start_dt = timezone.make_aware(datetime.strptime(start_date_or_datetime, '%Y-%m-%d %H:%M:%S'))
            else:
                raise ValueError('start_datetime must include time')
            
            if 'T' in end_date_or_datetime:
                end_dt = timezone.make_aware(datetime.fromisoformat(end_date_or_datetime))
            elif ' ' in end_date_or_datetime:
                end_dt = timezone.make_aware(datetime.strptime(end_date_or_datetime, '%Y-%m-%d %H:%M:%S'))
            else:
                raise ValueError('end_datetime must include time')
            
            date_filter = Q(created_at__range=(start_dt, end_dt))
        except ValueError as e:
            # Reset variables to None to indicate parsing failed
            start_dt = None
            end_dt = None
            date_filter = Q()
    
    return date_filter, start_dt, end_dt


def _get_transcription_stats(date_filter, channel_id, filtered_ids_qs=None):
    """
    Get transcription statistics
    
    Args:
        date_filter: Django Q object for date filtering
        channel_id (int): Channel ID to filter by
    
    Returns:
        int: Total number of transcriptions
    """
    transcriptions_query = TranscriptionDetail.objects.all()
    if date_filter:
        transcriptions_query = transcriptions_query.filter(date_filter)
    transcriptions_query = transcriptions_query.filter(
        Q(audio_segment__channel_id=channel_id)
    )
    if filtered_ids_qs is not None:
        transcriptions_query = transcriptions_query.filter(audio_segment__id__in=filtered_ids_qs)
    return transcriptions_query.count()


def _get_sentiment_stats(date_filter, start_dt, end_dt, channel_id, filtered_ids_qs=None):
    """
    Get sentiment statistics and breakdown using duration-weighted calculation
    Formula: Sum of (sentiment_score * duration_seconds) / Sum of duration_seconds
    
    Args:
        date_filter: Django Q object for date filtering
        start_dt: Start datetime object
        end_dt: End datetime object
        channel_id (int): Channel ID to filter by
    
    Returns:
        tuple: (avg_sentiment, sentiment_breakdown, analyses)
    """
    total_weighted_sentiment = 0
    total_duration = 0
    sentiment_breakdown = {'numeric': 0}
    
    analyses_query = TranscriptionAnalysis.objects.all()
    
    if date_filter:
        analyses_query = analyses_query.filter(transcription_detail__created_at__range=(start_dt, end_dt))
    analyses_query = analyses_query.filter(
        Q(transcription_detail__audio_segment__channel_id=channel_id)
    )
    if filtered_ids_qs is not None:
        analyses_query = analyses_query.filter(transcription_detail__audio_segment__id__in=filtered_ids_qs)
    
    analyses = analyses_query.all()
    
    for analysis in analyses:
        if analysis.sentiment:
            sentiment_value = analysis.sentiment
            
            try:
                # Since sentiment is stored as string format of int, convert directly
                if isinstance(sentiment_value, str):
                    sentiment_value = sentiment_value.strip()
                
                # Convert string to integer (since it stores int in string format)
                score = int(sentiment_value)
                sentiment_breakdown['numeric'] += 1
                
                # Get duration from audio segment
                duration_seconds = 0
                if analysis.transcription_detail and analysis.transcription_detail.audio_segment:
                    duration_seconds = analysis.transcription_detail.audio_segment.duration_seconds
                
                # Calculate weighted sentiment: sentiment_score * duration
                total_weighted_sentiment += score * duration_seconds
                total_duration += duration_seconds
                        
            except (ValueError, TypeError):
                # If conversion fails, skip this sentiment (shouldn't happen with integer strings)
                continue
    
    # Calculate weighted average: total_weighted_sentiment / total_duration (3 decimal places)
    avg_sentiment = (
        round((total_weighted_sentiment / total_duration), 3)
        if total_duration > 0 else None
    )
    return avg_sentiment, sentiment_breakdown, analyses


def _parse_bucket_prompt_line(line):
    """
    Parse a single bucket prompt line and return (primary, secondary) topic names.
    Handles formats like:
      - "FUN, 85, RELATIONSHIPS, 75"
      - "MENTAL, 100, FAITH JOURNEY, 0"
      - "RELATIONSHIPS, 90%, FUN, 85%"
    
    Requirements:
      - Must have at least 4 comma-separated values to determine primary/secondary
      - Ignores AI output prefixes like "Empty Result", "Output:", etc.
      - Ignores undefined/invalid tokens such as "Undefined", "undefined", "0%" when paired with undefined topic
    
    Returns tuple of (primary_or_None, secondary_or_None)
    """
    if not line:
        return None, None
    text = line.strip()
    if not text:
        return None, None
    
    # Skip AI output prefixes
    ai_prefixes = ["empty result", "output:", "result:", "analysis:", "response:"]
    text_lower = text.lower()
    for prefix in ai_prefixes:
        if text_lower.startswith(prefix):
            # Remove the prefix and any following newlines/whitespace
            text = text[len(prefix):].strip()
            if text.startswith('\n'):
                text = text[1:].strip()
            break
    
    # Normalize separators to comma
    parts = [p.strip() for p in text.replace("\t", ",").replace("|", ",").split(",")]
    
    # Must have at least 4 comma-separated values to determine primary/secondary
    if len(parts) < 4:
        return None, None
    
    def is_undefined(token):
        if token is None:
            return True
        t = str(token).strip().lower()
        return t in {"undefined", "undef", "none", "null", "na", "n/a", "", "empty result", "output"}
    
    def is_score(token):
        if token is None:
            return False
        t = str(token).strip().replace("%", "")
        if not t:
            return False
        try:
            float(t)
            return True
        except ValueError:
            return False
    
    # Extract first two valid topics (skip scores)
    topics = []
    i = 0
    while i < len(parts) and len(topics) < 2:
        token = parts[i]
        # Skip empty tokens
        if token == "":
            i += 1
            continue
        # If looks like a score, skip and move on
        if is_score(token):
            i += 1
            continue
        # This token is a candidate topic; ensure it's not undefined
        if not is_undefined(token):
            topics.append(token)
        # Advance; also skip next token if it's a score paired with this topic
        if i + 1 < len(parts) and is_score(parts[i+1]):
            i += 2
        else:
            i += 1
    
    # Return primary and secondary only if we found both
    primary = topics[0] if len(topics) > 0 else None
    secondary = topics[1] if len(topics) > 1 else None
    return primary, secondary


def _compute_bucket_rankings(analyses):
    """
    Compute primary and secondary bucket topic rankings from TranscriptionAnalysis.bucket_prompt
    Ignores undefined entries and rows without valid topics.
    Returns two sorted lists of dicts: [{ 'topic': TOPIC, 'count': N }, ...]
    """
    from collections import defaultdict
    primary_counts = defaultdict(int)
    secondary_counts = defaultdict(int)
    for analysis in analyses:
        bucket_text = getattr(analysis, 'bucket_prompt', None)
        if not bucket_text:
            continue
        # Bucket text may contain multiple lines; process each line
        lines = [l for l in str(bucket_text).split('\n') if l is not None]
        if not lines:
            lines = [str(bucket_text)]
        found_primary = None
        found_secondary = None
        for line in lines:
            p, s = _parse_bucket_prompt_line(line)
            # First valid pair wins for this analysis
            if found_primary is None and p:
                found_primary = p
            if found_secondary is None and s:
                found_secondary = s
            if found_primary is not None and found_secondary is not None:
                break
        if found_primary:
            primary_counts[found_primary.upper()] += 1
        if found_secondary:
            secondary_counts[found_secondary.upper()] += 1
    def to_sorted_list(counter):
        items = [{ 'topic': k, 'count': v } for k, v in counter.items()]
        items.sort(key=lambda x: x['count'], reverse=True)
        return items
    return to_sorted_list(primary_counts), to_sorted_list(secondary_counts)


def _get_topics_stats(analyses, show_all_topics=False):
    """
    Get topics statistics and distribution
    
    Args:
        analyses: QuerySet of TranscriptionAnalysis objects
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
    
    Returns:
        tuple: (unique_topics_count, topics_distribution, top_topics_ranking, unique_topics)
    """
    unique_topics = set()
    topic_counts = defaultdict(int)
    topic_audio_segments = defaultdict(set)  # Track audio segment IDs for each topic
    
    for analysis in analyses:
        if analysis.general_topics:
            topics_text = analysis.general_topics
            topic_lines = topics_text.split('\n')
            
            # Get audio segment ID from the analysis
            audio_segment_id = None
            if analysis.transcription_detail and analysis.transcription_detail.audio_segment:
                audio_segment_id = analysis.transcription_detail.audio_segment.id
            
            for line in topic_lines:
                line = line.strip()
                if line:
                    if line[0].isdigit():
                        # Handle both formats: "1. Topic" and "1 Topic"
                        if '. ' in line:
                            topic = line.split('. ', 1)[1]
                        elif ' ' in line:
                            topic = line.split(' ', 1)[1]
                        else:
                            topic = line
                    else:
                        topic = line
                    
                    topic = topic.strip()
                    if topic:
                        unique_topics.add(topic)
                        topic_counts[topic] += 1
                        # Add audio segment ID to the topic's set
                        if audio_segment_id:
                            topic_audio_segments[topic].add(audio_segment_id)
    
    # Filter out inactive topics if show_all_topics is False
    if not show_all_topics:
        # Get all inactive topic names from GeneralTopic model (case-insensitive)
        inactive_topics = set()
        for topic_name in GeneralTopic.objects.filter(is_active=False).values_list('topic_name', flat=True):
            inactive_topics.add(topic_name.lower())
        
        # Filter topic_counts and topic_audio_segments to exclude inactive topics
        filtered_topic_counts = defaultdict(int)
        filtered_topic_audio_segments = defaultdict(set)
        filtered_unique_topics = set()
        
        for topic, count in topic_counts.items():
            # Include topic if it's NOT in the inactive topics list (case-insensitive comparison)
            if topic.lower() not in inactive_topics:
                filtered_topic_counts[topic] = count
                filtered_topic_audio_segments[topic] = topic_audio_segments[topic]
                filtered_unique_topics.add(topic)
        
        # Update the variables with filtered data
        topic_counts = filtered_topic_counts
        topic_audio_segments = filtered_topic_audio_segments
        unique_topics = filtered_unique_topics
    
    unique_topics_count = len(unique_topics)
    
    # Create topics distribution with total count and audio segment IDs
    topics_distribution = []
    for topic, count in topic_counts.items():
        topics_distribution.append({
            'topic': topic.upper(),
            'value': count,  # Use total count instead of percentage
            'audioSegmentIds': list(topic_audio_segments[topic])  # List of audio segment IDs
        })
    
    # Sort by count descending (highest count first)
    topics_distribution.sort(key=lambda x: x['value'], reverse=True)
    
    # Create top topics ranking with rank, count, and percentage (top 10 only)
    total_analyses = len(analyses) if analyses else 1
    top_topics_ranking = []
    
    # Get top 10 topics by count
    top_10_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    for rank, (topic, count) in enumerate(top_10_topics, 1):
        percentage = round((count / total_analyses) * 100, 1)
        top_topics_ranking.append({
            'rank': rank,
            'topic': topic.upper(),
            'count': count,
            'percentage': percentage
        })
    
    return unique_topics_count, topics_distribution, top_topics_ranking, unique_topics


def _get_channel_stats(channel_id):
    """
    Get channel statistics and details
    
    Args:
        channel_id (int): Channel ID to filter by
    
    Returns:
        tuple: (active_shifts, channel_details)
    """
    active_shifts = Channel.objects.filter(is_deleted=False, id=channel_id).count()
    
    channels = Channel.objects.filter(is_deleted=False, id=channel_id)
    channel_details = []
    for channel in channels:
        channel_details.append({
            'id': channel.id,
            'name': channel.name,
            'channel_id': channel.channel_id,
            'project_id': channel.project_id
        })
    
    return active_shifts, channel_details


def _get_sentiment_timeline_data(start_dt, end_dt, avg_sentiment, channel_id, filtered_ids_qs=None):
    """
    Get sentiment data over time using duration-weighted calculation
    
    Args:
        start_dt: Start datetime object
        end_dt: End datetime object
        avg_sentiment: Average sentiment score
        channel_id (int): Channel ID to filter by
    
    Returns:
        list: List of sentiment data objects
    """
    sentiment_data = []
    
    if start_dt and end_dt:
        current_date = start_dt.date()
        end_date = end_dt.date()
        
        while current_date <= end_date:
            day_start = timezone.make_aware(datetime.combine(current_date, datetime.min.time()))
            day_end = timezone.make_aware(datetime.combine(current_date, datetime.max.time()))
            
            day_analyses = TranscriptionAnalysis.objects.filter(
                transcription_detail__created_at__range=(day_start, day_end)
            ).filter(
                Q(transcription_detail__audio_segment__channel_id=channel_id)
            )
            if filtered_ids_qs is not None:
                day_analyses = day_analyses.filter(transcription_detail__audio_segment__id__in=filtered_ids_qs)
            
            total_weighted_sentiment = 0
            total_duration = 0
            for analysis in day_analyses:
                if analysis.sentiment:
                    try:
                        sentiment_value = analysis.sentiment
                        if isinstance(sentiment_value, str):
                            sentiment_value = sentiment_value.strip()
                        
                        # Convert string to integer (since it stores int in string format)
                        score = int(sentiment_value)
                        
                        # Get duration from audio segment
                        duration_seconds = 0
                        if analysis.transcription_detail and analysis.transcription_detail.audio_segment:
                            duration_seconds = analysis.transcription_detail.audio_segment.duration_seconds
                        
                        # Calculate weighted sentiment: sentiment_score * duration
                        total_weighted_sentiment += score * duration_seconds

                        total_duration += duration_seconds
                    except (ValueError, TypeError):
                        # If conversion fails, skip this sentiment (shouldn't happen with integer strings)
                        continue
            
            if total_duration > 0:
                day_avg = round(total_weighted_sentiment / total_duration, 3)
                sentiment_data.append({
                    'date': current_date.strftime('%d/%m/%Y'),
                    'sentiment': day_avg
                })
            
            current_date += timedelta(days=1)
    else:
        # Create dummy sentiment data for last 5 days only if avg_sentiment exists
        if avg_sentiment is not None:
            for i in range(5):
                date = timezone.now().date() - timedelta(days=4-i)
                sentiment_data.append({
                    'date': date.strftime('%d/%m/%Y'),
                    'sentiment': avg_sentiment + (i * 2 - 4)
                })
    
    return sentiment_data


def _get_shift_analytics_data(start_dt, end_dt, channel_id, show_all_topics=False):
    """
    Get shift analytics data grouped by time shifts
    
    Args:
        start_dt: Start datetime object
        end_dt: End datetime object
        channel_id (int): Channel ID to filter by
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
    
    Returns:
        dict: Shift analytics data
    """
    if not start_dt or not end_dt:
        return {}
    
    # Define shift time ranges
    shifts = {
        'morning': {'start': 6, 'end': 14, 'title': 'Morning Shift (6AM-2PM)'},
        'afternoon': {'start': 14, 'end': 22, 'title': 'Afternoon Shift (2PM-10PM)'},
        'night': {'start': 22, 'end': 6, 'title': 'Night Shift (10PM-6AM)'}
    }
    
    shift_data = {}
    sentiment_by_shift = []
    transcription_count_by_shift = []
    top_topics_by_shift = {}
    

    for shift_key, shift_info in shifts.items():
        # Get transcriptions for this shift
        shift_transcriptions = []
        shift_sentiments = []
        shift_topics = defaultdict(int)
        
        # Query AudioSegments for this shift and channel
        audio_segments = AudioSegments.objects.filter(
            channel_id=channel_id,
            start_time__range=(start_dt, end_dt),
            is_active=True
        )
        
        for segment in audio_segments:
            # Check if segment falls within this shift
            segment_hour = segment.start_time.hour
            
            if shift_key == 'night':
                # Night shift spans across midnight
                if segment_hour >= shift_info['start'] or segment_hour < shift_info['end']:
                    shift_transcriptions.append(segment)
            else:
                # Regular shifts within same day
                if shift_info['start'] <= segment_hour < shift_info['end']:
                    shift_transcriptions.append(segment)
        
        # Get transcription details and analysis for this shift
        total_weighted_sentiment = 0
        total_duration = 0
        for segment in shift_transcriptions:
            try:
                transcription_detail = segment.transcription_detail
                if transcription_detail and hasattr(transcription_detail, 'analysis'):
                    analysis = transcription_detail.analysis
                    if analysis:
                        # Get sentiment
                        if analysis.sentiment:
                            try:
                                sentiment_value = int(analysis.sentiment.strip())
                                
                                # Get duration from audio segment
                                duration_seconds = 0
                                if segment:
                                    duration_seconds = segment.duration_seconds
                                
                                # Calculate weighted sentiment: sentiment_score * duration
                                total_weighted_sentiment += sentiment_value * duration_seconds
                                total_duration += duration_seconds
                            except (ValueError, TypeError):
                                continue
                        
                        # Get topics
                        if analysis.general_topics:
                            topics_text = analysis.general_topics
                            topic_lines = topics_text.split('\n')
                            
                            for line in topic_lines:
                                line = line.strip()
                                if line:
                                    if line[0].isdigit() and '. ' in line:
                                        topic = line.split('. ', 1)[1] if '. ' in line else line
                                    else:
                                        topic = line
                                    
                                    topic = topic.strip()
                                    if topic:
                                        shift_topics[topic] += 1
            except:
                continue
        
        # Filter out inactive topics if show_all_topics is False
        if not show_all_topics:
            # Get all inactive topic names from GeneralTopic model (case-insensitive)
            inactive_topics = set()
            for topic_name in GeneralTopic.objects.filter(is_active=False).values_list('topic_name', flat=True):
                inactive_topics.add(topic_name.lower())
            
            # Filter shift_topics to exclude inactive topics
            filtered_shift_topics = defaultdict(int)
            for topic, count in shift_topics.items():
                # Include topic if it's NOT in the inactive topics list (case-insensitive comparison)
                if topic.lower() not in inactive_topics:
                    filtered_shift_topics[topic] = count
            
            # Update shift_topics with filtered data
            shift_topics = filtered_shift_topics
        
        # Calculate shift statistics using duration-weighted formula
        total_transcriptions = len(shift_transcriptions)
        avg_sentiment = round(total_weighted_sentiment / total_duration, 2) if total_duration > 0 else 0.0
        
        # Get top topic for this shift
        top_topic = 'N/A'
        if shift_topics:
            top_topic = max(shift_topics.items(), key=lambda x: x[1])[0]
        
        # Build shift data
        shift_data[shift_key] = {
            'title': shift_info['title'],
            'total': total_transcriptions,
            'avgSentiment': avg_sentiment,
            'topTopic': top_topic
        }
        
        # Build sentiment by shift data
        sentiment_by_shift.append({
            'shift': shift_info['title'],
            'value': int(avg_sentiment) if avg_sentiment > 0 else 0
        })
        
        # Build transcription count by shift data
        colors = {'morning': '#3b82f6', 'afternoon': '#10b981', 'night': '#f59e0b'}
        transcription_count_by_shift.append({
            'shift': shift_info['title'].split(' ')[0],  # Just the shift name (Morning, Afternoon, Night)
            'count': total_transcriptions,
            'color': colors[shift_key]
        })
        
        # Build top topics by shift data
        top_topics_list = []
        sorted_topics = sorted(shift_topics.items(), key=lambda x: x[1], reverse=True)[:3]
        for rank, (topic, count) in enumerate(sorted_topics, 1):
            top_topics_list.append({
                'rank': rank,
                'topic': topic,
                'count': count
            })
        
        top_topics_by_shift[shift_key] = top_topics_list
    
    return {
        'shiftData': shift_data,
        'sentimentByShift': sentiment_by_shift,
        'transcriptionCountByShift': transcription_count_by_shift,
        'topTopicsByShift': top_topics_by_shift
    }


def _get_shift_analytics_data_v2(start_dt, end_dt, channel_id, show_all_topics=False):
    """
    Version 2: Get shift analytics data using dynamic shifts from ShiftAnalytics model only
    
    Args:
        start_dt: Start datetime object
        end_dt: End datetime object
        channel_id (int): Channel ID to filter by
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
    
    Returns:
        dict: Shift analytics data using dynamic shifts only
    """
    if not start_dt or not end_dt:
        return {}
    
    # Import required models
    from shift_analysis.models import Shift
    from shift_analysis.utils import filter_segments_by_shift
    
    # Get all active shifts
    active_shifts = Shift.objects.filter(is_active=True).order_by('start_time')
    
    shift_data = {}
    sentiment_by_shift = []
    transcription_count_by_shift = []
    top_topics_by_shift = {}
    
    # Process each active shift
    for shift in active_shifts:
        shift_key = shift.name.lower().replace(' ', '_')
        
        # Get audio segments for this shift using the shift filtering utility
        try:
            # Convert to UTC for the filtering function
            utc_start = start_dt.astimezone(ZoneInfo("UTC"))
            utc_end = end_dt.astimezone(ZoneInfo("UTC"))
            
            # Get segments filtered by this shift
            filtered_segments = filter_segments_by_shift(shift.id, utc_start, utc_end)
            
            # Further filter by channel
            shift_segments = filtered_segments.filter(channel_id=channel_id, is_active=True)
            
        except Exception as e:
            # If shift filtering fails, fall back to time-based filtering
            shift_segments = AudioSegments.objects.filter(
                channel_id=channel_id,
                start_time__range=(start_dt, end_dt),
                is_active=True
            )
            
            # Apply time-based filtering as fallback
            filtered_segments = []
            for segment in shift_segments:
                segment_hour = segment.start_time.hour
                shift_start_hour = shift.start_time.hour
                shift_end_hour = shift.end_time.hour
                
                # Check if segment falls within this shift
                if shift_start_hour <= shift_end_hour:
                    # Regular shift within same day
                    if shift_start_hour <= segment_hour < shift_end_hour:
                        filtered_segments.append(segment)
                else:
                    # Overnight shift
                    if segment_hour >= shift_start_hour or segment_hour < shift_end_hour:
                        filtered_segments.append(segment)
            
            shift_segments = filtered_segments
        
        # Get transcription details and analysis for this shift
        total_weighted_sentiment = 0
        total_duration = 0
        shift_topics = defaultdict(int)
        
        for segment in shift_segments:
            try:
                transcription_detail = segment.transcription_detail
                if transcription_detail and hasattr(transcription_detail, 'analysis'):
                    analysis = transcription_detail.analysis
                    if analysis:
                        # Get sentiment
                        if analysis.sentiment:
                            try:
                                sentiment_value = int(analysis.sentiment.strip())
                                
                                # Get duration from audio segment
                                duration_seconds = 0
                                if segment:
                                    duration_seconds = segment.duration_seconds
                                
                                # Calculate weighted sentiment: sentiment_score * duration
                                total_weighted_sentiment += sentiment_value * duration_seconds
                                total_duration += duration_seconds
                            except (ValueError, TypeError):
                                continue
                        
                        # Get topics
                        if analysis.general_topics:
                            topics_text = analysis.general_topics
                            topic_lines = topics_text.split('\n')
                            
                            for line in topic_lines:
                                line = line.strip()
                                if line:
                                    if line[0].isdigit() and '. ' in line:
                                        topic = line.split('. ', 1)[1] if '. ' in line else line
                                    else:
                                        topic = line
                                    
                                    topic = topic.strip()
                                    if topic:
                                        shift_topics[topic] += 1
            except:
                continue
        
        # Filter out inactive topics if show_all_topics is False
        if not show_all_topics:
            # Get all inactive topic names from GeneralTopic model (case-insensitive)
            inactive_topics = set()
            for topic_name in GeneralTopic.objects.filter(is_active=False).values_list('topic_name', flat=True):
                inactive_topics.add(topic_name.lower())
            
            # Filter shift_topics to exclude inactive topics
            filtered_shift_topics = defaultdict(int)
            for topic, count in shift_topics.items():
                # Include topic if it's NOT in the inactive topics list (case-insensitive comparison)
                if topic.lower() not in inactive_topics:
                    filtered_shift_topics[topic] = count
            
            # Update shift_topics with filtered data
            shift_topics = filtered_shift_topics
        
        # Calculate shift statistics using duration-weighted formula
        total_transcriptions = len(shift_segments)
        avg_sentiment = round(total_weighted_sentiment / total_duration, 2) if total_duration > 0 else 0.0
        
        # Get top topic for this shift
        top_topic = 'N/A'
        if shift_topics:
            top_topic = max(shift_topics.items(), key=lambda x: x[1])[0]
        
        # Build shift data
        shift_data[shift_key] = {
            'title': f"{shift.name} ({shift.start_time} - {shift.end_time})",
            'total': total_transcriptions,
            'avgSentiment': avg_sentiment,
            'topTopic': top_topic
        }
        
        # Build sentiment by shift data
        sentiment_by_shift.append({
            'shift': f"{shift.name} ({shift.start_time} - {shift.end_time})",
            'value': int(avg_sentiment) if avg_sentiment > 0 else 0
        })
        
        # Build transcription count by shift data
        colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4']
        color_index = list(active_shifts).index(shift) % len(colors)
        transcription_count_by_shift.append({
            'shift': shift.name,
            'count': total_transcriptions,
            'color': colors[color_index]
        })
        
        # Build top topics by shift data
        top_topics_list = []
        sorted_topics = sorted(shift_topics.items(), key=lambda x: x[1], reverse=True)[:3]
        for rank, (topic, count) in enumerate(sorted_topics, 1):
            top_topics_list.append({
                'rank': rank,
                'topic': topic,
                'count': count
            })
        
        top_topics_by_shift[shift_key] = top_topics_list
    
    return {
        'shiftData': shift_data,
        'sentimentByShift': sentiment_by_shift,
        'transcriptionCountByShift': transcription_count_by_shift,
        'topTopicsByShift': top_topics_by_shift
    }


def get_dashboard_stats(start_date_or_datetime, end_date_or_datetime, channel_id, show_all_topics=False, predefined_filter_id=None):
    """
    Main function to get all dashboard statistics with required datetime filtering and channel filtering
    
    Args:
        start_date_or_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        end_date_or_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        channel_id (int): Channel ID to filter by
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
        predefined_filter_id (int): Optional PredefinedFilter primary key to apply schedule filter
    
    Returns:
        dict: Complete dashboard statistics
    """
    # Build date/datetime filter
    date_filter, start_dt, end_dt = _build_date_filter(start_date_or_datetime, end_date_or_datetime)
    if start_dt is None or end_dt is None:
        raise ValueError("Failed to parse datetime parameters. Use YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS")
    
    # Apply predefined filter if provided
    filtered_ids_qs = None
    if predefined_filter_id is not None:
        utc_start = start_dt.astimezone(ZoneInfo("UTC"))
        utc_end = end_dt.astimezone(ZoneInfo("UTC"))
        filtered_ids_qs = filter_segments_by_predefined_filter(predefined_filter_id, utc_start, utc_end).values('id')
    
    # Get all statistics using separate functions
    total_transcriptions = _get_transcription_stats(date_filter, channel_id, filtered_ids_qs)
    avg_sentiment, sentiment_breakdown, analyses = _get_sentiment_stats(date_filter, start_dt, end_dt, channel_id, filtered_ids_qs)
    unique_topics_count, topics_distribution, top_topics_ranking, unique_topics = _get_topics_stats(analyses, show_all_topics)
    bucket_primary_ranking, bucket_secondary_ranking = _compute_bucket_rankings(analyses)
    active_shifts, channel_details = _get_channel_stats(channel_id)
    sentiment_data = _get_sentiment_timeline_data(start_dt, end_dt, avg_sentiment, channel_id, filtered_ids_qs)
    
    # Prepare response
    response = {
        'dashboardStats': {
            'totalTranscriptions': total_transcriptions,
            'avgSentimentScore': avg_sentiment,
            'uniqueTopics': unique_topics_count,
            'activeShifts': active_shifts,
            'details': {
                'sentimentBreakdown': sentiment_breakdown,
                'totalAnalyses': len(analyses),
                'channels': channel_details,
                'dateFilterApplied': True,
                'channelFilterApplied': True
            }
        },
        'topicsDistribution': topics_distribution,
        'topTopicsRanking': top_topics_ranking,
        'bucketRankings': {
            'primary': bucket_primary_ranking,
            'secondary': bucket_secondary_ranking
        },
        'sentimentData': sentiment_data,
        'dateRange': {
            'startDateOrDateTime': start_date_or_datetime,
            'endDateOrDateTime': end_date_or_datetime
        },
        'channelFilter': {
            'channelId': channel_id
        }
    }
    
    return response


def get_shift_analytics(start_date_or_datetime, end_date_or_datetime, channel_id, show_all_topics=False):
    """
    Main function to get shift analytics data
    
    Args:
        start_date_or_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        end_date_or_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        channel_id (int): Channel ID to filter by
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
    
    Returns:
        dict: Complete shift analytics data
    """
    # Build date/datetime filter
    date_filter, start_dt, end_dt = _build_date_filter(start_date_or_datetime, end_date_or_datetime)
    
    # Check if datetime parsing was successful
    if start_dt is None or end_dt is None:
        raise ValueError(f"Failed to parse datetime parameters. start_datetime: '{start_date_or_datetime}', end_datetime: '{end_date_or_datetime}'. Please use format YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, or YYYY-MM-DD HH:MM:SS")
    
    # Get shift analytics data
    shift_analytics = _get_shift_analytics_data(start_dt, end_dt, channel_id, show_all_topics)
    
    # Add metadata
    response = {
        **shift_analytics,
        'dateRange': {
            'startDateOrDateTime': start_date_or_datetime,
            'endDateOrDateTime': end_date_or_datetime
        },
        'channelFilter': {
            'channelId': channel_id
        }
    }
    
    return response


def get_shift_analytics_v2(start_date_or_datetime, end_date_or_datetime, channel_id, show_all_topics=False):
    """
    Version 2: Get shift analytics data using dynamic shifts from ShiftAnalytics and PredefinedFilter models
    
    Args:
        start_date_or_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        end_date_or_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format
        channel_id (int): Channel ID to filter by
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
    
    Returns:
        dict: Complete shift analytics data using dynamic shifts
    """
    # Build date/datetime filter
    date_filter, start_dt, end_dt = _build_date_filter(start_date_or_datetime, end_date_or_datetime)
    
    # Check if datetime parsing was successful
    if start_dt is None or end_dt is None:
        raise ValueError(f"Failed to parse datetime parameters. start_datetime: '{start_date_or_datetime}', end_datetime: '{end_date_or_datetime}'. Please use format YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, or YYYY-MM-DD HH:MM:SS")
    
    # Get shift analytics data using dynamic shifts
    shift_analytics = _get_shift_analytics_data_v2(start_dt, end_dt, channel_id, show_all_topics)
    
    # Add metadata
    response = {
        **shift_analytics,
        'dateRange': {
            'startDateOrDateTime': start_date_or_datetime,
            'endDateOrDateTime': end_date_or_datetime
        },
        'channelFilter': {
            'channelId': channel_id
        }
    }
    
    return response


def get_topic_audio_segments(topic_name, start_date_or_datetime=None, end_date_or_datetime=None, channel_id=None, show_all_topics=False):
    """
    Get all audio segments for a specific general topic
    
    Args:
        topic_name (str): Name of the general topic
        start_date_or_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (optional)
        end_date_or_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (optional)
        channel_id (int): Channel ID to filter by (optional)
        show_all_topics (bool): If True, show all topics including inactive ones. If False, filter out inactive topics
    
    Returns:
        dict: Audio segments data for the topic
    """
    # Build date/datetime filter if provided
    date_filter, start_dt, end_dt = _build_date_filter(start_date_or_datetime, end_date_or_datetime)
    
    # Get analyses with the same filtering logic as dashboard stats
    analyses_query = TranscriptionAnalysis.objects.all()
    
    if date_filter:
        analyses_query = analyses_query.filter(transcription_detail__created_at__range=(start_dt, end_dt))
    if channel_id:
        analyses_query = analyses_query.filter(
            Q(transcription_detail__audio_segment__channel_id=channel_id)
        )
    
    analyses = analyses_query.all()
    
    # Find audio segments for the specific topic
    topic_audio_segments = []
    topic_audio_segment_ids = set()
    
    for analysis in analyses:
        if analysis.general_topics:
            topics_text = analysis.general_topics
            topic_lines = topics_text.split('\n')
            
            # Get audio segment from the analysis
            audio_segment = None
            if analysis.transcription_detail and analysis.transcription_detail.audio_segment:
                audio_segment = analysis.transcription_detail.audio_segment
            
            # Check if this analysis contains our target topic
            topic_found = False
            for line in topic_lines:
                line = line.strip()
                if line:
                    if line[0].isdigit():
                        # Handle both formats: "1. Topic" and "1 Topic"
                        if '. ' in line:
                            topic = line.split('. ', 1)[1]
                        elif ' ' in line:
                            topic = line.split(' ', 1)[1]
                        else:
                            topic = line
                    else:
                        topic = line
                    topic = topic.strip()
                    if topic and topic.lower() == topic_name.lower():
                        topic_found = True
                        break
            
            # If topic found and we have an audio segment, add it to results
            if topic_found and audio_segment and audio_segment.id not in topic_audio_segment_ids:
                topic_audio_segment_ids.add(audio_segment.id)
                
                # Get transcription details
                transcription_detail = analysis.transcription_detail
                
                # Build audio segment data
                audio_segment_data = {
                    'id': audio_segment.id,
                    'start_time': audio_segment.start_time.isoformat(),
                    'end_time': audio_segment.end_time.isoformat(),
                    'duration_seconds': audio_segment.duration_seconds,
                    'is_recognized': audio_segment.is_recognized,
                    'is_active': audio_segment.is_active,
                    'is_analysis_completed': audio_segment.is_analysis_completed,
                    'is_audio_downloaded': audio_segment.is_audio_downloaded,
                    'file_name': audio_segment.file_name,
                    'file_path': audio_segment.file_path,
                    'title': audio_segment.title,
                    'title_before': audio_segment.title_before,
                    'title_after': audio_segment.title_after,
                    'metadata_json': audio_segment.metadata_json,
                    'channel_id': audio_segment.channel.id if audio_segment.channel else None,
                    'channel_name': audio_segment.channel.name if audio_segment.channel else None,
                    'notes': audio_segment.notes,
                    'created_at': audio_segment.created_at.isoformat(),
                    'transcription': {
                        'id': transcription_detail.id,
                        'transcript': transcription_detail.transcript,
                        'created_at': transcription_detail.created_at.isoformat()
                    } if transcription_detail else None,
                    'analysis': {
                        'id': analysis.id,
                        'summary': analysis.summary,
                        'sentiment': analysis.sentiment,
                        'general_topics': analysis.general_topics,
                        'iab_topics': analysis.iab_topics,
                        'bucket_prompt': analysis.bucket_prompt,
                        'created_at': analysis.created_at.isoformat()
                    } if analysis else None
                }
                
                topic_audio_segments.append(audio_segment_data)
    
    # Filter out inactive topics if show_all_topics is False
    if not show_all_topics:
        # Check if the topic is inactive
        try:
            general_topic = GeneralTopic.objects.get(topic_name__iexact=topic_name)
            if not general_topic.is_active:
                # If topic is inactive and show_all_topics is False, return empty results
                return {
                    'topic_name': topic_name,
                    'total_segments': 0,
                    'audio_segments': [],
                    'message': f'Topic "{topic_name}" is inactive and show_all_topics is False'
                }
        except GeneralTopic.DoesNotExist:
            # Topic not found in GeneralTopic model, but we still return results from analysis
            pass
    
    # Sort by start_time (most recent first)
    topic_audio_segments.sort(key=lambda x: x['start_time'], reverse=True)
    
    return {
        'topic_name': topic_name,
        'total_segments': len(topic_audio_segments),
        'audio_segments': topic_audio_segments,
        'date_range': {
            'start_date_or_datetime': start_date_or_datetime,
            'end_date_or_datetime': end_date_or_datetime
        },
        'channel_filter': {
            'channel_id': channel_id
        },
        'show_all_topics': show_all_topics
    }
