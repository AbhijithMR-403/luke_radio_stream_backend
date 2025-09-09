from rest_framework import serializers
from data_analysis.models import TranscriptionDetail, TranscriptionAnalysis, AudioSegments
from acr_admin.models import Channel
from django.db.models import Avg, Count, Q
from django.utils import timezone
from datetime import timedelta, datetime
from collections import defaultdict

class DashboardStatsSerializer(serializers.Serializer):
    dashboardStats = serializers.DictField()
    topicsDistribution = serializers.ListField()
    topTopicsRanking = serializers.ListField()
    sentimentData = serializers.ListField()
    dateRange = serializers.DictField(required=False)


def _build_date_filter(start_date, end_date):
    """
    Build date filter for database queries
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    
    Returns:
        tuple: (date_filter, start_dt, end_dt) or (Q(), None, None)
    """
    date_filter = Q()
    start_dt = None
    end_dt = None
    
    if start_date and end_date:
        try:
            start_dt = timezone.make_aware(datetime.strptime(start_date, '%Y-%m-%d'))
            end_dt = timezone.make_aware(datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59))
            date_filter = Q(created_at__range=(start_dt, end_dt))
        except ValueError:
            pass
    
    return date_filter, start_dt, end_dt


def _get_transcription_stats(date_filter, channel_id):
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
        Q(audio_segment__channel_id=channel_id) | 
        Q(unrecognized_audio__channel_id=channel_id)
    )
    return transcriptions_query.count()


def _get_sentiment_stats(date_filter, start_dt, end_dt, channel_id):
    """
    Get sentiment statistics and breakdown
    
    Args:
        date_filter: Django Q object for date filtering
        start_dt: Start datetime object
        end_dt: End datetime object
        channel_id (int): Channel ID to filter by
    
    Returns:
        tuple: (avg_sentiment, sentiment_breakdown, analyses)
    """
    sentiment_scores = []
    sentiment_breakdown = {'numeric': 0}
    
    analyses_query = TranscriptionAnalysis.objects.all()
    
    if date_filter:
        analyses_query = analyses_query.filter(transcription_detail__created_at__range=(start_dt, end_dt))
    analyses_query = analyses_query.filter(
        Q(transcription_detail__audio_segment__channel_id=channel_id) | 
        Q(transcription_detail__unrecognized_audio__channel_id=channel_id)
    )
    
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
                
                # Add the score directly since it's already in the correct format
                sentiment_scores.append(score)
                        
            except (ValueError, TypeError):
                # If conversion fails, skip this sentiment (shouldn't happen with integer strings)
                continue
    
    avg_sentiment = int(sum(sentiment_scores) / len(sentiment_scores)) if sentiment_scores else None
    return avg_sentiment, sentiment_breakdown, analyses


def _get_topics_stats(analyses):
    """
    Get topics statistics and distribution
    
    Args:
        analyses: QuerySet of TranscriptionAnalysis objects
    
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
            if analysis.transcription_detail:
                if analysis.transcription_detail.audio_segment:
                    audio_segment_id = analysis.transcription_detail.audio_segment.id
                elif analysis.transcription_detail.unrecognized_audio:
                    # For unrecognized audio, we'll use a different identifier
                    audio_segment_id = f"unrecognized_{analysis.transcription_detail.unrecognized_audio.id}"
            
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


def _get_sentiment_timeline_data(start_dt, end_dt, avg_sentiment, channel_id):
    """
    Get sentiment data over time
    
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
                Q(transcription_detail__audio_segment__channel_id=channel_id) | 
                Q(transcription_detail__unrecognized_audio__channel_id=channel_id)
            )
            
            day_sentiment_scores = []
            for analysis in day_analyses:
                if analysis.sentiment:
                    try:
                        sentiment_value = analysis.sentiment
                        if isinstance(sentiment_value, str):
                            sentiment_value = sentiment_value.strip()
                        
                        # Convert string to integer (since it stores int in string format)
                        score = int(sentiment_value)
                        # Add the score directly since it's already in the correct format
                        day_sentiment_scores.append(score)
                    except (ValueError, TypeError):
                        # If conversion fails, skip this sentiment (shouldn't happen with integer strings)
                        continue
            
            if day_sentiment_scores:
                day_avg = int(sum(day_sentiment_scores) / len(day_sentiment_scores))
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


def _get_shift_analytics_data(start_dt, end_dt, channel_id):
    """
    Get shift analytics data grouped by time shifts
    
    Args:
        start_dt: Start datetime object
        end_dt: End datetime object
        channel_id (int): Channel ID to filter by
    
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
                                shift_sentiments.append(sentiment_value)
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
        
        # Calculate shift statistics
        total_transcriptions = len(shift_transcriptions)
        avg_sentiment = round(sum(shift_sentiments) / len(shift_sentiments), 2) if shift_sentiments else 0.0
        
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


def get_dashboard_stats(start_date, end_date, channel_id):
    """
    Main function to get all dashboard statistics with required date filtering and channel filtering
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        channel_id (int): Channel ID to filter by
    
    Returns:
        dict: Complete dashboard statistics
    """
    # Build date filter
    date_filter, start_dt, end_dt = _build_date_filter(start_date, end_date)
    
    # Get all statistics using separate functions
    total_transcriptions = _get_transcription_stats(date_filter, channel_id)
    avg_sentiment, sentiment_breakdown, analyses = _get_sentiment_stats(date_filter, start_dt, end_dt, channel_id)
    unique_topics_count, topics_distribution, top_topics_ranking, unique_topics = _get_topics_stats(analyses)
    active_shifts, channel_details = _get_channel_stats(channel_id)
    sentiment_data = _get_sentiment_timeline_data(start_dt, end_dt, avg_sentiment, channel_id)
    
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
        'sentimentData': sentiment_data,
        'dateRange': {
            'startDate': start_date,
            'endDate': end_date
        },
        'channelFilter': {
            'channelId': channel_id
        }
    }
    
    return response


def get_shift_analytics(start_date, end_date, channel_id):
    """
    Main function to get shift analytics data
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        channel_id (int): Channel ID to filter by
    
    Returns:
        dict: Complete shift analytics data
    """
    # Build date filter
    date_filter, start_dt, end_dt = _build_date_filter(start_date, end_date)
    print(start_dt, end_dt)
    # Get shift analytics data
    shift_analytics = _get_shift_analytics_data(start_dt, end_dt, channel_id)
    print(shift_analytics)
    
    # Add metadata
    response = {
        **shift_analytics,
        'dateRange': {
            'startDate': start_date,
            'endDate': end_date
        },
        'channelFilter': {
            'channelId': channel_id
        }
    }
    
    return response
