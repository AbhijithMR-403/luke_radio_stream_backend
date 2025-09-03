from rest_framework import serializers
from data_analysis.models import TranscriptionDetail, TranscriptionAnalysis, AudioSegments
from acr_admin.models import Channel
from django.db.models import Avg, Count, Q
from django.utils import timezone
from datetime import timedelta, datetime

class DashboardStatsSerializer(serializers.Serializer):
    dashboardStats = serializers.DictField()
    dateRange = serializers.DictField(required=False)

def get_dashboard_stats(start_date=None, end_date=None):
    """
    Calculate and return dashboard statistics with optional date filtering
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
    """
    # Build date filter if dates are provided
    date_filter = Q()
    if start_date and end_date:
        try:
            # Parse dates and make them timezone-aware using Django's timezone
            start_dt = timezone.make_aware(datetime.strptime(start_date, '%Y-%m-%d'))
            end_dt = timezone.make_aware(datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59))
            date_filter = Q(created_at__range=(start_dt, end_dt))
        except ValueError:
            # If date parsing fails, continue without date filtering
            pass
    
    # Get total transcriptions with date filter
    transcriptions_query = TranscriptionDetail.objects
    if date_filter:
        transcriptions_query = transcriptions_query.filter(date_filter)
    total_transcriptions = transcriptions_query.count()
    
    # Calculate average sentiment score with date filter
    sentiment_scores = []
    sentiment_breakdown = {'positive': 0, 'neutral': 0, 'negative': 0, 'numeric': 0}
    analyses_query = TranscriptionAnalysis.objects
    if date_filter:
        # Filter analyses by transcription detail creation date
        analyses_query = analyses_query.filter(transcription_detail__created_at__range=(start_dt, end_dt))
    
    analyses = analyses_query.all()
    
    for analysis in analyses:
        if analysis.sentiment:
            # Handle sentiment that could be a number from OpenAI (int or string)
            sentiment_value = analysis.sentiment
            
            try:
                # Try to convert to float first (handles both int and string numbers)
                if isinstance(sentiment_value, str):
                    # Remove any extra whitespace and try to convert
                    sentiment_value = sentiment_value.strip()
                
                # Convert to float to handle both integer and decimal values
                score = float(sentiment_value)
                sentiment_breakdown['numeric'] += 1
                
                # Ensure the score is within a reasonable range (0-100)
                if 0 <= score <= 100:
                    sentiment_scores.append(score)
                else:
                    # If score is outside 0-100, normalize it or use default
                    # Assuming OpenAI might return -1 to 1 scale, convert to 0-100
                    if -1 <= score <= 1:
                        normalized_score = (score + 1) * 50  # Convert -1 to 1 scale to 0-100
                        sentiment_scores.append(normalized_score)
                    else:
                        # Use default score for out-of-range values
                        sentiment_scores.append(50)
                        
            except (ValueError, TypeError):
                # If conversion fails, try to parse text-based sentiment
                sentiment_text = str(sentiment_value).lower()
                if 'positive' in sentiment_text:
                    sentiment_scores.append(80)
                    sentiment_breakdown['positive'] += 1
                elif 'negative' in sentiment_text:
                    sentiment_scores.append(30)
                    sentiment_breakdown['negative'] += 1
                elif 'neutral' in sentiment_text:
                    sentiment_scores.append(60)
                    sentiment_breakdown['neutral'] += 1
                else:
                    sentiment_scores.append(50)  # Default score
    
    avg_sentiment = int(sum(sentiment_scores) / len(sentiment_scores)) if sentiment_scores else 50
    
    # Count unique topics (from general_topics field) with date filter
    unique_topics = set()
    topic_details = []
    for analysis in analyses:
        if analysis.general_topics:
            # Handle numbered list format: "1. Education\n2. Tourism\n3. Sponsorship"
            topics_text = analysis.general_topics
            
            # Split by newlines first, then handle numbered items
            topic_lines = topics_text.split('\n')
            
            for line in topic_lines:
                line = line.strip()
                if line:
                    # Remove numbering (e.g., "1. ", "2. ", etc.)
                    # This handles formats like "1. Education", "2. Tourism", etc.
                    if line[0].isdigit() and '. ' in line:
                        # Extract topic after the number and period
                        topic = line.split('. ', 1)[1] if '. ' in line else line
                    else:
                        # If no numbering, use the line as is
                        topic = line
                    
                    # Clean up the topic and add to set
                    topic = topic.strip()
                    if topic:
                        unique_topics.add(topic)
                        topic_details.append(topic)
    
    unique_topics_count = len(unique_topics)
    
    # Count active shifts (channels that are not deleted) - not affected by date
    active_shifts = Channel.objects.filter(is_deleted=False).count()
    
    # Get channel details
    channels = Channel.objects.filter(is_deleted=False)
    channel_details = []
    for channel in channels:
        channel_details.append({
            'id': channel.id,
            'name': channel.name,
            'channel_id': channel.channel_id,
            'project_id': channel.project_id
        })
    
    # Prepare response with dashboardStats wrapper
    response = {
        'dashboardStats': {
            'totalTranscriptions': total_transcriptions,
            'avgSentimentScore': avg_sentiment,
            'uniqueTopics': unique_topics_count,
            'activeShifts': active_shifts,
            'details': {
                'sentimentBreakdown': sentiment_breakdown,
                'totalAnalyses': len(analyses),
                'topicsList': list(unique_topics),
                'channels': channel_details,
                'dateFilterApplied': bool(date_filter)
            }
        }
    }
    
    # Add date range info if dates were provided
    if start_date and end_date:
        response['dateRange'] = {
            'startDate': start_date,
            'endDate': end_date
        }
    
    return response
