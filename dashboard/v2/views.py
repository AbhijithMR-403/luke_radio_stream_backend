from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework import permissions

from data_analysis.models import AudioSegments
from data_analysis.repositories import AudioSegmentDAO
from dashboard.models import UserSentimentPreference
from dashboard.v2.service.DashboardSummary import SummaryService
from dashboard.v2.serializer import SummaryQuerySerializer
from shift_analysis.utils import filter_segments_by_shift


class SummaryView(APIView):
    """
    API endpoint for sentiment summary with datetime and shift filtering
    """
    parser_classes = [JSONParser]
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get sentiment summary with filters
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required)
            shift_id (int): Optional shift ID to filter by (optional)
        """
        try:
            # Validate query parameters using serializer
            serializer = SummaryQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data['channel_id']
            shift_id_int = validated_data.get('shift_id')
            
            # Get audio segments filtered by datetime and channel using optimized repository method
            # The filter method automatically handles select_related for transcription_detail and analysis
            audio_segments_query = AudioSegmentDAO.filter(
                channel_id=channel_id,
                start_time=start_dt,
                end_time=end_dt,
                is_active=True,
                is_delete=False,
                has_content=True  # Ensures transcription_detail exists
            ).filter(
                transcription_detail__analysis__isnull=False  # Also ensure analysis exists
            )
            
            # Get all audio segments with transcription and analysis
            audio_segments = list(audio_segments_query)
            
            # Calculate average sentiment using SummaryService
            average_sentiment = SummaryService.get_average_sentiment(audio_segments)
            
            # Get sentiment preferences from UserSentimentPreference
            # Default values if no preference exists
            target_sentiment_score = 75
            low_sentiment_threshold = 20
            high_sentiment_threshold = 80
            
            try:
                sentiment_preference = UserSentimentPreference.objects.get(user=request.user)
                target_sentiment_score = sentiment_preference.target_sentiment_score
                low_sentiment_threshold = sentiment_preference.low_sentiment_score
                high_sentiment_threshold = sentiment_preference.high_sentiment_score
            except UserSentimentPreference.DoesNotExist:
                # If no preference exists, use default values
                pass
            
            # Calculate low sentiment percentage
            low_sentiment_percentage = SummaryService.get_low_sentiment_percentage(
                audio_segments,
                threshold=low_sentiment_threshold
            )
            
            # Calculate high sentiment percentage
            high_sentiment_percentage = SummaryService.get_high_sentiment_percentage(
                audio_segments,
                threshold=high_sentiment_threshold
            )
            
            # Calculate per day average sentiments
            per_day_average_sentiments = SummaryService.get_per_day_average_sentiments(
                audio_segments
            )
            
            # Build response
            response_data = {
                'average_sentiment': average_sentiment,
                'target_sentiment_score': target_sentiment_score,
                'low_sentiment_percentage': low_sentiment_percentage,
                'high_sentiment_percentage': high_sentiment_percentage,
                'per_day_average_sentiments': per_day_average_sentiments,
                'thresholds': {
                    'target_sentiment_score': target_sentiment_score,
                    'low_sentiment_threshold': low_sentiment_threshold,
                    'high_sentiment_threshold': high_sentiment_threshold
                },
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'channel_id': channel_id,
                    'shift_id': shift_id_int
                },
                'segment_count': len(audio_segments)
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch summary: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

