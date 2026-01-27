from rest_framework.views import APIView
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework import permissions
from django.http import HttpResponse

from dashboard.v2.service.DashboardSummary import SummaryService
from dashboard.v2.service.BucketCountService import BucketCountService
from dashboard.v2.service.TopicService import TopicService
from dashboard.v2.service.CSVExportService import CSVExportService
from dashboard.v2.service.WordCountService import WordCountService
from dashboard.v2.serializer import (
    SummaryQuerySerializer, 
    BucketCountQuerySerializer, 
    CategoryBucketCountQuerySerializer, 
    TopicQuerySerializer, 
    GeneralTopicCountByShiftQuerySerializer,
    CSVExportQuerySerializer,
    WordCountQuerySerializer
)


class SummaryView(GenericAPIView):
    """
    API endpoint for sentiment summary with datetime and shift filtering
    """
    serializer_class = SummaryQuerySerializer
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get sentiment summary with filters
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            shift_id (int): Optional shift ID to filter by (optional)
        """
        try:
            # Validate query parameters using serializer
            serializer = self.get_serializer(data=request.query_params)
            serializer.is_valid(raise_exception=True)
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            shift_id_int = validated_data.get('shift_id')
            
            # Get summary data from service
            summary_data = SummaryService.get_summary_data(
                channel_id=channel_id,
                start_dt=start_dt,
                end_dt=end_dt,
                shift_id=shift_id_int,
                report_folder_id=report_folder_id
            )
            
            # Build response with filters
            response_data = {
                **summary_data,
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'channel_id': channel_id,
                    'report_folder_id': report_folder_id,
                    'shift_id': shift_id_int
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch summary: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class BucketCountView(APIView):
    """
    API endpoint to get count of audio segments analyzed from bucket_prompt,
    classified by WellnessBucket categories (personal, community, spiritual)
    """
    parser_classes = [JSONParser]
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get bucket count by category with datetime filtering
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            shift_id (int): Optional shift ID to filter by (optional)
        """
        try:
            # Validate query parameters
            serializer = BucketCountQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            shift_id = validated_data.get('shift_id')
            
            # Get bucket counts from service
            bucket_data = BucketCountService.get_bucket_counts(
                start_dt=start_dt,
                end_dt=end_dt,
                channel_id=channel_id,
                shift_id=shift_id,
                report_folder_id=report_folder_id,
            )
            
            # Build response
            response_data = {
                **bucket_data,
                'filters': {
                    'start_datetime': start_dt,
                    'end_datetime': end_dt,
                    'channel_id': channel_id,
                    'report_folder_id': report_folder_id,
                    'shift_id': shift_id
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch bucket count: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CategoryBucketCountView(APIView):
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get bucket count percentages for a specific category with datetime filtering
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            category_name (str): Category name to filter by - one of: personal, community, spiritual (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            shift_id (int): Optional shift ID to filter by (optional)
        """
        try:
            # Validate query parameters
            serializer = CategoryBucketCountQuerySerializer(data=request.query_params)
            serializer.is_valid(raise_exception=True)
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            category_name = validated_data['category_name']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            shift_id = validated_data.get('shift_id')
            
            # Get bucket counts from service
            bucket_data = BucketCountService.get_category_bucket_counts(
                start_dt=start_dt,
                end_dt=end_dt,
                category_name=category_name,
                channel_id=channel_id,
                shift_id=shift_id,
                report_folder_id=report_folder_id
            )
            
            # Build response
            response_data = {
                **bucket_data,
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'category_name': category_name,
                    'channel_id': channel_id,
                    'report_folder_id': report_folder_id,
                    'shift_id': shift_id
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch category bucket count: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TopTopicsView(APIView):
    """
    API endpoint to get top 10 topics with both count and duration data
    """
    parser_classes = [JSONParser]
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get top topics with both count and duration data
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            shift_id (int): Optional shift ID to filter by (optional)
            show_all_topics (bool): If True, show all topics. If False, exclude topics that are in GeneralTopic model (default: False)
            sort_by (str): Sort the main 'top_topics' list by 'count' or 'duration' (default: 'duration')
        """
        try:
            # Validate query parameters
            serializer = TopicQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            shift_id = validated_data.get('shift_id')
            show_all_topics = validated_data.get('show_all_topics', False)
            sort_by = validated_data.get('sort_by', 'duration')
            
            # Get all topics with both count and duration calculated in a single pass
            # This ensures accurate data for both metrics
            topics_data = TopicService.get_topics_with_both_metrics(
                start_dt=start_dt,
                end_dt=end_dt,
                channel_id=channel_id,
                report_folder_id=report_folder_id,
                shift_id=shift_id,
                limit=1000,  # High limit to get all topics
                show_all_topics=show_all_topics
            )
            
            # Get all topics (unsorted)
            all_topics = topics_data['top_topics']
            
            # Create sorted copies for both metrics
            topics_sorted_by_count = sorted(all_topics, key=lambda x: x['count'], reverse=True)
            topics_sorted_by_duration = sorted(all_topics, key=lambda x: x['total_duration_seconds'], reverse=True)
            
            # Set the main top_topics list based on sort_by parameter
            if sort_by == 'count':
                main_topics = topics_sorted_by_count
            else:  # default to 'duration'
                main_topics = topics_sorted_by_duration
            
            # Build response with both sorted lists
            response_data = {
                'top_topics': main_topics,  # Main list sorted by requested parameter
                'top_topics_by_count': topics_sorted_by_count,  # Sorted by count
                'top_topics_by_duration': topics_sorted_by_duration,  # Sorted by duration
                'total_topics': topics_data['total_topics'],
                'filters': {
                    'start_datetime': start_dt.isoformat(),
                    'end_datetime': end_dt.isoformat(),
                    'channel_id': channel_id,
                    'report_folder_id': report_folder_id,
                    'shift_id': shift_id,
                    'sort_by': sort_by
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch top topics: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GeneralTopicCountByShiftView(APIView):
    """
    API endpoint to get count of general topics grouped by shift
    """
    parser_classes = [JSONParser]
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get general topic counts grouped by shift
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            show_all_topics (bool): If True, show all topics. If False, only show topics that are in GeneralTopic model (default: False)
        """
        try:
            # Validate query parameters
            serializer = GeneralTopicCountByShiftQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            show_all_topics = validated_data.get('show_all_topics', False)
            
            # Get general topic counts by shift from service
            result_data = TopicService.get_general_topic_counts_by_shift(
                start_dt=start_dt,
                end_dt=end_dt,
                channel_id=channel_id,
                report_folder_id=report_folder_id,
                show_all_topics=show_all_topics
            )
            
            # Build response
            response_data = {
                **result_data,
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'channel_id': channel_id,
                    'report_folder_id': report_folder_id,
                    'show_all_topics': show_all_topics
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch general topic counts by shift: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CSVExportView(APIView):
    """
    API endpoint to download transcription and analysis data as CSV
    """
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Download transcription and analysis data as CSV file
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            shift_id (int): Optional shift ID to filter by (optional)
        """
        try:
            # Validate query parameters
            serializer = CSVExportQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            shift_id = validated_data.get('shift_id')
            
            # Get CSV export data from service
            csv_data = CSVExportService.export_to_csv(
                start_dt=start_dt,
                end_dt=end_dt,
                channel_id=channel_id,
                report_folder_id=report_folder_id,
                shift_id=shift_id
            )
            
            # Create HTTP response with CSV
            response = HttpResponse(csv_data['content'], content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{csv_data["filename"]}"'
            
            return response
            
        except Exception as e:
            return Response(
                {'error': f'Failed to generate CSV export: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class WordCountView(APIView):
    """
    API endpoint to get word counts from transcriptions
    """
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get word counts from transcriptions filtered by date range, channel, and optional shift
        
        Query Parameters:
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (required)
            channel_id (int): Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id (int): Report folder ID to filter by (required if channel_id not provided)
            shift_id (int): Optional shift ID to filter by (optional)
        
        Returns:
            Dictionary containing:
            - word_counts: Dictionary mapping words to their counts (sorted by count descending)
            - total_words: Total number of words found
            - unique_words: Number of unique words
            - filters: Applied filter parameters
        """
        try:
            # Validate query parameters
            serializer = WordCountQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_dt = validated_data['start_datetime']
            end_dt = validated_data['end_datetime']
            channel_id = validated_data.get('channel_id')
            report_folder_id = validated_data.get('report_folder_id')
            shift_id = validated_data.get('shift_id')
            
            # Get word counts from service
            word_count_data = WordCountService.get_word_counts(
                start_dt=start_dt,
                end_dt=end_dt,
                channel_id=channel_id,
                report_folder_id=report_folder_id,
                shift_id=shift_id
            )
            
            # Build response
            response_data = {
                **word_count_data,
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'channel_id': channel_id,
                    'report_folder_id': report_folder_id,
                    'shift_id': shift_id
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch word counts: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

