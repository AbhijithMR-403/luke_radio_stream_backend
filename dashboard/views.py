from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from .serializer import get_dashboard_stats, DashboardStatsSerializer

# Create your views here.

class DashboardStatsView(APIView):
    """
    Class-based view for dashboard statistics with date range filtering
    """
    parser_classes = [JSONParser]
    
    def get(self, request):
        """
        Get dashboard statistics with required date/datetime range filtering and channel filtering
        
        Query Parameters:
            start_date (str): Start date in YYYY-MM-DD format (optional, alternative to start_datetime)
            end_date (str): End date in YYYY-MM-DD format (optional, alternative to end_datetime)
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS format (optional, alternative to start_date)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS format (optional, alternative to end_date)
            channel_id (int): Channel ID to filter by (required)
            show_all_topics (bool): If true, show all topics including inactive ones. If false or not provided, filter out inactive topics (optional)
        """
        try:
            # Get required parameters from query parameters - support both date and datetime formats
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            start_datetime = request.query_params.get('start_datetime')
            end_datetime = request.query_params.get('end_datetime')
            channel_id = request.query_params.get('channel_id')
            show_all_topics = request.query_params.get('show_all_topics', 'false').lower() == 'true'
            
            # Determine which format to use (prioritize datetime over date)
            start_param = start_datetime if start_datetime else start_date
            end_param = end_datetime if end_datetime else end_date
            
            # Validate that all required parameters are provided
            if not start_param or not end_param or not channel_id:
                return Response(
                    {'error': 'Either (start_date, end_date) or (start_datetime, end_datetime) and channel_id are all required parameters'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Validate that both dates/datetimes are provided together
            if (start_param and not end_param) or (end_param and not start_param):
                return Response(
                    {'error': 'Both start and end date/datetime must be provided together'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Convert channel_id to int
            try:
                channel_id = int(channel_id)
            except ValueError:
                return Response(
                    {'error': 'channel_id must be a valid integer'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get stats with required date/datetime filtering and channel filtering
            stats = get_dashboard_stats(start_date_or_datetime=start_param, end_date_or_datetime=end_param, channel_id=channel_id, show_all_topics=show_all_topics)
            serializer = DashboardStatsSerializer(stats)
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch dashboard stats: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class ShiftAnalyticsView(APIView):
    """
    Class-based view for shift analytics data with date range filtering
    """
    parser_classes = [JSONParser]
    
    def get(self, request):
        """
        Get shift analytics data with required date/datetime range filtering and channel filtering
        
        Query Parameters:
            start_date (str): Start date in YYYY-MM-DD format (optional, alternative to start_datetime)
            end_date (str): End date in YYYY-MM-DD format (optional, alternative to end_datetime)
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS format (optional, alternative to start_date)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS format (optional, alternative to end_date)
            channel_id (int): Channel ID to filter by (required)
            show_all_topics (bool): If true, show all topics including inactive ones. If false or not provided, filter out inactive topics (optional)
        """
        try:
            # Get required parameters from query parameters - support both date and datetime formats
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            start_datetime = request.query_params.get('start_datetime')
            end_datetime = request.query_params.get('end_datetime')
            channel_id = request.query_params.get('channel_id')
            show_all_topics = request.query_params.get('show_all_topics', 'false').lower() == 'true'
            
            # Determine which format to use (prioritize datetime over date)
            start_param = start_datetime if start_datetime else start_date
            end_param = end_datetime if end_datetime else end_date
            
            # Validate that all required parameters are provided
            if not start_param or not end_param or not channel_id:
                return Response(
                    {'error': 'Either (start_date, end_date) or (start_datetime, end_datetime) and channel_id are all required parameters'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Validate that both dates/datetimes are provided together
            if (start_param and not end_param) or (end_param and not start_param):
                return Response(
                    {'error': 'Both start and end date/datetime must be provided together'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Convert channel_id to int
            try:
                channel_id = int(channel_id)
            except ValueError:
                return Response(
                    {'error': 'channel_id must be a valid integer'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get shift analytics with required date/datetime filtering and channel filtering
            from .serializer import get_shift_analytics
            shift_analytics = get_shift_analytics(start_date_or_datetime=start_param, end_date_or_datetime=end_param, channel_id=channel_id, show_all_topics=show_all_topics)
            return Response(shift_analytics, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch shift analytics: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
