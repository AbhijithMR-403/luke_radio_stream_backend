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
        Get dashboard statistics with required date range filtering and channel filtering
        
        Query Parameters:
            start_date (str): Start date in YYYY-MM-DD format (required)
            end_date (str): End date in YYYY-MM-DD format (required)
            channel_id (int): Channel ID to filter by (required)
        """
        try:
            # Get required parameters from query parameters
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            channel_id = request.query_params.get('channel_id')
            
            # Validate that all required parameters are provided
            if not start_date or not end_date or not channel_id:
                return Response(
                    {'error': 'start_date, end_date, and channel_id are all required parameters'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Validate that both dates are provided together
            if (start_date and not end_date) or (end_date and not start_date):
                return Response(
                    {'error': 'Both start_date and end_date must be provided together'}, 
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
            
            # Get stats with required date filtering and channel filtering
            stats = get_dashboard_stats(start_date=start_date, end_date=end_date, channel_id=channel_id)
            serializer = DashboardStatsSerializer(stats)
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch dashboard stats: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
