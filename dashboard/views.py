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
        Get dashboard statistics with optional date range filtering
        
        Query Parameters:
            start_date (str): Start date in YYYY-MM-DD format (optional)
            end_date (str): End date in YYYY-MM-DD format (optional)
        """
        try:
            # Get date range from query parameters
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            
            # Validate that both dates are provided if one is provided
            if (start_date and not end_date) or (end_date and not start_date):
                return Response(
                    {'error': 'Both start_date and end_date must be provided together'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get stats with date filtering
            stats = get_dashboard_stats(start_date=start_date, end_date=end_date)
            serializer = DashboardStatsSerializer(stats)
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch dashboard stats: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
