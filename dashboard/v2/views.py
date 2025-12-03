from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework import permissions

from dashboard.v2.service.DashboardSummary import SummaryService
from dashboard.v2.service.BucketCountService import BucketCountService
from dashboard.v2.serializer import SummaryQuerySerializer, BucketCountQuerySerializer


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
            
            # Get summary data from service
            summary_data = SummaryService.get_summary_data(
                channel_id=channel_id,
                start_dt=start_dt,
                end_dt=end_dt,
                user=request.user,
                shift_id=shift_id_int
            )
            
            # Build response with filters
            response_data = {
                **summary_data,
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'channel_id': channel_id,
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
            channel_id (int): Channel ID to filter by (required)
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
            channel_id = validated_data['channel_id']
            shift_id = validated_data.get('shift_id')
            
            # Get bucket counts from service
            bucket_data = BucketCountService.get_bucket_counts(
                start_dt=start_dt,
                end_dt=end_dt,
                channel_id=channel_id,
                shift_id=shift_id
            )
            
            # Build response
            response_data = {
                **bucket_data,
                'filters': {
                    'start_datetime': request.query_params.get('start_datetime'),
                    'end_datetime': request.query_params.get('end_datetime'),
                    'channel_id': channel_id,
                    'shift_id': shift_id
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch bucket count: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

