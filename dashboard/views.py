import json
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views import View
from django.http import JsonResponse
from data_analysis.models import GeneralTopic
from .serializer import get_dashboard_stats, DashboardStatsSerializer, get_topic_audio_segments

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
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (optional, alternative to start_date)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS format (optional, alternative to end_date)
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


class TopicAudioSegmentsView(APIView):
    """
    Class-based view to fetch all audio segments for a specific general topic
    """
    parser_classes = [JSONParser]
    
    def get(self, request):
        """
        Get all audio segments for a specific general topic
        
        Query Parameters:
            topic_name (str): Name of the general topic (required)
            start_date (str): Start date in YYYY-MM-DD format (optional, alternative to start_datetime)
            end_date (str): End date in YYYY-MM-DD format (optional, alternative to end_datetime)
            start_datetime (str): Start datetime in YYYY-MM-DDTHH:MM:SS format (optional, alternative to start_date)
            end_datetime (str): End datetime in YYYY-MM-DDTHH:MM:SS format (optional, alternative to end_date)
            channel_id (int): Channel ID to filter by (required)
            show_all_topics (bool): If true, show all topics including inactive ones. If false or not provided, filter out inactive topics (optional)
        """
        try:
            # Get required parameters from query parameters
            topic_name = request.query_params.get('topic_name')
            start_date = request.query_params.get('start_date')
            end_date = request.query_params.get('end_date')
            start_datetime = request.query_params.get('start_datetime')
            end_datetime = request.query_params.get('end_datetime')
            channel_id = request.query_params.get('channel_id')
            show_all_topics = request.query_params.get('show_all_topics', 'false').lower() == 'true'
            
            # Validate required parameters
            if not topic_name:
                return Response(
                    {'error': 'topic_name is a required parameter'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            if not channel_id:
                return Response(
                    {'error': 'channel_id is a required parameter'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            # Determine which format to use (prioritize datetime over date)
            start_param = start_datetime if start_datetime else start_date
            end_param = end_datetime if end_datetime else end_date
            # Validate that both dates/datetimes are provided together if any are provided
            if (start_param and not end_param) or (end_param and not start_param):
                return Response(
                    {'error': 'Both start and end date/datetime must be provided together'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            print(start_date)
            # Convert channel_id to int
            try:
                channel_id = int(channel_id)
            except ValueError:
                return Response(
                    {'error': 'channel_id must be a valid integer'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            # Get audio segments for the topic
            audio_segments = get_topic_audio_segments(
                topic_name=topic_name,
                start_date_or_datetime=start_param,
                end_date_or_datetime=end_param,
                channel_id=channel_id,
                show_all_topics=show_all_topics
            )
            print("-------------")
            
            return Response(audio_segments, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch audio segments for topic: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )



@method_decorator(csrf_exempt, name='dispatch')
class GeneralTopicsManagementView(View):
    """API to manage general topics (add, update status, list)"""
    
    def get(self, request, *args, **kwargs):
        """Get all general topics with their status"""
        try:
            # Get query parameters
            status_filter = request.GET.get('status')  # 'active', 'inactive', or None for all
            
            # Build query with optimized filtering
            topics_query = GeneralTopic.objects.all()
            if status_filter == 'active':
                topics_query = topics_query.filter(is_active=True)
            elif status_filter == 'inactive':
                topics_query = topics_query.filter(is_active=False)
            
            topics = topics_query.order_by('topic_name')
            
            topics_data = []
            for topic in topics:
                topics_data.append({
                    'id': topic.id,
                    'topic_name': topic.topic_name,
                    'is_active': topic.is_active,
                    'created_at': topic.created_at.isoformat(),
                    'updated_at': topic.updated_at.isoformat()
                })
            
            return JsonResponse({
                'success': True,
                'data': {
                    'topics': topics_data,
                    'total_count': len(topics_data),
                    'active_count': GeneralTopic.objects.filter(is_active=True).count(),
                    'inactive_count': GeneralTopic.objects.filter(is_active=False).count()
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def post(self, request, *args, **kwargs):
        """Add or update topics (upsert functionality) - accepts list of topics"""
        try:
            data = json.loads(request.body)
            
            # Debug: Print the data type and content
            print(f"Data type: {type(data)}")
            print(f"Data content: {data}")
            
            # Check if data is a list
            if not isinstance(data, list):
                return JsonResponse({'success': False, 'error': 'Request body must be a list of topics'}, status=400)
            
            if not data:
                return JsonResponse({'success': False, 'error': 'Topics list cannot be empty'}, status=400)
            
            results = []
            created_count = 0
            updated_count = 0
            
            for i, topic_data in enumerate(data):
                print(f"Processing topic {i}: {topic_data}, type: {type(topic_data)}")
                
                if not isinstance(topic_data, dict):
                    return JsonResponse({'success': False, 'error': f'Topic at index {i} must be an object, got {type(topic_data)}'}, status=400)
                
                topic_name = topic_data.get('topic_name')
                is_active = topic_data.get('is_active', True)
                
                if not topic_name:
                    return JsonResponse({'success': False, 'error': 'topic_name is required for each topic'}, status=400)
                
                # Check if topic already exists
                existing_topic = GeneralTopic.objects.filter(topic_name__iexact=topic_name).first()
                
                if existing_topic:
                    # Update existing topic
                    existing_topic.is_active = is_active
                    existing_topic.save()
                    
                    action = 'updated'
                    updated_count += 1
                else:
                    # Create new topic
                    existing_topic = GeneralTopic.objects.create(
                        topic_name=topic_name,
                        is_active=is_active
                    )
                    action = 'created'
                    created_count += 1
                
                results.append({
                    'id': existing_topic.id,
                    'topic_name': existing_topic.topic_name,
                    'is_active': existing_topic.is_active,
                    'created_at': existing_topic.created_at.isoformat(),
                    'updated_at': existing_topic.updated_at.isoformat(),
                    'action': action
                })
            
            return JsonResponse({
                'success': True,
                'message': f'Processed {len(results)} topics: {created_count} created, {updated_count} updated',
                'summary': {
                    'total_processed': len(results),
                    'created': created_count,
                    'updated': updated_count
                },
                'data': results
            })
            
        except json.JSONDecodeError as e:
            return JsonResponse({'success': False, 'error': f'Invalid JSON data: {str(e)}'}, status=400)
        except Exception as e:
            import traceback
            print(f"Error in post method: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

