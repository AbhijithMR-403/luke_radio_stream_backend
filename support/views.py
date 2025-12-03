from django.core.mail import send_mail
from django.conf import settings
from rest_framework import permissions, status
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView
from django.http import HttpResponse
from django.db.models import Sum
from zoneinfo import ZoneInfo
import csv
from io import StringIO

from .models import SupportTicket, SupportTicketResponse
from .serializer import SupportTicketSerializer, SupportTicketResponseSerializer, TranscribedAudioQuerySerializer
from data_analysis.models import TranscriptionDetail


class SupportTicketListCreateAPIView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def get_permissions(self):
        if self.request.method == "GET":
            return [permissions.IsAuthenticated(), permissions.IsAdminUser()]
        return [permissions.IsAuthenticated()]

    def get(self, request):
        queryset = SupportTicket.objects.all().order_by("-created_at")

        class SupportTicketPagination(PageNumberPagination):
            page_size = 20
            page_size_query_param = "page_size"
            max_page_size = 100

        paginator = SupportTicketPagination()
        page = paginator.paginate_queryset(queryset, request, view=self)
        serializer = SupportTicketSerializer(page, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = SupportTicketSerializer(data=request.data, context={"request": request})
        if serializer.is_valid():
            ticket = serializer.save()
            output = SupportTicketSerializer(ticket)
            return Response(output.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)



class SupportTicketDetailAPIView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, request, ticket_id):
        try:
            ticket = SupportTicket.objects.get(id=ticket_id)
        except SupportTicket.DoesNotExist:
            return None
        if request.user.is_staff or ticket.user_id == request.user.id:
            return ticket
        return None

    def get(self, request, ticket_id):
        ticket = self.get_object(request, ticket_id)
        if ticket is None:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)
        serializer = SupportTicketSerializer(ticket)
        return Response(serializer.data)


class SupportTicketRespondAPIView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def post(self, request, ticket_id):
        try:
            ticket = SupportTicket.objects.get(id=ticket_id)
        except SupportTicket.DoesNotExist:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)

        serializer = SupportTicketResponseSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        response = SupportTicketResponse.objects.create(
            ticket=ticket,
            responder=request.user,
            message=serializer.validated_data["message"],
        )

        # Email notification to ticket owner
        if getattr(ticket.user, "email", None):
            subject = f"Support Response: {ticket.subject}"
            body = (
                f"Hello,\n\nYour support ticket has a new response.\n\n"
                f"Subject: {ticket.subject}\n"
                f"Ticket ID: {ticket.id}\n\n"
                f"Question:\n{ticket.description}\n\n"
                f"Response:\n{response.message}\n\n"
                f"You can view the full conversation here: {settings.FRONTEND_URL}/support/tickets/{ticket.id}\n\n"
                f"Regards,\nSupport Team"
            )
            try:
                send_mail(subject, body, settings.EMAIL_HOST_USER, [ticket.user.email], fail_silently=True)
            except Exception:
                # Intentionally fail silently to not block API on email errors
                pass

        output = SupportTicketResponseSerializer(response)
        return Response(output.data, status=status.HTTP_201_CREATED)


class TranscribedAudioStatsBaseMixin:
    """
    Base mixin with shared logic for transcribed audio statistics
    """
    def _format_duration(self, seconds):
        """
        Format duration in seconds to human-readable format (HH:MM:SS)
        """
        if not seconds:
            return "00:00:00"
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def _convert_to_channel_timezone(self, dt, channel_timezone):
        """
        Convert datetime to channel timezone
        
        Args:
            dt: datetime object (timezone-aware or None)
            channel_timezone: timezone string (e.g., 'America/New_York') or None for UTC
            
        Returns:
            str: ISO formatted datetime string in the channel timezone, or None if dt is None
        """
        if not dt:
            return None
        
        try:
            # Ensure timezone-aware
            from django.utils import timezone as django_timezone
            if django_timezone.is_naive(dt):
                dt = django_timezone.make_aware(dt)
            
            # Convert to channel timezone if specified
            if channel_timezone:
                try:
                    channel_zone = ZoneInfo(channel_timezone)
                    return dt.astimezone(channel_zone).isoformat()
                except Exception:
                    # If timezone is invalid, fall back to UTC
                    return dt.isoformat()
            else:
                # Default to UTC
                return dt.isoformat()
        except Exception:
            # If conversion fails, return original
            return dt.isoformat() if hasattr(dt, 'isoformat') else str(dt)
    
    def _get_transcribed_audio_data(self, start_date=None, end_date=None, channel_id=None):
        """
        Get transcribed audio data with filters
        
        Returns:
            tuple: (transcribed_audio_list, total_duration_seconds, total_duration_formatted)
        """
        # Query transcribed audio with related data
        queryset = TranscriptionDetail.objects.select_related(
            'rev_job',
            'audio_segment',
            'audio_segment__channel'
        ).filter(
            rev_job__status='transcribed',
            rev_job__duration_seconds__isnull=False,
            audio_segment__isnull=False  # Ensure audio_segment exists
        )
        
        # Apply date filter on audio segment start_time
        if start_date:
            queryset = queryset.filter(audio_segment__start_time__gte=start_date)
        if end_date:
            queryset = queryset.filter(audio_segment__start_time__lte=end_date)
        
        # Apply channel filter
        if channel_id:
            queryset = queryset.filter(audio_segment__channel_id=channel_id)
        
        # Exclude deleted segments
        queryset = queryset.filter(audio_segment__is_delete=False)
        
        # Calculate total duration
        total_duration_seconds = queryset.aggregate(
            total=Sum('rev_job__duration_seconds')
        )['total'] or 0.0
        
        # Get all transcribed audio data
        transcribed_audio_list = []
        for transcription in queryset.order_by('audio_segment__start_time'):
            audio_segment = transcription.audio_segment
            rev_job = transcription.rev_job
            
            # Get channel timezone
            channel_timezone = None
            if audio_segment and audio_segment.channel:
                channel_timezone = audio_segment.channel.timezone or 'UTC'
            
            # Convert created_at to channel timezone
            created_at_channel_tz = None
            if transcription.created_at:
                created_at_channel_tz = self._convert_to_channel_timezone(
                    transcription.created_at,
                    channel_timezone
                )
            
            transcribed_audio_list.append({
                'id': transcription.id,
                'audio_segment_id': audio_segment.id if audio_segment else None,
                'channel_id': audio_segment.channel.id if audio_segment and audio_segment.channel else None,
                'channel_name': audio_segment.channel.name if audio_segment and audio_segment.channel else None,
                'channel_timezone': channel_timezone or 'UTC',
                'start_time': audio_segment.start_time.isoformat() if audio_segment and audio_segment.start_time else None,
                'end_time': audio_segment.end_time.isoformat() if audio_segment and audio_segment.end_time else None,
                'duration_seconds': rev_job.duration_seconds if rev_job else None,
                'duration_formatted': self._format_duration(rev_job.duration_seconds) if rev_job and rev_job.duration_seconds else None,
                'transcript': transcription.transcript,
                'job_id': rev_job.job_id if rev_job else None,
                'created_at': transcription.created_at.isoformat() if transcription.created_at else None,
                'created_at_channel_tz': created_at_channel_tz,
                'file_name': audio_segment.file_name if audio_segment else None,
                'file_path': audio_segment.file_path if audio_segment else None,
            })
        
        # Format total duration
        total_duration_formatted = self._format_duration(total_duration_seconds)
        
        return transcribed_audio_list, total_duration_seconds, total_duration_formatted


class TranscribedAudioStatsView(TranscribedAudioStatsBaseMixin, APIView):
    """
    API endpoint for transcribed audio statistics (JSON response)
    """
    parser_classes = [JSONParser]
    # permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get transcribed audio statistics with filters (JSON response)
        
        Query Parameters:
            start_date (str): Start date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format (optional)
            end_date (str): End date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format (optional)
            channel_id (int): Channel ID to filter by (optional)
        """
        try:
            # Validate query parameters using serializer
            serializer = TranscribedAudioQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_date = validated_data.get('start_date')
            end_date = validated_data.get('end_date')
            channel_id = validated_data.get('channel_id')
            
            # Get transcribed audio data
            transcribed_audio_list, total_duration_seconds, total_duration_formatted = self._get_transcribed_audio_data(
                start_date=start_date,
                end_date=end_date,
                channel_id=channel_id
            )
            
            # Return JSON response
            response_data = {
                'total_duration_seconds': total_duration_seconds,
                'total_duration_formatted': total_duration_formatted,
                'total_count': len(transcribed_audio_list),
                'filters': {
                    'start_date': request.query_params.get('start_date'),
                    'end_date': request.query_params.get('end_date'),
                    'channel_id': channel_id
                },
                'transcribed_audio': transcribed_audio_list
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'error': f'Failed to fetch transcribed audio statistics: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TranscribedAudioStatsCSVView(TranscribedAudioStatsBaseMixin, APIView):
    """
    API endpoint for transcribed audio statistics CSV export
    """
    parser_classes = [JSONParser]
    # permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """
        Get transcribed audio statistics as CSV file
        
        Query Parameters:
            start_date (str): Start date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format (optional)
            end_date (str): End date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format (optional)
            channel_id (int): Channel ID to filter by (optional)
        """
        try:
            # Validate query parameters using serializer
            serializer = TranscribedAudioQuerySerializer(data=request.query_params)
            if not serializer.is_valid():
                return Response(
                    serializer.errors,
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get validated data
            validated_data = serializer.validated_data
            start_date = validated_data.get('start_date')
            end_date = validated_data.get('end_date')
            channel_id = validated_data.get('channel_id')
            
            # Get transcribed audio data
            transcribed_audio_list, total_duration_seconds, total_duration_formatted = self._get_transcribed_audio_data(
                start_date=start_date,
                end_date=end_date,
                channel_id=channel_id
            )
            
            # Generate and return CSV response
            return self._generate_csv_response(
                transcribed_audio_list,
                total_duration_seconds,
                total_duration_formatted,
                start_date,
                end_date,
                channel_id
            )
            
        except Exception as e:
            return Response(
                {'error': f'Failed to generate CSV: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _generate_csv_response(self, data, total_seconds, total_formatted, start_date, end_date, channel_id):
        """
        Generate CSV response with transcribed audio data
        """
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header with total duration
        writer.writerow(['Transcribed Audio Statistics'])
        writer.writerow([])
        writer.writerow(['Total Duration (seconds)', total_seconds])
        writer.writerow(['Total Duration (formatted)', total_formatted])
        writer.writerow(['Total Count', len(data)])
        writer.writerow([])
        writer.writerow(['Filters:'])
        writer.writerow(['Start Date', start_date.isoformat() if start_date else 'All'])
        writer.writerow(['End Date', end_date.isoformat() if end_date else 'All'])
        writer.writerow(['Channel ID', channel_id if channel_id else 'All'])
        writer.writerow([])
        writer.writerow([])
        
        # Determine timezone for column header
        # Check if all items have the same timezone
        timezones = set(item.get('channel_timezone', 'UTC') for item in data if item.get('channel_timezone'))
        if len(timezones) == 1:
            # All rows have the same timezone, use it in header
            header_timezone = timezones.pop()
        else:
            # Multiple timezones, use generic label
            header_timezone = 'Channel Timezone'
        
        # Write column headers
        writer.writerow([
            'ID',
            'Audio Segment ID',
            'Channel ID',
            'Channel Name',
            'Start Time',
            'End Time',
            'Duration (seconds)',
            'Duration (formatted)',
            'Transcript',
            'Job ID',
            'Created At',
            f'Created At ({header_timezone})',
            'File Name',
            'File Path'
        ])
        
        # Write data rows
        for item in data:
            writer.writerow([
                item['id'],
                item['audio_segment_id'],
                item['channel_id'],
                item['channel_name'],
                item['start_time'],
                item['end_time'],
                item['duration_seconds'],
                item['duration_formatted'],
                item['transcript'],
                item['job_id'],
                item['created_at'],
                item['created_at_channel_tz'],
                item['file_name'],
                item['file_path'],
            ])
        
        # Create HTTP response with CSV content
        response = HttpResponse(output.getvalue(), content_type='text/csv')
        
        # Generate filename with date range
        filename = 'transcribed_audio_stats'
        if start_date:
            filename += f'_{start_date.strftime("%Y%m%d")}'
        if end_date:
            filename += f'_{end_date.strftime("%Y%m%d")}'
        filename += '.csv'
        
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response

