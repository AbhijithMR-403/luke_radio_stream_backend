from rest_framework import generics, permissions, views
from rest_framework.response import Response
from rest_framework.exceptions import NotFound
from django.utils.dateparse import parse_datetime

from logger.repositories import AudioSegmentEditLogDAO, RevTranscriptionJobLogDAO
from logger.serializers import (
    AudioSegmentEditLogSerializer,
    DurationStatisticsSerializer,
)
from data_analysis.models import AudioSegments as AudioSegmentsModel, RevTranscriptionJob
from core_admin.models import Channel


class AudioSegmentEditLogListView(generics.ListAPIView):
    """
    Returns edit history for a given audio segment.

    Endpoint expects the audio segment id in the URL and optionally supports
    filtering by action or trigger_type via query parameters.
    """

    serializer_class = AudioSegmentEditLogSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        audio_segment_id = self.kwargs.get("audio_segment_id")
        if not AudioSegmentsModel.objects.filter(id=audio_segment_id).exists():
            raise NotFound("Audio segment not found.")

        queryset = AudioSegmentEditLogDAO.get_by_audio_segment(audio_segment_id)

        action = self.request.query_params.get("action")
        trigger_type = self.request.query_params.get("trigger_type")

        if action:
            queryset = queryset.filter(action=action)
        if trigger_type:
            queryset = queryset.filter(trigger_type=trigger_type)

        return queryset.select_related("audio_segment", "user").prefetch_related(
            "affected_segments"
        )


class RevTranscriptionJobLogStatisticsView(views.APIView):
    """
    Returns duration statistics for RevTranscriptionJob.
    
    Returns:
    - channels: List of total duration per channel
    - grand_total: Total duration across all channels
    
    Supports filtering by:
    - start_time: Filter by job's created_on (ISO format)
    - end_time: Filter by job's created_on (ISO format)
    - channels: Comma-separated list of channel IDs (e.g., "1,2,3")
    - audio_segment_start_time: Filter by audio_segment's start_time (ISO format)
    - audio_segment_end_time: Filter by audio_segment's start_time upper bound (ISO format)
    - rev_transcription_jobs: Comma-separated list of RevTranscriptionJob IDs (e.g., "1,2,3")
    """

    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        # Parse optional log time filters
        start_time = None
        end_time = None
        
        start_time_str = request.query_params.get("start_time")
        if start_time_str:
            start_time = parse_datetime(start_time_str)
            if not start_time:
                raise NotFound("Invalid start_time format. Use ISO format (e.g., 2024-01-01T00:00:00Z).")
        
        end_time_str = request.query_params.get("end_time")
        if end_time_str:
            end_time = parse_datetime(end_time_str)
            if not end_time:
                raise NotFound("Invalid end_time format. Use ISO format (e.g., 2024-01-01T00:00:00Z).")
        
        # Parse audio segment start_time filters
        audio_segment_start_time = None
        audio_segment_end_time = None
        
        audio_segment_start_time_str = request.query_params.get("audio_segment_start_time")
        if audio_segment_start_time_str:
            audio_segment_start_time = parse_datetime(audio_segment_start_time_str)
            if not audio_segment_start_time:
                raise NotFound("Invalid audio_segment_start_time format. Use ISO format (e.g., 2024-01-01T00:00:00Z).")
        
        audio_segment_end_time_str = request.query_params.get("audio_segment_end_time")
        if audio_segment_end_time_str:
            audio_segment_end_time = parse_datetime(audio_segment_end_time_str)
            if not audio_segment_end_time:
                raise NotFound("Invalid audio_segment_end_time format. Use ISO format (e.g., 2024-01-01T00:00:00Z).")
        
        # Parse channel IDs filter
        channel_ids = None
        channels_str = request.query_params.get("channels")
        if channels_str:
            try:
                # Split comma-separated channel IDs and convert to integers
                channel_ids = [int(ch_id.strip()) for ch_id in channels_str.split(',') if ch_id.strip()]
                
                # Validate that all channel IDs exist
                if channel_ids:
                    existing_channels = Channel.objects.filter(id__in=channel_ids).values_list('id', flat=True)
                    missing_channels = set(channel_ids) - set(existing_channels)
                    if missing_channels:
                        raise NotFound(f"Channel IDs not found: {', '.join(map(str, missing_channels))}")
            except ValueError:
                raise NotFound("Invalid channels format. Use comma-separated channel IDs (e.g., 1,2,3).")
        
        # Parse RevTranscriptionJob IDs filter
        rev_transcription_job_ids = None
        rev_transcription_jobs_str = request.query_params.get("rev_transcription_jobs")
        if rev_transcription_jobs_str:
            try:
                # Split comma-separated job IDs and convert to integers
                rev_transcription_job_ids = [int(job_id.strip()) for job_id in rev_transcription_jobs_str.split(',') if job_id.strip()]
                
                # Validate that all job IDs exist
                if rev_transcription_job_ids:
                    existing_jobs = RevTranscriptionJob.objects.filter(id__in=rev_transcription_job_ids).values_list('id', flat=True)
                    missing_jobs = set(rev_transcription_job_ids) - set(existing_jobs)
                    if missing_jobs:
                        raise NotFound(f"RevTranscriptionJob IDs not found: {', '.join(map(str, missing_jobs))}")
            except ValueError:
                raise NotFound("Invalid rev_transcription_jobs format. Use comma-separated job IDs (e.g., 1,2,3).")
        
        # Get statistics
        statistics = RevTranscriptionJobLogDAO.get_duration_statistics(
            start_time=start_time,
            end_time=end_time,
            channel_ids=channel_ids,
            audio_segment_start_time=audio_segment_start_time,
            audio_segment_end_time=audio_segment_end_time,
            rev_transcription_job_ids=rev_transcription_job_ids
        )
        
        # Serialize and return
        serializer = DurationStatisticsSerializer(statistics)
        return Response(serializer.data)
