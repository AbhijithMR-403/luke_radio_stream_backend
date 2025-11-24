from rest_framework import generics, permissions
from rest_framework.exceptions import NotFound

from data_analysis.models import AudioSegments as AudioSegmentsModel
from logger.repositories import AudioSegmentEditLogDAO
from logger.serializers import AudioSegmentEditLogSerializer


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
