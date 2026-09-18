from django.conf import settings
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from audio_recovery.models import RecoveredAudioFile
from audio_recovery.serializers import RecoverAudioSerializer, RecoveredAudioFileSerializer
from audio_recovery.services import cleanup_recovery, create_pending_recovery
from audio_recovery.tasks import recover_audio_task


class RecoverAudioView(APIView):
    """GET  /recover/ -> list recovery jobs (newest first), optionally filtered
    by channel_id / status query params.
    POST /recover/ -> queue the download + ACRCloud identification as a Celery
    task and return immediately (202) with the RecoveredAudioFile id to poll
    for status/segments."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        queryset = RecoveredAudioFile.objects.all().order_by("-created_at")

        channel_id = request.query_params.get("channel_id")
        if channel_id:
            queryset = queryset.filter(channel_id=channel_id)

        status_param = request.query_params.get("status")
        if status_param:
            queryset = queryset.filter(status=status_param)

        return Response(RecoveredAudioFileSerializer(queryset, many=True).data)

    def post(self, request):
        form = RecoverAudioSerializer(data=request.data)
        form.is_valid(raise_exception=True)
        data = form.validated_data

        if not settings.ACR_ACCESS_KEY or not settings.ACR_ACCESS_SECRET:
            return Response(
                {"error": "ACR_ACCESS_KEY / ACR_ACCESS_SECRET are not configured."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        recovered_audio_file = create_pending_recovery(
            data["channel"],
            data["recorded_at"],
            data["url"],
            created_by=request.user,
        )
        folder = data["recorded_at"].strftime("%Y%m%d")
        task = recover_audio_task.delay(
            recovered_audio_file.id,
            data["url"],
            settings.ACR_ACCESS_KEY,
            settings.ACR_ACCESS_SECRET,
            folder,
        )
        recovered_audio_file.celery_task_id = task.id
        recovered_audio_file.save(update_fields=["celery_task_id"])

        return Response(
            RecoveredAudioFileSerializer(recovered_audio_file).data,
            status=status.HTTP_202_ACCEPTED,
        )


class RecoveryStatusView(APIView):
    """GET /recover/<id>/status/ -> poll a queued recovery job's status, and
    once it succeeds, the AudioSegments it created."""

    permission_classes = [IsAuthenticated]

    def get(self, request, recovered_audio_file_id):
        recovered_audio_file = get_object_or_404(RecoveredAudioFile, pk=recovered_audio_file_id)
        return Response(RecoveredAudioFileSerializer(recovered_audio_file).data)


class RecoveryCleanupView(APIView):
    """POST /recover/<id>/cleanup/ -> delete every AudioSegments a failed
    recovery job left behind (and their files), in one call. Only works on
    jobs whose status is 'failed'. Resubmit the same URL via /recover/ afterwards."""

    permission_classes = [IsAuthenticated]

    def post(self, request, recovered_audio_file_id):
        get_object_or_404(RecoveredAudioFile, pk=recovered_audio_file_id)
        try:
            result = cleanup_recovery(recovered_audio_file_id)
        except ValueError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(result, status=status.HTTP_200_OK)
