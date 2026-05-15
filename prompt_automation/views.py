from django.core.exceptions import ValidationError as DjangoValidationError
from django.db.models import Prefetch
from rest_framework import generics, permissions, status
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from data_analysis.models import AudioSegments

from .models import Prompt, PromptResult, PromptRun
from .serializers import (
    PromptRunExecuteResponseSerializer,
    PromptRunExecuteSerializer,
    PromptRunListSerializer,
    PromptSerializer,
)
from .tasks import run_prompt_run_llm_task
from .utils import prepare_prompt_run


class PromptRunExecuteView(APIView):
    """Run each prompt against the combined transcripts of the given audio segments."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        serializer = PromptRunExecuteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        prompt_ids = serializer.validated_data["prompt_ids"]
        audio_segment_ids = serializer.validated_data["audio_segment_ids"]
        max_tokens = serializer.validated_data["max_tokens"]

        prompts_by_id = {
            p.pk: p for p in Prompt.objects.filter(id__in=prompt_ids)
        }
        prompts = [prompts_by_id[pid] for pid in prompt_ids]

        segments_by_id = {
            s.pk: s
            for s in AudioSegments.objects.filter(id__in=audio_segment_ids).select_related(
                "transcription_detail"
            )
        }
        audio_segments = [segments_by_id[sid] for sid in audio_segment_ids]

        try:
            prompt_run, results = prepare_prompt_run(
                request.user,
                prompts,
                audio_segments,
                max_tokens=max_tokens,
            )
        except DjangoValidationError as exc:
            err_dict = getattr(exc, "message_dict", None) or getattr(
                exc, "error_dict", None
            )
            if err_dict:
                detail = err_dict
            else:
                detail = [str(m) for m in exc.messages]
            return Response({"detail": detail}, status=status.HTTP_400_BAD_REQUEST)

        run_prompt_run_llm_task.delay(prompt_run.pk, max_tokens=max_tokens)

        response_serializer = PromptRunExecuteResponseSerializer(
            {
                "prompt_run_id": prompt_run.pk,
                "max_tokens": max_tokens,
                "audio_segments": audio_segments,
                "results": results,
            }
        )
        return Response(response_serializer.data, status=status.HTTP_202_ACCEPTED)


class PromptRunRetrieveView(APIView):
    """Return a stored prompt run in the same shape as ``PromptRunExecuteView`` POST."""

    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, pk):
        try:
            prompt_run = (
                PromptRun.objects.prefetch_related(
                    Prefetch(
                        "audio_segments",
                        queryset=AudioSegments.objects.select_related(
                            "transcription_detail"
                        ),
                    ),
                    Prefetch(
                        "results",
                        queryset=PromptResult.objects.order_by("id"),
                    ),
                ).get(pk=pk, user_id=request.user.pk)
            )
        except PromptRun.DoesNotExist:
            return Response(
                {"detail": "Prompt run not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        response_serializer = PromptRunExecuteResponseSerializer.from_prompt_run(
            prompt_run
        )
        return Response(response_serializer.data, status=status.HTTP_200_OK)


class PromptRunListView(generics.ListAPIView):
    """List prompt runs for the authenticated user with prompts, audio segments, and results.

    Query params:
    - channel_id: only runs that include at least one audio segment on this channel.
    """

    serializer_class = PromptRunListSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        audio_with_transcript = AudioSegments.objects.select_related(
            "transcription_detail"
        )
        user = self.request.user
        if not user.is_authenticated or user.pk is None:
            return PromptRun.objects.none().order_by("-created_at")

        qs = PromptRun.objects.filter(user_id=user.pk)

        channel_id_raw = self.request.query_params.get("channel_id")
        if channel_id_raw not in (None, ""):
            try:
                channel_id = int(channel_id_raw)
            except (TypeError, ValueError) as exc:
                raise ValidationError(
                    {"channel_id": "Must be a valid integer."}
                ) from exc
            qs = qs.filter(audio_segments__channel_id=channel_id).distinct()

        return (
            qs.prefetch_related(
                "prompts",
                Prefetch("audio_segments", queryset=audio_with_transcript),
                Prefetch(
                    "results",
                    queryset=PromptResult.objects.order_by("id"),
                ),
            )
            .order_by("-created_at")
        )


class PromptListCreateView(generics.ListCreateAPIView):
    serializer_class = PromptSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        queryset = Prompt.objects.all().order_by("-created_at")

        is_active = self.request.query_params.get("is_active")
        if is_active is not None:
            queryset = queryset.filter(is_active=is_active.lower() == "true")

        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(name__icontains=search)

        return queryset


class PromptDetailView(generics.RetrieveUpdateDestroyAPIView):
    serializer_class = PromptSerializer
    permission_classes = [permissions.IsAuthenticated]
    queryset = Prompt.objects.all()
