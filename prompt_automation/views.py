from django.core.exceptions import ValidationError as DjangoValidationError
from django.db.models import Prefetch
from rest_framework import generics, permissions, status
from rest_framework.response import Response
from rest_framework.views import APIView

from data_analysis.models import AudioSegments

from .models import Prompt, PromptResult, PromptRun
from .serializers import (
    PromptResultReadSerializer,
    PromptRunExecuteSerializer,
    PromptRunListSerializer,
    PromptSerializer,
)
from .utils import run_prompts_for_audio_segments


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
            for s in AudioSegments.objects.filter(id__in=audio_segment_ids)
        }
        audio_segments = [segments_by_id[sid] for sid in audio_segment_ids]

        try:
            results = run_prompts_for_audio_segments(
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

        prompt_run_id = results[0].prompt_run_id if results else None
        return Response(
            {
                "prompt_run_id": prompt_run_id,
                "max_tokens": max_tokens,
                "results": PromptResultReadSerializer(results, many=True).data,
            },
            status=status.HTTP_200_OK,
        )


class PromptRunListView(generics.ListAPIView):
    """List prompt runs for the current user with prompts, audio segments, and results."""

    serializer_class = PromptRunListSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        audio_with_transcript = AudioSegments.objects.select_related(
            "transcription_detail"
        )
        return (
            PromptRun.objects.filter(user=self.request.user)
            .prefetch_related(
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
