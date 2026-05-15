from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction

from config.validation import ValidationUtils
from data_analysis.models import AudioSegments
from openrouter.services import OpenRouterService

from .models import Prompt, PromptResult, PromptRun


def _transcripts_from_audio_segments(
    audio_segments: list[AudioSegments],
) -> list[str]:
    transcripts: list[str] = []
    for segment in audio_segments:
        try:
            raw = segment.transcription_detail.transcript
        except ObjectDoesNotExist as exc:
            raise ValidationError(
                f"Audio segment {segment.pk} has no transcription; "
                "attach a transcript before running prompts."
            ) from exc
        text = (raw or "").strip()
        if not text:
            raise ValidationError(
                f"Audio segment {segment.pk} has an empty transcript."
            )
        transcripts.append(text)
    return transcripts


def _llm_settings_for_audio_segments(audio_segments: list[AudioSegments]):
    channel_ids = {segment.channel_id for segment in audio_segments}
    if len(channel_ids) > 1:
        raise ValidationError(
            "All audio_segments must belong to the same channel "
            f"(got channels: {sorted(channel_ids)})"
        )
    channel_id = channel_ids.pop()

    settings = ValidationUtils.validate_settings_exist(channel_id)
    api_key = (settings.openai_api_key or "").strip()
    if not api_key:
        raise ValidationError(
            f"OpenRouter API key not configured for channel {channel_id} in GeneralSetting"
        )

    return {
        "api_key": api_key,
        "model": settings.chatgpt_model,
        "temperature": settings.chatgpt_temperature,
        "transcripts": _transcripts_from_audio_segments(audio_segments),
    }


def _validate_prompt_run_inputs(
    user,
    prompts: list[Prompt],
    audio_segments: list[AudioSegments],
    max_tokens: int,
) -> None:
    ValidationUtils.validate_required_field(user, "user")
    ValidationUtils.validate_list_not_empty(prompts, "prompts")
    ValidationUtils.validate_list_not_empty(audio_segments, "audio_segments")
    if max_tokens < 0:
        raise ValidationError("max_tokens must be non-negative")
    _llm_settings_for_audio_segments(audio_segments)


def prepare_prompt_run(
    user,
    prompts: list[Prompt],
    audio_segments: list[AudioSegments],
    max_tokens: int = 1000,
) -> tuple[PromptRun, list[PromptResult]]:
    """Validate inputs, create ``PromptRun`` and pending ``PromptResult`` rows."""
    _validate_prompt_run_inputs(user, prompts, audio_segments, max_tokens)

    with transaction.atomic():
        prompt_run = PromptRun.objects.create(user=user)
        prompt_run.prompts.set(prompts)
        prompt_run.audio_segments.set(audio_segments)
        results = [
            PromptResult.objects.create(
                prompt_run=prompt_run,
                prompt=prompt,
                status="pending",
            )
            for prompt in prompts
        ]

    return prompt_run, results


def execute_prompt_run_llm(prompt_run_id: int, max_tokens: int = 1000) -> None:
    """Run OpenRouter for each pending result on an existing prompt run."""
    if max_tokens < 0:
        raise ValidationError("max_tokens must be non-negative")

    prompt_run = (
        PromptRun.objects.prefetch_related(
            "prompts",
            "results",
        )
        .prefetch_related(
            "audio_segments__transcription_detail",
        )
        .get(pk=prompt_run_id)
    )
    audio_segments = list(prompt_run.audio_segments.all())
    llm = _llm_settings_for_audio_segments(audio_segments)

    for result in prompt_run.results.select_related("prompt").order_by("id"):
        if result.status not in ("pending", "processing"):
            continue

        PromptResult.objects.filter(pk=result.pk).update(status="processing")

        try:
            response_text = OpenRouterService.get_chat_completion_with_transcripts(
                bearer_token=llm["api_key"],
                model=llm["model"],
                system_prompt=result.prompt.content,
                transcripts=llm["transcripts"],
                max_tokens=max_tokens,
                temperature=llm["temperature"],
            )
        except Exception as exc:
            PromptResult.objects.filter(pk=result.pk).update(
                status="failed",
                error_message=str(exc),
            )
        else:
            PromptResult.objects.filter(pk=result.pk).update(
                status="completed",
                response=response_text,
            )
