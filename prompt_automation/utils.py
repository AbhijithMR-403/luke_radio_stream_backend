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


def run_prompts_for_audio_segments(
    user,
    prompts: list[Prompt],
    audio_segments: list[AudioSegments],
    max_tokens: int = 1000,
) -> list[PromptResult]:
    ValidationUtils.validate_required_field(user, "user")
    ValidationUtils.validate_list_not_empty(prompts, "prompts")
    ValidationUtils.validate_list_not_empty(audio_segments, "audio_segments")
    if max_tokens < 0:
        raise ValidationError("max_tokens must be non-negative")

    # All segments must share a channel so we know which API key / model to use.
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

    transcripts = _transcripts_from_audio_segments(audio_segments)

    effective_model =  settings.chatgpt_model
    effective_temperature = settings.chatgpt_temperature

    with transaction.atomic():
        prompt_run = PromptRun.objects.create(user=user)
        prompt_run.prompts.set(prompts)
        prompt_run.audio_segments.set(audio_segments)

    results: list[PromptResult] = []
    for prompt in prompts:
        result = PromptResult.objects.create(
            prompt_run=prompt_run,
            prompt=prompt,
            status="processing",
        )

        try:
            response_text = OpenRouterService.get_chat_completion_with_transcripts(
                bearer_token=api_key,
                model=effective_model,
                system_prompt=prompt.content,
                transcripts=transcripts,
                max_tokens=max_tokens,
                temperature=effective_temperature,
            )
        except Exception as exc:
            PromptResult.objects.filter(pk=result.pk).update(
                status="failed",
                error_message=str(exc),
            )
            result.refresh_from_db()
        else:
            PromptResult.objects.filter(pk=result.pk).update(
                status="completed",
                response=response_text,
            )
            result.refresh_from_db()

        results.append(result)

    return results
