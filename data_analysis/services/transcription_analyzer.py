from datetime import datetime, timedelta
import requests
from typing import Optional
import os
from django.utils import timezone
from decouple import config
from acr_admin.models import GeneralSetting, Channel
from openai import OpenAI
from django.core.exceptions import ValidationError
from config.validation import ValidationUtils

from data_analysis.models import RevTranscriptionJob, TranscriptionAnalysis, TranscriptionDetail, UnrecognizedAudio

class TranscriptionAnalyzer:
    @staticmethod
    def analyze_transcription(transcription_detail):
        if not isinstance(transcription_detail, TranscriptionDetail):
            raise ValidationError("transcription_detail must be a TranscriptionDetail instance")
        
        # Validate that transcription_detail has a transcript
        if not transcription_detail.transcript or not transcription_detail.transcript.strip():
            raise ValidationError("transcription_detail must have a non-empty transcript")
        
        # Check if analysis already exists and has all required fields
        try:
            existing_analysis = TranscriptionAnalysis.objects.filter(
                transcription_detail=transcription_detail
            ).first()
            
            if existing_analysis:
                # Check if all required fields have values
                required_fields = ['summary', 'sentiment', 'general_topics', 'iab_topics']
                missing_fields = []
                
                for field in required_fields:
                    field_value = getattr(existing_analysis, field)
                    if not field_value or not str(field_value).strip():
                        missing_fields.append(field)
                
                if not missing_fields:
                    print(f"Transcription analysis already exists with all fields for transcription_detail {transcription_detail.id}, skipping API calls")
                    return existing_analysis
                else:
                    print(f"Transcription analysis exists but missing fields: {missing_fields} for transcription_detail {transcription_detail.id}, will recreate")
                    # Delete the incomplete analysis to recreate it
                    existing_analysis.delete()
        except Exception as e:
            print(f"Error checking existing transcription analysis: {e}")
            # Continue with API calls if check fails

        # Validate OpenAI API key
        api_key = ValidationUtils.validate_openai_api_key()
        settings = ValidationUtils.validate_settings_exist()
        client = OpenAI(api_key=api_key)
        transcript = transcription_detail.transcript

        def chat_params(prompt, transcript, max_tokens):
            return {
                "model": settings.chatgpt_model or "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": transcript}
                ],
                "max_tokens": max_tokens if max_tokens > 0 else None,
                "temperature": settings.chatgpt_temperature,
                "top_p": settings.chatgpt_top_p,
                "frequency_penalty": settings.chatgpt_frequency_penalty,
                "presence_penalty": settings.chatgpt_presence_penalty,
            }

        # Summary
        summary_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.summarize_transcript_prompt, transcript, 150).items() if v is not None}
        )
        summary = summary_resp.choices[0].message.content.strip()
        # Sentiment
        sentiment_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.sentiment_analysis_prompt, transcript, 10).items() if v is not None}
        )
        sentiment = sentiment_resp.choices[0].message.content.strip()

        # General topics
        general_topics_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.general_topics_prompt, transcript, 100).items() if v is not None}
        )
        general_topics = general_topics_resp.choices[0].message.content.strip()

        # IAB topics
        iab_topics_resp = client.chat.completions.create(
            **{k: v for k, v in chat_params(settings.iab_topics_prompt, transcript, 100).items() if v is not None}
        )
        iab_topics = iab_topics_resp.choices[0].message.content.strip()

        # Store in TranscriptionAnalysis
        try:
            analysis = TranscriptionAnalysis.objects.create(
                transcription_detail=transcription_detail,
                summary=summary,
                sentiment=sentiment,
                general_topics=general_topics,
                iab_topics=iab_topics
            )
            print(f"Created new transcription analysis for transcription_detail {transcription_detail.id}")
            return analysis
        except Exception as e:
            print(f"Error creating transcription analysis: {e}")
            # Don't raise the error, just log it and continue

