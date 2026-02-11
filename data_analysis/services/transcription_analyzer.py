from datetime import datetime, timedelta
import requests
from typing import Optional
import os
from django.utils import timezone
from decouple import config
from core_admin.models import Channel, WellnessBucket
from core_admin.repositories import GeneralSettingService
from openai import OpenAI
from django.core.exceptions import ValidationError
from config.validation import ValidationUtils

from data_analysis.models import RevTranscriptionJob, TranscriptionAnalysis, TranscriptionDetail
from audio_policy.models import ContentTypeDeactivationRule

class TranscriptionAnalyzer:
    @staticmethod
    def get_bucket_prompt(channel: Channel | int):
        """
        Fetches wellness buckets from the active GeneralSetting and constructs a prompt string.
        Returns the constructed prompt or None if no buckets are found.
        """
        try:
            # Fetch active GeneralSetting with buckets
            active_setting = GeneralSettingService.get_active_setting(
                channel=channel,
                include_buckets=True,
                exclude_deleted_buckets=True
            )
            
            if not active_setting:
                print("No active GeneralSetting found")
                return None
            
            # Get buckets from the active setting
            buckets = active_setting.wellness_buckets.all()
            
            if not buckets:
                print("No wellness buckets found in active setting")
                return None
            
            prompt_intro = "Where"
            bucket_prompt_parts = []
            
            for bucket in buckets:
                # Convert bucket description to list of lines
                description_lines = bucket.description.strip().split('\n') if bucket.description else []
                # Filter out empty lines
                description_lines = [line.strip() for line in description_lines if line.strip()]
                
                part = f"{bucket.title} is defined as\n" + "\n".join(f"• {line}" for line in description_lines)
                bucket_prompt_parts.append(part)
            
            if not bucket_prompt_parts:
                print("No valid bucket descriptions found")
                return None
            
            full_prompt = prompt_intro + " " + " and ".join(bucket_prompt_parts)
            
            # Get list of bucket names for category_list
            category_list = [bucket.title for bucket in buckets]
            category_string = ", ".join(category_list)
            
            # Add classification prompt at the end
            classification_prompt = f"""Could you classify the following transcript with one of the following categories only: {category_string}. You must classify the transcript into a primary category and secondary category only. If the accuracy of these classifications are less than 80% accurate, then classify them undefined. Output a comma-separated value of the primary category followed by the percentage confidence measured in whole percentages then the secondary category followed by the percentage confidence measured in whole percentages. The most alike category, the primary category, should be first, with the secondary category second. It is ok to only have one category but not OK to add a third. All of the outputs should be comma-separated, i.e category one (or undefined), confidence percentage, category two (or undefined), confidence percentage. Where you cannot identify any decipherable text just return an empty result."""
            
            full_prompt += "\n" + classification_prompt
            return full_prompt
            
        except Exception as e:
            print(f"Error constructing bucket prompt: {e}")
            return None

    @staticmethod
    def check_and_deactivate_by_content_type(analysis, content_type_result):
        """
        Check if the content_type_prompt matches any active deactivation rules
        and deactivate the audio segment if it does.
        
        Args:
            analysis: TranscriptionAnalysis instance
            content_type_result: The content type classification result string
        """
        if not content_type_result or not content_type_result.strip():
            return
        
        try:
            # Get the audio segment from the transcription detail
            if not analysis.transcription_detail.audio_segment:
                return
            
            audio_segment = analysis.transcription_detail.audio_segment
            
            # Get the channel from the audio segment
            channel = audio_segment.channel
            
            # Get all active deactivation rules for this channel
            deactivation_rules = ContentTypeDeactivationRule.objects.filter(
                channel=channel,
                is_active=True
            )
            
            if not deactivation_rules.exists():
                return
            
            # Parse content_type_result - it might be in format like "Commercial, 85%" or comma-separated
            # Extract content type names (before percentages or commas)
            content_type_lower = content_type_result.lower().strip()
            
            # Check if any deactivation rule matches
            for rule in deactivation_rules:
                rule_content_type_lower = rule.content_type.lower().strip()
                
                # Check if the rule's content type appears in the result
                # This handles formats like "Commercial, 85%" or "Commercial, Advertisement, 75%"
                if rule_content_type_lower in content_type_lower:
                    # Create deactivation note
                    deactivation_note = f"🔴 Automatically deactivated due to content type match: '{rule.content_type}'. Content type classification: {content_type_result}"
                    
                    # Append to existing notes or create new note
                    if audio_segment.notes:
                        audio_segment.notes = f"{audio_segment.notes}\n{deactivation_note}"
                    else:
                        audio_segment.notes = deactivation_note
                    
                    # Deactivate the audio segment
                    audio_segment.is_active = False
                    audio_segment.save(update_fields=['is_active', 'notes'])
                    print(f"Deactivated audio segment {audio_segment.id} due to content type match: {rule.content_type}")
                    break  # Only need to match once
                    
        except Exception as e:
            print(f"Error checking content type deactivation rules: {e}")
            # Don't raise, just log the error

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
                required_fields = ['summary', 'sentiment', 'general_topics', 'iab_topics', 'bucket_prompt']
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
        api_key = ValidationUtils.validate_openai_api_key(transcription_detail.audio_segment.channel.id)
        settings = ValidationUtils.validate_settings_exist(transcription_detail.audio_segment.channel.id)
        client = OpenAI(api_key=api_key)
        transcript = transcription_detail.transcript

        def chat_params(prompt, transcript, max_tokens):
            return {
                "model": settings.chatgpt_model,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": transcript}
                ],
                "max_tokens": max_tokens if max_tokens > 0 else None,
                "temperature": settings.chatgpt_temperature,
                "top_p": settings.chatgpt_top_p,
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

        # Wellness bucket analysis
        bucket_prompt = TranscriptionAnalyzer.get_bucket_prompt(transcription_detail.audio_segment.channel.id)
        wellness_buckets = ""
        if bucket_prompt:
            wellness_buckets_resp = client.chat.completions.create(
                **{k: v for k, v in chat_params(bucket_prompt, transcript, 50).items() if v is not None}
            )
            wellness_buckets = wellness_buckets_resp.choices[0].message.content.strip()
        else:
            print("No wellness bucket prompt available, skipping bucket analysis")

        # Content type classification using GeneralSetting.determine_radio_content_type_prompt
        content_type_result = ""
        try:
            content_type_definitions = settings.content_type_prompt or ""
            determine_radio_content_type_prompt = settings.determine_radio_content_type_prompt or ""
            if determine_radio_content_type_prompt and determine_radio_content_type_prompt.strip():
                # Replace {{segments}} placeholder with the transcript
                content_type_instruction = determine_radio_content_type_prompt.replace("{{segments}}", content_type_definitions)

                content_type_resp = client.chat.completions.create(
                    **{k: v for k, v in chat_params(content_type_instruction, transcript, 30).items() if v is not None}
                )
                content_type_result = content_type_resp.choices[0].message.content.strip()
                print(content_type_result)
        except Exception as e:
            print(f"Error generating content type classification: {e}")

        # Store in TranscriptionAnalysis
        try:
            analysis = TranscriptionAnalysis.objects.create(
                transcription_detail=transcription_detail,
                summary=summary,
                sentiment=sentiment,
                general_topics=general_topics,
                iab_topics=iab_topics,
                bucket_prompt=wellness_buckets,
                content_type_prompt=content_type_result
            )
            print(f"Created new transcription analysis for transcription_detail {transcription_detail.id}")
            
            # Check if content type matches deactivation rules
            TranscriptionAnalyzer.check_and_deactivate_by_content_type(analysis, content_type_result)
            
            return analysis
        except Exception as e:
            print(f"Error creating transcription analysis: {e}")
            # Don't raise the error, just log it and continue

