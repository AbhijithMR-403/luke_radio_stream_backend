
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


class RevAISpeechToText:
    @staticmethod
    def create_transcription_job(media_path: str):
        """
        Creates a transcription job on Rev.ai using the provided media URL and notification URL.
        Uses revai_access_token from GeneralSetting if api_key is not provided.
        Returns the API response as JSON.
        """
        # Validate parameters
        ValidationUtils.validate_file_path(media_path)
        
        base_url = config('PUBLIC_BASE_URL')
        notification_url = f"{base_url}/api/rev-callback"
        media_url = f"{base_url}{media_path}"
        
        api_key = ValidationUtils.validate_revai_api_key()
        url = "https://api.rev.ai/speechtotext/v1/jobs"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        data = {
            "media_url": media_url,
            "notification_config": {
                "url": notification_url
            },
            "options": {
                "timestamps": False
            }
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def get_transcript_by_job_id(revid: RevTranscriptionJob, media_path: str):
        """
        Fetches the transcript for a given Rev.ai job ID using the access token from GeneralSetting.
        Inserts a TranscriptionDetail linked to the UnrecognizedAudio (by media_path) and the RevTranscriptionJob.
        Returns the transcript as plain text.
        """
        # Validate parameters
        if not isinstance(revid, RevTranscriptionJob):
            raise ValidationError("revid must be a RevTranscriptionJob instance")
        ValidationUtils.validate_file_path(media_path)
        
        api_key = ValidationUtils.validate_revai_api_key()
        url = f"https://api.rev.ai/speechtotext/v1/jobs/{revid.job_id}/transcript"
        headers = {
            "Authorization": f"Bearer {api_key}",
            # "Accept": "application/json",
            "Accept":"text/plain"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        transcript = response.text

        # Find the UnrecognizedAudio object by media_path
        try:
            unrec_audio = UnrecognizedAudio.objects.get(media_path=media_path)
        except UnrecognizedAudio.DoesNotExist:
            raise ValueError(f"UnrecognizedAudio with media_path {media_path} not found")

        # Check for existing TranscriptionDetail by unrecognized_audio and rev_job
        from django.core.exceptions import ObjectDoesNotExist
        existing_by_audio = None
        existing_by_job = None
        try:
            existing_by_audio = TranscriptionDetail.objects.get(unrecognized_audio=unrec_audio)
        except ObjectDoesNotExist:
            pass
        try:
            existing_by_job = TranscriptionDetail.objects.get(rev_job=revid)
        except ObjectDoesNotExist:
            pass

        if existing_by_audio and existing_by_job:
            if existing_by_audio == existing_by_job:
                # Both point to the same TranscriptionDetail (already present)
                print("\033[93mAlready present\033[0m")  # Yellow
                return existing_by_audio
            else:
                # Conflict: both exist but are different objects
                print("\033[91mConflict: UnrecognizedAudio and RevTranscriptionJob do not match for existing TranscriptionDetail\033[0m")  # Red
                return existing_by_audio or existing_by_job
        elif existing_by_audio or existing_by_job:
            # Conflict: one exists but not both together
            print("\033[91mConflict: UnrecognizedAudio or RevTranscriptionJob already linked to a different TranscriptionDetail\033[0m")  # Red
            return existing_by_audio or existing_by_job

        # Create TranscriptionDetail if neither exists
        transcription_detail = TranscriptionDetail.objects.create(
            unrecognized_audio=unrec_audio,
            rev_job=revid,
            transcript=transcript
        )
        return transcription_detail

