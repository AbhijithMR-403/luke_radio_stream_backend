
from datetime import datetime, timedelta
import requests
from typing import Optional, Dict, Any
import os
from django.utils import timezone
from decouple import config
from acr_admin.models import GeneralSetting, Channel
from openai import OpenAI
from django.core.exceptions import ValidationError
from config.validation import ValidationUtils

from data_analysis.models import RevTranscriptionJob, TranscriptionAnalysis, TranscriptionDetail, AudioSegments


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
    def create_and_save_transcription_job(segments_data: dict[str, list]) -> list[RevTranscriptionJob]:
        """
        Creates transcription jobs on Rev.ai for multiple audio segments and saves them to the database.
        
        Args:
            segments_data: Dictionary with 'success' and 'skipped' keys containing list of segment dictionaries
                          Each segment dict should have: segment_id, file_name
                          Example:
                          {
                              'success': [
                                  {
                                      'segment_id': 2958,
                                      'file_name': 'audio_3137_240401_20250812000152_9.mp3'
                                  }
                              ],
                              'skipped': [
                                  {
                                      'segment_id': 2959,
                                      'file_name': 'audio_3137_240401_20250812000201_1.mp3'
                                  }
                              ]
                          }
        
        Returns:
            list[RevTranscriptionJob]: List of created transcription job instances
            
        Raises:
            ValidationError: If validation fails
            requests.RequestException: If the API call fails
        """
        if not isinstance(segments_data, dict):
            raise ValidationError("segments_data must be a dictionary")
        
        # Check for both 'success' and 'skipped' keys
        success_segments = segments_data.get('success', [])
        skipped_segments = segments_data.get('skipped', [])
        
        if not isinstance(success_segments, list):
            raise ValidationError("'success' value must be a list")
        if not isinstance(skipped_segments, list):
            raise ValidationError("'skipped' value must be a list")
        
        # Combine both lists for processing
        all_segments = success_segments + skipped_segments
        
        if not all_segments:
            return []  # Return empty list if no segments to process
        
        created_jobs = []
        
        for segment_data in all_segments:
            if not isinstance(segment_data, dict):
                print(f"Skipping invalid segment data: {segment_data}")
                continue  # Skip invalid segment data
            
            # Extract required fields
            segment_id = segment_data.get('segment_id')
            
            if not segment_id:
                print(f"Skipping segment without segment_id: {segment_data}")
                continue  # Skip segments without segment_id
            
            # Get the AudioSegments instance
            try:
                audio_segment = AudioSegments.objects.get(pk=segment_id)
            except AudioSegments.DoesNotExist:
                print(f"Skipping segment - AudioSegments with id {segment_id} not found")
                continue  # Skip if AudioSegments not found
            
            # Always get file_path and media_url from audio_segment
            file_path = audio_segment.file_path
            media_url = f"/api/{audio_segment.file_path}"
            
            # Check if TranscriptionDetail already exists for this audio segment
            if TranscriptionDetail.objects.filter(audio_segment=audio_segment).exists():
                print(f"Skipping segment {segment_id} - TranscriptionDetail already exists")
                continue  # Skip this segment if TranscriptionDetail already exists
            
            # Validate that the audio segment is not recognized (should be unrecognized)
            if audio_segment.is_recognized:
                print(f"Skipping segment {segment_id} - Audio is already recognized")
                continue  # Skip recognized audio segments
            
            # Validate that the audio segment is active
            if not audio_segment.is_active:
                print(f"Skipping segment {segment_id} - Audio segment is not active")
                continue  # Skip inactive audio segments
            
            # Validate that the audio is downloaded
            if not audio_segment.is_audio_downloaded:
                print(f"Skipping segment {segment_id} - Audio is not downloaded")
                continue  # Skip undownloaded audio segments
            
            # Create the transcription job via Rev.ai API
            try:
                print(f"Creating transcription job for segment {segment_id} with file: {file_path}")
                api_response = RevAISpeechToText.create_transcription_job(media_url)
            except requests.RequestException as e:
                print(f"Failed to create transcription job for segment {segment_id}: {str(e)}")
                continue  # Skip if API call fails
            print(api_response)
            # Extract job details from API response
            job_id = api_response.get('id')
            job_name = api_response.get('name', '')
            status = api_response.get('status', '')
            created_on_str = api_response.get('created_on', '')
            
            # Parse created_on datetime
            created_on = None
            if created_on_str:
                try:
                    created_on = datetime.fromisoformat(created_on_str.replace('Z', '+00:00'))
                except ValueError:
                    created_on = timezone.now()
            else:
                created_on = timezone.now()
            
            # Create and save the RevTranscriptionJob instance
            try:
                transcription_job = RevTranscriptionJob(
                    job_id=job_id,
                    job_name=job_name,
                    media_url=media_url,
                    status=status,
                    created_on=created_on,
                    audio_segment=audio_segment,
                    retry_count=0
                )
                
                # Validate and save the model
                # transcription_job.full_clean()
                transcription_job.save()
                
                print(f"Successfully created transcription job {job_id} for segment {segment_id}")
                created_jobs.append(transcription_job)
            except Exception as e:
                print(f"Failed to save transcription job for segment {segment_id}: {str(e)}")
                continue  # Skip if saving fails
        
        return created_jobs

    @staticmethod
    def get_transcript_by_job_id(revid: RevTranscriptionJob, media_path: str):
        """
        Fetches the transcript for a given Rev.ai job ID using the access token from GeneralSetting.
        Inserts a TranscriptionDetail linked to the AudioSegments (by file_path) and the RevTranscriptionJob.
        Returns the transcript as plain text.
        """
        # Validate parameters
        if not isinstance(revid, RevTranscriptionJob):
            raise ValidationError("revid must be a RevTranscriptionJob instance")
        
        # Remove '/api' prefix from media_path if present
        if media_path.startswith('/api/'):
            media_path = media_path[5:]  # Remove '/api' prefix
        elif media_path.startswith('api/'):
            media_path = media_path[4:]  # Remove 'api' prefix
                
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

        # Find the AudioSegments object by file_path
        try:
            audio_segments = AudioSegments.objects.filter(file_path=media_path)
            if not audio_segments.exists():
                raise ValueError(f"AudioSegments with file_path {media_path} not found")
            
            # Log warning if multiple segments found with same file_path
            if audio_segments.count() > 1:
                print(f"Warning: Found {audio_segments.count()} AudioSegments with file_path {media_path}. Using the first one (ID: {audio_segments.first().id})")
            
            audio_segment = audio_segments.first()
        except Exception as e:
            raise ValueError(f"Error finding AudioSegments with file_path {media_path}: {str(e)}")

        # Check for existing TranscriptionDetail by audio_segment and rev_job
        from django.core.exceptions import ObjectDoesNotExist
        existing_by_segment = None
        existing_by_job = None
        try:
            existing_by_segment = TranscriptionDetail.objects.get(audio_segment=audio_segment)
        except ObjectDoesNotExist:
            pass
        try:
            existing_by_job = TranscriptionDetail.objects.get(rev_job=revid)
        except ObjectDoesNotExist:
            pass

        if existing_by_segment and existing_by_job:
            if existing_by_segment == existing_by_job:
                # Both point to the same TranscriptionDetail (already present)
                print("\033[93mAlready present\033[0m")  # Yellow
                return existing_by_segment
            else:
                # Conflict: both exist but are different objects
                print("\033[91mConflict: AudioSegments and RevTranscriptionJob do not match for existing TranscriptionDetail\033[0m")  # Red
                return existing_by_segment or existing_by_job
        elif existing_by_segment or existing_by_job:
            # Conflict: one exists but not both together
            print("\033[91mConflict: AudioSegments or RevTranscriptionJob already linked to a different TranscriptionDetail\033[0m")  # Red
            
            # Set is_analysis_completed = True on the AudioSegments object
            if audio_segment:
                audio_segment.is_analysis_completed = True
                audio_segment.save()
                print(f"\033[92mSet is_analysis_completed=True for AudioSegments ID: {audio_segment.id}\033[0m")  # Green
            
            return existing_by_segment or existing_by_job

        # Create TranscriptionDetail if neither exists
        transcription_detail = TranscriptionDetail.objects.create(
            audio_segment=audio_segment,
            rev_job=revid,
            transcript=transcript
        )
        return transcription_detail

