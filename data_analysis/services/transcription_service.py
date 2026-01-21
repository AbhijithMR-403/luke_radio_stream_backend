
from datetime import datetime, timedelta
import requests
from typing import Optional, Dict, Any
import os
from django.utils import timezone
from django.db.models import Q
from decouple import config
from core_admin.models import GeneralSetting, Channel
from openai import OpenAI
from django.core.exceptions import ValidationError
from config.validation import ValidationUtils

from data_analysis.models import RevTranscriptionJob, TranscriptionAnalysis, TranscriptionDetail, AudioSegments
from segmentor.models import TitleMappingRule


class RevAISpeechToText:
    @staticmethod
    def create_transcription_job(media_path: str, is_absolute_url: bool = False):
        """
        Create a transcription job on Rev.ai for the given media.
        
        Args:
            media_path: The media path or absolute URL.
            is_absolute_url: If True, use media_path as the absolute URL directly.
                           If False (default), construct URL from PUBLIC_BASE_URL + media_path.
        """
        # Validate parameters
        if not is_absolute_url:
            ValidationUtils.validate_file_path(media_path)
        
        base_url = config('PUBLIC_BASE_URL')
        notification_url = f"{base_url}/api/rev-callback"
        media_url = media_path if is_absolute_url else f"{base_url}{media_path}"
        
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
    def apply_title_mapping_or_skip(audio_segment: AudioSegments) -> bool:
        """
        If a TitleMappingRule exists for the segment's title_before and channel,
        update the segment's title to the mapped AudioUnrecognizedCategory name
        and return True to indicate transcription should be skipped.

        Returns:
            bool: True if mapping applied (skip transcription), False otherwise
        """
        try:
            before_title_value = (audio_segment.title_before or "").strip()
            if not before_title_value:
                return False

            rule = TitleMappingRule.objects.filter(
                is_active=True,
                before_title=before_title_value,
                category__channel=audio_segment.channel,
            ).first()
            if not rule:
                return False

            # Update title with mapped category name
            mapped_title = rule.category.name
            if audio_segment.title != mapped_title:
                audio_segment.title = mapped_title
                audio_segment.save(update_fields=["title"])

            # Respect rule.skip_transcription flag; default True in model
            return bool(rule.skip_transcription)
        except Exception as e:
            print(f"Error applying title mapping for segment {audio_segment.id}: {str(e)}")
            return False

    @staticmethod
    def create_and_save_transcription_job_v2(segments: list[dict]) -> list[RevTranscriptionJob]:
        """
        Create transcription jobs only for segments where requires_analysis is True.

        Expected input: a list of dicts (as produced by mark_requires_analysis),
        each containing at least: id, file_path, requires_analysis.
        """
        if not isinstance(segments, list):
            raise ValidationError("segments must be a list of dictionaries")

        created_jobs: list[RevTranscriptionJob] = []

        for seg in segments:
            if not isinstance(seg, dict):
                continue

            if not bool(seg.get("requires_analysis", False)):
                continue

            segment_id = seg.get("id") or seg.get("segment_id")
            file_path_from_payload = seg.get("file_path")

            audio_segment: AudioSegments | None = None
            if segment_id:
                try:
                    audio_segment = AudioSegments.objects.get(pk=segment_id)
                except AudioSegments.DoesNotExist:
                    audio_segment = None

            if audio_segment is None and file_path_from_payload:
                try:
                    audio_segment = AudioSegments.objects.filter(id=segment_id).first()
                except Exception:
                    audio_segment = None

            if audio_segment is None:
                continue

            # Check if TranscriptionDetail already exists for this audio segment (transcription already completed)
            if TranscriptionDetail.objects.filter(audio_segment=audio_segment).exists():
                print(f"Skipping segment {audio_segment.id} - TranscriptionDetail already exists")
                continue  # Skip this segment if TranscriptionDetail already exists
            
            # # Check if RevTranscriptionJob already exists (job already created, even if not completed)
            if RevTranscriptionJob.objects.filter(audio_segment=audio_segment).exists():
                print(f"Skipping segment {audio_segment.id} - RevTranscriptionJob already exists")
                continue  # Skip this segment if job already exists

            # Use audio_url directly for podcast channels, otherwise construct path
            if audio_segment.channel and audio_segment.channel.channel_type == 'podcast':
                media_url_path = audio_segment.audio_url
                is_absolute_url = True
            else:
                media_url_path = f"/api/{audio_segment.file_path}"
                is_absolute_url = False

            try:
                api_response = RevAISpeechToText.create_transcription_job(media_url_path, is_absolute_url=is_absolute_url)
            except requests.RequestException:
                continue

            job_id = api_response.get("id")
            job_name = api_response.get("name", "")
            status = api_response.get("status", "")
            created_on_str = api_response.get("created_on", "")

            created_on_dt = None
            if created_on_str:
                try:
                    created_on_dt = datetime.fromisoformat(created_on_str.replace("Z", "+00:00"))
                except ValueError:
                    created_on_dt = timezone.now()
            else:
                created_on_dt = timezone.now()

            try:
                job = RevTranscriptionJob(
                    job_id=job_id,
                    job_name=job_name,
                    media_url=media_url_path,
                    status=status,
                    created_on=created_on_dt,
                    audio_segment=audio_segment,
                    retry_count=0,
                )
                job.save()
                created_jobs.append(job)
            except Exception:
                continue

        return created_jobs

    @staticmethod
    def get_transcript_by_job_id(revid: RevTranscriptionJob, media_path: str, media_url: Optional[str] = None):
        """
        Fetches the transcript for a given Rev.ai job ID using the access token from GeneralSetting.
        Inserts a TranscriptionDetail linked to the AudioSegments (by file_path) and the RevTranscriptionJob.
        Returns the transcript as plain text.
        
        Args:
            revid: The RevTranscriptionJob instance.
            media_path: The file path of the media.
            media_url: Optional absolute URL path for the media (e.g., for podcast channels).
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

        # Find the AudioSegments object by file_path, or audio_url if media_url is provided
        try:
            if media_url:
                audio_segments = AudioSegments.objects.filter(Q(file_path=media_path) | Q(audio_url=media_url))
            else:
                audio_segments = AudioSegments.objects.filter(file_path=media_path)
            if not audio_segments.exists():
                search_paths = f"file_path={media_path}" + (f", audio_url={media_url}" if media_url else "")
                raise ValueError(f"AudioSegments not found with: {search_paths}")
            
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

