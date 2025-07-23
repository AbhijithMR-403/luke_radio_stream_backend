from celery import shared_task
import time
import logging

from acr_admin.utils import AudioDownloader, RevAISpeechToText, TranscriptionAnalyzer
logger = logging.getLogger(__name__)

@shared_task
def bulk_download_audio_task(project_id, channel_id, unrecognized):
    return AudioDownloader.bulk_download_audio(project_id, channel_id, unrecognized)

@shared_task
def analyze_transcription_task(job_id, media_url_path):
    from .models import RevTranscriptionJob
    job = RevTranscriptionJob.objects.get(pk=job_id)
    transcription_detail = RevAISpeechToText.get_transcript_by_job_id(job, media_url_path)
    TranscriptionAnalyzer.analyze_transcription(transcription_detail)
    return True