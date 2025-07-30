from celery import shared_task
import time
import logging

from data_analysis.utils import AudioDownloader, RevAISpeechToText, TranscriptionAnalyzer
logger = logging.getLogger(__name__)
from data_analysis.models import RevTranscriptionJob

@shared_task
def bulk_download_audio_task(project_id, channel_id, unrecognized):
    return AudioDownloader.bulk_download_audio(project_id, channel_id, unrecognized)

@shared_task
def analyze_transcription_task(job_id, media_url_path):
    job = RevTranscriptionJob.objects.get(pk=job_id)
    transcription_detail = RevAISpeechToText.get_transcript_by_job_id(job, media_url_path)
    TranscriptionAnalyzer.analyze_transcription(transcription_detail)
    return True