from celery import shared_task
import time
import logging

from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.services.unrecognized_audio import AudioDownloader
from data_analysis.models import RevTranscriptionJob

@shared_task
def bulk_download_audio_task(project_id, channel_id, unrecognized):
    return AudioDownloader.bulk_download_audio(project_id, channel_id, unrecognized)

@shared_task
def analyze_transcription_task(job_id, media_url_path):
    try:
        job = RevTranscriptionJob.objects.get(pk=job_id)
        transcription_detail = RevAISpeechToText.get_transcript_by_job_id(job, media_url_path)
        TranscriptionAnalyzer.analyze_transcription(transcription_detail)
        print(f"Successfully completed transcription analysis for job {job_id}")
        return True
    except Exception as e:
        print(f"Error in analyze_transcription_task for job {job_id}: {e}")
        # Don't raise the error, just log it and return False
        return False