from celery import shared_task
import time
import logging

from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.services.audio_segments import AudioDownloader
from data_analysis.services.audio_download import ACRCloudAudioDownloader
from data_analysis.models import RevTranscriptionJob

@shared_task
def bulk_download_audio_task(project_id, channel_id, unrecognized):
    return AudioDownloader.bulk_download_audio(project_id, channel_id, unrecognized)

# Latest update download audio task
@shared_task
def download_audio_task(project_id, channel_id, start_time, duration_seconds, filename=None, filepath=None):
    """
    Celery task to download audio for unrecognized segments using ACRCloudAudioDownloader
    """
    try:
        media_url = ACRCloudAudioDownloader.download_audio(
            project_id, 
            channel_id, 
            start_time, 
            duration_seconds, 
            filename, 
            filepath
        )
        print(f"Successfully downloaded audio for segment: {start_time} - {duration_seconds}s")
        
        # Return detailed file information
        return {
            'media_url': media_url,
            'filename': filename,
            'filepath': filepath,
            'project_id': project_id,
            'channel_id': channel_id,
            'start_time': start_time,
            'duration_seconds': duration_seconds,
            'status': 'success'
        }
    except Exception as e:
        print(f"Error downloading audio for segment {start_time}: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'filename': filename,
            'project_id': project_id,
            'channel_id': channel_id,
            'start_time': start_time,
            'duration_seconds': duration_seconds
        }

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