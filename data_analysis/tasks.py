from celery import shared_task
import time
import logging
from datetime import datetime, timedelta

from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.services.audio_segments import AudioDownloader, AudioSegments
from data_analysis.services.audio_download import ACRCloudAudioDownloader
from data_analysis.models import RevTranscriptionJob, AudioSegments

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
        
        # Set is_analysis_completed = True on the AudioSegments object
        if transcription_detail.audio_segment:
            audio_segment = transcription_detail.audio_segment
            audio_segment.is_analysis_completed = True
            audio_segment.save()
            print(f"Set is_analysis_completed=True for AudioSegments ID: {audio_segment.id}")
        
        print(f"Successfully completed transcription analysis for job {job_id}")
        return True
    except Exception as e:
        print(f"Error in analyze_transcription_task for job {job_id}: {e}")
        # Don't raise the error, just log it and return False
        return False

@shared_task
def process_today_audio_data_excluding_last_hour():
    """
    Periodic task to process today's audio data excluding the last hour.
    This ensures we don't process incomplete data from the most recent hour.
    Runs daily at 1 AM.
    """
    try:
        # For now, we'll use a default project_id and channel_id
        # In a real implementation, you might want to iterate through all active projects/channels
        project_id = 1  # Default project ID
        channel_id = 1  # Default channel ID
        
        print(f"Starting daily audio data processing for project {project_id}, channel {channel_id}")
        
        # Use the new combined method to process today's data excluding last hour
        segments = AudioSegments.process_today_data_excluding_last_hour(project_id, channel_id)
        
        if segments:
            print(f"Successfully processed {len(segments)} audio segments")
            
            # Count recognized vs unrecognized segments
            recognized_count = sum(1 for seg in segments if seg.get('is_recognized', False))
            unrecognized_count = len(segments) - recognized_count
            
            print(f"Segments breakdown: {recognized_count} recognized, {unrecognized_count} unrecognized")
            
            return {
                'status': 'success',
                'total_segments': len(segments),
                'recognized_segments': recognized_count,
                'unrecognized_segments': unrecognized_count,
                'project_id': project_id,
                'channel_id': channel_id,
                'timestamp': datetime.now().isoformat()
            }
        else:
            print("No audio segments were processed")
            return {
                'status': 'success',
                'total_segments': 0,
                'recognized_segments': 0,
                'unrecognized_segments': 0,
                'project_id': project_id,
                'channel_id': channel_id,
                'timestamp': datetime.now().isoformat(),
                'message': 'No segments found to process'
            }
            
    except Exception as e:
        print(f"Error in process_today_audio_data_excluding_last_hour: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }