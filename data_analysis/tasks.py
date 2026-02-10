from celery import shared_task, group
import logging
from datetime import datetime, timedelta
import logging

from data_analysis.services.analysis_prereq_check import mark_requires_analysis
from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.services.audio_segments import AudioSegments
from data_analysis.services.audio_download import ACRCloudAudioDownloader
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel 
from core_admin.models import Channel


logger = logging.getLogger(__name__)


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
def analyze_transcription_task(job_id, media_url_path, media_url=None):
    try:
        job = RevTranscriptionJob.objects.get(pk=job_id)
        transcription_detail = RevAISpeechToText.get_transcript_by_job_id(job, media_url_path, media_url)
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

def _handle_channel_processing(channel, segments_data):
    """
    Core logic to process audio segments for a channel.
    Extracted to be shared by both Today and Previous Day tasks.
    """
    if not segments_data or not segments_data.get('data'):
        logger.info(f"No data found for channel {channel.id}")
        return 0

    # Step 2: Process the audio data
    processed_segments = AudioSegments.process_audio_data(segments_data['data'], channel)
    if not processed_segments:
        logger.info(f"No segments processed for channel {channel.id}")
        return 0

    # Step 3: Insert and Merge
    inserted_segments = AudioSegmentsModel.insert_audio_segments(processed_segments, channel.id)
    inserted_segments = AudioSegments._merge_short_recognized_segments(inserted_segments, channel)

    if not inserted_segments:
        return 0

    # Step 4: Download audio (External Network Call)
    # We do NOT use atomic transactions here because this takes time.
    download_results = ACRCloudAudioDownloader.download_audio_segments_batch(inserted_segments)

    # Step 5: Map segments for marking logic
    segments_for_marking = [
        {
            "id": seg.id,
            "file_path": seg.file_path,
            "file_name": seg.file_name,
            "duration_seconds": seg.duration_seconds,
            "is_recognized": seg.is_recognized,
            "start_time": seg.start_time,
            "end_time": seg.end_time,
            "channel_id": channel.id,
            "title": seg.title,
            "title_before": seg.title_before,
            "title_after": seg.title_after,
        }
        for seg in inserted_segments
    ]
    marked_segments = mark_requires_analysis(segments_for_marking)

    # Step 6: Create transcription jobs (External Network Call)
    transcription_jobs = RevAISpeechToText.create_and_save_transcription_job_v2(marked_segments)
    
    return len(inserted_segments)


# --- Celery Sub-Task (The Worker) ---

@shared_task(bind=True)
def process_channel_task(self, channel_id, date_str=None, is_today=False):
    """
    Processes a single channel. This allows Celery to run 
    multiple channels in parallel across different workers.
    """
    try:
        channel = Channel.objects.get(pk=channel_id)
        
        if is_today:
            segments_data = AudioSegments.get_today_data_excluding_last_hour(
                channel.project_id, channel.channel_id
            )
        else:
            segments_data = AudioSegments.fetch_data(
                channel.project_id, channel.channel_id, date=date_str
            )

        count = _handle_channel_processing(channel, segments_data)
        
        return {
            'channel_id': channel_id,
            'status': 'success',
            'segments_count': count
        }
    except Exception as e:
        logger.error(f"Error in process_channel_task for channel {channel_id}: {str(e)}", exc_info=True)
        return {'channel_id': channel_id, 'status': 'error', 'message': str(e)}


# --- Orchestrator Tasks (The Schedulers) ---

@shared_task
def process_today_audio_data():
    """ Runs daily 1 hrs interval. Spawns parallel tasks for each channel. """
    channels = Channel.objects.filter(is_deleted=False, is_active=True, channel_type='broadcast')
    print(channels)
    # Use a group to run channel tasks in parallel
    for channel in channels:
        process_channel_task.delay(channel.id, is_today=True)


@shared_task
def process_previous_day_audio_data():
    """ Runs daily at 2 AM. Spawns parallel tasks for each channel. """
    previous_day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    channels = Channel.objects.filter(is_deleted=False, is_active=True, channel_type='broadcast')
    for channel in channels:
        process_channel_task.delay(channel.id, date_str=previous_day, is_today=False)

