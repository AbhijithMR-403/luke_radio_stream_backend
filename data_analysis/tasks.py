from celery import shared_task
from datetime import datetime, timedelta
import logging

from data_analysis.services.analysis_prereq_check import mark_requires_analysis
from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.services.audio_segments import AudioSegments
from data_analysis.services.audio_download import ACRCloudAudioDownloader
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel 
from core_admin.models import Channel
from core_admin.repositories import GeneralSettingService


logger = logging.getLogger(__name__)


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

def _handle_channel_processing(channel: Channel | int, segments_data):
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

    download_results = ACRCloudAudioDownloader.download_audio_segments_batch(inserted_segments, channel)

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


# --- Channel settings validation (for broadcast pipeline) ---

# GeneralSetting fields required for broadcast audio pipeline; if any is missing/empty, channel is deactivated
BROADCAST_SETTINGS_REQUIRED_FIELDS = [
    'revai_access_token',
    'acr_cloud_api_key',
    'openai_api_key',
    'summarize_transcript_prompt',
    'sentiment_analysis_prompt',
    'general_topics_prompt',
    'iab_topics_prompt',
    'determine_radio_content_type_prompt',
    'content_type_prompt',
]


def deactivate_channels_without_valid_settings():
    """
    For broadcast channels that are active and not deleted, ensure each has an
    active GeneralSetting with all required fields. If a channel has no settings
    or any important field is missing/empty, set the channel to inactive.
    Returns the list of Channel instances that are valid for processing.
    """
    channels = Channel.objects.filter(
        is_deleted=False, is_active=True, channel_type='broadcast'
    )
    valid_channels = []
    for channel in channels:
        settings = GeneralSettingService.get_active_setting(
            channel=channel, include_buckets=False
        )
        if not settings:
            logger.warning(
                "Channel id=%s has no active GeneralSetting; deactivating.", channel.id
            )
            channel.is_active = False
            channel.save()
            continue
        missing = []
        for field in BROADCAST_SETTINGS_REQUIRED_FIELDS:
            val = getattr(settings, field, None)
            if val is None or (isinstance(val, str) and not val.strip()):
                missing.append(field)
        if missing:
            logger.warning(
                "Channel id=%s missing required settings fields: %s; deactivating.",
                channel.id,
                missing,
            )
            channel.is_active = False
            channel.save()
            continue
        valid_channels.append(channel)
    return valid_channels


# --- Orchestrator Tasks (The Schedulers) ---

@shared_task
def process_today_audio_data():
    """ Runs daily 1 hrs interval. Spawns parallel tasks for each channel. """
    channels = deactivate_channels_without_valid_settings()
    for channel in channels:
        process_channel_task.delay(channel.id, is_today=True)


@shared_task
def process_previous_day_audio_data():
    """ Runs daily at 2 AM. Spawns parallel tasks for each channel. """
    previous_day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    channels = deactivate_channels_without_valid_settings()
    for channel in channels:
        process_channel_task.delay(channel.id, date_str=previous_day, is_today=False)

