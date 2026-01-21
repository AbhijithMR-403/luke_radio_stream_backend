from celery import shared_task
import logging

from core_admin.models import Channel
from rss_ingestion.service import RSSIngestionService
from data_analysis.services.analysis_prereq_check import mark_requires_analysis
from data_analysis.services.transcription_service import RevAISpeechToText

logger = logging.getLogger(__name__)


@shared_task
def ingest_podcast_rss_feed_task(channel_id: int):
    """
    Celery task to fetch and ingest RSS feed entries for a podcast channel.
    
    Args:
        channel_id: The ID of the Channel to process.
    
    Returns:
        Dictionary with ingestion results.
    """
    try:
        # Fetch the channel
        channel = Channel.objects.get(pk=channel_id, is_deleted=False)
        
        # Validate channel type
        if channel.channel_type != 'podcast':
            logger.warning(f"Channel {channel_id} is not a podcast channel, skipping RSS ingestion")
            return {
                'status': 'skipped',
                'channel_id': channel_id,
                'reason': 'Channel is not a podcast type'
            }
        
        # Validate RSS URL exists
        if not channel.rss_url:
            logger.error(f"Channel {channel_id} has no RSS URL configured")
            return {
                'status': 'error',
                'channel_id': channel_id,
                'error': 'No RSS URL configured for this channel'
            }
        
        logger.info(f"Starting RSS ingestion for channel {channel_id} ({channel.name})")
        
        # Fetch and process RSS feed
        service = RSSIngestionService(channel.rss_url)
        service.fetch()
        
        if not service.has_entries():
            logger.warning(f"No entries found in RSS feed for channel {channel_id}")
            return {
                'status': 'success',
                'channel_id': channel_id,
                'message': 'No entries found in RSS feed',
                'created_count': 0,
                'skipped_count': 0,
                'error_count': 0
            }
        
        # Insert entries into AudioSegments
        results = service.insert_to_audio_segments(channel)
        
        # Create transcription jobs for newly created segments
        transcription_jobs_count = 0
        created_segments = results.get('created_segments', [])
        
        if created_segments:
            # Convert model instances to dicts expected by mark_requires_analysis
            segments_for_marking = [
                {
                    "id": seg.id,
                    "requires_analysis": True
                }
                for seg in created_segments
            ]
            
            # # Create and save transcription jobs only for segments requiring analysis
            transcription_jobs = RevAISpeechToText.create_and_save_transcription_job_v2(segments_for_marking)
            transcription_jobs_count = len(transcription_jobs)
            
            logger.info(f"Created {transcription_jobs_count} transcription jobs for channel {channel_id}")
        
        logger.info(
            f"RSS ingestion completed for channel {channel_id}: "
            f"{results['created_count']} created, "
            f"{results['skipped_count']} skipped, "
            f"{results['error_count']} errors"
        )
        
        return {
            'status': 'success',
            'channel_id': channel_id,
            'channel_name': channel.name,
            'created_count': results['created_count'],
            'skipped_count': results['skipped_count'],
            'error_count': results['error_count'],
            'transcription_jobs_created': transcription_jobs_count,
            'skipped_entries': results['skipped_entries'],
            'errors': results['errors']
        }
        
    except Channel.DoesNotExist:
        logger.error(f"Channel {channel_id} not found")
        return {
            'status': 'error',
            'channel_id': channel_id,
            'error': 'Channel not found'
        }
    except Exception as e:
        logger.exception(f"Error ingesting RSS feed for channel {channel_id}: {e}")
        return {
            'status': 'error',
            'channel_id': channel_id,
            'error': str(e)
        }
