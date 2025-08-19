from celery import shared_task
import time
import logging
from datetime import datetime, timedelta

from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from data_analysis.services.transcription_service import RevAISpeechToText
from data_analysis.services.audio_segments import AudioSegments
from data_analysis.services.audio_download import ACRCloudAudioDownloader
from data_analysis.models import RevTranscriptionJob, AudioSegments as AudioSegmentsModel 
from acr_admin.models import Channel

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
def process_today_audio_data():
    """
    Periodic task to process today's audio data excluding the last hour for all channels.
    This ensures we don't process incomplete data from the most recent hour.
    Runs daily at 1 AM.
    """
    try:
        # Fetch all channels
        channels = Channel.objects.filter(is_deleted=False)
        
        if not channels:
            print("No active channels found")
            return {
                'status': 'success',
                'total_channels': 0,
                'total_segments': 0,
                'recognized_segments': 0,
                'unrecognized_segments': 0,
                'timestamp': datetime.now().isoformat(),
                'message': 'No active channels found'
            }
        
        print(f"Starting daily audio data processing for {len(channels)} channels")
        
        total_segments = 0
        total_recognized = 0
        total_unrecognized = 0
        channel_results = []
        
        for channel in channels:
            try:
                print(f"Processing channel {channel.id} (Project: {channel.project_id}, Channel: {channel.channel_id})")
                
                # Step 1: Get today's data excluding last hour for this channel
                segments_data = AudioSegments.get_today_data_excluding_last_hour(channel.project_id, channel.channel_id)
                
                if not segments_data or not segments_data.get('data'):
                    print(f"No data found for channel {channel.id}")
                    channel_results.append({
                        'channel_id': channel.id,
                        'project_id': channel.project_id,
                        'channel_id_acr': channel.channel_id,
                        'status': 'no_data',
                        'segments': 0
                    })
                    continue
                
                # Step 2: Process the audio data with channel object
                processed_segments = AudioSegments.process_audio_data(segments_data['data'], channel)
                
                if not processed_segments:
                    print(f"No processed segments for channel {channel.id}")
                    channel_results.append({
                        'channel_id': channel.id,
                        'project_id': channel.project_id,
                        'channel_id_acr': channel.channel_id,
                        'status': 'no_processed_segments',
                        'segments': 0
                    })
                    continue
                print(f"Processed segments: {processed_segments}")
                # Step 3: Insert audio segments into database
                inserted_segments = AudioSegmentsModel.insert_audio_segments(processed_segments, channel.id)
                
                if not inserted_segments:
                    print(f"No segments inserted for channel {channel.id}")
                    channel_results.append({
                        'channel_id': channel.id,
                        'project_id': channel.project_id,
                        'channel_id_acr': channel.channel_id,
                        'status': 'no_inserted_segments',
                        'segments': 0
                    })
                    continue
                
                # Step 4: Download audio for the inserted segments
                download_results = ACRCloudAudioDownloader.download_audio_segments_batch(inserted_segments)
                
                # Step 5: Create and save transcription jobs using download results
                transcription_jobs = RevAISpeechToText.create_and_save_transcription_job(download_results)
                
                # Count segments for this channel
                channel_segments = len(inserted_segments)
                channel_recognized = sum(1 for seg in inserted_segments if seg.is_recognized)
                channel_unrecognized = channel_segments - channel_recognized
                
                total_segments += channel_segments
                total_recognized += channel_recognized
                total_unrecognized += channel_unrecognized
                
                print(f"Channel {channel.id}: {channel_segments} segments ({channel_recognized} recognized, {channel_unrecognized} unrecognized)")
                
                channel_results.append({
                    'channel_id': channel.id,
                    'project_id': channel.project_id,
                    'channel_id_acr': channel.channel_id,
                    'status': 'success',
                    'segments': channel_segments,
                    'recognized': channel_recognized,
                    'unrecognized': channel_unrecognized,
                    'transcription_jobs': len(transcription_jobs) if transcription_jobs else 0
                })
                
            except Exception as e:
                print(f"Error processing channel {channel.id}: {e}")
                channel_results.append({
                    'channel_id': channel.id,
                    'project_id': channel.project_id,
                    'channel_id_acr': channel.channel_id,
                    'status': 'error',
                    'error': str(e),
                    'segments': 0
                })
        
        print(f"Completed processing {len(channels)} channels. Total: {total_segments} segments ({total_recognized} recognized, {total_unrecognized} unrecognized)")
        
        return {
            'status': 'success',
            'total_channels': len(channels),
            'total_segments': total_segments,
            'recognized_segments': total_recognized,
            'unrecognized_segments': total_unrecognized,
            'channel_results': channel_results,
            'timestamp': datetime.now().isoformat()
        }
            
    except Exception as e:
        print(f"Error in process_today_audio_data: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

@shared_task
def process_previous_day_audio_data():
    """
    Periodic task to process previous day's audio data for all channels.
    This fetches complete data from the previous day.
    Runs daily at 2 AM.
    """
    try:
        # Fetch all channels
        channels = Channel.objects.filter(is_deleted=False)
        
        if not channels:
            print("No active channels found")
            return {
                'status': 'success',
                'total_channels': 0,
                'total_segments': 0,
                'recognized_segments': 0,
                'unrecognized_segments': 0,
                'timestamp': datetime.now().isoformat(),
                'message': 'No active channels found'
            }
        
        # Get previous day's date in YYYYMMDD format
        previous_day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        print(f"Starting previous day audio data processing for {len(channels)} channels for date: {previous_day}")
        
        total_segments = 0
        total_recognized = 0
        total_unrecognized = 0
        channel_results = []
        
        for channel in channels:
            try:
                print(f"Processing channel {channel.id} (Project: {channel.project_id}, Channel: {channel.channel_id}) for date: {previous_day}")
                
                # Step 1: Get previous day's data for this channel
                segments_data = AudioSegments.fetch_data(channel.project_id, channel.channel_id, date=previous_day)
                
                if not segments_data or not segments_data.get('data'):
                    print(f"No data found for channel {channel.id} on date {previous_day}")
                    channel_results.append({
                        'channel_id': channel.id,
                        'project_id': channel.project_id,
                        'channel_id_acr': channel.channel_id,
                        'date': previous_day,
                        'status': 'no_data',
                        'segments': 0
                    })
                    continue
                
                # Step 2: Process the audio data with channel object
                processed_segments = AudioSegments.process_audio_data(segments_data['data'], channel)
                
                if not processed_segments:
                    print(f"No processed segments for channel {channel.id}")
                    channel_results.append({
                        'channel_id': channel.id,
                        'project_id': channel.project_id,
                        'channel_id_acr': channel.channel_id,
                        'date': previous_day,
                        'status': 'no_processed_segments',
                        'segments': 0
                    })
                    continue
                print(f"Processed segments: {processed_segments}")
                # Step 3: Insert audio segments into database
                inserted_segments = AudioSegmentsModel.insert_audio_segments(processed_segments, channel.id)
                
                if not inserted_segments:
                    print(f"No segments inserted for channel {channel.id}")
                    channel_results.append({
                        'channel_id': channel.id,
                        'project_id': channel.project_id,
                        'channel_id_acr': channel.channel_id,
                        'date': previous_day,
                        'status': 'no_inserted_segments',
                        'segments': 0
                    })
                    continue
                
                # Step 4: Download audio for the inserted segments
                download_results = ACRCloudAudioDownloader.download_audio_segments_batch(inserted_segments)
                
                # Step 5: Create and save transcription jobs using download results
                transcription_jobs = RevAISpeechToText.create_and_save_transcription_job(download_results)
                
                # Count segments for this channel
                channel_segments = len(inserted_segments)
                channel_recognized = sum(1 for seg in inserted_segments if seg.is_recognized)
                channel_unrecognized = channel_segments - channel_recognized
                
                total_segments += channel_segments
                total_recognized += channel_recognized
                total_unrecognized += channel_unrecognized
                
                print(f"Channel {channel.id}: {channel_segments} segments ({channel_recognized} recognized, {channel_unrecognized} unrecognized)")
                
                channel_results.append({
                    'channel_id': channel.id,
                    'project_id': channel.project_id,
                    'channel_id_acr': channel.channel_id,
                    'date': previous_day,
                    'status': 'success',
                    'segments': channel_segments,
                    'recognized': channel_recognized,
                    'unrecognized': channel_unrecognized,
                    'transcription_jobs': len(transcription_jobs) if transcription_jobs else 0
                })
                
            except Exception as e:
                print(f"Error processing channel {channel.id}: {e}")
                channel_results.append({
                    'channel_id': channel.id,
                    'project_id': channel.project_id,
                    'channel_id_acr': channel.channel_id,
                    'date': previous_day,
                    'status': 'error',
                    'error': str(e),
                    'segments': 0
                })
        
        print(f"Completed processing {len(channels)} channels for date {previous_day}. Total: {total_segments} segments ({total_recognized} recognized, {total_unrecognized} unrecognized)")
        
        return {
            'status': 'success',
            'date': previous_day,
            'total_channels': len(channels),
            'total_segments': total_segments,
            'recognized_segments': total_recognized,
            'unrecognized_segments': total_unrecognized,
            'channel_results': channel_results,
            'timestamp': datetime.now().isoformat()
        }
            
    except Exception as e:
        print(f"Error in process_previous_day_audio_data: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }