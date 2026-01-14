import os
from datetime import datetime
from django.utils import timezone
from decouple import config

from core_admin.models import Channel
from data_analysis.models import AudioSegments as AudioSegmentsModel, RevTranscriptionJob, TranscriptionQueue
from data_analysis.services.transcription_service import RevAISpeechToText


def create_segment_download_and_queue(channel: Channel, start_dt: datetime, end_dt: datetime, *,
    user=None,
    title: str | None = None,
    title_before: str | None = None,
    title_after: str | None = None,
    transcribe: bool = True):
	if not isinstance(channel, Channel):
		raise ValueError("channel must be a valid Channel instance")
	if end_dt <= start_dt:
		raise ValueError("end_dt must be after start_dt")

	duration_seconds = int((end_dt - start_dt).total_seconds())

	# Build file_name and file_path using same format as audio_segments service
	start_time_str = start_dt.strftime("%Y%m%d%H%M%S")
	file_name = f"audio_{channel.project_id}_{channel.channel_id}_{start_time_str}_{duration_seconds}.mp3"
	# Create folder structure: start_date/file_name
	start_date = start_dt.strftime("%Y%m%d")
	file_path = f"media/{start_date}/{file_name}"
	
	# Ensure the directory exists
	media_dir = os.path.join(os.getcwd(), "media", start_date)
	os.makedirs(media_dir, exist_ok=True)

	# Determine recognized vs unrecognized and titles
	is_recognized = bool(title)
	title_before_value = title_before if title_before else (None if is_recognized else 'UNKNOWN')
	title_after_value = title_after if title_after else (None if is_recognized else 'UNKNOWN')

	segment_payload = {
		'start_time': start_dt,
		'end_time': end_dt,
		'duration_seconds': duration_seconds,
		'is_recognized': is_recognized,
		'is_active': True,
		'file_name': file_name,
		'file_path': file_path,
		'channel': channel,
		'source': 'user',
		'created_by': user
	}
	if is_recognized:
		segment_payload['title'] = title
	else:
		segment_payload['title_before'] = title_before_value
		segment_payload['title_after'] = title_after_value

	# Check if segment with same file_path already exists
	existing_segment = AudioSegmentsModel.objects.filter(file_path=file_path).first()
	if existing_segment:
		# Update existing segment
		existing_segment.start_time = start_dt
		existing_segment.end_time = end_dt
		existing_segment.duration_seconds = duration_seconds
		existing_segment.is_recognized = is_recognized
		existing_segment.is_active = True
		existing_segment.source = 'user'
		existing_segment.created_by = user
		if is_recognized:
			existing_segment.title = title
			existing_segment.title_before = None
			existing_segment.title_after = None
		else:
			existing_segment.title = None
			existing_segment.title_before = title_before_value
			existing_segment.title_after = title_after_value
		existing_segment.save()
		created_segment = existing_segment
	else:
		# Create new segment
		created_segment = AudioSegmentsModel.insert_single_audio_segment(segment_payload)

	# Download audio now (blocking)
	from data_analysis.services.audio_download import ACRCloudAudioDownloader
	media_url = ACRCloudAudioDownloader.download_audio(
		project_id=channel.project_id,
		channel_id=channel.channel_id,
		start_time=start_dt,
		duration_seconds=duration_seconds,
		filepath=file_path
	)
	created_segment.is_audio_downloaded = True
	created_segment.save()

	queue_entry = None
	rev_job = None
	if transcribe:
		# Check if transcription was queued recently (120 seconds cooldown)
		existing_queue = TranscriptionQueue.objects.filter(audio_segment=created_segment).first()
		if existing_queue:
			time_since_queued = timezone.now() - existing_queue.queued_at
			if time_since_queued.total_seconds() < 120:
				raise ValueError(f"Transcription was queued recently. Please wait {120 - int(time_since_queued.total_seconds())} more seconds before trying again.")
		
		created_segment.is_manually_processed = True

		# Queue transcription
		queue_entry = TranscriptionQueue.objects.filter(audio_segment=created_segment).first()
		if queue_entry:
			queue_entry.queued_at = timezone.now()
			queue_entry.is_transcribed = False
			queue_entry.is_analyzed = False
			queue_entry.completed_at = None
			queue_entry.save()
		else:
			queue_entry = TranscriptionQueue.objects.create(
				audio_segment=created_segment,
				is_transcribed=False,
				is_analyzed=False
			)

		# Start transcription job
		media_path = "/api/" + created_segment.file_path
		transcription_job = RevAISpeechToText.create_transcription_job(media_path)
		job_id = transcription_job.get('id')
		if not job_id:
			raise RuntimeError("Failed to create transcription job")

		rev_job = RevTranscriptionJob.objects.create(
			job_id=job_id,
			job_name=f"Transcription for segment {created_segment.id}",
			media_url=f"{config('PUBLIC_BASE_URL')}{created_segment.file_path}",
			status='in_progress',
			job_type='async',
			language='en',
			created_on=timezone.now(),
			audio_segment=created_segment
		)
	created_segment.save()

	return {
		'segment': created_segment,
		'queue': queue_entry,
		'rev_job': rev_job,
		'media_url': media_url
	}


