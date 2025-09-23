from data_analysis.models import TranscriptionQueue


def modify_audio_segment_manually_processed():
    """
    Here AudioSegments.is_manually_processed field was add later so we need to modify the existing audio segments to set this field to True.
    For now TranscriptionQueue can we utilzed this one contain all the audio segments that are manually processed.
    So we need to set the is_manually_processed field to True for all the audio segments in TranscriptionQueue.
    """
    transcription_queue = TranscriptionQueue.objects.all()
    for queue in transcription_queue:
        queue.audio_segment.is_manually_processed = True
        queue.audio_segment.save()
