from celery import shared_task

from audio_recovery.models import RecoveredAudioFile
from audio_recovery.services import recover_audio_from_url


@shared_task(bind=True)
def recover_audio_task(self, recovered_audio_file_id, url, access_key, access_secret, folder):
    recovered_audio_file = RecoveredAudioFile.objects.get(pk=recovered_audio_file_id)
    if not recovered_audio_file.celery_task_id:
        recovered_audio_file.celery_task_id = self.request.id or ""
        recovered_audio_file.save(update_fields=["celery_task_id"])

    recover_audio_from_url(recovered_audio_file_id, url, access_key, access_secret, folder)
