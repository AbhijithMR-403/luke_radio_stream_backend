from celery import shared_task

from db_backup.models import DBBackup
from db_backup.services import BackupLocked, cleanup_backups, run_backup


@shared_task(bind=True, soft_time_limit=60 * 60 * 2, time_limit=60 * 60 * 2 + 300)
def run_db_backup(self, backup_id):
    backup = DBBackup.objects.get(pk=backup_id)
    if not backup.celery_task_id:
        backup.celery_task_id = self.request.id or ""
        backup.save(update_fields=["celery_task_id"])
    try:
        run_backup(backup)
    except BackupLocked:
        # Row already marked failed with an explanatory message.
        pass
    return {"backup_id": backup_id, "status": backup.status}


@shared_task
def scheduled_db_backup():
    backup = DBBackup.objects.create(trigger=DBBackup.Trigger.SCHEDULED)
    try:
        run_backup(backup)
    except BackupLocked:
        pass
    return {"backup_id": backup.id, "status": backup.status}


@shared_task
def cleanup_old_db_backups():
    return cleanup_backups()
