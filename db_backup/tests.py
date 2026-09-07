import shutil
import tempfile
from unittest.mock import patch

from django.core.cache import cache
from django.test import override_settings
from rest_framework.test import APITestCase

_TMP_BACKUP_DIR = tempfile.mkdtemp(prefix="db_backup_test_")

from accounts.models import RadioUser
from db_backup.models import DBBackup
from db_backup.services import LOCK_KEY, BackupLocked, run_backup


class _FakeProc:
    def __init__(self, returncode=0, stderr=b""):
        self.returncode = returncode
        self.stderr = stderr


def _fake_dump(cmd, stdout=None, stderr=None, env=None, timeout=None):
    if stdout is not None:
        stdout.write(b"PGDMP fake archive bytes")
    return _FakeProc(returncode=0)


class DBBackupApiTests(APITestCase):
    def setUp(self):
        cache.delete(LOCK_KEY)
        self.admin = RadioUser.objects.create_user(
            email="admin@example.com", password="x", name="Admin"
        )
        self.admin.is_admin = True
        self.admin.save(update_fields=["is_admin"])
        self.user = RadioUser.objects.create_user(
            email="user@example.com", password="x", name="User"
        )

    def test_non_admin_forbidden(self):
        self.client.force_authenticate(self.user)
        self.assertEqual(self.client.get("/api/db-backup/").status_code, 403)

    @patch("db_backup.views.run_db_backup.delay")
    def test_create_queues_task_and_returns_202(self, mock_delay):
        mock_delay.return_value.id = "task-123"
        self.client.force_authenticate(self.admin)
        resp = self.client.post("/api/db-backup/")
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.data["status"], DBBackup.Status.PENDING)
        backup = DBBackup.objects.get(pk=resp.data["id"])
        self.assertEqual(backup.requested_by, self.admin)
        self.assertEqual(backup.celery_task_id, "task-123")
        mock_delay.assert_called_once_with(backup.id)

    def test_download_conflict_when_not_ready(self):
        self.client.force_authenticate(self.admin)
        backup = DBBackup.objects.create(status=DBBackup.Status.RUNNING)
        resp = self.client.get(f"/api/db-backup/{backup.id}/download/")
        self.assertEqual(resp.status_code, 409)


@override_settings(MEDIA_ROOT=_TMP_BACKUP_DIR)
class DBBackupServiceTests(APITestCase):
    def setUp(self):
        cache.delete(LOCK_KEY)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_TMP_BACKUP_DIR, ignore_errors=True)
        super().tearDownClass()

    @patch("db_backup.services.subprocess.run", side_effect=_fake_dump)
    def test_run_backup_success(self, _mock):
        backup = DBBackup.objects.create()
        run_backup(backup)
        backup.refresh_from_db()
        self.assertEqual(backup.status, DBBackup.Status.SUCCESS)
        self.assertTrue(backup.file_name.endswith(".dump"))
        self.assertGreater(backup.size_bytes, 0)
        self.assertIsNone(cache.get(LOCK_KEY))

    @patch("db_backup.services.subprocess.run")
    def test_run_backup_records_pg_dump_failure(self, mock_run):
        mock_run.side_effect = lambda *a, **k: _FakeProc(returncode=1, stderr=b"boom")
        backup = DBBackup.objects.create()
        run_backup(backup)
        backup.refresh_from_db()
        self.assertEqual(backup.status, DBBackup.Status.FAILED)
        self.assertIn("boom", backup.error)
        self.assertIsNone(cache.get(LOCK_KEY))

    def test_second_backup_is_locked_out(self):
        cache.add(LOCK_KEY, 999, timeout=60)
        self.addCleanup(cache.delete, LOCK_KEY)
        backup = DBBackup.objects.create()
        with self.assertRaises(BackupLocked):
            run_backup(backup)
        backup.refresh_from_db()
        self.assertEqual(backup.status, DBBackup.Status.FAILED)
