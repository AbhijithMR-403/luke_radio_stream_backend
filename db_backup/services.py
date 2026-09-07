"""Core pg_dump logic, kept separate from the Celery task wrapper."""
import os
import subprocess
from pathlib import Path

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

from db_backup.models import DBBackup

LOCK_KEY = "db_backup:running"
LOCK_TTL = 60 * 60 * 3  # 3h safety net; released in finally


class BackupLocked(Exception):
    """Raised when another backup is already in progress."""


def _db_config():
    return settings.DATABASES["default"]


def _pg_dump_path():
    return getattr(settings, "DB_BACKUP_PG_DUMP_PATH", "pg_dump")


SUBDIR = "backups"


def _backups_dir() -> Path:
    # Kept under MEDIA_ROOT so the FileField / download view resolve correctly.
    path = Path(settings.MEDIA_ROOT) / SUBDIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def _pg_dump_version() -> str:
    try:
        out = subprocess.run(
            [_pg_dump_path(), "--version"],
            capture_output=True, text=True, timeout=15,
        )
        return out.stdout.strip()
    except Exception:
        return ""


def run_backup(backup: DBBackup) -> DBBackup:
    """Run pg_dump for `backup`, updating its row as it progresses.

    Uses a cache-based lock so two dumps never run at once. Always releases
    the lock and always leaves the row in a terminal state (success/failed).
    """
    if not cache.add(LOCK_KEY, backup.pk, timeout=LOCK_TTL):
        holder = cache.get(LOCK_KEY)
        backup.status = DBBackup.Status.FAILED
        backup.error = f"Another backup (#{holder}) is already running."
        backup.finished_at = timezone.now()
        backup.save(update_fields=["status", "error", "finished_at"])
        raise BackupLocked(backup.error)

    db = _db_config()
    backup.status = DBBackup.Status.RUNNING
    backup.started_at = timezone.now()
    backup.pg_dump_version = _pg_dump_version()
    backup.save(update_fields=["status", "started_at", "pg_dump_version"])

    stamp = backup.started_at.strftime("%Y%m%d_%H%M%S")
    file_name = f"{db.get('NAME', 'db')}_{stamp}.dump"
    out_path = _backups_dir() / file_name

    cmd = [
        _pg_dump_path(),
        "--format=custom",
        "--compress=6",
        "--no-owner",
        "--no-privileges",
        "--host", str(db.get("HOST") or "localhost"),
        "--port", str(db.get("PORT") or "5432"),
        "--username", str(db.get("USER") or ""),
        "--dbname", str(db.get("NAME") or ""),
    ]
    env = {**os.environ}
    if db.get("PASSWORD"):
        env["PGPASSWORD"] = str(db["PASSWORD"])

    time_limit = getattr(settings, "DB_BACKUP_TIMEOUT_SECONDS", 60 * 60 * 2)

    try:
        with open(out_path, "wb") as stdout:
            proc = subprocess.run(
                cmd, stdout=stdout, stderr=subprocess.PIPE,
                env=env, timeout=time_limit,
            )
        if proc.returncode != 0:
            raise RuntimeError(
                proc.stderr.decode("utf-8", "replace").strip()
                or f"pg_dump exited with code {proc.returncode}"
            )

        backup.file.name = f"{SUBDIR}/{file_name}"
        backup.file_name = file_name
        backup.size_bytes = out_path.stat().st_size
        backup.status = DBBackup.Status.SUCCESS
        backup.error = ""
    except Exception as exc:
        out_path.unlink(missing_ok=True)
        backup.status = DBBackup.Status.FAILED
        backup.error = str(exc)[:5000]
    finally:
        backup.finished_at = timezone.now()
        backup.save(update_fields=[
            "file", "file_name", "size_bytes", "status", "error", "finished_at",
        ])
        cache.delete(LOCK_KEY)

    return backup


def cleanup_backups() -> dict:
    """Delete backups older than the retention window, always keeping the most
    recent N successful dumps regardless of age."""
    retention_days = getattr(settings, "DB_BACKUP_RETENTION_DAYS", 14)
    keep_min = getattr(settings, "DB_BACKUP_KEEP_MIN", 5)
    cutoff = timezone.now() - timezone.timedelta(days=retention_days)

    protected = set(
        DBBackup.objects.filter(status=DBBackup.Status.SUCCESS)
        .order_by("-created_at")
        .values_list("pk", flat=True)[:keep_min]
    )
    stale = DBBackup.objects.filter(created_at__lt=cutoff).exclude(pk__in=protected)

    removed = 0
    for backup in stale:
        if backup.file:
            backup.file.delete(save=False)
        backup.delete()
        removed += 1
    return {"removed": removed, "retention_days": retention_days}
