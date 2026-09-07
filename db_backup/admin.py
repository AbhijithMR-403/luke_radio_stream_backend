from django.contrib import admin

from db_backup.models import DBBackup


@admin.register(DBBackup)
class DBBackupAdmin(admin.ModelAdmin):
    list_display = ("id", "status", "trigger", "file_name", "size_bytes", "created_at")
    list_filter = ("status", "trigger")
    readonly_fields = [f.name for f in DBBackup._meta.fields]
