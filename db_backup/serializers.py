from rest_framework import serializers

from db_backup.models import DBBackup


class DBBackupSerializer(serializers.ModelSerializer):
    requested_by_email = serializers.EmailField(
        source="requested_by.email", read_only=True, default=None
    )
    duration_seconds = serializers.FloatField(read_only=True)
    download_url = serializers.SerializerMethodField()

    class Meta:
        model = DBBackup
        fields = [
            "id",
            "status",
            "trigger",
            "requested_by",
            "requested_by_email",
            "file_name",
            "size_bytes",
            "download_url",
            "pg_dump_version",
            "celery_task_id",
            "error",
            "created_at",
            "started_at",
            "finished_at",
            "duration_seconds",
        ]
        read_only_fields = fields

    def get_download_url(self, obj):
        if obj.status != DBBackup.Status.SUCCESS or not obj.file:
            return None
        request = self.context.get("request")
        path = f"/api/db-backup/{obj.id}/download/"
        return request.build_absolute_uri(path) if request else path
