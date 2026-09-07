from django.http import FileResponse
from django.shortcuts import get_object_or_404
from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.views import APIView

from db_backup.models import DBBackup
from db_backup.permissions import IsAdminUser
from db_backup.serializers import DBBackupSerializer
from db_backup.tasks import run_db_backup


class DBBackupListCreateView(generics.ListCreateAPIView):
    """GET  /api/db-backup/   -> list backups (newest first)
    POST /api/db-backup/   -> queue a new backup, returns 202 immediately
    """

    queryset = DBBackup.objects.select_related("requested_by").all()
    serializer_class = DBBackupSerializer
    permission_classes = [IsAdminUser]

    def create(self, request, *args, **kwargs):
        backup = DBBackup.objects.create(
            requested_by=request.user, trigger=DBBackup.Trigger.MANUAL
        )
        task = run_db_backup.delay(backup.id)
        backup.celery_task_id = task.id
        backup.save(update_fields=["celery_task_id"])

        serializer = self.get_serializer(backup)
        return Response(serializer.data, status=status.HTTP_202_ACCEPTED)


class DBBackupDetailView(generics.RetrieveDestroyAPIView):
    """GET    /api/db-backup/<id>/  -> poll status
    DELETE /api/db-backup/<id>/  -> delete row + dump file
    """

    queryset = DBBackup.objects.select_related("requested_by").all()
    serializer_class = DBBackupSerializer
    permission_classes = [IsAdminUser]

    def perform_destroy(self, instance):
        if instance.file:
            instance.file.delete(save=False)
        instance.delete()


class DBBackupDownloadView(APIView):
    """GET /api/db-backup/<id>/download/ -> stream the dump file."""

    permission_classes = [IsAdminUser]

    def get(self, request, pk):
        backup = get_object_or_404(DBBackup, pk=pk)
        if backup.status != DBBackup.Status.SUCCESS or not backup.file:
            return Response(
                {"detail": f"Backup is not ready (status: {backup.status})."},
                status=status.HTTP_409_CONFLICT,
            )
        response = FileResponse(
            backup.file.open("rb"),
            as_attachment=True,
            filename=backup.file_name or "backup.dump",
        )
        response["Content-Type"] = "application/octet-stream"
        return response
