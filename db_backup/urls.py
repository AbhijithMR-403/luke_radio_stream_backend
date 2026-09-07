from django.urls import path

from db_backup.views import (
    DBBackupDetailView,
    DBBackupDownloadView,
    DBBackupListCreateView,
)

urlpatterns = [
    path("db-backup/", DBBackupListCreateView.as_view(), name="db-backup-list-create"),
    path("db-backup/<int:pk>/", DBBackupDetailView.as_view(), name="db-backup-detail"),
    path(
        "db-backup/<int:pk>/download/",
        DBBackupDownloadView.as_view(),
        name="db-backup-download",
    ),
]
