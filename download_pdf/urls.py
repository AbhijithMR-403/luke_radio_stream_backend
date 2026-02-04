from django.urls import path
from . import views

urlpatterns = [
    path("download_pdf/dashboard", views.DashboardPdfDownloadView.as_view(), name="dashboard_pdf_download"),
]
