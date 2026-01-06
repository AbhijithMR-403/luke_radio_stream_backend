from django.urls import path
from .views import ACRCloudFileUploadView, ACRCloudBucketsView

urlpatterns = [
    path('acr-cloud/upload-file/', ACRCloudFileUploadView.as_view(), name='acr-cloud-upload-file'),
    path('acr-cloud/buckets/', ACRCloudBucketsView.as_view(), name='acr-cloud-buckets'),
]


