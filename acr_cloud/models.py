from django.db import models
from django.conf import settings


class ACRCloudCustomFileUpload(models.Model):
    """
    Model to store details about files uploaded to ACR Cloud.
    """
    # Upload details
    bucket_id = models.CharField(max_length=255, help_text="ACR Cloud bucket ID")
    audio_url = models.URLField(max_length=512, help_text="URL of the audio file uploaded")
    title = models.CharField(max_length=255, null=True, blank=True, help_text="Optional title for the file")
    
    # Status tracking
    status = models.CharField(
        max_length=50,
        choices=[
            ('success', 'Success'),
            ('failed', 'Failed'),
            ('error', 'Error'),
        ],
        default='success',
        help_text="Status of the upload operation"
    )
    error_message = models.TextField(null=True, blank=True, help_text="Error message if upload failed")
    
    # User tracking
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='acr_cloud_custom_uploads',
        help_text="User who initiated the upload"
    )
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text="Timestamp when the upload was initiated")
    
    class Meta:
        db_table = 'acr_cloud_custom_file_upload'
        verbose_name = 'ACR Cloud Custom File Upload'
        verbose_name_plural = 'ACR Cloud Custom File Uploads'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['bucket_id']),
            models.Index(fields=['status']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"Upload to bucket {self.bucket_id} - {self.status} ({self.created_at})"
