from django.conf import settings
from django.db import models

from data_analysis.models import AudioSegments

# Create your models here.

class Prompt(models.Model):
    name = models.CharField(max_length=255)

    content = models.TextField()

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)


class PromptRun(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("processing", "Processing"),
        ("completed", "Completed"),
        ("failed", "Failed"),
    ]

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    audio_segments = models.ManyToManyField(AudioSegments)

    prompts = models.ManyToManyField(Prompt)

    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default="pending"
    )

    created_at = models.DateTimeField(auto_now_add=True)


class PromptResult(models.Model):
    prompt_run = models.ForeignKey(
        PromptRun,
        on_delete=models.CASCADE,
        related_name="results"
    )

    prompt = models.ForeignKey(
        Prompt,
        on_delete=models.CASCADE
    )

    response = models.TextField()

    created_at = models.DateTimeField(auto_now_add=True)