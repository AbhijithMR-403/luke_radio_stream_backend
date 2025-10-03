from django.db import models

# Create your models here.


class AudioUnrecognizedCategory(models.Model):
    """Admin-managed group title for unrecognized audio segments (e.g., News, Traffic Report)."""

    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Unrecognized Audio Category"
        verbose_name_plural = "Unrecognized Audio Categories"
        ordering = ["name"]

    def __str__(self):
        return self.name


class TitleMappingRule(models.Model):
    """Maps a segment's title_before to a category using a chosen matching strategy.

    This does not perform matching itself; it's only configuration that can be
    applied by services/tasks when processing unrecognized segments.
    """

    category = models.ForeignKey(
        AudioUnrecognizedCategory,
        on_delete=models.PROTECT,
        related_name="rules",
    )
    before_title = models.CharField(
        max_length=255,
        help_text="String or pattern to match against segment title_before",
        db_index=True,
    )
    skip_transcription = models.BooleanField(
        default=True,
        help_text="If true, segments matching this rule are excluded from transcription",
    )
    is_active = models.BooleanField(default=True)
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Title Mapping Rule"
        verbose_name_plural = "Title Mapping Rules"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["is_active"]),
        ]

    def __str__(self):
        return f"{self.category.name}: {self.before_title}"
