from django.db import models
from acr_admin.models import Channel

# Create your models here.


class AudioUnrecognizedCategory(models.Model):
    """Admin-managed group title for unrecognized audio segments (e.g., News, Traffic Report)."""

    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name="unrecognized_categories")
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
    after_title = models.CharField(
        max_length=255,
        blank=True,
        help_text="String or pattern to match against segment title_after",
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
        if self.after_title:
            return f"{self.category.name}: {self.before_title} -> {self.after_title}"
        return f"{self.category.name}: {self.before_title}"

    def clean(self):
        """Ensure before_title is unique per channel (via related category.channel)."""
        super().clean()
        if self.before_title and self.category and self.category.channel:
            qs = TitleMappingRule.objects.filter(
                before_title=self.before_title,
                category__channel=self.category.channel,
            )
            if self.pk:
                qs = qs.exclude(pk=self.pk)
            if qs.exists():
                from django.core.exceptions import ValidationError
                raise ValidationError({
                    'before_title': 'A rule with this before_title already exists for this channel.'
                })
