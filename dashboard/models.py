from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from django.conf import settings


class UserSentimentPreference(models.Model):
    """
    Model to store user preferences for sentiment score thresholds.
    Users can set their preferred low and high sentiment scores (0-100).
    """
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='sentiment_preference',
        help_text="User who owns these sentiment preferences"
    )
    low_sentiment_score = models.PositiveIntegerField(
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Low sentiment threshold score (0-100). Scores below this are considered low sentiment."
    )
    high_sentiment_score = models.PositiveIntegerField(
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="High sentiment threshold score (0-100). Scores above this are considered high sentiment."
    )
    target_sentiment_score = models.PositiveIntegerField(
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Target sentiment score (0-100). The desired sentiment score the user wants to achieve."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def clean(self):
        """Validate that low_sentiment_score is less than high_sentiment_score"""
        super().clean()
        if self.low_sentiment_score >= self.high_sentiment_score:
            raise ValidationError({
                'low_sentiment_score': 'Low sentiment score must be less than high sentiment score.',
                'high_sentiment_score': 'High sentiment score must be greater than low sentiment score.'
            })

    def save(self, *args, **kwargs):
        """Override save to call clean validation"""
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.user.email} - Low: {self.low_sentiment_score}, High: {self.high_sentiment_score}, Target: {self.target_sentiment_score}"

    class Meta:
        verbose_name = "User Sentiment Preference"
        verbose_name_plural = "User Sentiment Preferences"
