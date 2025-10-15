from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from datetime import time
from acr_admin.models import Channel
from accounts.models import RadioUser


class Shift(models.Model):
    """
    Model to store shift information with start and end times.
    """
    name = models.CharField(max_length=100, help_text="Name of the shift (e.g., 'Morning Shift', 'Night Shift')")
    start_time = models.TimeField(help_text="Start time of the shift")
    end_time = models.TimeField(help_text="End time of the shift")
    description = models.TextField(blank=True, null=True, help_text="Optional description of the shift")
    timezone = models.CharField(max_length=64, help_text="IANA timezone for interpreting start/end times")
    is_active = models.BooleanField(default=True, help_text="Whether this shift is currently active")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['start_time']
        verbose_name = "Shift"
        verbose_name_plural = "Shifts"

    def __str__(self):
        return f"{self.name} ({self.start_time} - {self.end_time})"

    def clean(self):
        """Allow overnight windows; only forbid 24h zero-length if undesired"""
        if self.start_time and self.end_time and self.start_time == self.end_time:
            raise ValidationError("Start and end time cannot be the same")


class PredefinedFilter(models.Model):
    """
    Model to store predefined filters for shift analysis.
    Each filter is associated with a specific channel and can have different schedules for each day.
    """
    DAYS_OF_WEEK = [
        ('monday', 'Monday'),
        ('tuesday', 'Tuesday'),
        ('wednesday', 'Wednesday'),
        ('thursday', 'Thursday'),
        ('friday', 'Friday'),
        ('saturday', 'Saturday'),
        ('sunday', 'Sunday'),
    ]
    
    name = models.CharField(max_length=200, help_text="Name of the predefined filter")
    description = models.TextField(blank=True, null=True, help_text="Description of what this filter is used for")
    channel = models.ForeignKey(
        Channel,
        on_delete=models.CASCADE,
        related_name='predefined_filters',
        help_text="Channel this filter belongs to"
    )
    timezone = models.CharField(max_length=64, help_text="IANA timezone for interpreting schedules")
    is_active = models.BooleanField(default=True, help_text="Whether this filter is currently active")
    created_by = models.ForeignKey(
        RadioUser,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='created_filters',
        help_text="User who created this filter"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']
        verbose_name = "Predefined Filter"
        verbose_name_plural = "Predefined Filters"

    def __str__(self):
        return f"{self.name} - {self.channel.name}"


class FilterSchedule(models.Model):
    """
    Model to store time ranges for predefined filters.
    Each schedule is tied to a specific day of the week, allowing different hours for each day.
    """
    DAYS_OF_WEEK = [
        ('monday', 'Monday'),
        ('tuesday', 'Tuesday'),
        ('wednesday', 'Wednesday'),
        ('thursday', 'Thursday'),
        ('friday', 'Friday'),
        ('saturday', 'Saturday'),
        ('sunday', 'Sunday'),
    ]
    
    predefined_filter = models.ForeignKey(
        PredefinedFilter, 
        on_delete=models.CASCADE, 
        related_name='schedules',
        help_text="The predefined filter this schedule belongs to"
    )
    day_of_week = models.CharField(
        max_length=10,
        choices=DAYS_OF_WEEK,
        help_text="Day of the week this schedule applies to"
    )
    start_time = models.TimeField(help_text="Start time for this schedule")
    end_time = models.TimeField(help_text="End time for this schedule")
    notes = models.TextField(blank=True, null=True, help_text="Optional notes for this specific schedule")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['day_of_week', 'start_time']
        verbose_name = "Filter Schedule"
        verbose_name_plural = "Filter Schedules"
        unique_together = ['predefined_filter', 'day_of_week', 'start_time', 'end_time']

    def __str__(self):
        return f"{self.predefined_filter.name} - {self.get_day_of_week_display()} ({self.start_time} - {self.end_time})"

    def clean(self):
        """Allow overnight windows; only forbid 24h zero-length if undesired"""
        if self.start_time and self.end_time and self.start_time == self.end_time:
            raise ValidationError("Start and end time cannot be the same")
