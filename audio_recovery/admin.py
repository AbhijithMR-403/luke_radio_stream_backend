from django.contrib import admin

from audio_recovery.models import RecoveredAudioFile, RecoveredAudioSegment


@admin.register(RecoveredAudioFile)
class RecoveredAudioFileAdmin(admin.ModelAdmin):
    list_display = ("id", "channel", "status", "recorded_at", "duration_seconds", "created_at")
    list_filter = ("channel", "status")
    search_fields = ("source_url",)
    date_hierarchy = "recorded_at"


@admin.register(RecoveredAudioSegment)
class RecoveredAudioSegmentAdmin(admin.ModelAdmin):
    list_display = ("id", "recovered_audio_file", "start_sec", "end_sec", "audio_segment", "created_at")
    list_filter = ("recovered_audio_file__channel",)
