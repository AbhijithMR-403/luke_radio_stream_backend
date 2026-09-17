from django.urls import path

from audio_recovery.views import RecoverAudioView, RecoveryStatusView

urlpatterns = [
    path("recover/", RecoverAudioView.as_view(), name="audio-recovery-recover"),
    path("recover/<int:recovered_audio_file_id>/status/", RecoveryStatusView.as_view(), name="audio-recovery-status"),
]
