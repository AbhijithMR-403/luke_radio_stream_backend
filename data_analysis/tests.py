from unittest.mock import patch

from django.test import TestCase

from core_admin.models import Channel
from data_analysis.tasks import process_channel_task


class ProcessChannelTaskMonitoringFailsafeTests(TestCase):
    """A bug in the monitoring instrumentation must never break ACRCloud ingestion itself."""

    @patch("data_analysis.tasks.record_ingestion_health", side_effect=Exception("monitoring boom"))
    @patch("data_analysis.tasks.AudioSegments.get_today_data_excluding_last_hour", return_value={"data": []})
    def test_process_channel_task_succeeds_when_monitoring_raises(self, mock_fetch, mock_record):
        channel = Channel.objects.create(
            channel_type="broadcast", channel_id=1, project_id=2, is_active=True, is_deleted=False
        )

        result = process_channel_task(channel.id, is_today=True)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["segments_count"], 0)
        mock_record.assert_called_once()
