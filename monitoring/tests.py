from datetime import datetime, timedelta, timezone as dt_timezone
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APITestCase

from core_admin.models import Channel, GeneralSetting
from data_analysis.models import AudioSegments
from monitoring.models import AlertRecipient, ChannelMonitoringConfig, ComponentHealth
from monitoring.services import maybe_send_alert, record_ingestion_health
from monitoring.tasks import check_openai_billing, check_system_health


def make_channel(**kwargs):
    defaults = dict(channel_type="broadcast", channel_id=111, project_id=222, is_active=True, is_deleted=False)
    defaults.update(kwargs)
    return Channel.objects.create(**defaults)


def make_valid_general_setting(channel):
    return GeneralSetting.objects.create(
        channel=channel,
        is_active=True,
        revai_access_token="rev-token",
        acr_cloud_api_key="acr-key",
        openai_api_key="sk-test",
        summarize_transcript_prompt="p",
        sentiment_analysis_prompt="p",
        general_topics_prompt="p",
        iab_topics_prompt="p",
        determine_radio_content_type_prompt="p",
        content_type_prompt="p",
    )


def make_segment(channel, start_time, end_time):
    return AudioSegments.objects.create(
        channel=channel,
        start_time=start_time,
        end_time=end_time,
        duration_seconds=int((end_time - start_time).total_seconds()),
        file_name="test.mp3",
        file_path=f"media/test_{start_time.timestamp()}.mp3",
        audio_location_type="file_path",
        is_recognized=True,
        title="Test Song",
    )


class RecordIngestionHealthTests(TestCase):
    def setUp(self):
        self.channel = make_channel()

    def test_malformed_response_is_unhealthy(self):
        record_ingestion_health(self.channel, {"error": "boom"}, 0, False, "20260101", timezone.now())
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_UNHEALTHY)
        self.assertIn("Invalid ACRCloud response", row.message)
        self.assertIsNone(row.segments_received)

    def test_non_dict_response_is_unhealthy(self):
        record_ingestion_health(self.channel, None, 0, False, "20260101", timezone.now())
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_UNHEALTHY)

    def test_zero_segments_is_unhealthy(self):
        run_started_at = timezone.now()
        record_ingestion_health(self.channel, {"data": []}, 0, False, "20260101", run_started_at)
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_UNHEALTHY)
        self.assertEqual(row.segments_received, 0)
        self.assertEqual(row.segments_saved, 0)

    def test_normal_response_is_healthy(self):
        run_started_at = timezone.now()
        day_start = datetime(2026, 1, 1, tzinfo=dt_timezone.utc)
        make_segment(self.channel, day_start, day_start + timedelta(hours=23, minutes=59))
        record_ingestion_health(self.channel, {"data": [{}]}, 1, False, "20260101", run_started_at)
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_HEALTHY)
        self.assertAlmostEqual(row.coverage_ratio, 1.0, places=1)
        self.assertIsNotNone(row.last_success_at)

    def test_coverage_gap_is_warning(self):
        run_started_at = timezone.now()
        day_start = datetime(2026, 1, 1, tzinfo=dt_timezone.utc)
        # Only ~7 of 24 hours covered -> ratio well below the 0.7 warning threshold.
        make_segment(self.channel, day_start, day_start + timedelta(hours=7))
        record_ingestion_health(self.channel, {"data": [{}]}, 1, False, "20260101", run_started_at)
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_WARNING)
        self.assertLess(row.coverage_ratio, 0.7)

    def test_expected_volume_config_flags_unhealthy_on_severe_drop(self):
        ChannelMonitoringConfig.objects.create(channel=self.channel, expected_segments_per_hour=20)
        run_started_at = timezone.now()
        day_start = datetime(2026, 1, 1, tzinfo=dt_timezone.utc)
        # Full-day coverage (so the coverage check alone would pass) but far too few segments saved.
        make_segment(self.channel, day_start, day_start + timedelta(hours=23, minutes=59))
        record_ingestion_health(self.channel, {"data": [{}] * 5}, 5, False, "20260101", run_started_at)
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_UNHEALTHY)
        self.assertIn("Expected ~", row.message)

    def test_channel_without_config_falls_back_to_zero_segments_rule(self):
        run_started_at = timezone.now()
        day_start = datetime(2026, 1, 1, tzinfo=dt_timezone.utc)
        make_segment(self.channel, day_start, day_start + timedelta(hours=23, minutes=59))
        # Low but nonzero count, no ChannelMonitoringConfig -> not flagged (no generic warning tier).
        record_ingestion_health(self.channel, {"data": [{}] * 2}, 2, False, "20260101", run_started_at)
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_HEALTHY)

    def test_near_midnight_zero_length_window_is_healthy(self):
        record_ingestion_health(
            self.channel, {"data": []}, 0, True, None, datetime(2026, 1, 1, 0, 30, tzinfo=dt_timezone.utc)
        )
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=self.channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_HEALTHY)


class CheckSystemHealthTests(TestCase):
    def test_stale_channel_is_flagged_unhealthy(self):
        channel = make_channel()
        make_valid_general_setting(channel)
        ComponentHealth.objects.create(
            component=ComponentHealth.COMPONENT_ACR_INGESTION,
            channel=channel,
            status=ComponentHealth.STATUS_HEALTHY,
            last_run_at=timezone.now() - timedelta(hours=3),
        )

        check_system_health()

        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_UNHEALTHY)
        self.assertIn("No ingestion run recorded", row.message)

    def test_channel_with_no_row_yet_is_not_flagged(self):
        channel = make_channel()
        make_valid_general_setting(channel)

        check_system_health()

        self.assertFalse(
            ComponentHealth.objects.filter(
                component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=channel
            ).exists()
        )

    def test_stalled_processing_backlog_flagged(self):
        channel = make_channel()
        old_cutoff = timezone.now() - timedelta(hours=7)
        for i in range(25):
            seg = make_segment(channel, old_cutoff, old_cutoff + timedelta(seconds=30))
            AudioSegments.objects.filter(pk=seg.pk).update(created_at=old_cutoff)

        check_system_health()

        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_PROCESSING_PIPELINE, channel=None)
        self.assertEqual(row.status, ComponentHealth.STATUS_WARNING)
        self.assertEqual(row.details["stalled_count"], 25)

    @patch("monitoring.tasks.validate_openai_api_key")
    def test_openai_check_runs_for_configured_channel(self, mock_validate):
        mock_validate.return_value = {"is_valid": False, "error_message": "bad key"}
        channel = make_channel()
        make_valid_general_setting(channel)

        check_openai_billing()

        mock_validate.assert_called_once()
        row = ComponentHealth.objects.get(component=ComponentHealth.COMPONENT_OPENAI_BILLING, channel=channel)
        self.assertEqual(row.status, ComponentHealth.STATUS_UNHEALTHY)

    @patch("monitoring.tasks.validate_openai_api_key")
    def test_check_system_health_does_not_touch_openai(self, mock_validate):
        # OpenAI billing is checked by its own hourly task now, not on check_system_health's
        # 15-min cadence at all.
        channel = make_channel()
        make_valid_general_setting(channel)

        check_system_health()

        mock_validate.assert_not_called()


class MaybeSendAlertTests(TestCase):
    def setUp(self):
        self.channel = make_channel()
        self.recipient = AlertRecipient.objects.create(email="a@example.com", is_active=True)

    def _row(self, status, last_alert_sent_at=None):
        return ComponentHealth.objects.create(
            component=ComponentHealth.COMPONENT_ACR_INGESTION,
            channel=self.channel,
            status=status,
            last_alert_sent_at=last_alert_sent_at,
        )

    @patch("monitoring.services.send_alert_via_ghl")
    def test_worsening_transition_sends_alert(self, mock_send):
        row = self._row(ComponentHealth.STATUS_UNHEALTHY)
        maybe_send_alert(row, previous_status=ComponentHealth.STATUS_HEALTHY)
        mock_send.assert_called_once()
        row.refresh_from_db()
        self.assertIsNotNone(row.last_alert_sent_at)

    @patch("monitoring.services.send_alert_via_ghl")
    def test_repeated_unhealthy_within_reminder_window_no_alert(self, mock_send):
        row = self._row(ComponentHealth.STATUS_UNHEALTHY, last_alert_sent_at=timezone.now())
        maybe_send_alert(row, previous_status=ComponentHealth.STATUS_UNHEALTHY)
        mock_send.assert_not_called()

    @patch("monitoring.services.send_alert_via_ghl")
    def test_reminder_sent_after_window_elapsed(self, mock_send):
        row = self._row(ComponentHealth.STATUS_UNHEALTHY, last_alert_sent_at=timezone.now() - timedelta(hours=25))
        maybe_send_alert(row, previous_status=ComponentHealth.STATUS_UNHEALTHY)
        mock_send.assert_called_once()

    @patch("monitoring.services.send_alert_via_ghl")
    def test_recovery_sends_alert_and_resets_last_alert_sent_at(self, mock_send):
        row = self._row(ComponentHealth.STATUS_HEALTHY, last_alert_sent_at=timezone.now())
        maybe_send_alert(row, previous_status=ComponentHealth.STATUS_UNHEALTHY)
        mock_send.assert_called_once()
        row.refresh_from_db()
        self.assertIsNone(row.last_alert_sent_at)

    @patch("monitoring.services.send_alert_via_ghl")
    def test_no_recipients_no_call(self, mock_send):
        AlertRecipient.objects.all().delete()
        row = self._row(ComponentHealth.STATUS_UNHEALTHY)
        maybe_send_alert(row, previous_status=ComponentHealth.STATUS_HEALTHY)
        mock_send.assert_not_called()

    @patch("monitoring.services.send_alert_via_ghl")
    def test_one_recipient_failure_does_not_block_others(self, mock_send):
        AlertRecipient.objects.create(email="b@example.com", is_active=True)
        mock_send.side_effect = [Exception("boom"), None]
        row = self._row(ComponentHealth.STATUS_UNHEALTHY)
        maybe_send_alert(row, previous_status=ComponentHealth.STATUS_HEALTHY)
        self.assertEqual(mock_send.call_count, 2)


class HealthCheckViewTests(APITestCase):
    def test_empty_db_returns_200_unknown(self):
        response = self.client.get("/api/health/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], ComponentHealth.STATUS_UNKNOWN)

    def test_all_healthy_returns_200_healthy(self):
        channel = make_channel()
        ComponentHealth.objects.create(
            component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=channel, status=ComponentHealth.STATUS_HEALTHY
        )
        ComponentHealth.objects.create(
            component=ComponentHealth.COMPONENT_PROCESSING_PIPELINE, channel=None, status=ComponentHealth.STATUS_HEALTHY
        )
        response = self.client.get("/api/health/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], ComponentHealth.STATUS_HEALTHY)

    def test_one_unhealthy_row_returns_200_with_unhealthy_overall(self):
        channel = make_channel()
        ComponentHealth.objects.create(
            component=ComponentHealth.COMPONENT_ACR_INGESTION, channel=channel, status=ComponentHealth.STATUS_UNHEALTHY
        )
        response = self.client.get("/api/health/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], ComponentHealth.STATUS_UNHEALTHY)

    def test_unauthenticated_access_succeeds(self):
        response = self.client.get("/api/health/")
        self.assertEqual(response.status_code, 200)


class ValidateOpenAIApiKeyTests(TestCase):
    @patch("monitoring.integrations.requests.post")
    def test_valid_key(self, mock_post):
        from monitoring.integrations import validate_openai_api_key

        mock_post.return_value.status_code = 200
        result = validate_openai_api_key("sk-good")
        self.assertTrue(result["is_valid"])
        # Confirms this hits a real (billed) endpoint, not the free /v1/models list.
        self.assertIn("embeddings", mock_post.call_args.args[0])

    @patch("monitoring.integrations.requests.post")
    def test_invalid_key_401(self, mock_post):
        from monitoring.integrations import validate_openai_api_key

        mock_post.return_value.status_code = 401
        mock_post.return_value.json.return_value = {"error": {"message": "Incorrect API key provided"}}
        result = validate_openai_api_key("sk-bad")
        self.assertFalse(result["is_valid"])
        self.assertIn("Incorrect API key", result["error_message"])

    @patch("monitoring.integrations.requests.post")
    def test_quota_exceeded_429(self, mock_post):
        from monitoring.integrations import validate_openai_api_key

        mock_post.return_value.status_code = 429
        mock_post.return_value.json.return_value = {"error": {"message": "You exceeded your current quota"}}
        result = validate_openai_api_key("sk-noquota")
        self.assertFalse(result["is_valid"])
        self.assertIn("quota", result["error_message"])

    @patch("monitoring.integrations.requests.post")
    def test_network_error_fails_open(self, mock_post):
        import requests as requests_module

        from monitoring.integrations import validate_openai_api_key

        mock_post.side_effect = requests_module.exceptions.ConnectionError("down")
        result = validate_openai_api_key("sk-whatever")
        self.assertTrue(result["is_valid"])
