from rest_framework.test import APITestCase
from rest_framework import status
from unittest.mock import patch
from .models import GeneralSetting, WellnessBucket


class SettingsAndBucketsViewTestCase(APITestCase):
    """Test cases for SettingsAndBucketsView API"""
    
    def setUp(self):
        self.url = '/api/settings'
        self.settings = GeneralSetting.objects.create(
            openai_org_id='test@example.com',
            summarize_transcript_prompt='Test prompt',
            sentiment_analysis_prompt='Test prompt',
            general_topics_prompt='Test prompt',
            iab_topics_prompt='Test prompt'
        )
        self.bucket = WellnessBucket.objects.create(
            title='Emotional Wellness',
            description='Emotional wellness bucket',
            category='personal'
        )
    
    def test_get_settings_and_buckets_success(self):
        """Test GET returns settings and buckets"""
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.assertIsNotNone(response.data['settings'])
        self.assertIsInstance(response.data['buckets'], list)
        self.assertEqual(len(response.data['buckets']), 1)
    
    def test_get_settings_and_buckets_empty(self):
        """Test GET when no data exists"""
        GeneralSetting.objects.all().delete()
        WellnessBucket.objects.all().delete()
        
        response = self.client.get(self.url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.assertIsNone(response.data['settings'])
        self.assertEqual(len(response.data['buckets']), 0)
    
    @patch('acr_admin.serializer.OpenAIUtils.validate_api_key')
    @patch('acr_admin.serializer.ACRCloudUtils.validate_api_key')
    def test_put_update_settings_success(self, mock_acr_validate, mock_openai_validate):
        """Test PUT updates settings successfully"""
        mock_openai_validate.return_value = (True, None, 'newemail@example.com')
        mock_acr_validate.return_value = (True, None)
        
        data = {
            'settings': {
                'summarize_transcript_prompt': 'Updated prompt',
                'bucket_definition_error_rate': 85,
                'chatgpt_temperature': 0.7
            },
            'buckets': []
        }
        
        response = self.client.put(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.settings.refresh_from_db()
        self.assertEqual(self.settings.bucket_definition_error_rate, 85)
        self.assertEqual(self.settings.chatgpt_temperature, 0.7)
    
    @patch('acr_admin.serializer.OpenAIUtils.validate_api_key')
    @patch('acr_admin.serializer.ACRCloudUtils.validate_api_key')
    def test_put_create_bucket_success(self, mock_acr_validate, mock_openai_validate):
        """Test PUT creates new bucket successfully"""
        mock_openai_validate.return_value = (True, None, None)
        mock_acr_validate.return_value = (True, None)
        
        data = {
            'settings': {},
            'buckets': [
                {
                    'title': 'New Bucket',
                    'description': 'New bucket description',
                    'category': 'spiritual'
                }
            ]
        }
        
        response = self.client.put(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.assertEqual(WellnessBucket.objects.count(), 1)
        new_bucket = WellnessBucket.objects.get(title='New Bucket')
        self.assertEqual(new_bucket.category, 'spiritual')
    
    def test_put_validation_error_missing_fields(self):
        """Test PUT with missing required fields"""
        data = {
            'settings': {},
            'buckets': [
                {
                    'title': 'Test',
                    # Missing description and category
                }
            ]
        }
        
        response = self.client.put(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertFalse(response.data['success'])
        self.assertIn('error', response.data)
    
    def test_put_validation_error_invalid_category(self):
        """Test PUT with invalid category"""
        data = {
            'settings': {},
            'buckets': [
                {
                    'title': 'Test',
                    'description': 'Test description',
                    'category': 'invalid_category'
                }
            ]
        }
        
        response = self.client.put(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertFalse(response.data['success'])
        self.assertIn('error', response.data)


