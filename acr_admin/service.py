from django.core.exceptions import ValidationError
from .models import GeneralSetting, WellnessBucket
from .serializer import general_setting_to_dict, wellness_bucket_to_dict


class SettingsAndBucketsService:
    """Service for updating settings and buckets"""
    
    GENERAL_SETTING_FIELDS = [
        'openai_api_key', 'openai_org_id', 'acr_cloud_api_key', 'revai_access_token',
        'summarize_transcript_prompt', 'sentiment_analysis_prompt', 'general_topics_prompt', 'iab_topics_prompt',
        'bucket_definition_error_rate', 'chatgpt_model', 'chatgpt_max_tokens',
        'chatgpt_temperature', 'chatgpt_top_p', 'chatgpt_frequency_penalty', 'chatgpt_presence_penalty',
        'determine_radio_content_type_prompt', 'content_type_prompt', 'radio_segment_types', 'radio_segment_error_rate'
    ]
    
    @classmethod
    def update_settings_and_buckets(cls, validated_data):
        """Update settings and buckets from validated data"""
        settings_data = validated_data.get('settings') or {}
        buckets = validated_data.get('buckets', [])
        
        # Update settings
        settings_obj, _ = GeneralSetting.objects.get_or_create(id=settings_data.get('id', 1))
        for field in cls.GENERAL_SETTING_FIELDS:
            if field in settings_data:
                setattr(settings_obj, field, settings_data[field])
        settings_obj.save()
        
        # Update buckets
        bucket_ids_in_payload = set()
        for bucket in buckets:
            bucket_id = bucket.get('id')
            if bucket_id:
                try:
                    wb = WellnessBucket.objects.get(id=bucket_id)
                except WellnessBucket.DoesNotExist:
                    raise ValidationError(f'Bucket with id {bucket_id} not found')
            else:
                wb = WellnessBucket()
            
            wb.title = bucket.get('title', '')
            wb.description = bucket.get('description', '')
            wb.category = bucket.get('category')
            wb.save()
            bucket_ids_in_payload.add(wb.id)
        
        # Delete orphaned buckets
        existing_ids = set(WellnessBucket.objects.values_list('id', flat=True))
        WellnessBucket.objects.filter(id__in=existing_ids - bucket_ids_in_payload).delete()
        
        return {
            'settings': general_setting_to_dict(settings_obj),
            'buckets': [wellness_bucket_to_dict(b) for b in WellnessBucket.objects.all()]
        }
