from data_analysis.services.transcription_analyzer import TranscriptionAnalyzer
from .models import Channel, GeneralSetting, WellnessBucket

def channel_to_dict(channel):
    return {
        'id': channel.id,
        'name': channel.name,
        'channel_id': channel.channel_id,
        'project_id': channel.project_id,
        'timezone': channel.timezone,
        'created_at': channel.created_at.isoformat() if channel.created_at else None,
        'is_deleted': channel.is_deleted,
    }

def general_setting_to_dict(settings):
    return {
        'id': settings.id,
        'openai_api_key': settings.openai_api_key,
        'openai_org_id': settings.openai_org_id,
        'acr_cloud_api_key': settings.acr_cloud_api_key,
        'revai_access_token': settings.revai_access_token,
        'summarize_transcript_prompt': settings.summarize_transcript_prompt,
        'sentiment_analysis_prompt': settings.sentiment_analysis_prompt,
        'general_topics_prompt': settings.general_topics_prompt,
        'iab_topics_prompt': settings.iab_topics_prompt,
        'updated_at': settings.updated_at.isoformat() if settings.updated_at else None,
        'bucket_prompt': TranscriptionAnalyzer.get_bucket_prompt(),
        'bucket_definition_error_rate': settings.bucket_definition_error_rate,
        'chatgpt_model': settings.chatgpt_model,
        'chatgpt_max_tokens': settings.chatgpt_max_tokens,
        'chatgpt_temperature': settings.chatgpt_temperature,
        'chatgpt_top_p': settings.chatgpt_top_p,
        'chatgpt_frequency_penalty': settings.chatgpt_frequency_penalty,
        'chatgpt_presence_penalty': settings.chatgpt_presence_penalty,
        'determine_radio_content_type_prompt': settings.determine_radio_content_type_prompt,
        'radio_segment_types': settings.radio_segment_types,
        'radio_segment_error_rate': settings.radio_segment_error_rate,
    }

def wellness_bucket_to_dict(bucket):
    return {
        'id': bucket.id,
        'title': bucket.title,
        'description': bucket.description,
    }
