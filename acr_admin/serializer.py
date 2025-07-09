from .models import Channel, GeneralSetting, WellnessBucket

def channel_to_dict(channel):
    return {
        'id': channel.id,
        'name': channel.name,
        'channel_id': channel.channel_id,
        'project_id': channel.project_id,
        'created_at': channel.created_at.isoformat() if channel.created_at else None,
        'is_deleted': channel.is_deleted,
    }

def general_setting_to_dict(settings):
    return {
        'id': settings.id,
        'openai_api_key': settings.openai_api_key,
        'openai_org_id': settings.openai_org_id,
        'arc_cloud_api_key': settings.arc_cloud_api_key,
        'revai_access_token': settings.revai_access_token,
        'summarize_transcript_prompt': settings.summarize_transcript_prompt,
        'sentiment_analysis_prompt': settings.sentiment_analysis_prompt,
        'general_topics_prompt': settings.general_topics_prompt,
        'iab_topics_prompt': settings.iab_topics_prompt,
        'updated_at': settings.updated_at.isoformat() if settings.updated_at else None,
    }

def wellness_bucket_to_dict(bucket):
    return {
        'bucket_id': bucket.bucket_id,
        'title': bucket.title,
        'description': bucket.description,
        'prompt': bucket.prompt,
    }
