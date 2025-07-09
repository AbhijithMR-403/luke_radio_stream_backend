from .models import Channel

def channel_to_dict(channel):
    return {
        'id': channel.id,
        'name': channel.name,
        'channel_id': channel.channel_id,
        'project_id': channel.project_id,
        'created_at': channel.created_at.isoformat() if channel.created_at else None,
        'is_deleted': channel.is_deleted,
    }
