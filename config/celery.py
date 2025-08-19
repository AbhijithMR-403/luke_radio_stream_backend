from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('config')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Celery Beat Schedule Configuration
app.conf.beat_schedule = {
    # Process today's audio data excluding last hour - runs every hour
    'process-today-audio-data': {
        'task': 'data_analysis.tasks.process_today_audio_data',
        'schedule': 3600.0,  # Every 3600 seconds (1 hour)
    },
    # Process previous day's audio data - runs daily at 2 AM
    'process-previous-day-audio-data': {
        'task': 'data_analysis.tasks.process_previous_day_audio_data',
        'schedule': crontab(hour=2, minute=0),  # Daily at 2:00 AM
    }
}

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
