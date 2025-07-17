from celery import shared_task
import time
import logging
logger = logging.getLogger(__name__)

@shared_task
def add(x, y):
    logger.info(f"Adding {x} and {y}")
    time.sleep(5)
    print(x+y)
    return x + y 