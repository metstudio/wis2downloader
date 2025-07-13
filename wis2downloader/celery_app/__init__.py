
from celery import Celery
from wis2downloader.app import CONFIG

use_celery = CONFIG.get('use_celery', False)

# Ensure that the Celery app is initialized only if use_celery is True
if use_celery:
    # Check if the required configuration keys are present
    if 'CELERY_BROKER_URL' not in CONFIG or 'CELERY_RESULT_BACKEND' not in CONFIG:
        raise ValueError("CELERY_BROKER_URL and CELERY_RESULT_BACKEND must be set in the configuration.")

    # Define the broker and result backend URLs
    app = Celery(
        'wis2downloader',
        broker=CONFIG.get('CELERY_BROKER_URL'),
        result_backend=CONFIG.get('CELERY_RESULT_BACKEND'),
        include=['wis2downloader.tasks']
    )

    # Optional configuration
    app.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
    )
