
from celery import Celery
from wis2downloader.app import CONFIG

use_celery = CONFIG.get('use_celery', False)

# Ensure that the Celery app is initialized only if use_celery is True
if use_celery:
    celery_config = CONFIG.get('celery_config', {})
    # Check if the required configuration keys are present
    if 'CELERY_BROKER_URL' not in celery_config or 'CELERY_RESULT_BACKEND' not in celery_config:
        raise ValueError("CELERY_BROKER_URL and CELERY_RESULT_BACKEND must be set in the configuration.")

    # Define the broker and result backend URLs
    app = Celery(
        'wis2downloader',
        broker=celery_config.get('CELERY_BROKER_URL'),
        result_backend=celery_config.get('CELERY_RESULT_BACKEND'),
        include=['wis2downloader.tasks']
    )

    # Optional configuration
    app.conf.update(
        task_serializer=celery_config.get('task_serializer', 'json'),
        result_serializer=celery_config.get('result_serializer', 'json'),
        accept_content=celery_config.get('accept_content', ['json']),
        result_serializer= celery_config.get('result_serializer', 'json'),
        task_time_limit=celery_config.get('task_time_limit', 300),
        task_soft_time_limit=celery_config.get('task_soft_time_limit', 240),
        timezone= celery_config.get('timezone', 'UTC'),
        enable_utc= celery_config.get('enable_utc', True),
        worker_concurrency= celery_config.get('worker_concurrency', 4),
    )
