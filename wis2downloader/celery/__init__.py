from celery import Celery

# Configure your Celery app
app = Celery('wis2downloader',
             broker='redis://localhost:6379/0',  # or 'amqp://guest:guest@localhost:5672//' for RabbitMQ
             backend='redis://localhost:6379/1', # or 'rpc://' for RabbitMQ
             include=['wis2downloader.tasks'])

# Optional: Configuration for tasks
app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
    result_expires=3600,
)

# if __name__ == '__main__':
#     app.start()