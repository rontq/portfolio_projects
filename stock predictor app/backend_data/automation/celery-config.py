from celery import Celery
from celery.schedules import crontab

# Initialize Celery app
app = Celery('data_fetch', broker='redis://localhost:6379/0')

# Celery configuration
app.conf.update(
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
)

# Periodic task configuration
app.conf.beat_schedule = {
    'update-tasks-daily': {
        'task': 'tasks.update_all_tasks',  # The name of the task you want to schedule
        'schedule': crontab(hour=17, minute=0, day_of_week='mon-fri'),  # Run at 5:00 PM PST (hour 17 in 24-hour format)
        'args': (True, None),  # Arguments for the task (force_update=True, start_date=None)
    },
}

# Timezone configuration
app.conf.timezone = 'US/Pacific'  # Set timezone to PST (Pacific Time)
