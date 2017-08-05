import json
from celery import Celery
from celery.schedules import crontab
from datetime import timedelta

from Common.workflow_manager import get_workflow_manager

wfm = get_workflow_manager()

CELERY = Celery('Celery.tasks',
                backend=wfm.redis_helper.get_celery_url(),
                broker=wfm.rabbit_helper.get_celery_url())

CELERY.conf.accept_content = ['json', 'msgpack']
CELERY.conf.result_serializer = 'msgpack'
CELERY.conf.update(
    result_expires = 60,
    beat_schedule = {
        'check_schedule': {
            'task': 'Celery.tasks.check_schedule',
            'schedule': timedelta(seconds=1)
        },
    }
)

@CELERY.task()
def execute_function(id, *args, **kwargs):
    return wfm.function_manager.execute_function(id, *args, **kwargs)

@CELERY.task()
def add(x, y):
    return x+y

@CELERY.task()
def check_schedule():
    wfm.schedule_helper.schedule_worker()
