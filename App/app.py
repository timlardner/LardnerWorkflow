from Common.workflow_manager import get_workflow_manager
from Common.log_helper import Logger
from celery.schedules import crontab
from datetime import timedelta
import json

log = Logger.log

def test_schedule():
    wfm = get_workflow_manager()
    fm = wfm.function_manager
    tm = wfm.task_manager

def add_scheduled_task():
    wfm = get_workflow_manager()
    name = 'add_two_numbers'
    args = (4,5)
    kwargs = dict()
    schedule = timedelta(seconds=30)
    wfm.schedule_helper.add_scheduled_task(name,schedule,True, *args,**kwargs)

def get_tasks():
    wfm = get_workflow_manager()
    return wfm.schedule_helper.get_scheduled_tasks()

def do_tasks(tasks):
    wfm = get_workflow_manager()
    wfm.schedule_helper.process_scheduled_tasks(tasks)

if __name__ == '__main__':
    #add_scheduled_task()
    #tasks = get_tasks()
    #do_tasks(tasks)
    wfm = get_workflow_manager()
    #wfm.schedule_helper.change_task_state('add_two_numbers', False)
    wfm.schedule_helper.print_scheduled()

