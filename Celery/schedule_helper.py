from Common.log_helper import Logger
from Common.generic_helper import Generic_Helper
log = Logger.log
import dill as pickle
from datetime import datetime, timedelta
from celery.schedules import crontab


class Schedule_Helper(Generic_Helper):
    def __init__(self):
        super(Schedule_Helper, self).__init__()
        self.RH = None
        self.prefix = self.config_helper.get('Task Scheduler', 'prefix', 'scheduled-task:')

    def get_RH(self):
        if not self.RH:
            from Common.workflow_manager import get_workflow_manager
            self.RH = get_workflow_manager().redis_helper
        return self.RH

    def get_schedules(self):
        for key in self.get_RH().r.scan_iter('celery-task-meta-*'):
            log.info(key.decode('utf-8'))
            log.info(self.get_RH().r.get(key))

    def generate_schedule_key(self, name):
        return '{}{}'.format(self.prefix, name)

    def change_task_state(self, name, value):
        key = self.generate_schedule_key(name)
        tasks = pickle.loads(self.get_RH().r.get(key))
        tasks['active'] = value
        self.get_RH().set_key(key, pickle.dumps(tasks))

    def add_scheduled_task(self, name, schedule_type, catchup, *args, function=None, **kwargs):
        from Common.workflow_manager import get_workflow_manager

        if function:
            get_workflow_manager().function_manager.upload_function(name=name, function=function)

        if not get_workflow_manager().function_manager.check_function_exists(name):
            raise Exception('Cannot add a scheduled task for a non-existent function')

        key = '{}{}'.format(self.prefix,name)

        if catchup:
            last_run = None
        else:
            last_run = datetime.utcnow()

        task = dict()
        task['name'] = name
        task['active'] = True
        task['args'] = args
        task['kwargs'] = kwargs
        task['schedule'] = schedule_type
        task['last_run'] = last_run
        task['run_count'] = 0
        task['next_run'] = None

        task_string = pickle.dumps(task)
        self.get_RH().r.set(key,task_string)
        log.info('Added a new registered task: {}'.format(name))

    def get_scheduled_tasks(self):
        tasks = self.get_RH().get_prefix(self.prefix)
        scheduled = dict()
        for task in tasks:
            _val = pickle.loads(self.get_RH().r.get(task))
            scheduled[_val['name']] = _val
        return scheduled

    def process_scheduled_tasks(self, scheduled):
        from Common.workflow_manager import get_workflow_manager
        now = datetime.utcnow()
        for item in scheduled.values():
            to_run = False
            # Calculate if we need to run
            if not item['active']:
                log.info('Task {} is disabled'.format(item['name']))
                continue
            if isinstance(item['schedule'], timedelta):
                if not item['last_run']:
                    to_run = True
                else:
                    time_since_last_run = now - item['last_run']
                    if time_since_last_run > item['schedule']:
                        to_run = True
                    log.info('We need to run {}'.format(item['name']))
            elif isinstance(item['schedule'], crontab):
                log.info('Processing crontab stuff')
            else:
                raise('Unknown schedule type')

            if to_run:
                get_workflow_manager().function_manager.execute_function_remote(item['name'], *item['args'],
                                                                                **item['kwargs'])
                item['last_run'] = now
                item['run_count'] += 1
                key = self.generate_schedule_key(item['name'])
                self.get_RH().set_key(key, pickle.dumps(item))

    def schedule_worker(self):
        candidate_tasks = self.get_scheduled_tasks()
        self.process_scheduled_tasks(candidate_tasks)


    def print_scheduled(self):
        tasks = self.get_scheduled_tasks()
        log.warning(tasks)
