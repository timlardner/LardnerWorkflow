from Common.config_helper import Config_Helper
from Redis.function_manager import Function_Manager
from Redis.redis_helper import Redis_Helper
from Rabbit.rabbit_helper import Rabbit_Helper
from Celery.task_manager import Task_Manager
from Celery.schedule_helper import Schedule_Helper

from Common.log_helper import Logger
log = Logger.log

class Workflow_Manager:
    def __init__(self):
        log.info('Initialised workflow manager')
        self.config_helper = Config_Helper()
        self.redis_helper = Redis_Helper()
        self.task_manager = Task_Manager()
        self.rabbit_helper = Rabbit_Helper()
        self.schedule_helper = Schedule_Helper()
        self.function_manager = Function_Manager()

def get_workflow_manager():
    global WFM
    if 'WFM' not in globals():
        from Common.workflow_manager import Workflow_Manager
        WFM = Workflow_Manager()
    return WFM


