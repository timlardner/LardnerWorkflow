from Common.workflow_manager import get_workflow_manager
from Common.log_helper import Logger
from datetime import timedelta
log = Logger.log

def add_three_numbers(x, y, z):
    from Common.log_helper import Logger
    log = Logger.log
    log.warning(x+y+z)

if __name__ == '__main__':
    wfm = get_workflow_manager()

    args = (2,3,4)
    kwargs = dict()
    wfm.schedule_helper.add_scheduled_task('add_three_numbers', timedelta(seconds=5), True, *args, function=add_three_numbers, **kwargs)