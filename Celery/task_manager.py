class Task_Manager:
    def __init__(self):
        self.wfm = None

    @staticmethod
    def task_complete(task):
        return task.ready()

    def delete_result(self, task):
        task_key = 'celery-task-meta-{}'.format(str(task))
        self.get_wfm().redis_helper.delete(task_key)

    def get_wfm(self):
        if not self.wfm:
            from Common.workflow_manager import get_workflow_manager
            self.wfm = get_workflow_manager()
        return self.wfm

    def get_result_and_clean(self, task):
        result = task.get()
        self.delete_result(task)
        return result

    def drop_results(self):
        self.get_wfm().redis_helper.drop_prefix('celery-task-meta-')