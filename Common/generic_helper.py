from Common.config_helper import Config_Helper

class Generic_Helper:
    def __init__(self):
        self.config_helper = Config_Helper()

    def get_celery_url(self):
        return None