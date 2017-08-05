from Common.utils import singleton
import logging
import logging.handlers

@singleton
class Logger():
    def __init__(self):
        self.log = self.setup_custom_logger('wfm')
        self.log.setLevel(logging.WARNING)


    def setup_custom_logger(self, name):
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

        handler = logging.StreamHandler()
        f_handler = logging.FileHandler(filename='../Log/Workflow.log')
        handler.setFormatter(formatter)
        f_handler.setFormatter(formatter)
        f_handler.setLevel(logging.DEBUG)
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(f_handler)
        logger.addHandler(handler)
        return logger