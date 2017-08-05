import dill as pickle

from Common.log_helper import Logger
from Redis.redis_helper import Redis_Helper
log = Logger.log


class Function_Manager(Redis_Helper):
    def __init__(self):
        super(Function_Manager, self).__init__()
        self.prefix = self.config_helper.get('Function Manager','prefix', 'function:')
        self.r = self.get_redis_connection()
        self.allow_overwrite = self.config_helper.get_boolean('Function Manager','allow_overwrite',True)

    def print_all_functions(self):
        for key in self.r.scan_iter(self.prefix):
            log.info(key.decode('utf-8'))

    def drop_functions(self):
        self.drop_prefix('function:')

    def check_function_exists(self, name):
        return self.r.exists('{}{}'.format(self.prefix, name))

    def upload_function(self, name=None, function=lambda x: x):
        if not name:
            name = function.__name__
            log.warning('No name defined. Using default function name: {}'.format(name))
        flat_function = pickle.dumps(function)
        function_name = '{}{}'.format(self.prefix, name)
        if self.r.exists(function_name) and not self.allow_overwrite:
            log.error('Unable to set "{}". Key exists'.format(function_name))
            return
        self.r.set(function_name, flat_function)
        log.info('Registered new function with name: {}'.format(function_name))

    def download_function(self, name):
        function_name = '{}{}'.format(self.prefix, name)
        return pickle.loads(self.r.get(function_name))

    def execute_function(self, name, *args, **kwargs):
        function_name = '{}{}'.format(self.prefix,name)
        if not self.r.exists(function_name):
            raise FunctionException('The function does not exist in the cache')
        f = pickle.loads(self.r.get(function_name))
        return f(*args,**kwargs)

    def execute_function_remote(self, name, *args, **kwargs):
        from Celery import tasks
        return tasks.execute_function.delay(name, *args, **kwargs)


class FunctionException(Exception):
    def __init__(self, *args, **kwargs):
        super(FunctionException, self).__init__(*args, **kwargs)
