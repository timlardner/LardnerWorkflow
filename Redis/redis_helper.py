import redis
from Common.generic_helper import Generic_Helper
from Common.log_helper import Logger
log = Logger.log

class Redis_Helper(Generic_Helper):
    def __init__(self):
        super(Redis_Helper, self).__init__()
        self.host = self.config_helper.get('Redis', 'host', 'localhost')
        self.port = self.config_helper.get('Redis', 'port', '6379')
        self.db = self.config_helper.get('Redis', 'db', '0')
        self.r = self._init_redis_connection()

    def get_redis_connection(self):
        if not self.r:
            self.r = self._init_redis_connection()
        return self.r

    def _init_redis_connection(self):
        return redis.StrictRedis(host=self.host, port=self.port, db=self.db)

    def print_keys(self):
        log.info('='*60)
        log.info('Printing all Redis keys...')
        for key in sorted(self.r.scan_iter()):
            log.info(key.decode('utf-8'))
        log.info('=' * 60)

    def set_key(self, key, value):
        self.r.set(key, value)

    def clear_keys(self):
        for key in self.r.scan_iter():
            self.r.delete(key)

    def delete(self, key):
        self.r.delete(key)

    def drop_prefix(self, prefix):
        for key in self.r.scan_iter(prefix+'*'):
            self.r.delete(key)

    def get_prefix(self, prefix):
        keys = []
        for key in self.r.scan_iter(prefix+'*'):
            keys.append(key)
        return keys

    def insert_generic(self, key, value):
        log.info('Inserting into {}'.format(key))
        log.info(value)
        self.r.set(key, value)

    def get_celery_url(self):
        return 'redis://{}:{}/{}'.format(self.host, self.port, self.db)