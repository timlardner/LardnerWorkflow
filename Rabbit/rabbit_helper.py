from Common.generic_helper import Generic_Helper

class Rabbit_Helper(Generic_Helper):
    def __init__(self):
        super(Rabbit_Helper, self).__init__()
        self.username = self.config_helper.get('Rabbit','username','admin')
        self.password = self.config_helper.get('Rabbit','password','mypass')
        self.host = self.config_helper.get('Rabbit','host','localhost')

    def get_celery_url(self):
        return 'amqp://{}:{}@{}//'.format(self.username,self.password,self.host)