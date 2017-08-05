import configparser


class Config_Helper:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('Config/config.ini')

    def get(self, section, property, default=None):
        return self.config.get(section,property,fallback=default)

    def get_boolean(self, section, property, default):
        value = self.config.get(section,property,fallback=default)
        if isinstance(value, bool):
            return value
        else:
            return value.lower() == 'true'
