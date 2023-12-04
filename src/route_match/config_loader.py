# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

import configparser

class ConfigLoader:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)

    def load_config(self, config_path):
        config = configparser.ConfigParser()
        with open(config_path, 'r', encoding='utf-8') as configfile:
            config.read_file(configfile)
        return config

