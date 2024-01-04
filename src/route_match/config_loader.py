# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

import configparser

class ConfigLoader:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.db_config = self.get_db_config()

    def load_config(self, config_path):
        config = configparser.ConfigParser()
        with open(config_path, 'r', encoding='utf-8') as configfile:
            config.read_file(configfile)
        return config

    def get_db_config(self):
        """
        提取数据库配置信息
        """
        db_config = {
            'host': self.config['database']['host'],
            'user': self.config['database']['user'],
            'passwd': self.config['database']['passwd'],
            'database': self.config['database']['database']
        }
        return db_config
