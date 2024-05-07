# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

from config_loader import ConfigLoader
import os
import pandas as pd
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from route_matcher import RouteMatcher
from task_manager import TaskManager
from database_manager import DatabaseManager
from collections import defaultdict

def load_config():
    """加载配置文件并返回数据库配置和SDK配置"""
    current_file_path = os.path.abspath(__file__)
    current_dir_path = os.path.dirname(current_file_path)
    parent_parent_dir_path = os.path.dirname(os.path.dirname(current_dir_path))
    config_path = os.path.join(parent_parent_dir_path, 'config.ini')

    config_loader = ConfigLoader(config_path)
    config = config_loader.config
    db_config = config_loader.db_config

    return config, db_config

def get_vehicle_ids(file_path):
    """从给定的Excel文件路径读取车辆ID列表"""
    df = pd.read_excel(file_path, engine='openpyxl')
    return df['VIN']


def get_vehicle_ids_by_city(excel_path):
    df = pd.read_excel(excel_path)
    vehicle_ids_city = pd.Series(df.city.values, index=df.VIN).to_dict()

    return vehicle_ids_city

#待修改，暂时使用读取excel获取vin关联订单和车型
def get_id_order_model(file_path):
    df = pd.read_excel(file_path, engine='openpyxl', usecols=['VIN','车牌', '订单号', '车型', '购车客户', '所属区域', 'city'])
    return df


#以下代码用来单独触发任务，项目正常运行时无需用到
# if __name__ == '__main__':
#     config, db_config = load_config()
#
#     data_fetcher = DataFetcher(config)
#     vehicle_ids_city = get_vehicle_ids_by_city(config['filepath']['excel_path'])
#
#     # vehicle_ids_city = {
#     #     # 'LJSKB8PP0KD005707': 'yangzhou',
#     #     # 'LJSKB8PP2KD005661': 'yangzhou',
#     #     'LJSKB8KX4ND001865': 'chongqing',
#     #     'LJSKB8KXXPD000609': 'chongqing',
#     # }
#
#     start_time = datetime.strptime('2024-04-05 06:00:00', "%Y-%m-%d %H:%M:%S")
#     end_time = datetime.strptime('2024-04-05 22:00:00', "%Y-%m-%d %H:%M:%S")
#
#     city_vehicle_ids = defaultdict(list)
#     for vehicle_id, city in vehicle_ids_city.items():
#         city_vehicle_ids[city].append(vehicle_id)
#
#     for city, vehicle_ids in city_vehicle_ids.items():
#         db_manager = DatabaseManager(db_config, city)
#         route_matcher = RouteMatcher(db_config, city)
#         task_manager = TaskManager(data_fetcher, route_matcher)
#         task_manager.manage_tasks(vehicle_ids, start_time, end_time, db_manager, city)

