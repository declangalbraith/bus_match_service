# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

from config_loader import ConfigLoader
import os
import pandas as pd

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

#待修改，暂时使用读取excel获取vin关联订单和车型
def get_id_order_model(file_path):
    df = pd.read_excel(file_path, engine='openpyxl', usecols=['VIN','车牌', '订单号', '车型', '购车客户', '所属区域'])
    return df


#以下代码用来单独触发任务，项目正常运行时无需用到
# if __name__ == '__main__':
#     config, db_config = load_config()
#
#     data_fetcher = DataFetcher(config)
#     route_matcher = RouteMatcher(db_config)
#     task_manager = TaskManager(data_fetcher, route_matcher)
#
#     db_manager = DatabaseManager(db_config)
#
#     vehicle_ids = get_vehicle_ids(config['filepath']['excel_path'])
#     start_time = datetime.strptime('2023-12-12 06:00:00', "%Y-%m-%d %H:%M:%S")
#     end_time = datetime.strptime('2023-12-12 22:00:00', "%Y-%m-%d %H:%M:%S")
#
#     task_manager.manage_tasks(vehicle_ids, start_time, end_time,db_manager)

#测试坡度计算
# config, db_config = load_config()
# slopecacu = SlopeCacu(config, db_config)
# slope_result = slopecacu.process_route("627路(吴堡客运站--江都公交西站)")
# print(slope_result)
