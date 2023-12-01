# 作 者： Liuyaoqiu
# 日 期： 2023/11/30
import configparser
import json
import mysql.connector
import pandas as pd
from math import radians, cos, sin, asin, sqrt

#匹配率阈值，大于等于该阈值时，认为是找到匹配路线
MATCH_RATE_THRESHOLD = 0.95

# 读取配置文件
config = configparser.ConfigParser()
with open('../config.ini', 'r', encoding='utf-8') as configfile:
    config.read_file(configfile)

def haversine(lon1, lat1, lon2, lat2):
    """
    使用Haversine公式计算两点间距离。
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r

def calculate_route_coverage(via_stations_list):
    """
    获取每条线路的路径长度
    """
    total_distance = 0
    for i in range(len(via_stations_list) - 1):
        start_station = via_stations_list[i]
        end_station = via_stations_list[i + 1]
        distance = haversine(start_station['longitude'], start_station['latitude'],
                             end_station['longitude'], end_station['latitude'])
        total_distance += distance
    return total_distance

def get_db_config():
    return {
        'host': config['database']['host'],
        'user': config['database']['user'],
        'passwd': config['database']['passwd'],
        'database': config['database']['database']
    }

def get_route_data():
    """
    查数据库，获取所有路线和途经站点
    """
    db_config = get_db_config()
    with mysql.connector.connect(**db_config) as cnx:
        with cnx.cursor() as cursor:
            query = "SELECT route_name, via_stations FROM bus_routes"
            cursor.execute(query)
            return cursor.fetchall()

def process_route_data(route_data):
    """
    处理从数据库得到的路线数据，
    建立站点字典，包含各站点及关联路线；
    计算每条路线的总站点数量
    计算每条路线的路径长度
    """
    stations_dict = {}
    total_stations_per_route = {}
    route_coverage = {}
    # 遍历站点
    for route_name, via_stations in route_data:
        # 解析 JSON 数据
        via_stations_list = json.loads(via_stations)
        coverage = calculate_route_coverage(via_stations_list)
        route_coverage[route_name] = coverage

        # 遍历站点
        for station in via_stations_list:
            station_name = station['name']
            station_position = (station['longitude'], station['latitude'])

            # 如果站点不在字典中，则添加它
            if station_name not in stations_dict:
                stations_dict[station_name] = {
                    'position': station_position,
                    'routes': [route_name]
                }
            else:
                # 如果站点已存在，只需添加路线名称
                if route_name not in stations_dict[station_name]['routes']:
                    stations_dict[station_name]['routes'].append(route_name)

        # 计算并存储每条路线的总站点数
        total_stations_per_route[route_name] = len(via_stations_list)

    return stations_dict, total_stations_per_route, route_coverage

def calculate_route_match_rates(df, stations_dict, total_stations_per_route):
    """
    匹配所有关联的公交路线，并计算每个公交路线的匹配率
    :param df: 待匹配的轨迹位置
    :param stations_dict:各站点关联的路线
    :param total_stations_per_route:各路线站点数量
    :return:所有关联的公交路线，并计算每个公交路线的匹配率
    """
    # 初始化路线匹配次数字典和已匹配站点集合字典
    route_match_counts = {}
    matched_stations_per_route = {route_name: set() for route_name in total_stations_per_route}

    # 遍历轨迹中的每个位置点
    for index, row in df.iterrows():
        for station_name, data in stations_dict.items():
            station_position = data['position']
            if abs(row['经度'] - station_position[0]) < 0.01 and abs(row['纬度'] - station_position[1]) < 0.01:
                # 遍历每个站点关联的所有路线
                for route_name in data['routes']:
                    if station_name not in matched_stations_per_route[route_name]:
                        # 如果站点在路线的已匹配站点集合中不存在，增加匹配次数
                        route_match_counts[route_name] = route_match_counts.get(route_name, 0) + 1
                        # 将站点添加到路线的已匹配站点集合中
                        matched_stations_per_route[route_name].add(station_name)

    # 计算匹配率
    route_match_rates = {}
    for route_name, match_count in route_match_counts.items():
        total_stations = total_stations_per_route[route_name]
        route_match_rates[route_name] = match_count / total_stations
    return route_match_rates

def find_best_route(route_match_rates, route_coverage):
    if not route_match_rates:
        return None
    # 找出匹配率最高的路线
    top_match_rate = max(route_match_rates.values())
    top_matched_routes = [route for route, rate in route_match_rates.items() if rate == top_match_rate]
    # 从匹配率最高的路线中选择路径覆盖度最高的路线
    top_route = max(top_matched_routes, key=lambda route: route_coverage[route])
    return top_route

if __name__ == '__main__':
    #第1步、读取数据库，获取所有路线信息
    route_data = get_route_data()
    #第2步、数据预处理，解析各站点关联路线、每条路线站点总数、每条路线路径长度
    stations_dict, total_stations_per_route, route_coverage = process_route_data(route_data)

    #第31步、获取待匹配轨迹数据
    excel_path = config['filepath']['excel_path']
    df = pd.read_excel(excel_path, engine='openpyxl')

    #判断轨迹数据是否存在
    if len(df)!=0:
        df['经度'] = df['经度'].round(2)
        df['纬度'] = df['纬度'].round(2)
        df = df.drop_duplicates(subset=['经度', '纬度'])
        #第4步、计算获取所有关联路线
        route_match_rates = calculate_route_match_rates(df, stations_dict, total_stations_per_route)
        #第5步、基于所有关联路线，优选出最佳路线
        best_route = find_best_route(route_match_rates, route_coverage)

        if best_route:
            # 判断最佳路线是否满足匹配阈值
            if route_match_rates[best_route]>=MATCH_RATE_THRESHOLD:
                print(f"最优路线：{best_route}, 匹配率：{route_match_rates[best_route]:.2%}, 路径覆盖度：{route_coverage[best_route]:.2f}km")
            else:
                print("没有完全匹配的路线")
                print(f"最近似路线：{best_route}, 匹配率：{route_match_rates[best_route]:.2%}, 路径覆盖度：{route_coverage[best_route]:.2f}km")
        else:
            print("未找到匹配的路线")
    else:
        print("当日无驾驶轨迹")