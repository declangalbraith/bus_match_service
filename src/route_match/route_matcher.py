# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

import mysql.connector
import json
from math import radians, cos, sin, asin, sqrt
from database_manager import DatabaseManager

class RouteMatcher:
    def __init__(self, db_config):
        self.stations_dict, self.total_stations_per_route, self.route_coverage = self.load_route_data(db_config)
        self.db_manager = DatabaseManager(db_config)

    @staticmethod
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

    def calculate_route_coverage(self,via_stations_list):
        """
        获取每条线路的路径长度
        """
        total_distance = 0
        for i in range(len(via_stations_list) - 1):
            start_station = via_stations_list[i]
            end_station = via_stations_list[i + 1]
            distance = RouteMatcher.haversine(start_station['longitude'], start_station['latitude'],
                                 end_station['longitude'], end_station['latitude'])
            total_distance += distance
        return total_distance

    def load_route_data(self, db_config):
        with mysql.connector.connect(**db_config) as cnx:
            with cnx.cursor() as cursor:
                query = "SELECT route_name, via_stations FROM bus_routes"
                cursor.execute(query)
                results = cursor.fetchall()

        stations_dict = {}
        total_stations_per_route = {}
        route_coverage = {}
        # 遍历站点
        for route_name, via_stations in results:
            # 解析 JSON 数据
            via_stations_list = json.loads(via_stations)
            coverage = self.calculate_route_coverage(via_stations_list)
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

    def match_route(self, vehicle_history):
        """
            匹配所有关联的公交路线，并计算每个公交路线的匹配率
            :param df: 待匹配的轨迹位置
            :param stations_dict:各站点关联的路线
            :param total_stations_per_route:各路线站点数量
            :return:所有关联的公交路线，并计算每个公交路线的匹配率
            """
        # 初始化路线匹配次数字典和已匹配站点集合字典
        route_match_counts = {}
        matched_stations_per_route = {route_name: set() for route_name in self.total_stations_per_route}

        # 遍历轨迹中的每个位置点
        for index, row in vehicle_history.iterrows():
            for station_name, data in self.stations_dict.items():
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
            total_stations = self.total_stations_per_route[route_name]
            route_match_rates[route_name] = match_count / total_stations

        if not route_match_rates:
            return None,None,None
        # 找出匹配率最高的路线
        top_match_rate = max(route_match_rates.values())
        top_matched_routes = [route for route, rate in route_match_rates.items() if rate == top_match_rate]
        # 从匹配率最高的路线中选择路径覆盖度最高的路线
        top_route = max(top_matched_routes, key=lambda route: self.route_coverage[route])

        #将匹配率达到0.9的路线轨迹保存
        if top_match_rate>=0.9:
            self.db_manager.insert_match_line_GPS(top_route,vehicle_history)

        return top_route,route_match_rates,self.route_coverage
