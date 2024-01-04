# 作 者： Liuyaoqiu
# 日 期： 2024/1/4

import pandas as pd
import math
import datetime
from scipy.spatial import cKDTree
from database_manager import DatabaseManager
import json

class PositionUtil:
    EARTH_RADIUS = 6378.137  # 地球半径（单位：千米）

    @staticmethod
    def rad(d):
        """ 角度转弧度 """
        return d * math.pi / 180.0

    @staticmethod
    def get_distance(lat1, lng1, lat2, lng2):
        """ 计算两个GPS坐标点之间的距离 """
        rad_lat1 = PositionUtil.rad(lat1)
        rad_lat2 = PositionUtil.rad(lat2)
        a = rad_lat1 - rad_lat2
        b = PositionUtil.rad(lng1) - PositionUtil.rad(lng2)
        s = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                    math.cos(rad_lat1) * math.cos(rad_lat2) * math.pow(math.sin(b / 2), 2)))
        s = s * PositionUtil.EARTH_RADIUS
        s = round(s * 10000) / 10000  # 米转千米
        return s

    @staticmethod
    def linear_plot(a1, a2, num):
        """ 线性插值 """
        linear_arr = []
        d = (a2 - a1) / num
        for i in range(num):
            linear_arr.append(a1 + d * i)
        return linear_arr

def get_b(x, y, j):
    """ 使用最小二乘法计算线性回归的斜率（b值） """
    xp = sum(x[:j]) / j
    yp = sum(y[:j]) / j

    zpp = sum((xi - xp) * (yi - yp) for xi, yi in zip(x[:j], y[:j]))
    xxp = sum((xi - xp) ** 2 for xi in x[:j])

    b = zpp / xxp if xxp != 0 else 0
    return b

class SlopeCacu:
    def __init__(self, config, db_config):
        self.config = config
        self.db_config = db_config

    def get_route_data(self, line_name):
        db_client = DatabaseManager(self.db_config)
        line_GPS_data = db_client.get_line_GPS("bus_routes", lineName=line_name)
        gps_col = 'via_stations'

        lngArr, latArr = self.parse_gps_data(line_GPS_data, gps_col)
        return lngArr, latArr

    def parse_gps_data(self, gps_data, gps_col):
        lngArr, latArr = [], []
        for entry in gps_data:
            via_stations = json.loads(entry[gps_col])
            for station in via_stations:
                if station['longitude'] != 0 and station['latitude'] != 0:
                    lngArr.append(station['longitude'])
                    latArr.append(station['latitude'])
        return lngArr, latArr

    def get_map(self,lngArr,latArr):
        # 计算经纬度的最大最小值
        min_lng, max_lng = min(lngArr), max(lngArr)
        min_lat, max_lat = min(latArr), max(latArr)

        col_data = pd.read_parquet(self.config['filepath']['parquet_path'], columns=['经度', '纬度', '高程'])
        # 筛选落在矩形区域内的点
        filtered_data = col_data[(col_data['经度'] >= min_lng) & (col_data['经度'] <= max_lng) &
                                 (col_data['纬度'] >= min_lat) & (col_data['纬度'] <= max_lat)]

        lngArr1 = filtered_data['经度'].tolist()  # 列表C
        latArr1 = filtered_data['纬度'].tolist()  # 列表D
        altitudeArr1 = filtered_data["高程"].tolist()  # 列表E

        return lngArr1,latArr1,altitudeArr1

    def interpolate_and_match(self, lngArr, latArr, lngArr1,latArr1,altitudeArr1):
        lngArrPlot = []  # 列表A1
        latArrPlot = []  # 列表B1

        for i in range(len(lngArr) - 1):
            lng1 = float(lngArr[i])
            lat1 = float(latArr[i])
            lng2 = float(lngArr[i + 1])
            lat2 = float(latArr[i + 1])

            dist1 = PositionUtil.get_distance(lat1, lng1, lat2, lng2)

            PLOTDISTANCE = 0.05
            if dist1 > PLOTDISTANCE:
                num = int(dist1 / PLOTDISTANCE)
                latPlot = PositionUtil.linear_plot(lat1, lat2, num)
                lngPlot = PositionUtil.linear_plot(lng1, lng2, num)

                lngArrPlot.extend(lngPlot)
                latArrPlot.extend(latPlot)
            elif dist1 != 0:
                lngArrPlot.append(lng1)
                latArrPlot.append(lat1)

        lngArrPlot.append(lng2)
        latArrPlot.append(lat2)

        # 高程匹配
        # 使用 k-d树进行高程匹配
        lng_lat_arr1 = list(zip(lngArr1, latArr1))  # 将经度和纬度合并为元组列表
        tree = cKDTree(lng_lat_arr1)  # 创建 k-d树

        newAltitudeArr = []
        for lng, lat in zip(lngArrPlot, latArrPlot):
            _, index = tree.query((lng, lat), k=1)  # 查询最近的点
            newAltitudeArr.append(altitudeArr1[index])

        return lngArrPlot, latArrPlot,newAltitudeArr

    def calculate_slope(self, lngArrPlot, latArrPlot, newAltitudeArr):
        # 每x米线性拟合一次坡度
        PLOT_DISTANCE = 100
        PLOT_NUM = 4
        miles = 0
        sumD = 0
        gradArr = []
        mileArr = []
        gpsArr = []
        x = []
        y = []
        slope_result = []

        for i in range(len(latArrPlot) - 1):
            lng1 = lngArrPlot[i]
            lat1 = latArrPlot[i]
            lng2 = lngArrPlot[i + 1]
            lat2 = latArrPlot[i + 1]
            dist1 = PositionUtil.get_distance(lat1, lng1, lat2, lng2)
            miles += dist1
            x.append(miles * 1000)
            y.append(newAltitudeArr[i + 1])
            sumD += dist1 * 1000

            if sumD > PLOT_DISTANCE and len(x) > PLOT_NUM:
                f = get_b(x, y, len(x))
                gradArr.append(f)
                mileArr.append(miles)
                gpsArr.append(f"{lng2},{lat2}")
                x = []
                y = []
                sumD = 0

        # 输出阈值坡度及相关信息
        gradArr1 = []
        mileArr1 = []
        gpsArr1 = []
        preMile = 0
        gradMax = 0
        # print("最大坡度 最大坡度距离 当前平均坡度 持续距离 起始距离 结束距离 GPS位置")
        # print("最大坡度 持续距离 GPS位置")
        for i in range(1, len(gradArr)):
            GRAD = 0.03
            if gradArr[i] * gradArr[i - 1] < 0:
                if gradMax >= GRAD or gradMax <= -GRAD:
                    index = gradArr.index(gradMax)
                    gradMaxMile = mileArr[index] - mileArr[index - 1]
                    gradSum = gradArr1[0] * (mileArr1[0] - preMile)
                    distance = mileArr[i - 1] - preMile
                    for j in range(1, len(mileArr1)):
                        gradSum += (mileArr1[j] - mileArr1[j - 1]) * gradArr1[j]
                    # print(gradMax, distance,gpsArr[index])
                    slope_result.append(["坡度:"+str(gradMax), "持续距离:"+str(distance),"GPS:"+gpsArr[index]])

                gradArr1 = []
                mileArr1 = []
                gpsArr1 = []
                gradMax = 0
                preMile = mileArr[i - 1]

            gradArr1.append(gradArr[i])
            mileArr1.append(mileArr[i])
            gpsArr1.append(gpsArr[i])
            if abs(gradMax) < abs(gradArr[i]):
                gradMax = gradArr[i]

        return slope_result

    def process_route(self, line_name):
        lngArr, latArr = self.get_route_data(line_name)
        lngArr1,latArr1,altitudeArr1=self.get_map(lngArr, latArr)
        lngArrPlot, latArrPlot,newAltitudeArr = self.interpolate_and_match(lngArr, latArr,lngArr1,latArr1,altitudeArr1)
        slope_result = self.calculate_slope(lngArrPlot, latArrPlot, newAltitudeArr)
        return slope_result
