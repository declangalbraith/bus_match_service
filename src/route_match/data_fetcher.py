# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

import requests
import pandas as pd
import json

class DataFetcher:
    def __init__(self, config):
        self.config = config
        self.authorization = self.get_authorization()

    def get_authorization(self):
        """获取SDK接口调用的鉴权"""
        params = {
            "username": self.config['SDK']['USERNAME'],
            "password": self.config['SDK']['PASSWORD']
        }
        response = requests.post(self.config['SDK']['LOGIN_URL'], data=params)
        if response.status_code != 200:
            raise Exception("获取Authorization失败")
        data = json.loads(response.text)["data"]
        return data["token_type"] + " " + data["access_token"]

    @staticmethod
    def get_get_response_body_json(param_map, url, authorization):
        """
        调用SDK接口获取数据
        :param param_map: 用户名、密码
        :param url: url
        :param authorization: token
        :return: 获取数据
        """
        real_url = url
        if param_map:
            real_url += "?"
            for key, value in param_map.items():
                real_url += f"{key}={value}&"
            real_url = real_url[:-1]  # Remove the last "&" character
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "Authorization": authorization
        }
        try:
            response = requests.get(real_url, headers=headers)
            if response.status_code != 200:
                print(f"调用接口：{real_url} 失败")
                return None
            return json.loads(response.text)
        except Exception as e:
            print(f"调用接口：{real_url} 失败: {str(e)}")
            return None

    def fetch_vehicle_history(self,vin, start_time, end_time):
        authorization = self.get_authorization()
        timestamp_start = int(start_time.timestamp() * 1000)
        timestamp_end = int(end_time.timestamp() * 1000)
        params = {
            "vin": vin,
            "timeStar": timestamp_start,
            "timeEnd": timestamp_end
        }

        url = self.config['SDK']['GET_URL']

        # 调用获取国标历史数据的方法
        data = DataFetcher.get_get_response_body_json(params, url, authorization)

        # 检查data是否为None
        if data is None or 'data' not in data or 'gbDataList' not in data['data']:
            print(f"获取数据失败或数据格式不正确: {vin}")
            return pd.DataFrame()

        # 提取指定字段的数据行
        rows = []
        for item in data["data"]["gbDataList"]:
            row = [item["MDT_PO_LON"], item["MDT_PO_LAT"]]
            rows.append(row)
        # 将结果转换为dataFrame格式
        df = pd.DataFrame(rows, columns=["经度", "纬度"])
        # 将结果元素转换为数值
        for column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        df = df.dropna()

        print(f"数据获取成功：{vin}")
        return df
