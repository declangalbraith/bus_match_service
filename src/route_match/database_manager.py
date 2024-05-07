# 作 者： Liuyaoqiu
# 日 期： 2023/12/12

import mysql.connector
from mysql.connector import Error
import json

class DatabaseManager:
    def __init__(self, db_config, city):
        self.db_config = db_config
        self.city = city
        self.table_prefix = f"bus_routes_{city.lower()}"

    def get_table_name(self, table_type):
        return f"{self.table_prefix}_{table_type}"

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            return self.connection
        except Error as e:
            print(f"Error connecting to MySQL database: {e}")
            return None

    def insert_match_result(self, vin, matched_route, match_rate, route_coverage, date, city):
        table_name = "match_results" if city.lower() == "yangzhou" else f"match_results_{city.lower()}"
        try:
            connection = self.connect()
            cursor = connection.cursor()
            insert_query = f"""
                    INSERT INTO {table_name} (vin, matched_route, match_rate, route_coverage, date)
                    VALUES (%s, %s, %s, %s, %s)
                    """
            cursor.execute(insert_query, (vin, matched_route, match_rate, route_coverage, date))
            connection.commit()
            cursor.close()
            connection.close()
        except mysql.connector.Error as e:
            print(f"Error: {e}")

    def get_vins_by_order_or_model(self, query_value, query_type,vehicle_df):
        #需要查询库关联表，通过订单/车型查询响应的vin列表
        #先暂时用查询excel表代替
        vehicle_df['订单号'] = vehicle_df['订单号'].astype(str)
        if query_type == 'order':
            vins = vehicle_df[vehicle_df['订单号'] == query_value]['VIN'].tolist()
            print("查询的vin:",vins)
        elif query_type == 'model':
            vins = vehicle_df[vehicle_df['车型'] == query_value]['VIN'].tolist()
            print("查询的vin:", vins)
        else:
            vins = []

        return vins

    def get_match_results(self, start_date=None, end_date=None, vins=None, city='default'):
        if vins is not None and len(vins) == 0:
            return []

        table_name = f"match_results_{city.lower()}"
        query = f"SELECT * FROM {table_name}"
        conditions = []
        params = []

        # 检查并添加日期范围条件
        if start_date and end_date:
            conditions.append("date BETWEEN %s AND %s")
            params.extend([start_date, end_date])
        elif start_date:  # 只有起始日期
            conditions.append("date >= %s")
            params.append(start_date)
        elif end_date:  # 只有结束日期
            conditions.append("date <= %s")
            params.append(end_date)

        # 检查并添加VIN条件
        if vins:
            vin_placeholders = ', '.join(['%s'] * len(vins))
            conditions.append("vin IN (" + vin_placeholders + ")")
            params.extend(vins)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        try:
            connection = self.connect()
            if connection.is_connected():
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query, tuple(params))
                results = cursor.fetchall()
                cursor.close()
                connection.close()
                return results
        except Error as e:
            print(f"Error reading data from MySQL table: {e}")
            return []

    def insert_match_line_GPS(self,top_route,vehicle_history):
        vehicle_history = vehicle_history.rename(columns={'经度': 'longitude', '纬度': 'latitude'})
        vehicle_history['longitude'] = vehicle_history['longitude'].round(3)
        vehicle_history['latitude'] = vehicle_history['latitude'].round(3)
        # 删除重复的行
        vehicle_history = vehicle_history.drop_duplicates()
        vehicle_history_json = vehicle_history.to_dict(orient='records')
        vehicle_history_json_str = json.dumps(vehicle_history_json,ensure_ascii=False)

        #将匹配的路线插入数据表
        try:
            connection = self.connect()
            if connection.is_connected():
                cursor = connection.cursor()

                # 检查top_route是否已存在
                query_check = "SELECT COUNT(*) FROM bus_line_gps WHERE route_name = %s"
                cursor.execute(query_check, (top_route,))
                if cursor.fetchone()[0] == 0:
                    # 如果不存在，插入新记录
                    query_insert = """
                            INSERT INTO bus_line_gps (route_name, line_GPS)
                            VALUES (%s, %s)
                            """
                    cursor.execute(query_insert, (top_route, vehicle_history_json_str))
                    connection.commit()
                    print(f"Inserted {top_route} into bus_line_gps")
                else:
                    print(f"Route {top_route} already exists in bus_line_gps")
                cursor.close()
        except Error as e:
            print(f"Error interacting with MySQL: {e}")
        finally:
            if connection.is_connected():
                connection.close()

    def get_line_GPS(self, tableName = None,lineName=None):
        """根据线路名称查询GPS信息"""
        query = "SELECT * FROM "+self.table_prefix
        conditions = []
        params = []

        if lineName is not None:
            conditions.append("route_name = %s")
            params.append(lineName)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        try:
            connection = self.connect()
            if connection.is_connected():
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query, tuple(params))
                results = cursor.fetchall()
                cursor.close()
                connection.close()
                return results
        except Error as e:
            print(f"Error reading data from MySQL table: {e}")
            return []