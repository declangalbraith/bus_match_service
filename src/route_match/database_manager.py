# 作 者： Liuyaoqiu
# 日 期： 2023/12/12

import mysql.connector
from mysql.connector import Error

class DatabaseManager:
    def __init__(self, db_config):
        self.db_config = db_config

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            return self.connection
        except Error as e:
            print(f"Error connecting to MySQL database: {e}")
            return None

    def insert_match_result(self, vin, matched_route, match_rate, route_coverage, date):
        try:
            connection = self.connect()
            cursor = connection.cursor()
            insert_query = """
            INSERT INTO match_results (vin, matched_route, match_rate, route_coverage, date)
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

    def get_match_results(self, date=None, vins=None):
        if vins is not None and len(vins) == 0:
            return []

        query = "SELECT * FROM match_results"
        conditions = []
        params = []

        if date:
            conditions.append("date = %s")
            params.append(date)
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