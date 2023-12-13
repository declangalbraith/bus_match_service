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

    def get_match_results(self, date=None, vin=None):
        results = []
        query = "SELECT * FROM match_results"
        conditions = []

        if date:
            conditions.append(f"date = '{date}'")
        if vin:
            conditions.append(f"vin = '{vin}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        try:
            connection = self.connect()
            if connection.is_connected():
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                connection.close()
        except Error as e:
            print(f"Error reading data from MySQL table: {e}")
        return results
