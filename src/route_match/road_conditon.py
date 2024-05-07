# 作 者： Liuyaoqiu
# 日 期： 2024/1/10

from database_manager import DatabaseManager
import json
from slope_cacu import SlopeCacu

class RoadCondition:
    def __init__(self, config, db_config,col_data):
        self.config = config
        self.db_config = db_config
        self.col_data = col_data

    def get_line_GPS(self, city_name,line_name):
        slopecacu = SlopeCacu(self.config, self.db_config,self.col_data)
        lngArr, latArr = slopecacu.get_route_data(city_name,line_name)
        return lngArr, latArr

    def get_route_data(self, line_name, city):
        db_client = DatabaseManager(self.db_config, city)
        line_GPS_data = db_client.get_line_GPS("bus_routes", lineName=line_name)

        line_GPS_data = line_GPS_data[0]

        gps_col = 'via_stations'
        via_stations = json.loads(line_GPS_data[gps_col])
        return via_stations