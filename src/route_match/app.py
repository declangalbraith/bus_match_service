# 作 者： Liuyaoqiu
# 日 期： 2023/12/12
# app.py

from flask import Flask, request, jsonify
from database_manager import DatabaseManager
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from main import load_config, get_vehicle_ids_by_city,get_id_order_model
from route_matcher import RouteMatcher
from task_manager import TaskManager
from slope_cacu import SlopeCacu
from road_conditon import RoadCondition
import pandas as pd
from collections import defaultdict
import json
import re
from pypinyin import pinyin, Style

app = Flask(__name__)
scheduler = BackgroundScheduler()

#配置初始化
config, db_config = load_config()

#高程图初始化加载

with open('D:/Users/liuya/matchBusRoute/elevation_config.json', 'r', encoding='utf-8') as file:
    elevation_config = json.load(file)

#加载每个城市的高程数据
CITY_DATABASE = {}
for city, info in elevation_config.items():
    CITY_DATABASE[city] = pd.read_parquet(info['parquet_path'], columns=['经度', '纬度', '高程'])

def scheduled_task():
    """
    定时任务，每日凌晨2点开始匹配车辆清单中的公交路线信息,并将结果保存入库
    :return:
    """
    try:
        # config, db_config = load_config()
        data_fetcher = DataFetcher(config)

        #每次任务前先更新车辆信息
        # data_fetcher.fetch_and_save_vehicle_info()

        # route_matcher = RouteMatcher(db_config)
        # task_manager = TaskManager(data_fetcher, route_matcher)
        # db_manager = DatabaseManager(db_config)

        # vehicle_ids = get_vehicle_ids(config['filepath']['excel_path'])
        vehicle_ids_city = get_vehicle_ids_by_city(config['filepath']['excel_path'])

        # 设定结束时间为前一天的22:00
        end_time = datetime.now().replace(hour=22, minute=0, second=0, microsecond=0)- timedelta(days=1)
        # 设定开始时间为前一天的10:00
        start_time = end_time.replace(hour=8, minute=0, second=0)
        print("Start scheduled task-------------------------------------------")
        # task_manager.manage_tasks(vehicle_ids, start_time, end_time, db_manager)
        city_vehicle_ids = defaultdict(list)
        for vehicle_id, city in vehicle_ids_city.items():
            city_vehicle_ids[city].append(vehicle_id)

        for city, vehicle_ids in city_vehicle_ids.items():
            db_manager = DatabaseManager(db_config, city)
            route_matcher = RouteMatcher(db_config, city)
            task_manager = TaskManager(data_fetcher, route_matcher)
            task_manager.manage_tasks(vehicle_ids, start_time, end_time, db_manager, city)
        print("Scheduled task completed successfully.")
    except Exception as e:
        print(f"Error during scheduled task: {e}")

scheduler.add_job(func=scheduled_task, trigger='cron', hour=2, minute=0)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

#请求路线匹配的路由
@app.route('/matchresults', methods=['GET'])
def get_match_results():
    """
    请求查询，支持通过订单号、车型或者单车VIN进行检索
    :return:
    """
    # 默认和最大日期范围
    DEFAULT_DATE_RANGE = 30  # 天
    MAX_DATE_RANGE = 60  # 天

    start_date = request.args.get('start_date')  # 起始日期
    end_date = request.args.get('end_date')  # 结束日期
    page = int(request.args.get('page', 1))  # 当前页码，默认为第一页
    per_page = int(request.args.get('per_page', 10))  # 每页显示的记录数，默认为10

    # 如果没有提供日期参数，则使用默认的日期范围
    if not start_date or not end_date:
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=DEFAULT_DATE_RANGE)
    else:
        # 如果提供的日期是字符串，则进行转换
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # 校验日期范围
    if (end_date - start_date).days > MAX_DATE_RANGE:
        # return jsonify({'error': '查询时间范围不能超过60天'}), 400
        return jsonify({'status': 400, 'error': '查询时间范围不能超过60天'}), 400

    # 至少需要一个其他查询参数
    if not any([request.args.get(param) for param in ['vin', 'model', 'order', 'customer', 'city']]):
        # return jsonify({'error': '至少填写1个查询条件'}), 400
        return jsonify({'status': 400, 'error': '至少填写1个查询条件'}), 400

    try:
        vehicle_df = get_id_order_model(config['filepath']['excel_path'])

        # 根据提供的参数进一步筛选数据集
        vin_or_plate = request.args.get('vin')
        # 根据VIN或车牌号进行筛选,支持模糊匹配
        # if vin_or_plate:
        #     vehicle_df = vehicle_df[(vehicle_df['VIN'] == vin_or_plate) | (vehicle_df['车牌'] == vin_or_plate)]
        if vin_or_plate:
            vehicle_df = vehicle_df[vehicle_df['VIN'].str.contains(vin_or_plate, na=False, case=False)
                                    | vehicle_df['车牌'].str.contains(vin_or_plate, na=False, case=False)]
        if 'model' in request.args:
            vehicle_df = vehicle_df[vehicle_df['车型'] == request.args.get('model')]
        if 'order' in request.args:
            vehicle_df = vehicle_df[vehicle_df['订单号'] == int(request.args.get('order'))]
        if 'customer' in request.args:
            vehicle_df = vehicle_df[vehicle_df['购车客户'] == request.args.get('customer')]
        if 'city' in request.args:
            vehicle_df = vehicle_df[vehicle_df['所属区域'] == request.args.get('city')]

        # 如果筛选后没有任何记录，则返回错误
        if vehicle_df.empty:
            # return jsonify({'error': '没有搜索到匹配车辆'}), 404
            return jsonify({'status': 404, 'error': '没有搜索到匹配车辆'}), 404

        vins = vehicle_df['VIN'].tolist()
        cities = vehicle_df['city'].tolist()
        # results = db_manager.get_match_results(start_date=start_date, end_date=end_date, vins=vins)
        city_vehicle_ids = defaultdict(list)
        for vin, city in zip(vins, cities):
            city_vehicle_ids[city].append(vin)
        results = []
        for city, city_vins in city_vehicle_ids.items():
            db_manager = DatabaseManager(db_config,city)
            city_results = db_manager.get_match_results(start_date=start_date, end_date=end_date, vins=city_vins,city=city)
            results.extend(city_results)

        total_counts = len(results)

        if results:
            results_df = pd.DataFrame(results)
            results_df = results_df.rename(columns={'vin': 'VIN'})

            # total_counts = len(results_df)

            # 分页查询
            start = (page - 1) * per_page
            end = start + per_page
            paged_results_df = results_df.iloc[start:end]

            final_df = pd.merge(
                vehicle_df,
                paged_results_df,
                on='VIN',
                how='inner'
            )

            final_json_list = final_df.to_dict(orient='records')
            return jsonify({'status': 200, 'data': final_json_list, 'totalcounts': total_counts})
        else:
            filtered_vehicle_df = vehicle_df.copy()
            filtered_vehicle_df['date'] = pd.NA
            filtered_vehicle_df['match_rate'] = pd.NA
            filtered_vehicle_df['matched_route'] = pd.NA
            filtered_vehicle_df['route_coverage'] = pd.NA
            return jsonify({'status': 200, 'data': filtered_vehicle_df.to_dict(orient='records'), 'totalcounts': total_counts})
    except Exception as e:
        # return jsonify({'error': str(e)}), 500
        return jsonify({'status': 500, 'error': str(e)}), 500

#请求坡度计算的路由
@app.route('/calculateslope', methods=['GET'])
def calculate_slope():

    city_name = request.args.get('city')
    route_name = request.args.get('route_name')

    if not city_name:
        return jsonify({'error': 'city_name is required'}), 400

    # 检查 city_name 是否在 CITY_DATABASE 中
    if city_name not in CITY_DATABASE:
        return jsonify({'status': 400,'error': '该地区暂未录入高程库'}), 400

    if not route_name:
        return jsonify({'status': 400,'error': 'Route name is required'}), 400

    try:
        slopecacu = SlopeCacu(config, db_config,CITY_DATABASE[city_name])
        slope_results = slopecacu.process_route(chinese_to_pinyin(extract_city(city_name)),route_name)

        # 格式化结果
        formatted_results = []
        for slope_tuple in slope_results:
            slope_info = {
                "gradient": slope_tuple[0].split(":")[1],
                "distance": slope_tuple[1].split(":")[1],
                "gps": slope_tuple[2].split(":")[1]
            }
            formatted_results.append(slope_info)

        response = {
            "status": 200,
            "data": formatted_results
        }

        return jsonify(response)

    except Exception as e:
        app.logger.error(f"Error calculating slope: {e}")
        return jsonify({'status': 500,'error': str(e)}), 500

#请求公交路况查询的路由
@app.route('/roadcondition', methods=['GET'])
def road_condition():
    city_name = request.args.get('city')
    route_name = request.args.get('route_name')

    if not city_name:
        return jsonify({'status': 400,'error': 'city_name is required'}), 400

    # 检查 city_name 是否在 CITY_DATABASE 中
    if city_name not in CITY_DATABASE:
        return jsonify({'status': 400,'error': '该地区暂未录入高程库'}), 400

    if not route_name:
        return jsonify({'status': 400,'error': 'Route name is required'}), 400

    try:
        roadcondition = RoadCondition(config, db_config,CITY_DATABASE[city_name])
        lngArr, latArr = roadcondition.get_line_GPS(chinese_to_pinyin(extract_city(city_name)),route_name)
        via_stations = roadcondition.get_route_data(route_name,chinese_to_pinyin(extract_city(city_name)))

        line_gps = [{'lng': lng, 'lat': lat} for lng, lat in zip(lngArr, latArr)]

        combined_response = {
            "status": 200,
            "data": {
                "line_gps": line_gps,
                "via_stations": via_stations
            }
        }

        # 使用jsonify返回合并后的数据
        return jsonify(combined_response)

    except Exception as e:
        app.logger.error(f"Error get roadcondition: {e}")
        return jsonify({'status': 500, 'error': str(e)}), 500

def extract_city(region_name):
    # 处理特殊行政单位和直辖市
    special_cases = ["北京市", "天津市", "上海市", "重庆市"]
    for case in special_cases:
        if case in region_name:
            return case.replace("市", "")

    # 正则表达式处理一般情况
    # 解释:
    # (?:.*?省|.*?自治区)? —— 非捕获，匹配任何省份或自治区
    # ([^市]+市|.*?州|.*?地区|.*?县) —— 捕获市、州、地区或县
    match = re.search(r"(?:.*?省|.*?自治区)?([^市]+市|.*?州|.*?地区|.*?县)", region_name)
    if match:
        city = match.group(1)
        # 移除'市'后缀
        if city.endswith("市"):
            return city[:-1]
        return city
    return "未匹配到城市名"


def chinese_to_pinyin(chinese_text):
    # 将中文转换为拼音，去除声调
    pinyin_text = pinyin(chinese_text, style=Style.NORMAL, heteronym=False)
    # 将嵌套的列表转换为单一字符串
    flattened_pinyin = ''.join([item[0] for item in pinyin_text])
    return flattened_pinyin

#使用waitress-serve后无需使用以下代码
# if __name__ == '__main__':
#     # app.run(debug=True)
#     app.run(host='0.0.0.0', port=8000,debug=True)



