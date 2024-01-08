# 作 者： Liuyaoqiu
# 日 期： 2023/12/12
# app.py

from flask import Flask, request, jsonify
from database_manager import DatabaseManager
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from main import load_config, get_vehicle_ids,get_id_order_model
from route_matcher import RouteMatcher
from task_manager import TaskManager
from slope_cacu import SlopeCacu
import pandas as pd

app = Flask(__name__)
scheduler = BackgroundScheduler()
#配置初始化
config, db_config = load_config()

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

        route_matcher = RouteMatcher(db_config)
        task_manager = TaskManager(data_fetcher, route_matcher)
        db_manager = DatabaseManager(db_config)

        vehicle_ids = get_vehicle_ids(config['filepath']['excel_path'])

        # 设定结束时间为前一天的22:00
        end_time = datetime.now().replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1)
        # 设定开始时间为前一天的10:00
        start_time = end_time.replace(hour=10, minute=0, second=0)
        print("Start scheduled task-------------------------------------------")
        task_manager.manage_tasks(vehicle_ids, start_time, end_time, db_manager)
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
    # config, db_config = load_config()

    query_type = request.args.get('query_type')
    query_value = request.args.get('query_value')
    date = request.args.get('date')

    try:
        db_manager = DatabaseManager(db_config)
        vehicle_df = get_id_order_model(config['filepath']['excel_path'])

        if query_type in ['order', 'model']:
            vins = db_manager.get_vins_by_order_or_model(query_value, query_type,vehicle_df)
            results = db_manager.get_match_results(date=date, vins=vins)
            filtered_vehicle_df = vehicle_df[vehicle_df['VIN'].isin(vins)]
        elif query_type == 'vin':
            results = db_manager.get_match_results(date=date, vins=[query_value])
            filtered_vehicle_df = vehicle_df[vehicle_df['VIN'].isin([query_value])]
        else:
            return jsonify({'error': 'Invalid query type'}), 400

        if(results!=[]):
            results_df = pd.DataFrame(results)
            merged_df = pd.merge(
                filtered_vehicle_df,
                results_df,
                left_on='VIN',
                right_on='vin',
                how='left'
            )
            final_df = merged_df[[
                'date', 'VIN', '车牌', '订单号', '车型', '购车客户', '所属区域', 'matched_route', 'match_rate',
                'route_coverage'
            ]]
            final_json_list = final_df.to_dict(orient='records')
            return jsonify(final_json_list)
        else:
            filtered_vehicle_df = filtered_vehicle_df.copy()

            filtered_vehicle_df['date'] = pd.NA
            filtered_vehicle_df['match_rate'] = pd.NA
            filtered_vehicle_df['matched_route'] = pd.NA
            filtered_vehicle_df['route_coverage'] = pd.NA
            return jsonify(filtered_vehicle_df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

#请求坡度计算的路由
@app.route('/calculateslope', methods=['GET'])
def calculate_slope():
    # config, db_config = load_config()
    CITY_DATABASE = ["江苏省扬州市"]

    city_name = request.args.get('city')
    route_name = request.args.get('route_name')

    if not city_name:
        return jsonify({'error': 'city_name is required'}), 400

    # 检查 city_name 是否在 CITY_DATABASE 中
    if city_name not in CITY_DATABASE:
        return jsonify({'error': '该城市未录入高程库'}), 400

    if not route_name:
        return jsonify({'error': 'Route name is required'}), 400

    try:
        slopecacu = SlopeCacu(config, db_config)
        slope_result = slopecacu.process_route(route_name)

        return jsonify(slope_result)

    except Exception as e:
        app.logger.error(f"Error calculating slope: {e}")
        return jsonify({'error': str(e)}), 500


#使用waitress-serve后无需使用以下代码
# if __name__ == '__main__':
#     # app.run(debug=True)
#     app.run(host='0.0.0.0', port=5000,debug=False)



