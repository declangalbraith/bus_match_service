# 作 者： Liuyaoqiu
# 日 期： 2023/12/12
# app.py

from flask import Flask, request, jsonify
from database_manager import DatabaseManager
from config_loader import ConfigLoader
import os
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from main import load_config, get_vehicle_ids
from route_matcher import RouteMatcher
from task_manager import TaskManager

app = Flask(__name__)
scheduler = BackgroundScheduler()

def scheduled_task():
    try:
        config, db_config = load_config()

        data_fetcher = DataFetcher(config)
        route_matcher = RouteMatcher(db_config)
        task_manager = TaskManager(data_fetcher, route_matcher)
        db_manager = DatabaseManager(db_config)

        vehicle_ids = get_vehicle_ids(config['filepath']['excel_path'])

        end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        start_time = end_time.replace(hour=0, minute=0, second=0)
        print("Start scheduled task-------------------------------------------")
        task_manager.manage_tasks(vehicle_ids, start_time, end_time, db_manager)

        print("Scheduled task completed successfully.")
    except Exception as e:
        print(f"Error during scheduled task: {e}")

# scheduled_task()

scheduler.add_job(func=scheduled_task, trigger='cron', hour=2, minute=0)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

@app.route('/match-results', methods=['GET'])
def get_match_results():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, '..', '..', 'config.ini')
    # Load configuration
    config = ConfigLoader(config_path).config
    db_config = {
        'host': config['database']['host'],
        'user': config['database']['user'],
        'passwd': config['database']['passwd'],
        'database': config['database']['database']
    }
    # print(db_config)

    db_manager = DatabaseManager(db_config)

    date = request.args.get('date')
    vin = request.args.get('vin')

    try:
        results = db_manager.get_match_results(date=date, vin=vin)

        return jsonify(results)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

