# 作 者： Liuyaoqiu
# 日 期： 2023/12/4

import threading
import queue
from datetime import datetime, timedelta

#匹配率阈值，大于等于该阈值时，认为是找到匹配路线
MATCH_RATE_THRESHOLD = 0.95

class TaskManager:
    def __init__(self, data_fetcher, route_matcher):
        self.data_fetcher = data_fetcher
        self.route_matcher = route_matcher

    def fetch_and_match(self, vin, start_time, end_time, result_queue):
        """
        获取车辆历史数据并进行路线匹配，将结果存储在结果队列中。
        """
        vehicle_history = self.data_fetcher.fetch_vehicle_history(vin, start_time, end_time)
        matched_route,route_match_rates,route_coverage = self.route_matcher.match_route(vehicle_history)
        if matched_route:
            # 判断最佳路线是否满足匹配阈值
            if route_match_rates[matched_route] >= MATCH_RATE_THRESHOLD:
                print(
                    f"{vin}最优路线：{matched_route}, 匹配率：{route_match_rates[matched_route]:.2%}, 路径覆盖度：{route_coverage[matched_route]:.2f}km")
            else:
                print(f"{vin}没有完全匹配的路线")
                print(
                    f"{vin}最近似路线：{matched_route}, 匹配率：{route_match_rates[matched_route]:.2%}, 路径覆盖度：{route_coverage[matched_route]:.2f}km")
            result_queue.put((vin, matched_route,route_match_rates[matched_route],route_coverage[matched_route]))
        else:
            print(f"{vin}未找到匹配的路线")

    def worker(self, task_queue, result_queue):
        while True:
            try:
                task = task_queue.get()
                if task is None:
                    task_queue.task_done()
                    break

                vin, start_time, end_time = task
                self.fetch_and_match(vin, start_time, end_time, result_queue)
                task_queue.task_done()
            except Exception as e:
                print(f"在处理 {vin} 时发生错误: {e}")
                task_queue.task_done()

    def manage_tasks(self, vehicle_ids, start_time, end_time,db_manager):
        """
        管理和分配任务到线程。
        """
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        threads = []

        # 创建工作线程
        for _ in range(5):  # 线程数量
            thread = threading.Thread(target=self.worker, args=(task_queue, result_queue))
            thread.start()
            threads.append(thread)

        # 向队列中添加任务
        for vin in vehicle_ids:
            task_queue.put((vin, start_time, end_time))

        # 停止信号
        for _ in range(len(threads)):
            task_queue.put(None)

        task_queue.join()

        # 等待所有任务完成
        for thread in threads:
            thread.join()

        # 计算前一天的日期
        # previous_day = datetime.now().date() - timedelta(days=1)
        save_day = start_time.date()

        # 处理结果
        results = []
        while not result_queue.empty():
            route_request= result_queue.get()
            results.append(route_request)
            # 数据库保存vin、匹配路线、匹配率、覆盖路线、日期
            vin = route_request[0]
            matched_route = route_request[1]
            match_rate = route_request[2]
            route_coverage = route_request[3]
            print("存储展示：",vin,matched_route,match_rate,route_coverage,save_day)
            db_manager.insert_match_result(vin, matched_route, match_rate, route_coverage, save_day)

        return results
