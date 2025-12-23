import numpy as np
import pandas as pd
import xlwings as xw
from openpyxl import load_workbook

import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False

import warnings
warnings.filterwarnings("ignore")

import pymysql

import json
import random
import datetime
from datetime import timedelta ,datetime, timezone
import time
import sys
from pathlib import Path
import gc
from apscheduler.schedulers.background import BackgroundScheduler
# 将项目根目录添加到 sys.path
sys.path.append(str(Path(__file__).parent.parent))  # 依实际路径调整
from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class Ant_Sg:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()

    def select(self):
        sql_ant = '''
        select ant.create_time, ant.true_name, ant.mobile, ant.virtual_order_number, ant.has_close_pay
        
        from db_digua_business.t_ant_sgjh ant
        where create_time >= DATE_ADD(CURDATE() ,INTERVAL -1 day )             -- 前天数据
        and  create_time < CURDATE()
        '''
        df_ant = self.clean.query(sql_ant)
        return df_ant

    def ant_result(self, path):
        now = datetime.now().strftime('%Y%m%d%H%M')
        print(f'执行定时任务：现在是{now}...')
        print('正在查询数据...')

        df_ant = self.select()
        df_ant.loc[:, '支付状态'] = np.where(df_ant['has_close_pay'] == 'Y', '支付成功', '未支付')

        with pd.ExcelWriter(path + f'蚂蚁链愚公计划_{now}.xlsx', engine='openpyxl') as writer:
            df_ant.to_excel(writer, sheet_name=now, index=False)


if __name__ == '__main__':
    Ant = Ant_Sg()
    hour = 8
    minute = 59
    path = r'\\digua\迪瓜租机\001愚公计划/'


    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天8点59分开始执行，获取前一天的数据
    dy_job = scheduler.add_job(Ant.ant_result, 'cron', hour=hour, minute=minute, args=[path])
    print('定时任务创建完毕...\n正在执行定时任务douyin_job_save...')
    # Ant.ant_result(path)

    print(scheduler.get_jobs())
    scheduler.start()
    print('定时任务执行完毕,请查看数据...')
    # 模拟主程序
    try:

        while True:
            next_run_time = dy_job.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()

                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
        # 程序退出前进行垃圾回收
        gc.collect()

