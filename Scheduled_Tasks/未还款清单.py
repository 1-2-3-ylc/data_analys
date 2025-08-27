import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
import numpy as np
from sqlalchemy.dialects.mssql.information_schema import columns

plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False

import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
from datetime import timedelta ,datetime, timezone
import time
from dateutil.relativedelta import relativedelta
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
sys.path.append(str(Path(__file__).parent.parent))  # 依实际路径调整
from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class Unpaid_List:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()


    def select_date(self):
        sql = '''
                SELECT om.order_number,tprm.overdue_type,ymos.reality_refund_date

                FROM db_digua_business.t_postlease_receivables_monitoring  tprm
                LEFT JOIN db_digua_business.t_order om on tprm.order_id = om.id 
                LEFT JOIN db_rent.ya_merchant_order_stages ymos on om.id = ymos.order_id
                where DATE_FORMAT(ymos.refund_date, '%Y-%m-%d')>='2025-08-01'
                and ymos.refund_date <= DATE_ADD(CURRENT_DATE,INTERVAL +3 day )
                '''

        df_order = self.clean.query(sql)
        return df_order



    def run(self):
        df = self.select_date()
        # 读取excel文件清单
        # 跑出清单：
        # 1.当前无逾期
        # 2.当期未还款
        # 3.还款日为观察点及后3天，比如8.1跑的，那么还款日为8.1-8.4的同时满足 1和2的清单
        df_list = pd.read_excel('F:/需求/倩姐需求/zzyj_list.xlsx')

        # 拼接df_list与df,以订单号进行匹配
        df_list_merge = df_list.merge(df, on='order_number', how='inner')
        # 获取当前无逾期的清单
        # df_list_merge[df_list_merge.overdue_type.notna()].head()
        # df_list_merge_no_overdue = df_list_merge[df_list_merge.overdue_type == ''] 不需要
        # 获取当前未还款的清单
        # df_list_merge_no_repay = df_list_merge[df_list_merge.实付日期new.isna()] 不需要
        # 获取今天的日期
        # 更新实付日期
        df_list_merge['实付日期new'] = pd.to_datetime(df_list_merge['实付日期new'])
        df_list_merge['实付日期new'] = np.where(df_list_merge['reality_refund_date'].notna(), df_list_merge['reality_refund_date'], df_list_merge['实付日期new'])
        today = pd.Timestamp(datetime.now().date())
        # 获取当前需要还款的清单,应付日期为今天到后3天的范围，还款日为观察点及后3天，比如8.1跑的，那么还款日为8.1-8.4的同时满足 1和2的清单
        df_list_merge_repay = df_list_merge[
            (df_list_merge.应付日期 >= today) & (df_list_merge.应付日期 <= today + timedelta(days=3)) & (
                        df_list_merge.overdue_type == '') & (df_list_merge.实付日期new.isna())]
        # 基于订单号去重
        df_list_merge_repay = df_list_merge_repay.drop_duplicates(subset=['order_number'])

        return df_list_merge_repay

    def my_job(self, hour, minute, path1):

        now_date = datetime.now().strftime('%Y-%m-%d')
        print(f'执行定时任务：现在是{now_date}的{hour}:{minute}')
        df_list_merge_repay = self.run()
        # 仅获取order_number	true_name	mobile	下单日期	总期数	剩余未还期数	当前期数	应付日期	实付日期new	overdue_type
        df_list_merge_repay = df_list_merge_repay[['order_number', 'true_name', 'mobile', '下单日期', '总期数', '剩余未还期数', '当前期数', '应付日期', '实付日期new', 'overdue_type']].rename(columns={'实付日期new': '实付日期'})
        # 保存到excel
        with pd.ExcelWriter(path1 + f'预警客户清单{now_date}.xlsx', engine='openpyxl') as writer:
            df_list_merge_repay.to_excel(writer, sheet_name='当期需还款', index=False)

if __name__ == '__main__':

    T = Unpaid_List()


    hour = 8
    minute = 59

    path1 = 'F:/需求/倩姐需求/订单还款详情/'
    # 实时手动跑
    # T.my_job(hour, minute, path1)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天9点30分开始执行
    job = scheduler.add_job(T.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path1])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    print('定时任务执行完毕,请查看数据...')
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
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