import pandas as pd
import numpy as np
import pymysql
import time
import warnings
import gc
warnings.filterwarnings('ignore')
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from Class_Model.All_Class import Data_Clean

class Small_Page:
    def __init__(self):
        self.clean = Data_Clean()

    def select_data(self, date):
        sql_page = f'''
        with page as (
        select 
        tpbh.user_id, date(tpbh.create_time) 创建日期
        ,replace(case when JSON_VALID(tpbh.param) THEN JSON_EXTRACT(tpbh.param, '$.id') end,'"','') as page_id
        from db_digua_business.t_page_browsing_history tpbh
        where tpbh.page_name='微页面'
        )
        select 创建日期,page_id 页面ID, tsp.name 页面名称, tsp.remark 备注,
        count(user_id) pv, count(distinct user_id) uv
        from page p
        left join db_digua_business.t_small_page tsp on tsp.id=p.page_id
        where page_id in (224,190,174,162,260,232,44,265, 267, 272, 282, 284, 287, 288) 
        and 创建日期>='{date}' and 创建日期<current_date
        GROUP BY 创建日期,page_id
        '''
        df_page = self.clean.query(sql_page)
        return df_page

    def uv_sum(self, df):
        # 获取每个页面ID的最大日期和最小日期
        min_date = pd.to_datetime(df.groupby('页面ID').创建日期.min()).dt.strftime('%Y%m%d')
        max_date = pd.to_datetime(df.groupby('页面ID').创建日期.max()).dt.strftime('%Y%m%d')
        min_max = min_date.astype(str)+'-'+max_date.astype(str)
        df.loc[:, '日期范围'] = df.页面ID.map(min_max, na_action='ignore')
        df.loc[:, '备注'] = df.备注.fillna('无')
        # 获取每个页面ID的pv和uv数量
        df_group = df.groupby(['日期范围', '页面ID', '页面名称', '备注']).agg({'pv': 'sum', 'uv': 'sum'})
        df_group = df_group.reset_index()
        return df_group

    def my_job(self, hour, minute, path):
        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        print('正在查询数据...')
        # 判断是不是本月的第一天，如果是则获取上个月的数据，如果不是则获取本月1日到当前的数据
        date = None
        if datetime.today().strftime('%d')=='01':
            date = ((datetime.today()-pd.DateOffset(months=1)).replace(day=1)).strftime('%Y-%m-%d')
        else:
            date = (datetime.today().replace(day=1)).strftime('%Y-%m-%d')
        df_page = self.select_data(date)
        print('数据查询完毕！\n正在计算汇总数据...')
        df_page_sum = self.uv_sum(df_page)
        print('汇总数据计算完毕！\n正在计算昨天的数据...')
        yesterday = pd.to_datetime((datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d'))
        df_page.loc[:, '创建日期'] = pd.to_datetime(df_page.创建日期)
        df_page = df_page[df_page.创建日期==yesterday].iloc[:, :-1]
        print('昨天数据计算完毕！\n开始写入数据...')

        with pd.ExcelWriter(path + f'微页面活动页_{today}.xlsx', engine='xlsxwriter') as writer:
            df_page.to_excel(writer, sheet_name='每日活动页', index=False)
            df_page_sum.to_excel(writer, sheet_name='汇总活动页', index=False)
        print('写入数据完毕！')
        del df_page, df_page_sum
        gc.collect()
        print("回收内存执行完毕！\n")

if __name__ == '__main__':
    hour = 9
    minute = 15
    path = r'\\digua\迪瓜租机\002数据监测\5.活动页面/'
    sp = Small_Page()
    sp.my_job(hour, minute, path)
    scheduler = BackgroundScheduler()
    # 每天9点15开始执行
    job_channel = scheduler.add_job(sp.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    # 查看是否添加了任务
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job_channel.next_run_time
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
        gc.collect()