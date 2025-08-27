import numpy as np
import pandas as pd
import xlwings as xw
from apscheduler.triggers.cron import CronTrigger
from openpyxl import load_workbook
import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
from apscheduler.schedulers.background import BackgroundScheduler
import gc
import warnings
warnings.filterwarnings("ignore")
import requests
import hmac
import hashlib
import base64
import pymysql
from sqlalchemy import create_engine
import json
import time
from datetime import timedelta , datetime, timezone
from dateutil.relativedelta import relativedelta
import re
import os
import glob
import sys

from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class myw:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        # 实例化All_Model类
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()



    def select_date(self, date1,date2):
        # 按结束时间买断订单数
        # too.status,1:未买断，2:已买断，3：部分买断
        sql_lzy = f'''
        select
        ybt.order_id 订单ID, tod.new_actual_money 买断价, too.discount_money 优惠金额, (tod.new_actual_money-too.discount_money) 实收买断金额,
        tprm.purchase_amount 采购金额, ybt.`status`, ybt.follow_log_tag, ybt.type, om.has_actual, date_format(ybt.end_time, '%Y-%m') 月份
        from db_rent.ya_buyout_task ybt
        left join (select order_id, discount_money from db_digua_business.t_order_out where discount_money>0) too on too.order_id=ybt.order_id
        left join db_digua_business.t_postlease_receivables_monitoring tprm on tprm.order_id=ybt.order_id
        left join db_digua_business.t_order_details tod on tod.order_id=ybt.order_id
        left join db_digua_business.t_order om on om.id=ybt.order_id
        -- where date_format(ybt.end_time, '%Y-%m')='2025-07'
        where ybt.end_time>=date_add(current_date, interval - {date1} {date2})
        '''
        # 提前买断订单对应期数
        sql_stages = '''
        select order_id 订单ID, sort 期数 from db_rent.ya_merchant_order_stages where date_format(refund_date, '%Y-%m')='2025-08'
        '''
        # 按发布时间买断订单数
        sql_amd = f'''
        select ybt.order_id 发布时间订单号, tod.new_actual_money 发布时间买断价, date_format(ybt.add_time, '%Y-%m') 月份 from db_rent.ya_buyout_task ybt 
        left join db_digua_business.t_order_details tod on tod.order_id=ybt.order_id
        -- where date_format(ybt.add_time, '%Y-%m')='2025-07'
        where ybt.add_time>=date_add(current_date, interval - {date1} {date2})
        '''
        # 优召
        """
        sql_yz = f'''
        select
        om.id, turt.discount_status, tdi.money, tdi.type, tprm.all_rental, tprm.all_deposit, tprm.purchase_amount, om.status, tod.new_actual_money, date_format(om.create_time, '%Y-%m') 月份
        from db_digua_business.t_user_recall_task turt 
        left join db_digua_business.t_member_user tmu on tmu.id_card_num=turt.id_card_num
        left join db_digua_business.t_order om on om.user_id=tmu.id
        left join db_digua_business.t_postlease_receivables_monitoring tprm on tprm.order_id=om.id
        left join db_digua_business.t_discount_item tdi on tdi.user_id=om.user_id
        left join db_digua_business.t_order_details tod on tod.order_id=om.id
        where date_format(om.create_time, '%Y-%m')='2025-07'
        -- where om.create_time>=date_add(current_date, interval - date1 {date2})
        '''
        """
        sql_yz = f'''
        select
        om.id order_id, om.create_time 下单时间,turt.discount_status, turt.id_card_num,tdi.money tdi_money,om.order_number ,turt.create_time 优召时间,turt.complete_time
        ,tdi.type,  tdi.create_time 优惠订单创建时间, tdi.use_type
        , tprm.all_rental, tprm.all_deposit, tprm.purchase_amount, om.status, tod.new_actual_money, date_format(turt.complete_time, '%Y-%m') 月份
        , tos.money 分期金额
        from db_digua_business.t_user_recall_task turt 
        left join db_digua_business.t_order om on om.order_number=turt.order_number
        left join db_digua_business.t_postlease_receivables_monitoring tprm on tprm.order_number=turt.order_number
        left join db_digua_business.t_discount_item tdi on tdi.user_id=om.user_id
        left join db_digua_business.t_order_details tod on tod.order_id=om.id
        left join db_digua_business.t_order_stages tos on tos.order_id=om.id
        where turt.complete_time>=date_add(current_date, interval - {date1} {date2})
        -- date_format(turt.complete_time, '%Y-%m-%d')>='2025-07-01'
        -- and date_format(turt.complete_time, '%Y-%m-%d')<'2025-08-01';

        '''


        sql_wl = f'''
        select  
        tort.order_id, tprm.purchase_amount, tod.new_actual_money, tdi.money, tdi.type, om.status, date_format(om.create_time, '%Y-%m') 月份, tort.task_status
        from db_digua_business.t_order_retention_task tort
        left join db_digua_business.t_order om on om.id=tort.order_id
        left join db_digua_business.t_postlease_receivables_monitoring tprm on tprm.order_id=om.id
        left join db_digua_business.t_discount_item tdi on tdi.user_id=om.user_id
        left join db_digua_business.t_order_details tod on tod.order_id=om.id
        -- where date_format(om.create_time, '%Y-%m')='2025-07'
        where om.create_time>=date_add(current_date, interval - {date1} {date2})
        
        '''
        df_wl = self.clean.query(sql_wl)
        df_yz = self.clean.query(sql_yz)
        # df_yz = df_yz.drop_duplicates(subset=['id'])
        df_amd = self.clean.query(sql_amd)
        df_lzy = self.clean.query(sql_lzy)
        df_stages = self.clean.query(sql_stages)

        return df_wl, df_yz, df_amd, df_lzy, df_stages

    def get_date(self, df_wl, df_yz, df_amd, df_lzy, df_stages):
        # 买断
        df_lzy.loc[:, '实际买断订单数'] = np.where((df_lzy.status.isin([3, 4])) & (df_lzy.has_actual == 1), 1, 0)
        df_lzy.loc[:, '实收买断金额'] = np.where((df_lzy.status.isin([3, 4])) & (df_lzy.has_actual == 1),df_lzy.实收买断金额, 0)
        df_lzy.loc[:, '优惠金额'] = df_lzy.优惠金额.fillna(0)
        df_lzy.loc[:, '实收买断金额'] = df_lzy.实收买断金额.fillna(df_lzy.买断价)

        def md(df):
            # 计算月份的数据及优惠比例和毛利率
            df_g = df.groupby('月份').agg(
                {'订单ID': 'count', '采购金额': 'sum', '买断价': 'sum', '实际买断订单数': 'sum', '实收买断金额': 'sum',
                 '优惠金额': 'sum'}).rename(columns={'订单ID': '发布买断订单数'})
            df_g.loc[:, '优惠比例'] = (df_g.优惠金额 / df_g.采购金额).map(lambda x: format(x, '.2%'))
            df_g.loc[:, '发放优惠前的毛利率'] = ((df_g.买断价 - df_g.采购金额) / df_g.采购金额).map(
                lambda x: format(x, '.2%'))
            df_g.loc[:, '发放优惠后的毛利率'] = ((df_g.实收买断金额 - df_g.采购金额) / df_g.采购金额).map(
                lambda x: format(x, '.2%'))
            return df_g
        # 判断是否是特殊申请
        df_lzy_ts = md(df_lzy[(df_lzy.status.isin([3, 4])) & (df_lzy.follow_log_tag == 4) & (df_lzy.has_actual == 1)])
        # 判断是否是提前买断
        df_lzy_tq = df_lzy[(df_lzy.status.isin([3, 4])) & (df_lzy.follow_log_tag != 4) & (df_lzy.type == 1) & (df_lzy.has_actual == 1)]
        df_lzy_tq2 = md(df_lzy_tq)
        df_lzy_tq3 = df_lzy_tq.merge(df_stages, on='订单ID', how='left')
        # 判断是否是到期买断
        df_lzy_dq = df_lzy[(df_lzy.status.isin([3, 4])) & (df_lzy.follow_log_tag != 4) & (df_lzy.type == 0) & (df_lzy.has_actual == 1)]
        df_lzy_dq = md(df_lzy_dq)
        df_amd_g = df_amd.groupby('月份').agg({'发布时间订单号': 'count', '发布时间买断价': 'sum'})
        df_lzy_dq = df_lzy_dq.merge(df_amd_g, on='月份', how='left')

        # 优召
        # df_yz.loc[:, '进件订单数'] = np.where((df_yz.status == 1) | (df_yz.status == 13), 0, 1)
        # df_yz.loc[:, '出库订单数'] = np.where(df_yz.status.isin([2, 3, 4, 5, 6, 8, 15]), 1, 0)
        # df_yz.loc[:, '优惠券金额'] = np.where((df_yz.discount_status == 'Y') & (df_yz.type == 1), df_yz.money, 0)
        # df_yz.lo   c[:, '买断价'] = np.where(df_yz.出库订单数 == 1, df_yz.new_actual_money, 0)
        # df_yz_g = df_yz.groupby('月份').agg(
        #     {'id': 'count', '进件订单数': 'sum', '出库订单数': 'sum', '优惠券金额': 'sum', 'purchase_amount': 'sum',
        #      'all_rental': 'mean', 'all_deposit': 'mean', '买断价': 'sum'}).rename(
        #     columns={'id': '发布优召订单数', 'purchase_amount': '采购成本', 'all_rental': '总租金均值',
        #              'all_deposit': '总押金均值'})

        df_yz = df_yz.sort_values('优惠订单创建时间', ascending=False).groupby('id_card_num').head(1)
        # df_yz = df_yz.drop_duplicates(subset=['id_card_num'])
        df_yz["下单日期"] = df_yz["下单时间"].dt.date
        df_yz["下单日期"] = pd.to_datetime(df_yz["下单日期"], errors="coerce")
        df_yz["优召日期"] = df_yz["优召时间"].dt.date
        df_yz["优召日期"] = pd.to_datetime(df_yz["下单日期"], errors="coerce")
        df_yz["完成日期"] = df_yz["complete_time"].dt.date
        df_yz["完成日期"] = pd.to_datetime(df_yz["下单日期"], errors="coerce")

        df_yz.loc[:, '进件订单数'] = np.where(((df_yz.status == 1) | (df_yz.status == 13)) & (
                    (df_yz.下单日期 >= df_yz.优召日期) & (df_yz.下单日期 <= df_yz.完成日期)), 0, 1)
        df_yz.loc[:, '出库订单数'] = np.where((df_yz.order_number.notna()), 1,0)  # (df_yz.status.isin([2, 3, 4, 5, 6, 8, 15]))&
        # '优惠卷类型：1、商品满减券；2、首期优惠券；3、商品延期券；4、商品买断券', 5、租中优惠券
        df_yz.loc[:, '优惠券金额'] = np.where(
            (df_yz.discount_status == 'Y') & (df_yz.use_type == 1) & (df_yz.type == 5), df_yz.tdi_money,
            np.where((df_yz.discount_status == 'Y') & (df_yz.use_type == 2) & (df_yz.type == 5),
                     df_yz.分期金额 * (1 - df_yz.tdi_money / 100), 0)
        )
        df_yz.loc[:, '买断价'] = np.where(df_yz.出库订单数 == 1, df_yz.new_actual_money, 0)
        df_yz.loc[:, '采购成本'] = np.where(df_yz.出库订单数 == 1, df_yz.purchase_amount, 0)
        df_yz_g = df_yz.groupby('月份').agg(
            {'进件订单数': 'sum', '出库订单数': 'sum', '优惠券金额': 'sum', '采购成本': 'sum', 'all_rental': 'mean',
              'all_deposit': 'mean', '买断价': 'sum'}).rename(
            columns={'all_rental': '总租金均值', 'all_deposit': '总押金均值'})

        df_yz_g.loc[:, '租售比'] = (df_yz_g.总租金均值 / df_yz_g.总押金均值).map(lambda x: format(x, '.2%'))
        df_yz_g.loc[:, '优惠比例'] = (df_yz_g.优惠券金额 / df_yz_g.采购成本).map(lambda x: format(x, '.2%'))
        df_yz_g.loc[:, '优惠前毛利率'] = ((df_yz_g.买断价 - df_yz_g.采购成本) / df_yz_g.采购成本).map(
            lambda x: format(x, '.2%'))
        df_yz_g.loc[:, '优惠后毛利率'] = (
                    (df_yz_g.买断价 - df_yz_g.采购成本 - df_yz_g.优惠券金额) / df_yz_g.采购成本).map(
            lambda x: format(x, '.2%'))

        # 挽留
        df_wl.loc[:, '挽留成功订单数'] = np.where(df_wl.task_status == 'SUCCESS', 1, 0)
        df_wl.loc[:, '出库'] = np.where(df_wl.status.isin([2, 3, 4, 5, 6, 8, 15]), 1, 0)
        df_wl.loc[:, '买断优惠券'] = np.where(df_wl.type == 4, df_wl.money, 0)
        df_wl.loc[:, '租中优惠券'] = np.where(df_wl.type == 1, df_wl.money, 0)
        df_wl.loc[:, '优惠券总金额'] = np.where(df_wl.type.isin([1, 4]), df_wl.money, 0)
        df_wl.loc[:, '采购金额'] = np.where(df_wl.status.isin([2, 3, 4, 5, 6, 8, 15]), df_wl.purchase_amount, 0)
        df_wl.loc[:, '买断价'] = np.where(df_wl.status.isin([2, 3, 4, 5, 6, 8, 15]), df_wl.new_actual_money, 0)
        df_wl_g = df_wl.groupby('月份').agg(
            {'order_id': 'count', '挽留成功订单数': 'sum', '出库': 'sum', '采购金额': 'sum', '买断价': 'sum', '优惠券总金额': 'sum', '买断优惠券': 'sum',
             '租中优惠券': 'sum'}).rename(columns={'order_id': '挽留发布订单数'})
        df_wl_g.loc[:, '优惠比例'] = (df_wl_g.优惠券总金额 / df_wl_g.采购金额).map(lambda x: format(x, '.2%'))
        df_wl_g.loc[:, '买断优惠券比例'] = (df_wl_g.买断优惠券 / df_wl_g.采购金额).map(lambda x: format(x, '.2%'))
        df_wl_g.loc[:, '租中优惠券比例'] = (df_wl_g.租中优惠券 / df_wl_g.采购金额).map(lambda x: format(x, '.2%'))
        df_wl_g.loc[:, '发放优惠前的毛利率'] = ((df_wl_g.买断价 - df_wl_g.采购金额) / df_wl_g.采购金额).map(
            lambda x: format(x, '.2%'))
        df_wl_g.loc[:, '发放优惠后的毛利率'] = (
                    (df_wl_g.买断价 - df_wl_g.优惠券总金额 - df_wl_g.采购金额) / df_wl_g.采购金额).map(
            lambda x: format(x, '.2%'))

        return df_lzy_ts, df_lzy_tq2, df_lzy_tq3, df_lzy_dq, df_yz_g, df_wl_g

    def run(self, date1, date2):
        df_wl, df_yz, df_amd, df_lzy, df_stages = self.select_date(date1, date2)
        df_lzy_ts, df_lzy_tq2, df_lzy_tq3, df_lzy_dq, df_yz_g, df_wl_g = self.get_date(df_wl, df_yz, df_amd, df_lzy, df_stages)
        return df_lzy_ts, df_lzy_tq2, df_lzy_tq3, df_lzy_dq, df_yz_g, df_wl_g

    def my_job(self, hour, minute, path, date1, date2):
        t_date = datetime.now().strftime('%Y%m%d%H')
        df_lzy_ts, df_lzy_tq2, df_lzy_tq3, df_lzy_dq, df_yz_g, df_wl_g = self.run(date1, date2)
        print(f'执行定时任务：现在是{t_date}的{hour}:{minute}')
        with pd.ExcelWriter(path + f'买断-优召-挽留订单_{t_date}.xlsx', engine='xlsxwriter') as writer:
            df_lzy_ts.to_excel(writer, sheet_name='特殊申请')
            df_lzy_tq2.to_excel(writer, sheet_name='提前买断')
            df_lzy_tq3.to_excel(writer, sheet_name='提前买断明细', index=False)
            df_lzy_dq.to_excel(writer, sheet_name='到期买断')
            df_yz_g.to_excel(writer, sheet_name='优召')
            # df_wl_g.to_excel(writer, sheet_name='挽留')

        del df_lzy_ts, df_lzy_tq2, df_lzy_tq3, df_lzy_dq, df_yz_g, df_wl_g
        gc.collect()
        print("回收内存执行完毕！\n")

if __name__ == '__main__':
    myw = myw()
    hour = 10
    minute = 30
    path = r'\\digua\迪瓜租机\22.买断-优召-挽留/'
    # 实时
    # myw.my_job(hour, minute, path, 7, 'day')
    # myw.my_job(hour, minute, path, 1, 'month')
    scheduler = BackgroundScheduler()
    # 每周一的10点30分开始执行
    job = scheduler.add_job(myw.my_job, CronTrigger(day_of_week='mon', hour=hour, minute=minute), args=[hour, minute, path, 7, 'day'])
    # 每月1号的10点31分开始执行
    job1 = scheduler.add_job(myw.my_job, CronTrigger(day=1, hour=hour, minute=minute+1), args=[hour, minute, path, 1, 'month'])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
            next_run_time1 = job1.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            elif next_run_time1:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time1 - now).total_seconds()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
        gc.collect()