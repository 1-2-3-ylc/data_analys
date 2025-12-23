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

    # 自营的当前未逾期(om.overdue=0)的在租的(status=4)未付的 且 非当前预警池(回捞？) 且 应付日期-系统日期 >= θ 且 应付日期-系统日期<= 3(ymos.)
    # order_number	true_name	id_card_num	mobile	下单日期	订单状态	总期数	当前期数	应付日期	实付日期new
    def select_date(self):
        sql_order = '''
                SELECT om.order_number, om.create_time, om.id order_id, om.merchant_name
                , ymos.reality_refund_date, ymos.sort, ymos.refund_date
                , tmu.true_name, tmu.id_card_num, tmu.mobile
                ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
                when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
                when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as status2
                FROM db_digua_business.t_postlease_receivables_monitoring  tprm
                LEFT JOIN db_digua_business.t_order om on tprm.order_id = om.id 
                LEFT JOIN db_rent.ya_merchant_order_stages ymos on om.id = ymos.order_id
                LEFT JOIN db_digua_business.t_member_user tmu on om.user_id = tmu.id
                where DATE_FORMAT(ymos.refund_date, '%Y-%m-%d')>=CURDATE()
                and ymos.refund_date <= DATE_ADD(CURRENT_DATE,INTERVAL +3 day )
                and ymos.reality_refund_date is null
                and om.status = 4
                and om.overdue = 0
                ;
                '''
        df_order = self.clean.query(sql_order)

        # 获取每个订单(order_id)的最大follow_time的订单号
        # sql = '''
        # SELECT order_number
        # from db_rent.ya_renting_remind
        # where status=2 -- 2 表示跟进中
        # -- and date_format(end_time, '%Y-%m-%d') >= CURDATE()
        # AND follow_time is not null
        # GROUP BY order_number
        # HAVING follow_time = MAX(follow_time)
        # '''
        sql = '''
        SELECT distinct order_number 
        -- 每天早上4点更新状态
        from db_rent.ya_renting_remind
        -- 2 表示跟进中，1 待处理
        where status in (1, 2) 
        
        -- date_format(end_time, '%Y-%m-%d') >= CURDATE()
        '''
        df_yrr = self.clean.query(sql)

        # 获取每个订单(order_id)的最大分期期数(sort),
        sql_sort = '''
        select order_id, max(sort) as max_sort
        from db_rent.ya_merchant_order_stages
        group by order_id
        '''
        df_sort = self.clean.query(sql_sort)

        return df_order, df_yrr, df_sort

    def drop_merchant(self, df):
        '''
        删除商家数据
        :param df: 传入带有商家的数据
        :return: 返回剔除了商家的数据 将在进件剔除商家数据
        '''
        # 剔除商家数据只保留自营租机业务数据
        df.drop(df[df['merchant_name'] == "深圳优优大数据科技有限公司"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "优优2店"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "小豚租（代收）"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "苏州蚁诺宝"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "租着用电脑数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "北京海鸟窝科技有限公司"].index, inplace=True)

        df.drop(df[df['merchant_name'] == "汇客好租"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "澄心优租"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "CPS渠道合作"].index, inplace=True)
        # df.drop(df[df['sku_attributes'].str.contains(pat='探路者', regex=False) == True].index, inplace=True)
        # 趣智数码  单
        df.drop(df[df['merchant_name'] == "趣智数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "格木木二奢名品"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "广州康基贸易有限公司"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "线下小店"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "乙辉数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "呱子笔记本电脑"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "南京聚格网络科技"].index, inplace=True)

        df.drop(df[df['merchant_name'] == "星晟数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "蘑菇时间"].index, inplace=True)
        # df.drop(df[df['merchant_name'] == "云启德曜"].index, inplace=True) # 拒量
        df.drop(df[df['merchant_name'] == "艾欧尼亚数码"].index, inplace=True)

        df.drop(df[df['merchant_name'].str.contains(pat='探路者', regex=False) == True].index, inplace=True)
        # 二三级量：进件算自营但出库是别人家的
        reject_merchants = ["小蚂蚁租机", "兴鑫兴通讯", "人人享租", "崇胜数码", "喜卓灵租机", "喜卓灵新租机",
                            "云启德曜"]
        df.drop(df[df['merchant_name'].isin(reject_merchants)].index, inplace=True)
        return df

    def run(self):
        df_order, df_yrr, df_sort = self.select_date()
        # 剔除商家数据
        df_order = self.drop_merchant(df_order)


        df_order["下单日期"] = df_order["create_time"].dt.date
        df_order["下单日期"] = pd.to_datetime(df_order["下单日期"], errors="coerce")
        # df_order与df_sort左关联
        df_order = pd.merge(df_order, df_sort, on="order_id", how="left")

        # 剔除当前在预警池的未结束的订单，即剔除df_order中在df_yrr中的订单
        order_number_yrr = df_yrr.order_number.to_list()
        df_order = df_order[~df_order.order_number.isin(order_number_yrr)]

        return df_order


    def my_job(self, hour, minute, path1):

        now_date = datetime.now().strftime('%Y-%m-%d')
        print(f'执行定时任务：现在是{now_date}的{hour}:{minute}')
        df_order = self.run()
        # 仅获取order_number	true_name	id_card_num	mobile	下单日期	订单状态	总期数	当前期数	应付日期	实付日期
        df_order = df_order[['order_number', 'true_name', 'id_card_num', 'mobile', '下单日期', 'status2',
                            'max_sort', 'sort', 'refund_date', 'reality_refund_date', 'merchant_name']].rename(
            columns={'status2': '订单状态', 'max_sort': '总期数', 'sort': '当前期数', 'refund_date': '应付日期', 'reality_refund_date': '实付日期'})
        # 保存到excel
        with pd.ExcelWriter(path1 + f'预警客户清单{now_date}.xlsx', engine='openpyxl') as writer:
            df_order.to_excel(writer, sheet_name='当期需还款', index=False)

if __name__ == '__main__':

    T = Unpaid_List()


    hour = 8
    minute = 50

    path1 = r'\\digua\迪瓜租机\26.预警客户清单/'
    # 实时手动跑
    T.my_job(hour, minute, path1)
    # print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天8点30分开始执行
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