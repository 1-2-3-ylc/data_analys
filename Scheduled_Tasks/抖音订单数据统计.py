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

class Douyin_Order:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()

    def select_data(self):
        sql1 = '''  
        SELECT om.id as order_id ,om.order_number,om.all_money 总租金, om.create_time
        ,om.status, om.user_mobile
        ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
        when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
        when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as status2 
        ,case when locate('租物',pa.name)>0 or locate('租物',cc.name)>0 or locate('芝麻',pa.name)>0 or locate('芝麻',cc.name)>0  then '芝麻租物' when locate('抖音',pa.name)>0 then '抖音渠道' when locate('搜索',cc.name)>0 then '搜索渠道' else '其他渠道' end as channel_type 
        ,tod.sku_attributes,tod.product_id 商品id
        ,cc.name as channel_name         -- 来源渠道
        ,cc.channel_type_id              -- 渠道id
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_name
        ,pa.type
        ,tos.sort, tos.money, tos.real_pay_money
        ,om.order_method, om.order_type, tmu.id_card_num, tmu.true_name, tolog.delivery_province_name,
        tolog.delivery_city_name, tolog.delivery_county_name, top.total_describes
        from  db_digua_business.t_order  om
        -- 渠道名称
        left join db_digua_business.t_channel cc on om.channel = cc.scene 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        -- 商品信息
        left join db_digua_business.t_order_details tod on om.id = tod.order_id
        -- 订单物流表
        left join db_digua_business.t_order_logistics tolog on tolog.order_id=om.id
        -- 分期表
        left join db_digua_business.t_order_stages tos on om.id = tos.order_id
        left join db_digua_business.t_member_user tmu on om.user_id = tmu.id
        -- 备注信息合并 
        left join ( SELECT  t.order_id,JSON_ARRAYAGG(t.describes) as total_describes from db_digua_business.t_order_personnel t   GROUP BY 1 ) top 
        on om.id = top.order_id 
        where om.user_mobile is not null 
        -- and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        -- ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        and om.status not in (13)
        and om.create_time BETWEEN (CURDATE() - INTERVAL 1 DAY) + INTERVAL 18 HOUR 
                    AND CURDATE() + INTERVAL 18 HOUR 
        '''
        df_order = self.clean.query(sql1)
        return df_order

    def clean_data(self, df_order):
        df = df_order.copy()
        df["下单日期"] = df["create_time"]#.dt.date
        # df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'],
                                       x['order_type']), axis=1)
        # 删除重复单号
        df.drop_duplicates(subset=["order_id"], inplace=True)
        # 删除身份证空值行
        df.dropna(subset=["id_card_num"], axis=0, inplace=True)
        # df.drop(df[df['activity_name'] == "1000单秘密计划"].index, inplace=True)
        # df.drop(df[df['activity_name'] == "1000单秘密计划-无优惠"].index, inplace=True)
        # df.drop(df[df['activity_name'] == "1000单曙光计划"].index, inplace=True)
        # df.drop(df[df['activity_name'] == "线下门店3个月试行"].index, inplace=True)
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)
        return df

    def douyin_data(self, df):
        # 获取抖音渠道的数据
        df_dy = df[df['归属渠道'] == '抖音渠道']

        def getvalue(s):
            color_list = json.loads(s)
            for j in range(0, len(color_list)):
                if color_list[j]["key"] == "租赁时长":
                    return color_list[j]["value"]

        df_dy.loc[:, "租赁时长"] = df_dy.apply(lambda x: getvalue(x["sku_attributes"]), axis=1)
        # 获取首期租金：sort=1时的money
        df_dy.loc[:, "首期租金"] = np.where(df_dy["sort"] == 1, df_dy["money"], 0)
        # 获取已付租金，real_pay_money的总和
        df_dy.loc[:, "已付租金"] = df_dy.groupby(["order_id"])["real_pay_money"].transform(sum)
        df_dy.loc[:, "订单状态"] = df_dy["status2"]
        df_dy.loc[:, "收货地址"] = df_dy.delivery_province_name + df_dy.delivery_city_name + df_dy.delivery_county_name

        dy_dy_res = df_dy[
            ['下单日期', 'order_number', '商品id', '租赁时长', '总租金', '首期租金', '已付租金', '订单状态',
             'true_name', '收货地址', 'total_describes']].rename(
            columns={'order_number': '订单编号', 'true_name': '收货人名称', 'total_describes': '备注'})
        return dy_dy_res

    def run(self):
        df_order = self.select_data()
        print('数据查询完毕！\n正在清理数据...')
        df = self.clean_data(df_order)
        print('数据清理完毕！\n正在计算抖音日订单数据...')
        dy_dy_res = self.douyin_data(df)
        return dy_dy_res

    def douyin_job_save(self, path):

        now = datetime.now().strftime('%Y%m%d%H%M')
        print(f'执行定时任务：现在是{now}...')
        print('正在查询数据...')
        df_dy_res = self.run()
        with pd.ExcelWriter(path + f'抖音每日订单数据统计_{now}.xlsx', engine='openpyxl') as writer:
            df_dy_res.to_excel(writer, sheet_name=now, index=False)

        # 显式删除不再需要的变量
        del df_dy_res
        # 调用垃圾回收
        gc.collect()
        print("回收内存执行完毕！\n")


if __name__ == '__main__':
    DY_O = Douyin_Order()
    hour = 18
    minute = 1
    path = r'\\digua\迪瓜租机\25.抖音数据\抖音订单每日统计需求/'


    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天18点01分开始执行，获取每天18点前的数据
    dy_job = scheduler.add_job(DY_O.douyin_job_save, 'cron', hour=hour, minute=minute, args=[path])
    print('定时任务创建完毕...\n正在执行定时任务douyin_job_save...')
    # DY_O.douyin_job_save(path)

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






