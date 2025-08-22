import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns
#sns.set(style="darkgrid")
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False


from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import time

import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
import datetime
from datetime import timedelta ,timezone , datetime
from apscheduler.schedulers.background import BackgroundScheduler
from Class_Model.All_Class import Data_Clean

class Rate:
    def __init__(self):
        self.clean = Data_Clean()

    def query(self, sql,
              host="rm-wz930e5269fur1ht1mo.mysql.rds.aliyuncs.com",
              user="wxz",
              password="5JRcY9SaiepVlIq7iuPo",
              database='',
              port=3306
              ):
        conn = pymysql.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            max_allowed_packet=1073741824,
            charset="utf8")
        try:
            df = pd.read_sql(sql, con=conn)
            conn.close()
        except:
            print('error')
            conn.close()
            raise
        return df

    def select_data(self):
        sql1 = '''    
        select o.order_number,od.product_id,od.product_name,o.merchant_id,o.merchant_name,o.`status`,o.create_time,op.pay_date,ol.go_express_date,o.alipay_order_id,cc.`name` as channel_name,pa.name as activity_name,o.order_method 
        ,tmer.shop_type
        from db_digua_business.t_order o 
        left join db_digua_business.t_order_pay op on op.order_id = o.id and op.pay_type = 'ZFBYSQ' and op.sync_mini_order = 'Y'
        left join db_digua_business.t_order_logistics ol on o.id = ol.order_id 
        left join db_digua_business.t_order_details od on od.order_id = o.id
        -- 渠道名称
        left join db_digua_business.t_channel cc on o.channel = cc.scene 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on o.activity_id = pa.id
        LEFT JOIN db_digua_business.t_merchant tmer on tmer.id = o.merchant_id
        where o.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -30 day ) 
        and op.`status` in (2,5)
        '''

        df = self.query(sql1)
        return df

    # 数据处理
    def clean_data(self, df):
        # 处理日期
        df["下单日期"] = pd.to_datetime(df["create_time"]).dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        df.loc[:, "归属渠道"] = df.apply(lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"]),axis=1)

        return df

    def xcx(self, df, date1, date2):
        df = df[(df['下单日期'] >= date1) & (df['下单日期'] <= date2)]
        df_all = df[['order_number', 'product_id', 'product_name', 'merchant_id','merchant_name', 'status', 'create_time', 'pay_date', 'go_express_date',
                    'alipay_order_id', 'channel_name','下单日期', '月份', '来源渠道', '归属渠道']]
        df_all['create_time'] = df_all['create_time'].dt.date
        df_all['pay_date'] = df_all['pay_date'].dt.date
        df_all['go_express_date'] = df_all['go_express_date'].dt.date
        df_all.rename(columns={'order_number': '订单编号', 'product_id': '商品ID', 'product_name': '商品名称','merchant_id': '店铺ID', 'merchant_name': '店铺名称',
                            'status': '订单状态', 'create_time': '订单生成时间', 'pay_date': '支付时间','go_express_date': '发货时间', 'alipay_order_id': '支付宝流水号',
                            'order_number': '订单编号'}, inplace=True)
        df_all['发货时效'] = (pd.to_datetime(df_all['发货时间']) - pd.to_datetime(df_all['支付时间'])).dt.days
        df_all = df_all[['订单编号', '商品ID', '商品名称', '店铺ID',
                        '店铺名称', '订单状态', '订单生成时间', '支付时间', '发货时间', '发货时效',
                        '支付宝流水号', 'channel_name',
                        '下单日期', '月份', '来源渠道', '归属渠道']]

        df_all_zh = df_all.groupby('订单生成时间').agg({'支付时间': 'count', '发货时间': 'count'})
        df_all_zh['发货率'] = df_all_zh['发货时间'] / df_all_zh['支付时间'] * 100
        df_all_zh['发货率'] = df_all_zh['发货率'].apply(lambda x: f'{x: .2f}%')
        df_all_zh.rename(columns={'支付时间': '进件量', '发货时间': '发货数'}, inplace=True)

        return df_all, df_all_zh

    def get_data(self, df, date1, date2):
        df_all, df_all_zh = self.xcx(df, date1, date2)
        # 排除商家
        df_2 = df[df['shop_type'] != 2]
        df_all_2, df_all_zh_2 = self.xcx(df_2, date1, date2)
        # 芝麻发货率
        df_zms = df[df['归属渠道'] == '芝麻租物']
        df_zm, df_zm_zh = self.xcx(df_zms, date1, date2)
        # 芝麻租物iPhone系列发货率
        df_zm_iPhone = df_zms[df_zms['product_name'].str.contains(r'iPhone15|iPhone 15|iPhone16|iPhone 16')]
        df_zm_iPhone, df_zm_iPhone_zh = self.xcx(df_zm_iPhone, date1, date2)

        return  df_all, df_all_zh, df_all_2, df_all_zh_2, df_zm, df_zm_zh, df_zm_iPhone, df_zm_iPhone_zh

    def run(self):
        date1 = '2024-10-01'
        date2 = datetime.now().strftime('%Y-%m-%d')
        print('正在查询数据...')
        df = self.select_data()
        print('数据查询完毕...\n开始数据处理...')
        df1 = self.clean_data(df)
        print('数据处理完毕...\n开始获取数据...')
        df_all, df_all_zh, df_all_2, df_all_zh_2, df_zm, df_zm_zh, df_zm_iPhone, df_zm_iPhone_zh = self.get_data(df1, date1, date2)
        print('数据获取完毕...')
        return df_all, df_all_zh, df_all_2, df_all_zh_2, df_zm, df_zm_zh, df_zm_iPhone, df_zm_iPhone_zh

    def my_job(self, hour, minute,path, Today):
        print(f'执行定时任务：现在是每日的{hour}点{minute}分...\n开始写入数据...')
        df_all, df_all_zh, df_all_2, df_all_zh_2, df_zm, df_zm_zh, df_zm_iPhone, df_zm_iPhone_zh = self.run()
        with pd.ExcelWriter(path + f'小程序发货率_{Today}.xlsx', engine='openpyxl') as writer:
            df_all_zh.to_excel(writer, sheet_name='小程序发货率_全域转化')
        with pd.ExcelWriter(path + f'小程序发货率_{Today}.xlsx', engine='openpyxl', mode='a') as writer:
            df_zm_zh.to_excel(writer, sheet_name='小程序发货率_芝麻转化')
            df_zm_iPhone_zh.to_excel(writer, sheet_name='小程序发货率_iPhone转化')
            df_all.to_excel(writer, sheet_name='小程序发货率_全域明细', index=False)
            df_all_2.to_excel(writer, sheet_name='小程序发货率_全域明细_自营', index=False)
            df_zm.to_excel(writer, sheet_name='小程序发货率_芝麻明细', index=False)
            df_zm_iPhone.to_excel(writer, sheet_name='小程序发货率_iPhone明细')
        print('数据写入完成...')


if __name__ == '__main__':
    hour = 18
    minute = 10
    path = r'\\digua\迪瓜租机\19.小程序发货率/'
    Today = str(datetime.now().strftime('%Y%m%d%H'))
    r = Rate()
    # r.my_job(hour, minute, path, Today)

    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(r.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path, Today])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    # 查看是否添加了任务
    print(scheduler.get_jobs())
    scheduler.start()
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