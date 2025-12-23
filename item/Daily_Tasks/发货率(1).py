import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Cython.Compiler.Errors import message

plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
import time

import warnings
warnings.filterwarnings("ignore")
import pymysql
from datetime import timedelta ,timezone , datetime
from apscheduler.schedulers.background import BackgroundScheduler
import os
import sys

# 获取脚本文件的绝对路径
script_path = os.path.abspath(__file__)

# 获取脚本文件所在的目录
script_directory = os.path.dirname(script_path)

print("脚本文件所在的目录是：", script_directory)
module_dir = os.path.join(script_directory, '../Class_Model')
print("module_dir:",module_dir)
sys.path.append(module_dir)


from All_Class import Data_Clean

class Rate:
    def __init__(self):
        self.clean = Data_Clean()
        # 替换为你的 Webhook 地址
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=d4072f19c1ebe08ea7a71a22df26337eb2fb51327c0ffeac14f8b53b4ed29c78"
        # 替换为你的密钥，如果没有设置则留空
        self.secret = "SEC953fc60a7f3cec15501e044bbe0f93d3bcbb5d68cb6628599f6a0eff94a2a6d4"

    def select_data(self):
        sql1 = '''    
        select o.order_number,od.product_id,od.product_name,o.merchant_id,o.merchant_name,o.`status`,o.create_time,op.pay_date,ol.go_express_date,
        o.alipay_order_id,cc.`name` as channel_name,pa.name as activity_name,o.order_method,cc.channel_type_id, o.order_type, o.day, o.has_order_check, pa.type,
        tpr.min_create_time, op.status as status_o
        from db_digua_business.t_order o 
        left join db_digua_business.t_order_pay op on op.order_id = o.id and op.pay_type = 'ZFBYSQ' and op.sync_mini_order = 'Y'
        left join db_digua_business.t_order_logistics ol on o.id = ol.order_id 
        left join db_digua_business.t_order_details od on od.order_id = o.id
        -- 渠道名称
        left join db_digua_business.t_channel cc on o.channel = cc.scene 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on o.activity_id = pa.id
        LEFT JOIN db_digua_business.t_merchant tmer on tmer.id = o.merchant_id
        left join (SELECT tpr.order_id, min(tpr.create_time) as min_create_time FROM db_digua_business.t_pay_record tpr GROUP BY tpr.order_id) tpr on tpr.order_id=o.id
        where o.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -30 day ) 
        and tmer.shop_type!=2
        '''

        df = self.clean.query(sql1)
        return df

    # 数据处理
    def clean_data(self, df):
        # 处理日期
        df["下单日期"] = pd.to_datetime(df["create_time"]).dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        df.loc[:, "归属渠道"] = df.apply(lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'], x['order_type']),axis=1)

        return df

    # 统计小程序发货率
    def xcx(self, df):
        # 筛选所需要的字段
        df = df[df.status_o.isin([2, 5])]
        df_all = df[['order_number', 'product_id', 'product_name', 'merchant_id','merchant_name', 'status', 'create_time', 'pay_date', 'go_express_date',
                    'alipay_order_id', 'channel_name','下单日期', '月份', '来源渠道', '归属渠道']]
        df_all = df_all.drop_duplicates(subset=['order_number'])
        # 取到日期并对字段进行重命名
        df_all['create_time'] = df_all['create_time'].dt.date
        df_all['pay_date'] = df_all['pay_date'].dt.date
        df_all['go_express_date'] = df_all['go_express_date'].dt.date
        df_all.rename(columns={'order_number': '订单编号', 'product_id': '商品ID', 'product_name': '商品名称','merchant_id': '店铺ID', 'merchant_name': '店铺名称',
                            'status': '订单状态', 'create_time': '订单生成时间', 'pay_date': '支付时间','go_express_date': '发货时间', 'alipay_order_id': '支付宝流水号'}, inplace=True)
        df_all['发货时效'] = (pd.to_datetime(df_all['发货时间']) - pd.to_datetime(df_all['支付时间'])).dt.days
        # 筛选7天发货时效并计算发货率l
        df_all.loc[:, '发货数'] = np.where(pd.to_datetime(df_all['发货时间'])-pd.to_datetime(df_all['支付时间'])<=pd.Timedelta(hours=168), 1, 0)
        df_all_zh = df_all.groupby('订单生成时间').agg({'支付时间': 'count', '发货数': 'sum'})
        df_all_zh.loc['汇总', :] = df_all_zh.sum(axis=0)
        df_all_zh['发货率'] = (df_all_zh['发货数'] / df_all_zh['支付时间'] * 100).apply(lambda x: f'{x: .2f}%')
        df_all_zh.rename(columns={'支付时间': '进件量'}, inplace=True)

        return df_all_zh

    # 获取48小时发货率
    def fh48(self, df):
        # 取到所需数据
        df = df[(df.day>=90)&(df.type==2)&(df.has_order_check=='Y')]
        # 去除不需要的数据
        df = df.drop_duplicates(subset=['order_number'])
        df2_zm = df[(~df.status.isin([13]))]
        # 获取48小时内发货数并计算发货率
        df2_zm.loc[:, '发货数'] = np.where((df2_zm.status.isin([3, 4])) & (df2_zm.go_express_date - df2_zm.pay_date <= pd.Timedelta(hours=48)), 1, 0)
        df2_zm.loc[:, '支付时间'] = df2_zm.pay_date.dt.date
        df2_zm_group = df2_zm.groupby('支付时间').agg({'order_number': 'count', '发货数': 'sum'}).rename(columns={'order_number': '支付成功数'})
        df2_zm_group.loc['汇总', :] = df2_zm_group.sum(axis=0)
        df2_zm_group['发货率'] = (df2_zm_group.发货数 / df2_zm_group.支付成功数).map(lambda x: format(x, '.2%'))
        return df2_zm_group

    def get_data(self, df):
        # 近30天发货率
        # 芝麻租物
        df_zm = df[df['归属渠道'] == '芝麻租物']
        df_zm_zh_30 = self.xcx(df_zm)
        # 搜索渠道
        df_ss = df[df['归属渠道'] == '搜索渠道']
        df_ss_zh_30 = self.xcx(df_ss)

        # 近7天发货率
        # 芝麻租物
        df_zm_7 = df_zm[df_zm.下单日期>=(datetime.now()-timedelta(days=7))]
        df_zm_zh_7 = self.xcx(df_zm_7)
        # 搜索渠道
        df_ss_7 = df_ss[df_ss.下单日期 >= (datetime.now() - timedelta(days=7))]
        df_ss_zh_7 = self.xcx(df_ss_7)

        # 48小时发货率 总体
        df_48h = self.fh48(df)
        # 芝麻租物
        df_zm_48h = self.fh48(df_zm)
        # 搜索渠道
        df_ss_48h = self.fh48(df_ss)
        return  df_zm_zh_30, df_ss_zh_30, df_zm_zh_7, df_ss_zh_7, df_48h, df_zm_48h, df_ss_48h

    def run(self):
        print('正在查询数据...')
        df = self.select_data()
        print('数据查询完毕...\n开始数据处理...')
        df1 = self.clean_data(df)
        print('数据处理完毕...\n开始获取数据...')
        df_zm_zh_30, df_ss_zh_30, df_zm_zh_7, df_ss_zh_7, df_48h, df_zm_48h, df_ss_48h = self.get_data(df1)
        print('数据获取完毕...')
        return df_zm_zh_30, df_ss_zh_30, df_zm_zh_7, df_ss_zh_7, df_48h, df_zm_48h, df_ss_48h

    # 数据异常提醒
    def data_tips(self, df_zm_zh_30, df_ss_zh_30):
        # 获取总发货率，修改为数值类型
        rate_zm = pd.to_numeric(df_zm_zh_30.loc['汇总', '发货率'].replace('%', ''))/100
        rate_ss = pd.to_numeric(df_ss_zh_30.loc['汇总', '发货率'].replace('%', ''))/100
        num = 0.16
        message = ''
        # 判断是否跌出预设值，如跌出预设值则需要进行提醒
        if rate_zm<num or rate_ss<num:
            message += f'''近30天发货率，租物{df_zm_zh_30.loc['汇总', '发货率']}，搜索{df_ss_zh_30.loc['汇总', '发货率']}，低于{str(num*100)+'%'}  警报警报！！！'''
        self.clean.send_dingtalk_message(self.webhook, self.secret, message)

    def my_job(self, hour, minute,path):
        print(f'执行定时任务：现在是每日的{hour}点{minute}分...')
        Today = str(datetime.now().strftime('%Y%m%d'))
        df_zm_zh_30, df_ss_zh_30, df_zm_zh_7, df_ss_zh_7, df_48h, df_zm_48h, df_ss_48h = self.run()
        print('开始写入数据...')
        with pd.ExcelWriter(path + f'小程序发货率_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_zm_zh_30.to_excel(writer, sheet_name='芝麻租物近30天发货率')
            df_zm_zh_7.to_excel(writer, sheet_name='芝麻租物近7天发货率')
            df_ss_zh_30.to_excel(writer, sheet_name='搜索渠道近30天发货率')
            df_ss_zh_7.to_excel(writer, sheet_name='搜索渠道近7天发货率')
        with pd.ExcelWriter(path + f'48小时发货率_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_48h.to_excel(writer, sheet_name='总体48小时发货率')
            df_zm_48h.to_excel(writer, sheet_name='芝麻租物48小时发货率')
            df_ss_48h.to_excel(writer, sheet_name='搜索渠道48小时发货率')
        self.data_tips(df_zm_zh_30, df_ss_zh_30)
        print('数据写入完成...')


if __name__ == '__main__':
    hour = 10
    minute = 30
    path = r'\\digua\迪瓜租机\002数据监测\2.发货率/'
    r = Rate()
    r.my_job(hour, minute, path)

    # print('正在创建定时任务...')
    # scheduler = BackgroundScheduler()
    # job = scheduler.add_job(r.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path])
    # print('定时任务创建完毕...\n正在执行定时任务my_job...')
    # # 查看是否添加了任务
    # print(scheduler.get_jobs())
    # scheduler.start()
    # # 模拟主程序
    # try:
    #     while True:
    #         next_run_time = job.next_run_time
    #         if next_run_time:
    #             now = datetime.now(timezone.utc)
    #             sleep_duration = (next_run_time - now).total_seconds()
    #             if sleep_duration > 0:
    #                 time.sleep(sleep_duration)
    #         else:
    #             time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    # except (KeyboardInterrupt, SystemExit):
    #     # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
    #     scheduler.shutdown()
